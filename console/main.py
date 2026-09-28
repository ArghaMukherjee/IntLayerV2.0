"""Operations console backend for the Integration Layer (V2).

Reads the integration, dwh and console schemas with the console_user login (read-only
except for the console schema) and the BigQuery data plane. Actions that change requests
or workflows go through the Integration Layer API server-side, so API keys never reach
the browser.
"""
import asyncio
import logging
import time
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import httpx
import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from jsonschema import Draft202012Validator, FormatChecker
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from ilcommon.bigquery_store import BigQuerySettings, BigQueryStore
from ilcommon.templating import has_placeholders, render

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
STATIC = Path(__file__).parent / "static"

RANGES = {"1h": ("1 hour", "5 minutes"), "24h": ("24 hours", "1 hour"),
          "7d": ("7 days", "6 hours"), "30d": ("30 days", "1 day")}
# Legacy IANA names some browsers still report; the postgres image ships only canonical names
TZ_ALIASES = {"Asia/Calcutta": "Asia/Kolkata", "Asia/Saigon": "Asia/Ho_Chi_Minh", "Asia/Katmandu": "Asia/Kathmandu",
              "Asia/Rangoon": "Asia/Yangon", "Asia/Dacca": "Asia/Dhaka", "Europe/Kiev": "Europe/Kyiv",
              "America/Buenos_Aires": "America/Argentina/Buenos_Aires", "Pacific/Truk": "Pacific/Chuuk",
              "Atlantic/Faeroe": "Atlantic/Faroe", "Asia/Ulan_Bator": "Asia/Ulaanbaatar"}
LATENCY_EDGES = [10, 50, 100, 500, 1000, 5000, 30000, 300000]
LATENCY_LABELS = ["< 10 ms", "10-50 ms", "50-100 ms", "100-500 ms", "0.5-1 s", "1-5 s", "5-30 s", "30 s-5 min", "> 5 min"]


SOURCE_APPS = ("WWMG_Source", "MyCareHub")


class Settings(BaseSettings):
    """CONSOLE_* environment variables (BigQuery: BQ_*)."""

    model_config = SettingsConfigDict(env_prefix="CONSOLE_", extra="ignore")

    database_url: str = "postgresql://console_user:console@localhost:5432/integration"
    il_base_url: str = "http://localhost:8000"
    source_keys: str = ""      # "WWMG_Source:<key>,MyCareHub:<key>" (the console sends as either app)
    callback_urls: str = ""    # "WWMG_Source:http://wwmg-source:8002/hooks/il,MyCareHub:http://mycarehub:8003/hooks/il"
    admin_api_key: str = ""
    consumer_url: str = "http://localhost:8001"
    wwmg_url: str = "http://localhost:8002"
    mycarehub_url: str = "http://localhost:8003"

    @staticmethod
    def _pairs(text: str) -> dict[str, str]:
        return {k.strip(): v.strip() for k, v in (p.split(":", 1) for p in text.split(",") if ":" in p)}

    def key_for(self, app: str) -> str:
        return self._pairs(self.source_keys).get(app, "")

    def callback_for(self, app: str) -> str | None:
        return self._pairs(self.callback_urls).get(app)


# ----------------------------------------------------------------------------- request models
class SendRequest(BaseModel):
    workflow_name: str
    source_app: Literal["WWMG_Source", "MyCareHub"] = "WWMG_Source"
    payload: Any
    idempotency_key: str | None = Field(default=None, max_length=200)
    use_callback: bool = True
    priority: int = Field(default=0, ge=0, le=9)
    render_placeholders: bool = True


class SampleIn(BaseModel):
    workflow_name: str
    name: str = Field(min_length=1, max_length=100)
    description: str | None = None
    payload: Any


class ValidateIn(BaseModel):
    workflow_name: str
    payload: Any


class TemplateIn(BaseModel):
    template: Any


class WorkflowIn(BaseModel):
    target_app: str = Field(min_length=1, max_length=64)
    description: str | None = None
    input_schema: dict
    output_schema: dict | None = None
    max_retries: int = Field(default=3, ge=0, le=20)
    storage_backend: Literal["postgres", "bigquery"] = "postgres"
    is_active: bool = True


class JobIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    description: str | None = None
    workflow_name: str
    source_app: Literal["WWMG_Source", "MyCareHub"] = "WWMG_Source"
    sample_id: int | None = None
    payload_template: Any = None
    interval_seconds: int | None = Field(default=None, ge=5, le=86400)
    requests_per_run: int = Field(default=1, ge=1, le=100)
    max_runs: int | None = Field(default=None, ge=1)
    use_callback: bool = True
    status: Literal["ACTIVE", "PAUSED"] = "PAUSED"

    @model_validator(mode="after")
    def payload_source(self):
        if self.sample_id is None and self.payload_template is None:
            raise ValueError("Choose a payload sample or provide a payload template")
        if self.status == "ACTIVE" and self.interval_seconds is None:
            raise ValueError("A job without an interval runs manually only; create it as PAUSED")
        return self


class IntervalIn(BaseModel):
    interval_seconds: int = Field(ge=10, le=604800)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    timezones: dict[str, str] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.pool = AsyncConnectionPool(settings.database_url, min_size=1, max_size=8, open=False,
                                             kwargs={"autocommit": True, "row_factory": dict_row})
        await app.state.pool.open(wait=False)
        app.state.http = httpx.AsyncClient(timeout=10)
        bq_settings = BigQuerySettings()
        app.state.bq = BigQueryStore(bq_settings) if bq_settings.enabled else None
        app.state.bq_health = {"ok": False, "detail": {"error": "not checked yet"}}

        async def watch_bigquery():   # slow BigQuery never blocks the dashboards
            while True:
                try:
                    ping = await asyncio.to_thread(app.state.bq.ping)
                    app.state.bq_health = {"ok": ping["ok"], "latency_ms": ping["latency_ms"],
                                           "detail": {"target": ping["target"]}, "checked_at": datetime.now(timezone.utc)}
                except Exception as exc:
                    app.state.bq_health = {"ok": False, "detail": {"error": f"{type(exc).__name__}: {exc}"[:200]},
                                           "checked_at": datetime.now(timezone.utc)}
                await asyncio.sleep(15)

        watcher = asyncio.create_task(watch_bigquery()) if app.state.bq is not None else None
        yield
        if watcher:
            watcher.cancel()
        await app.state.http.aclose()
        await app.state.pool.close()

    app = FastAPI(title="Integration Layer Console", version="2.0.0", lifespan=lifespan)

    # ------------------------------------------------------------------ db / http helpers
    async def fetch_all(sql: str, params: Any = None) -> list[dict]:
        async with app.state.pool.connection(timeout=5) as conn:
            return await (await conn.execute(sql, params)).fetchall()

    async def fetch_one(sql: str, params: Any = None) -> dict | None:
        async with app.state.pool.connection(timeout=5) as conn:
            return await (await conn.execute(sql, params)).fetchone()

    async def write(sql: str, params: Any = None) -> dict | None:
        try:
            async with app.state.pool.connection(timeout=5) as conn:
                cur = await conn.execute(sql, params)
                return await cur.fetchone() if cur.description else None
        except psycopg.errors.UniqueViolation:
            raise HTTPException(409, detail="The name is already in use")
        except psycopg.errors.ForeignKeyViolation:
            raise HTTPException(422, detail="Unknown workflow or sample")
        except psycopg.errors.CheckViolation as exc:
            raise HTTPException(422, detail=f"Invalid value: {exc.diag.message_primary}")

    async def probe(name: str, url: str, extra: str | None = None) -> dict:
        result = {"name": name, "url": url, "ok": False}
        t0 = time.perf_counter()
        try:
            response = await app.state.http.get(url, timeout=5)
            result["ok"] = response.is_success
            result["latency_ms"] = int((time.perf_counter() - t0) * 1000)
            result["detail"] = response.json()
            if extra:
                result["extra"] = (await app.state.http.get(extra, timeout=5)).json()
        except Exception as exc:
            result["detail"] = {"error": f"{type(exc).__name__}: {exc}"}
        return result

    async def call(method: str, url: str, **kwargs) -> dict:
        """Calls another service and returns status code, body and latency (for display)."""
        t0 = time.perf_counter()
        try:
            response = await app.state.http.request(method, url, **kwargs)
        except httpx.HTTPError as exc:
            raise HTTPException(502, detail=f"{url} unreachable: {exc}")
        try:
            body = response.json()
        except ValueError:
            body = {"text": response.text[:2000]}
        return {"http_status": response.status_code, "response": body,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "request": {"method": method, "url": url.replace(settings.il_base_url, "{IL}")}}

    def il(path: str) -> str:
        return settings.il_base_url + path

    async def input_validator(workflow_name: str) -> Draft202012Validator:
        row = await fetch_one("SELECT input_schema FROM integration.workflow_registry WHERE workflow_name = %s",
                              (workflow_name,))
        if row is None:
            raise HTTPException(404, detail=f"Workflow '{workflow_name}' not found")
        return Draft202012Validator(row["input_schema"], format_checker=FormatChecker())

    async def valid_tz(tz: str, offset_minutes: int | None = None) -> str:
        """A zone PostgreSQL accepts: the name, its canonical alias, else the browser's fixed UTC offset, else UTC."""
        key = f"{tz}|{offset_minutes}"
        if key not in timezones:
            candidates = [tz, TZ_ALIASES.get(tz)]
            if offset_minutes is not None:
                sign = "-" if offset_minutes >= 0 else "+"   # POSIX: east of UTC is negative
                candidates.append(f"UTC{sign}{abs(offset_minutes) // 60:02d}:{abs(offset_minutes) % 60:02d}")
            timezones[key] = "UTC"
            for name in filter(None, candidates):
                try:
                    await fetch_one("SELECT now() AT TIME ZONE %s", (name,))
                    timezones[key] = name
                    break
                except psycopg.Error:
                    continue
        return timezones[key]

    # ------------------------------------------------------------------ both data planes
    async def bq(method: str, *args, **kwargs):
        """Calls the BigQuery store in a thread; None when BigQuery is disabled."""
        if app.state.bq is None:
            return None
        return await asyncio.to_thread(getattr(app.state.bq, method), *args, **kwargs)

    bq_cache: dict[tuple, tuple[float, Any]] = {}

    async def bq_safe(method: str, *args, default=None, ttl: float = 0, **kwargs):
        """BigQuery call that never raises; results can be reused for `ttl` seconds (dashboards poll often)."""
        key = (method, args, tuple(sorted(kwargs.items())))
        if ttl and key in bq_cache and time.monotonic() - bq_cache[key][0] < ttl:
            return bq_cache[key][1]
        try:
            result = await bq(method, *args, **kwargs)
            result = default if result is None else result
            if ttl:
                bq_cache[key] = (time.monotonic(), result)
            return result
        except Exception as exc:
            logging.getLogger("console").warning("BigQuery %s failed: %s", method, exc)
            return default

    def percentile(values: list[float], q: float) -> float | None:
        values = sorted(v for v in values if v is not None)
        if not values:
            return None
        k = (len(values) - 1) * q
        lo, hi = int(k), min(int(k) + 1, len(values) - 1)
        return values[lo] + (values[hi] - values[lo]) * (k - lo)

    def ms(a, b) -> float | None:
        return (b - a).total_seconds() * 1000 if a and b else None

    async def recent_rows(hours: int = 24) -> list[dict]:
        """Light rows from both backends received in the last `hours` (small at this volume)."""
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        pg = await fetch_all(
            """SELECT request_id, workflow_name, source_app, status::text AS status, retry_count, received_at, picked_at,
                      workflow_completed_at, processing_duration_ms, callback_status, callback_delivered_at,
                      'postgres' AS storage_backend
               FROM integration.request_metadata WHERE received_at >= %s""", (since,))
        since_bucket = since.replace(second=0, microsecond=0)   # stable cache key within a minute
        return pg + await bq_safe("recent", since_bucket, default=[], ttl=4)

    async def services() -> list[dict]:
        probes = await asyncio.gather(
            probe("Integration Layer", f"{settings.il_base_url}/health/ready"),
            probe("ConsumerApplication", f"{settings.consumer_url}/health/ready", f"{settings.consumer_url}/stats"),
            probe("WWMG_Source", f"{settings.wwmg_url}/health"),
            probe("MyCareHub", f"{settings.mycarehub_url}/health"))
        if app.state.bq is None:
            probes.append({"name": "BigQuery", "ok": False, "detail": {"error": "disabled"}})
        else:
            probes.append({"name": "BigQuery", **app.state.bq_health})
        return probes

    # ------------------------------------------------------------------ dashboard
    @app.get("/api/overview")
    async def overview():
        rows, pg_status, pg_queue, bq_summary, svc = await asyncio.gather(
            recent_rows(24),
            fetch_all("SELECT status::text AS status, count(*) AS n FROM integration.request_metadata GROUP BY 1"),
            fetch_one("""SELECT min(received_at) FILTER (WHERE status = 'PENDING') AS oldest_pending,
                                count(*) FILTER (WHERE callback_status = 'PENDING') AS cb_pending,
                                count(*) FILTER (WHERE callback_status = 'DELIVERED') AS cb_delivered,
                                count(*) FILTER (WHERE callback_status = 'FAILED') AS cb_failed
                         FROM integration.request_metadata"""),
            bq_safe("summary", default={"counts": {}, "callbacks": {}, "oldest_pending": None}, ttl=4),
            services())
        by = Counter(r["status"] for r in rows)
        e2e = [ms(r["received_at"], r["workflow_completed_at"]) for r in rows if r["status"] == "COMPLETED"]
        kpis = {"total": len(rows), "completed": by["COMPLETED"], "failed": by["FAILED"], "cancelled": by["CANCELLED"],
                "in_flight": by["PENDING"] + by["IN_PROGRESS"], "retried": sum(1 for r in rows if r["retry_count"]),
                "p50_ms": percentile(e2e, 0.5), "p95_ms": percentile(e2e, 0.95)}
        counts = {s: 0 for s in ("PENDING", "IN_PROGRESS", "COMPLETED", "FAILED", "CANCELLED")}
        per_backend = {"postgres": dict(counts), "bigquery": dict(counts)}
        for r in pg_status:
            counts[r["status"]] += r["n"]; per_backend["postgres"][r["status"]] += r["n"]
        for st, n in bq_summary["counts"].items():
            counts[st] = counts.get(st, 0) + n; per_backend["bigquery"][st] = n
        oldest = [t for t in (pg_queue["oldest_pending"], bq_summary["oldest_pending"]) if t]
        cb = bq_summary["callbacks"]
        queue = {"oldest_pending_s": (datetime.now(timezone.utc) - min(oldest)).total_seconds() if oldest else None,
                 "callbacks_pending": pg_queue["cb_pending"] + cb.get("PENDING", 0),
                 "callbacks_delivered": pg_queue["cb_delivered"] + cb.get("DELIVERED", 0),
                 "callbacks_failed": pg_queue["cb_failed"] + cb.get("FAILED", 0)}
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        hours = [now - timedelta(hours=23 - i) for i in range(24)]
        hourly = []
        for h in hours:
            bucket = [r for r in rows if h <= r["received_at"] < h + timedelta(hours=1)]
            hourly.append({"hour": h, "received": len(bucket),
                           "postgres": sum(1 for r in bucket if r["storage_backend"] == "postgres"),
                           "bigquery": sum(1 for r in bucket if r["storage_backend"] == "bigquery"),
                           "completed": sum(1 for r in bucket if r["status"] == "COMPLETED"),
                           "failed": sum(1 for r in bucket if r["status"] == "FAILED")})
        return {"kpis": kpis, "statuses": [{"status": k, "count": v} for k, v in counts.items()],
                "per_backend": per_backend, "queue": queue, "hourly": hourly, "services": svc,
                "backend_24h": dict(Counter(r["storage_backend"] for r in rows))}

    # ------------------------------------------------------------------ requests
    @app.get("/api/requests")
    async def list_requests(status: str | None = None, workflow: str | None = None, backend: str | None = None,
                            q: str | None = None, limit: int = Query(100, ge=1, le=500)):
        rows: list[dict] = []
        if backend in (None, "", "postgres"):
            sql = ["""SELECT request_id, correlation_id, idempotency_key, source_app, target_app, workflow_name,
                             status::text AS status, success_flag, workflow_status, error_code, retry_count, received_at,
                             workflow_completed_at, processing_duration_ms, callback_status, 'postgres' AS storage_backend
                      FROM integration.request_metadata WHERE true"""]
            params: dict = {"limit": limit}
            if status:
                sql.append("AND status::text = %(status)s"); params["status"] = status
            if workflow:
                sql.append("AND workflow_name = %(workflow)s"); params["workflow"] = workflow
            if q:
                sql.append("AND (request_id::text ILIKE %(q)s OR correlation_id::text ILIKE %(q)s"
                           " OR idempotency_key ILIKE %(q)s OR input_json::text ILIKE %(q)s)")
                params["q"] = f"%{q}%"
            sql.append("ORDER BY received_at DESC LIMIT %(limit)s")
            rows += await fetch_all(" ".join(sql), params)
        if backend in (None, "", "bigquery"):
            rows += await bq_safe("list", workflow=workflow, status=status, q=q, limit=limit, default=[])
        rows.sort(key=lambda r: r["received_at"], reverse=True)
        now = datetime.now(timezone.utc)
        return [{k: r.get(k) for k in ("request_id", "correlation_id", "idempotency_key", "source_app", "target_app",
                                        "workflow_name", "status", "success_flag", "workflow_status", "error_code",
                                        "retry_count", "received_at", "workflow_completed_at", "processing_duration_ms",
                                        "callback_status", "storage_backend")}
                | {"end_to_end_ms": ms(r["received_at"], r.get("workflow_completed_at") or now)} for r in rows[:limit]]

    async def find_request(request_id: UUID) -> dict | None:
        row = await fetch_one("SELECT *, status::text AS status FROM integration.request_metadata WHERE request_id = %s",
                              (request_id,))
        if row:
            for key in ("client_ip", "il_server_ip", "db_server_addr"):
                row[key] = str(row[key]) if row[key] is not None else None
            return row | {"storage_backend": "postgres"}
        route = await fetch_one("SELECT storage_backend FROM integration.request_routes WHERE request_id = %s", (request_id,))
        if route and route["storage_backend"] == "bigquery":
            return await bq("get", str(request_id))
        return None

    @app.get("/api/requests/{request_id}")
    async def request_detail(request_id: UUID):
        row = await find_request(request_id)
        if row is None:
            raise HTTPException(404, detail="Request not found")
        if row["storage_backend"] == "bigquery":
            events_q = bq_safe("events", str(request_id), default=[])
        else:
            events_q = fetch_all("SELECT *, new_status::text AS new_status FROM integration.request_event_log"
                                 " WHERE request_id = %s ORDER BY event_id", (request_id,))
        events, calls, submission = await asyncio.gather(events_q,
            fetch_all("""SELECT call_id, direction, http_method, api_url, endpoint, response_status_code,
                                latency_ms, client_ip::text, server_hostname, request_headers, response_body, called_at
                         FROM integration.api_call_log WHERE request_id = %s ORDER BY call_id""", (request_id,)),
            fetch_one("""SELECT s.job_id, j.name AS job_name, s.run_id, s.http_status, s.latency_ms
                         FROM console.job_submissions s JOIN console.jobs j ON j.job_id = s.job_id
                         WHERE s.request_id = %s""", (request_id,)))
        return {"request": row, "events": events, "api_calls": calls, "job": submission}

    @app.post("/api/requests")
    async def send_request(body: SendRequest):
        payload = render(body.payload) if body.render_placeholders else body.payload
        data = {"workflow_name": body.workflow_name, "payload": payload, "priority": body.priority}
        if body.idempotency_key:
            data["idempotency_key"] = body.idempotency_key
        if body.use_callback and settings.callback_for(body.source_app):
            data["callback_url"] = settings.callback_for(body.source_app)
        result = await call("POST", il("/v1/requests"), json=data, headers={"X-API-Key": settings.key_for(body.source_app)})
        result["request"]["body"] = data
        result["request"]["source_app"] = body.source_app
        return result

    @app.post("/api/requests/{request_id}/cancel")
    async def cancel(request_id: UUID):
        row = await find_request(request_id)
        if row is None:
            raise HTTPException(404, detail="Request not found")
        return await call("POST", il(f"/v1/requests/{request_id}/cancel"), params={"reason": "Cancelled from console"},
                          headers={"X-API-Key": settings.key_for(row["source_app"])})

    @app.post("/api/requests/{request_id}/replay")
    async def replay(request_id: UUID):
        return await call("POST", il(f"/v1/admin/requests/{request_id}/replay"),
                          headers={"X-API-Key": settings.admin_api_key})

    # ------------------------------------------------------------------ workflows
    @app.get("/api/workflows")
    async def workflows():
        rows = await fetch_all("""SELECT w.*, (SELECT count(*) FROM integration.request_metadata r
                                               WHERE r.workflow_name = w.workflow_name) AS postgres_requests,
                                             (SELECT count(*) FROM integration.request_routes r
                                               WHERE r.workflow_name = w.workflow_name) AS bigquery_requests
                                  FROM integration.workflow_registry w ORDER BY workflow_name""")
        for w in rows:
            w["request_count"] = w["postgres_requests"] + w["bigquery_requests"]
        return rows

    @app.put("/api/workflows/{name}")
    async def upsert_workflow(name: str, body: WorkflowIn):
        return await call("PUT", il(f"/v1/admin/workflows/{name}"), json=body.model_dump(),
                          headers={"X-API-Key": settings.admin_api_key})

    # ------------------------------------------------------------------ payload library
    @app.get("/api/samples")
    async def samples(workflow: str | None = None):
        return await fetch_all("""SELECT s.*, (SELECT count(*) FROM console.jobs j WHERE j.sample_id = s.sample_id)
                                             AS job_count
                                  FROM console.payload_samples s
                                  WHERE %(wf)s::text IS NULL OR s.workflow_name = %(wf)s
                                  ORDER BY s.workflow_name, s.name""", {"wf": workflow})

    @app.post("/api/samples", status_code=201)
    async def create_sample(body: SampleIn):
        return await write("""INSERT INTO console.payload_samples (workflow_name, name, description, payload)
                              VALUES (%s, %s, %s, %s) RETURNING *""",
                           (body.workflow_name, body.name, body.description, Jsonb(body.payload)))

    @app.put("/api/samples/{sample_id}")
    async def update_sample(sample_id: int, body: SampleIn):
        row = await write("""UPDATE console.payload_samples
                                SET workflow_name = %s, name = %s, description = %s, payload = %s
                              WHERE sample_id = %s RETURNING *""",
                          (body.workflow_name, body.name, body.description, Jsonb(body.payload), sample_id))
        if row is None:
            raise HTTPException(404, detail="Sample not found")
        return row

    @app.delete("/api/samples/{sample_id}")
    async def delete_sample(sample_id: int):
        in_use = await fetch_one("""SELECT count(*) AS n FROM console.jobs
                                    WHERE sample_id = %s AND payload_template IS NULL""", (sample_id,))
        if in_use["n"]:
            raise HTTPException(409, detail=f"Used by {in_use['n']} job(s) as their payload; change or delete them first")
        row = await write("DELETE FROM console.payload_samples WHERE sample_id = %s RETURNING sample_id", (sample_id,))
        if row is None:
            raise HTTPException(404, detail="Sample not found")
        return {"deleted": sample_id}

    @app.post("/api/samples/validate")
    async def validate_sample(body: ValidateIn):
        validator = await input_validator(body.workflow_name)
        rendered = render(body.payload)
        errors = [{"path": "$" + "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in e.absolute_path),
                   "message": e.message, "rule": e.validator}
                  for e in sorted(validator.iter_errors(rendered), key=lambda e: list(map(str, e.absolute_path)))]
        return {"valid": not errors, "errors": errors, "rendered": rendered,
                "has_placeholders": has_placeholders(body.payload)}

    @app.post("/api/templates/preview")
    async def preview(body: TemplateIn):
        return {"rendered": [render(body.template, {"seq": i + 1, "job": "preview"}) for i in range(3)]}

    # ------------------------------------------------------------------ traffic jobs
    JOB_SELECT = """
        SELECT j.*, s.name AS sample_name, s.payload AS sample_payload,
               r.run_id AS last_run_id, r.trigger AS last_trigger, r.accepted AS last_accepted,
               r.rejected AS last_rejected, r.errors AS last_errors, r.error AS last_error,
               (SELECT count(*) FROM console.job_submissions x WHERE x.job_id = j.job_id) AS submissions,
               (SELECT count(*) FROM console.job_submissions x WHERE x.job_id = j.job_id AND x.http_status < 300)
                   AS accepted_total
        FROM console.jobs j
        LEFT JOIN console.payload_samples s ON s.sample_id = j.sample_id
        LEFT JOIN LATERAL (SELECT * FROM console.job_runs r WHERE r.job_id = j.job_id
                           ORDER BY run_id DESC LIMIT 1) r ON true"""

    @app.get("/api/jobs")
    async def jobs():
        return await fetch_all(JOB_SELECT + " ORDER BY j.job_id")

    @app.get("/api/jobs/{job_id}")
    async def job_detail(job_id: int):
        job = await fetch_one(JOB_SELECT + " WHERE j.job_id = %s", (job_id,))
        if job is None:
            raise HTTPException(404, detail="Job not found")
        runs = await fetch_all("SELECT * FROM console.job_runs WHERE job_id = %s ORDER BY run_id DESC LIMIT 25",
                               (job_id,))
        return {"job": job, "runs": runs}

    @app.get("/api/job-runs/{run_id}")
    async def job_run(run_id: int):
        subs = await fetch_all("""SELECT s.*, r.status AS request_status, r.workflow_status
                                  FROM console.job_submissions s
                                  LEFT JOIN integration.request_metadata r ON r.request_id = s.request_id
                                  WHERE s.run_id = %s ORDER BY s.seq""", (run_id,))
        return subs

    def job_params(body: JobIn) -> dict:
        return body.model_dump() | {"payload_template": Jsonb(body.payload_template)
                                    if body.payload_template is not None else None}

    @app.post("/api/jobs", status_code=201)
    async def create_job(body: JobIn):
        return await write(
            """INSERT INTO console.jobs (name, description, workflow_name, source_app, sample_id, payload_template,
                   interval_seconds, requests_per_run, max_runs, use_callback, status, next_run_at)
               VALUES (%(name)s, %(description)s, %(workflow_name)s, %(source_app)s, %(sample_id)s, %(payload_template)s,
                   %(interval_seconds)s, %(requests_per_run)s, %(max_runs)s, %(use_callback)s, %(status)s,
                   CASE WHEN %(status)s = 'ACTIVE' THEN now() END)
               RETURNING *""", job_params(body))

    @app.put("/api/jobs/{job_id}")
    async def update_job(job_id: int, body: JobIn):
        row = await write(
            """UPDATE console.jobs SET name = %(name)s, description = %(description)s,
                   workflow_name = %(workflow_name)s, source_app = %(source_app)s, sample_id = %(sample_id)s,
                   payload_template = %(payload_template)s, interval_seconds = %(interval_seconds)s,
                   requests_per_run = %(requests_per_run)s, max_runs = %(max_runs)s,
                   use_callback = %(use_callback)s, status = %(status)s,
                   next_run_at = CASE WHEN %(status)s = 'ACTIVE' THEN coalesce(next_run_at, now()) END
               WHERE job_id = %(job_id)s RETURNING *""", job_params(body) | {"job_id": job_id})
        if row is None:
            raise HTTPException(404, detail="Job not found")
        return row

    @app.delete("/api/jobs/{job_id}")
    async def delete_job(job_id: int):
        row = await write("DELETE FROM console.jobs WHERE job_id = %s RETURNING job_id", (job_id,))
        if row is None:
            raise HTTPException(404, detail="Job not found")
        return {"deleted": job_id}

    @app.post("/api/jobs/{job_id}/run")
    async def run_job(job_id: int):
        row = await write("UPDATE console.jobs SET run_requested = true WHERE job_id = %s RETURNING job_id", (job_id,))
        if row is None:
            raise HTTPException(404, detail="Job not found")
        return {"queued": True, "message": "The scheduler will run it within a few seconds"}

    @app.post("/api/jobs/{job_id}/pause")
    async def pause_job(job_id: int):
        row = await write("UPDATE console.jobs SET status = 'PAUSED' WHERE job_id = %s RETURNING status", (job_id,))
        if row is None:
            raise HTTPException(404, detail="Job not found")
        return row

    @app.post("/api/jobs/{job_id}/resume")
    async def resume_job(job_id: int):
        job = await fetch_one("SELECT interval_seconds, status FROM console.jobs WHERE job_id = %s", (job_id,))
        if job is None:
            raise HTTPException(404, detail="Job not found")
        if job["interval_seconds"] is None:
            raise HTTPException(409, detail="This job has no schedule; use Run now, or edit it to add an interval")
        return await write(
            """UPDATE console.jobs SET status = 'ACTIVE', next_run_at = now(),
                      run_count = CASE WHEN status = 'COMPLETED' THEN 0 ELSE run_count END
               WHERE job_id = %s RETURNING status""", (job_id,))

    # ------------------------------------------------------------------ system jobs
    @app.get("/api/system-jobs")
    async def system_jobs():
        jobs_, runs = await asyncio.gather(
            fetch_all("SELECT * FROM console.system_jobs ORDER BY job_name"),
            fetch_all("SELECT * FROM console.system_job_runs ORDER BY run_id DESC LIMIT 30"))
        return {"jobs": jobs_, "runs": runs}

    async def system_job_update(name: str, sql: str, params: tuple = ()) -> dict:
        row = await write(sql + " WHERE job_name = %s RETURNING *", params + (name,))
        if row is None:
            raise HTTPException(404, detail="System job not found")
        return row

    @app.post("/api/system-jobs/{name}/run")
    async def run_system_job(name: str):
        return await system_job_update(name, "UPDATE console.system_jobs SET run_requested = true")

    @app.post("/api/system-jobs/{name}/pause")
    async def pause_system_job(name: str):
        return await system_job_update(name, "UPDATE console.system_jobs SET is_paused = true")

    @app.post("/api/system-jobs/{name}/resume")
    async def resume_system_job(name: str):
        return await system_job_update(name, "UPDATE console.system_jobs SET is_paused = false, next_run_at = now()")

    @app.put("/api/system-jobs/{name}")
    async def reschedule_system_job(name: str, body: IntervalIn):
        return await system_job_update(
            name, """UPDATE console.system_jobs SET interval_seconds = %s,
                            next_run_at = least(next_run_at, now() + make_interval(secs => %s))""",
            (body.interval_seconds, body.interval_seconds))

    # ------------------------------------------------------------------ ConsumerApplication control
    @app.post("/api/consumer/{action}")
    async def consumer_control(action: Literal["pause", "resume"], backend: Literal["postgres", "bigquery"] | None = None):
        """Pause/resume ConsumerApplication's workers (all, or one backend's queue)."""
        return await call("POST", f"{settings.consumer_url}/worker/{action}", params={"backend": backend} if backend else None)

    # ------------------------------------------------------------------ analytics (data warehouse)
    @app.get("/api/analytics")
    async def analytics(range: Literal["1h", "24h", "7d", "30d"] = "24h", workflow: str | None = None,
                        backend: Literal["postgres", "bigquery", ""] | None = None,
                        tz: str = "UTC", offset: int | None = Query(None, ge=-840, le=840)):
        span, bucket = RANGES[range]
        p = {"range": span, "bucket": bucket, "workflow": workflow or None, "backend": backend or None,
             "tz": await valid_tz(tz, offset), "edges": LATENCY_EDGES}
        x = """WITH x AS (
                 SELECT f.*, s.status_code, s.is_terminal, w.workflow_name
                 FROM dwh.fact_request f
                 JOIN dwh.dim_status s ON s.status_key = f.status_key
                 JOIN dwh.dim_workflow w ON w.workflow_key = f.workflow_key
                 WHERE f.received_at >= now() - %(range)s::interval
                   AND (%(workflow)s::text IS NULL OR w.workflow_name = %(workflow)s)
                   AND (%(backend)s::text IS NULL OR f.storage_backend = %(backend)s))"""
        pct = "percentile_cont({q}) WITHIN GROUP (ORDER BY {col}) FILTER (WHERE status_code = 'COMPLETED')"
        kpi_sql = x + f"""
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE status_code = 'COMPLETED') AS completed,
                   count(*) FILTER (WHERE status_code = 'FAILED') AS failed,
                   count(*) FILTER (WHERE status_code = 'CANCELLED') AS cancelled,
                   count(*) FILTER (WHERE NOT is_terminal) AS in_flight,
                   count(*) FILTER (WHERE retry_count > 0) AS retried,
                   count(*) FILTER (WHERE workflow_status = 'REJECTED') AS rejected,
                   {pct.format(q=0.5, col='end_to_end_ms')} AS p50_e2e,
                   {pct.format(q=0.95, col='end_to_end_ms')} AS p95_e2e,
                   {pct.format(q=0.95, col='queue_wait_ms')} AS p95_queue,
                   {pct.format(q=0.95, col='processing_ms')} AS p95_processing,
                   sum(input_bytes) AS input_bytes, sum(output_bytes) AS output_bytes
            FROM x"""
        series_sql = x + """, b AS (
                 SELECT generate_series(date_bin(%(bucket)s::interval, now() - %(range)s::interval, TIMESTAMPTZ '2000-01-01'),
                                        date_bin(%(bucket)s::interval, now(), TIMESTAMPTZ '2000-01-01'),
                                        %(bucket)s::interval) AS t)
            SELECT b.t AS bucket, count(x.request_id) AS received,
                   count(x.request_id) FILTER (WHERE x.storage_backend = 'postgres') AS postgres,
                   count(x.request_id) FILTER (WHERE x.storage_backend = 'bigquery') AS bigquery,
                   count(x.request_id) FILTER (WHERE x.status_code = 'COMPLETED') AS completed,
                   count(x.request_id) FILTER (WHERE x.status_code = 'FAILED') AS failed,
                   count(x.request_id) FILTER (WHERE x.is_terminal) AS finished,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY x.end_to_end_ms)
                       FILTER (WHERE x.status_code = 'COMPLETED') AS p95_e2e
            FROM b LEFT JOIN x ON x.received_at >= b.t AND x.received_at < b.t + %(bucket)s::interval
            GROUP BY b.t ORDER BY b.t"""
        outcome_sql = x + """
            SELECT coalesce(workflow_status, status_code) AS outcome, status_code, count(*) AS count
            FROM x GROUP BY 1, 2 ORDER BY 3 DESC"""
        errors_sql = x + """
            SELECT error_code, count(*) AS count FROM x WHERE error_code IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 8"""
        latency_sql = x + """
            SELECT width_bucket(end_to_end_ms, %(edges)s::bigint[]) AS bucket, count(*) AS count
            FROM x WHERE status_code = 'COMPLETED' GROUP BY 1"""
        retry_sql = x + "SELECT least(retry_count, 5) AS retries, count(*) AS count FROM x GROUP BY 1 ORDER BY 1"
        heat_sql = """
            SELECT extract(isodow FROM f.received_at AT TIME ZONE %(tz)s)::int AS dow,
                   extract(hour FROM f.received_at AT TIME ZONE %(tz)s)::int AS hour, count(*) AS count
            FROM dwh.fact_request f JOIN dwh.dim_workflow w ON w.workflow_key = f.workflow_key
            WHERE f.received_at >= now() - interval '7 days'
              AND (%(workflow)s::text IS NULL OR w.workflow_name = %(workflow)s)
              AND (%(backend)s::text IS NULL OR f.storage_backend = %(backend)s)
            GROUP BY 1, 2"""
        by_wf_sql = x + f"""
            SELECT workflow_name, count(*) AS total,
                   count(*) FILTER (WHERE status_code = 'COMPLETED') AS completed,
                   count(*) FILTER (WHERE status_code = 'FAILED') AS failed,
                   count(*) FILTER (WHERE workflow_status = 'REJECTED') AS rejected,
                   {pct.format(q=0.5, col='end_to_end_ms')} AS p50_e2e,
                   {pct.format(q=0.95, col='end_to_end_ms')} AS p95_e2e,
                   round(avg(retry_count), 2) AS avg_retries
            FROM x GROUP BY 1 ORDER BY 2 DESC"""
        wm_sql = """SELECT w.job_name, w.last_run_at, nullif(w.last_loaded_at, '-infinity') AS last_loaded_at,
                           w.rows_loaded, j.job_name AS system_job, j.interval_seconds, j.next_run_at, j.is_paused,
                           j.run_requested
                    FROM dwh.etl_watermark w
                    JOIN console.system_jobs j ON j.job_name = CASE w.job_name WHEN 'fact_request' THEN 'dwh_load_incremental'
                                                                                ELSE 'dwh_load_bigquery' END
                    ORDER BY w.job_name"""
        backend_sql = x + f"""
            SELECT storage_backend, count(*) AS total,
                   count(*) FILTER (WHERE status_code = 'COMPLETED') AS completed,
                   count(*) FILTER (WHERE status_code = 'FAILED') AS failed,
                   {pct.format(q=0.5, col='end_to_end_ms')} AS p50_e2e,
                   {pct.format(q=0.95, col='end_to_end_ms')} AS p95_e2e,
                   {pct.format(q=0.95, col='queue_wait_ms')} AS p95_queue
            FROM x GROUP BY 1 ORDER BY 1"""
        kpis, series, outcomes, errors, latency, retries, heat, by_wf, wm, by_backend = await asyncio.gather(
            fetch_one(kpi_sql, p), fetch_all(series_sql, p), fetch_all(outcome_sql, p), fetch_all(errors_sql, p),
            fetch_all(latency_sql, p), fetch_all(retry_sql, p), fetch_all(heat_sql, p), fetch_all(by_wf_sql, p),
            fetch_all(wm_sql), fetch_all(backend_sql, p))
        counts = {r["bucket"]: r["count"] for r in latency}
        return {
            "range": range, "bucket": bucket, "tz": p["tz"], "kpis": kpis, "series": series,
            "outcomes": outcomes, "errors": errors, "retries": retries, "heatmap": heat, "by_workflow": by_wf,
            "by_backend": by_backend,
            "latency": [{"label": label, "count": counts.get(i, 0)} for i, label in enumerate(LATENCY_LABELS)],
            "warehouse": wm,
        }

    # ------------------------------------------------------------------ telemetry
    @app.get("/api/telemetry")
    async def telemetry(kind: Literal["all", "state", "http", "job"] = "all", limit: int = Query(80, ge=10, le=300)):
        endpoints_sql = """
            SELECT direction, http_method, coalesce(endpoint, 'callback_url') AS endpoint, count(*) AS calls,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms) AS p50_ms,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_ms,
                   count(*) FILTER (WHERE response_status_code >= 400 OR response_status_code IS NULL) AS errors,
                   max(called_at) AS last_call
            FROM integration.api_call_log WHERE called_at >= now() - interval '24 hours'
            GROUP BY 1, 2, 3 ORDER BY calls DESC"""
        codes_sql = """
            SELECT coalesce(response_status_code::text, 'network error') AS code, count(*) AS count
            FROM integration.api_call_log WHERE called_at >= now() - interval '24 hours'
            GROUP BY 1 ORDER BY 1"""
        db_sql = """
            SELECT pg_database_size(current_database()) AS db_bytes,
                   (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()) AS connections,
                   (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state = 'active') AS active,
                   (SELECT count(*) FROM integration.request_metadata) AS requests,
                   (SELECT count(*) FROM integration.request_routes) AS bigquery_routes,
                   (SELECT count(*) FROM integration.api_call_log) AS api_calls,
                   (SELECT count(*) FROM integration.request_event_log) AS events,
                   (SELECT count(*) FROM console.job_submissions) AS job_submissions,
                   (SELECT max(last_finished_at) FROM console.system_jobs) AS scheduler_last_run,
                   (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
                      AND usename = 'il_scheduler_user') AS scheduler_sessions,
                   version() AS version"""
        parts = {
            "state": """SELECT 'state' AS kind, e.event_at AS ts, e.request_id::text AS ref,
                               e.new_status::text AS title,
                               coalesce(e.old_status::text || ' -> ', '') || e.new_status::text AS detail,
                               e.actor AS source, e.error_code AS code, NULL::int AS latency_ms
                        FROM integration.request_event_log e ORDER BY e.event_id DESC LIMIT %(limit)s""",
            "http": """SELECT 'http' AS kind, a.called_at, a.request_id::text,
                              a.http_method || ' ' || coalesce(a.endpoint, a.api_url),
                              a.direction, coalesce(a.client_ip::text, a.server_hostname),
                              coalesce(a.response_status_code::text, 'ERR'), a.latency_ms
                       FROM integration.api_call_log a ORDER BY a.call_id DESC LIMIT %(limit)s""",
            "job": """(SELECT 'job', r.started_at, r.job_id::text, j.name,
                              r.trigger || ': ' || r.accepted || ' accepted, ' || r.rejected || ' rejected, '
                                  || r.errors || ' errors', 'il-scheduler', NULL,
                              (extract(epoch FROM r.finished_at - r.started_at) * 1000)::int
                       FROM console.job_runs r JOIN console.jobs j ON j.job_id = r.job_id
                       ORDER BY r.run_id DESC LIMIT %(limit)s)
                      UNION ALL
                      (SELECT 'job', started_at, NULL, job_name, trigger || ': ' || coalesce(result, ''),
                              'il-scheduler', status, duration_ms
                       FROM console.system_job_runs ORDER BY run_id DESC LIMIT %(limit)s)""",
        }
        chosen = parts.values() if kind == "all" else [parts[kind]]
        stream_sql = "SELECT * FROM (" + " UNION ALL ".join(f"({q})" for q in chosen) + \
                     ") s(kind, ts, ref, title, detail, source, code, latency_ms) ORDER BY ts DESC LIMIT %(limit)s"
        rows, endpoints, codes, db, stream, svc, bq_events, bq_summary = await asyncio.gather(
            recent_rows(24), fetch_all(endpoints_sql), fetch_all(codes_sql), fetch_one(db_sql),
            fetch_all(stream_sql, {"limit": limit}), services(),
            bq_safe("recent_events", limit, default=[]) if kind in ("all", "state") else asyncio.sleep(0, []),
            bq_safe("summary", default={"counts": {}}, ttl=4))
        stream += [{"kind": "state", "ts": e["event_at"], "ref": e["request_id"], "title": e["new_status"],
                    "detail": (f"{e['old_status']} -> " if e["old_status"] else "") + e["new_status"] + " (BigQuery)",
                    "source": e["actor"], "code": e["error_code"], "latency_ms": None} for e in bq_events]
        stream.sort(key=lambda e: e["ts"], reverse=True)

        def stage(values):
            return {"p50": percentile(values, 0.5), "p95": percentile(values, 0.95)}

        stages = {"requests": len(rows)}
        for backend in ("all", "postgres", "bigquery"):
            sel = [r for r in rows if backend == "all" or r["storage_backend"] == backend]
            stages[backend] = {
                "requests": len(sel),
                "queue": stage([ms(r["received_at"], r["picked_at"]) for r in sel]),
                "processing": stage([r["processing_duration_ms"] for r in sel]),
                "callback": stage([ms(r["workflow_completed_at"], r["callback_delivered_at"]) for r in sel]),
                "e2e": stage([ms(r["received_at"], r["workflow_completed_at"]) for r in sel if r["status"] == "COMPLETED"]),
            }
        db["bigquery_rows"] = sum(bq_summary["counts"].values())
        return {"stages": stages, "endpoints": endpoints, "codes": codes, "db": db, "stream": stream[:limit],
                "services": svc}

    @app.get("/api/callbacks")
    async def callbacks():
        """Callbacks received by WWMG_Source and MyCareHub, newest first."""
        async def fetch(name: str, url: str) -> list[dict]:
            try:
                response = await app.state.http.get(url, timeout=3)
                return [c | {"receiver": name, "_received_at": c.get("_received_at") or c.get("received_at")}
                        for c in response.json()]
            except Exception:
                return []
        wwmg, mch = await asyncio.gather(fetch("WWMG_Source", f"{settings.wwmg_url}/hooks/il/received"),
                                         fetch("MyCareHub", f"{settings.mycarehub_url}/api/notifications"))
        items = wwmg + mch
        items.sort(key=lambda c: c["_received_at"] or "", reverse=True)
        return items[:100]

    @app.get("/api/api-calls")
    async def api_calls(limit: int = Query(100, ge=1, le=500)):
        return await fetch_all("""SELECT call_id, request_id, direction, http_method, api_url, endpoint,
                                         response_status_code, latency_ms, client_ip::text, server_hostname,
                                         request_headers, response_body, called_at
                                  FROM integration.api_call_log ORDER BY call_id DESC LIMIT %s""", (limit,))

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    app.mount("/static", StaticFiles(directory=STATIC), name="static")

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(STATIC / "index.html")

    return app


app = create_app()
