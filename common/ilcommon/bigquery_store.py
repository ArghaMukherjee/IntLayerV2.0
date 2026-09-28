"""BigQuery data plane: request storage and queue operations.

Mirrors the PostgreSQL SQL functions (submit, claim, heartbeat, complete, fail,
reap, cancel, replay, callbacks) using BigQuery DML. BigQuery has no row locks,
so a claim is a single UPDATE that stamps the rows it takes with a random claim
token; the rows are then read back by that token. BigQuery serialises concurrent
DML on a table (a conflicting statement fails and is retried here), so two
workers can never claim the same request.

JSON columns are stored as STRING (JSON text) so the same schema works on real
BigQuery and on the local emulator. The client is synchronous; async callers use
asyncio.to_thread.

Portability rules (real BigQuery and the goccy emulator behave the same with them):
  * the clock is always the @now parameter, never CURRENT_TIMESTAMP()/CURRENT_DATE();
  * NULL parameters are inlined as typed NULL literals;
  * rows changed by an UPDATE are counted through an op_token column, because the
    emulator does not report num_dml_affected_rows.
"""
import json
import logging
import random
import re
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any

from google.api_core import exceptions as gexc
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
from pydantic_settings import BaseSettings, SettingsConfigDict

log = logging.getLogger(__name__)


class BigQuerySettings(BaseSettings):
    """BQ_* environment variables."""

    model_config = SettingsConfigDict(env_prefix="BQ_", extra="ignore")

    enabled: bool = True
    project: str = "intlayer-local"
    dataset: str = "integration"
    location: str = "US"
    # e.g. http://bigquery:9050 for the local emulator. Empty = real BigQuery, authenticated with
    # Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS pointing at a service-account key).
    emulator_host: str = ""

    @property
    def is_emulator(self) -> bool:
        return bool(self.emulator_host)


REQUEST_SCHEMA = [
    ("request_id", "STRING", "REQUIRED"), ("request_date", "DATE", "REQUIRED"), ("correlation_id", "STRING", "NULLABLE"),
    ("idempotency_key", "STRING", "NULLABLE"), ("source_app", "STRING", "REQUIRED"), ("target_app", "STRING", "REQUIRED"),
    ("workflow_name", "STRING", "REQUIRED"), ("priority", "INT64", "NULLABLE"), ("input_json", "STRING", "NULLABLE"),
    ("input_schema_version", "INT64", "NULLABLE"), ("output_json", "STRING", "NULLABLE"), ("api_url", "STRING", "NULLABLE"),
    ("http_method", "STRING", "NULLABLE"), ("request_headers", "STRING", "NULLABLE"), ("callback_url", "STRING", "NULLABLE"),
    ("client_ip", "STRING", "NULLABLE"), ("il_server_hostname", "STRING", "NULLABLE"), ("il_server_ip", "STRING", "NULLABLE"),
    ("il_instance_id", "STRING", "NULLABLE"), ("consumer_worker_host", "STRING", "NULLABLE"),
    ("consumer_worker_id", "STRING", "NULLABLE"), ("db_server_addr", "STRING", "NULLABLE"),
    ("status", "STRING", "REQUIRED"), ("success_flag", "BOOL", "NULLABLE"), ("workflow_status", "STRING", "NULLABLE"),
    ("error_code", "STRING", "NULLABLE"), ("error_message", "STRING", "NULLABLE"), ("retry_count", "INT64", "NULLABLE"),
    ("max_retries", "INT64", "NULLABLE"), ("next_attempt_at", "TIMESTAMP", "NULLABLE"), ("locked_by", "STRING", "NULLABLE"),
    ("locked_until", "TIMESTAMP", "NULLABLE"), ("claim_token", "STRING", "NULLABLE"), ("received_at", "TIMESTAMP", "NULLABLE"),
    ("picked_at", "TIMESTAMP", "NULLABLE"), ("workflow_completed_at", "TIMESTAMP", "NULLABLE"),
    ("processing_duration_ms", "INT64", "NULLABLE"), ("callback_status", "STRING", "NULLABLE"),
    ("callback_attempts", "INT64", "NULLABLE"), ("callback_next_attempt_at", "TIMESTAMP", "NULLABLE"),
    ("callback_locked_until", "TIMESTAMP", "NULLABLE"), ("callback_claim_token", "STRING", "NULLABLE"),
    ("callback_last_error", "STRING", "NULLABLE"), ("callback_delivered_at", "TIMESTAMP", "NULLABLE"),
    ("created_at", "TIMESTAMP", "NULLABLE"), ("updated_at", "TIMESTAMP", "NULLABLE"),
    ("op_token", "STRING", "NULLABLE"),
]
EVENT_SCHEMA = [
    ("event_id", "STRING", "REQUIRED"), ("request_id", "STRING", "REQUIRED"), ("request_date", "DATE", "NULLABLE"),
    ("old_status", "STRING", "NULLABLE"), ("new_status", "STRING", "REQUIRED"), ("retry_count", "INT64", "NULLABLE"),
    ("actor", "STRING", "NULLABLE"), ("error_code", "STRING", "NULLABLE"), ("error_message", "STRING", "NULLABLE"),
    ("event_at", "TIMESTAMP", "REQUIRED"),
]
TYPES = {name: typ for name, typ, _ in REQUEST_SCHEMA}
JSON_COLUMNS = ("input_json", "output_json", "request_headers")
RETRYABLE_MESSAGES = ("could not serialize", "concurrent update", "database is locked", "resources exceeded")


def _param(name: str, value: Any, typ: str | None = None) -> bigquery.ScalarQueryParameter:
    if typ is None:
        if isinstance(value, bool):
            typ = "BOOL"
        elif isinstance(value, int):
            typ = "INT64"
        elif isinstance(value, float):
            typ = "FLOAT64"
        elif isinstance(value, datetime):
            typ = "TIMESTAMP"
        elif isinstance(value, date):
            typ = "DATE"
        else:
            typ = "STRING"
    return bigquery.ScalarQueryParameter(name, typ, value)


def _decode(row: bigquery.Row | dict) -> dict:
    out = dict(row.items())
    for col in JSON_COLUMNS:
        if isinstance(out.get(col), str):
            try:
                out[col] = json.loads(out[col])
            except ValueError:
                pass
    out["storage_backend"] = "bigquery"
    return out


class BigQueryStore:
    def __init__(self, settings: BigQuerySettings | None = None):
        self.settings = settings or BigQuerySettings()
        s = self.settings
        if s.is_emulator:
            self.client = bigquery.Client(project=s.project, credentials=AnonymousCredentials(),
                                          client_options=ClientOptions(api_endpoint=s.emulator_host))
        else:
            self.client = bigquery.Client(project=s.project, location=s.location)
        self.dataset_ref = f"{s.project}.{s.dataset}"
        self.t = f"`{s.project}.{s.dataset}.request_metadata`"
        self.e = f"`{s.project}.{s.dataset}.request_event_log`"

    @property
    def label(self) -> str:
        return f"{'emulator' if self.settings.is_emulator else 'bigquery'}:{self.dataset_ref}"

    # ------------------------------------------------------------------ plumbing
    def query(self, sql: str, params: dict | None = None, types: dict | None = None) -> tuple[bigquery.QueryJob, list]:
        """Runs a parameterised statement, retrying serialisation conflicts. Returns (job, rows).

        @now is bound to the current UTC time; None values become typed NULL literals."""
        params = dict(params or {})
        types = dict(types or {})
        if "@now" in sql:
            params.setdefault("now", datetime.now(timezone.utc))
        for name in [k for k, v in params.items() if v is None]:
            params.pop(name)
            sql = re.sub(rf"@{name}\b", f"CAST(NULL AS {types.get(name, 'STRING')})", sql)
        qp = [_param(k, v, types.get(k)) for k, v in params.items() if re.search(rf"@{k}\b", sql)]
        config = bigquery.QueryJobConfig(query_parameters=qp)
        for attempt in range(6):
            try:
                job = self.client.query(sql, job_config=config)
                return job, list(job.result())
            except (gexc.BadRequest, gexc.InternalServerError, gexc.ServiceUnavailable, gexc.Conflict) as exc:
                msg = str(exc).lower()
                transient = not isinstance(exc, gexc.BadRequest) or any(m in msg for m in RETRYABLE_MESSAGES)
                if not transient or attempt == 5:
                    raise
                time.sleep(0.15 * (2 ** attempt) + random.random() * 0.1)
        raise RuntimeError("unreachable")

    def _update(self, sql: str, params: dict, types: dict | None = None) -> int:
        """Runs an UPDATE that sets op_token = @op and returns how many rows it changed."""
        token = uuid.uuid4().hex
        job, _ = self.query(sql, {**params, "op": token}, types)
        if job.num_dml_affected_rows is not None:
            return int(job.num_dml_affected_rows)
        _, rows = self.query(f"SELECT COUNT(*) AS n FROM {self.t} WHERE op_token = @op", {"op": token})
        return rows[0]["n"]

    def cached_ping(self, max_age: float = 15) -> dict:
        """ping() result reused for max_age seconds, so health checks don't queue behind real work."""
        cached = getattr(self, "_ping_cache", None)
        if cached and time.monotonic() - cached[0] < max_age:
            return cached[1]
        try:
            result = self.ping()
        except Exception as exc:
            result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:300], "target": self.label}
        self._ping_cache = (time.monotonic(), result)
        return result

    def ping(self) -> dict:
        t0 = time.perf_counter()
        _, rows = self.query("SELECT 1 AS ok")
        return {"ok": rows[0]["ok"] == 1, "latency_ms": int((time.perf_counter() - t0) * 1000), "target": self.label}

    def ensure_schema(self) -> list[str]:
        """Creates the dataset and tables if missing (idempotent). Returns what was created."""
        created = []
        try:
            self.client.get_dataset(self.dataset_ref)
        except gexc.NotFound:
            ds = bigquery.Dataset(self.dataset_ref)
            ds.location = self.settings.location
            self.client.create_dataset(ds, exists_ok=True)
            created.append(self.dataset_ref)
        for name, schema, partition in (("request_metadata", REQUEST_SCHEMA, "request_date"),
                                        ("request_event_log", EVENT_SCHEMA, "request_date")):
            ref = f"{self.dataset_ref}.{name}"
            try:
                self.client.get_table(ref)
                continue
            except gexc.NotFound:
                pass
            table = bigquery.Table(ref, schema=[bigquery.SchemaField(n, t, mode=m) for n, t, m in schema])
            if not self.settings.is_emulator:  # partitioning/clustering on real BigQuery only
                table.time_partitioning = bigquery.TimePartitioning(field=partition)
                if name == "request_metadata":
                    table.clustering_fields = ["status", "target_app", "workflow_name"]
            self.client.create_table(table, exists_ok=True)
            created.append(ref)
        return created

    def _event(self, request_id: str, old: str | None, actor: str, where_sql: str = "request_id = @rid",
               params: dict | None = None) -> None:
        """Appends one audit event per matching row, taking new status/error from the row."""
        self.query(
            f"""INSERT INTO {self.e} (event_id, request_id, request_date, old_status, new_status, retry_count,
                                      actor, error_code, error_message, event_at)
                SELECT CONCAT(@eid, '-', request_id), request_id, request_date, @old, status, retry_count, @actor,
                       error_code, error_message, @now
                FROM {self.t} WHERE {where_sql}""",
            {"eid": uuid.uuid4().hex[:12], "old": old, "actor": actor,
             **({"rid": request_id} if "@rid" in where_sql else {}), **(params or {})},
            {"old": "STRING"})

    # ------------------------------------------------------------------ submit / read
    def submit(self, fields: dict) -> dict:
        """Inserts a new PENDING request. fields holds the metadata columns (request_id, workflow, payload...)."""
        row = dict(fields)
        for col in JSON_COLUMNS:
            if row.get(col) is not None and not isinstance(row[col], str):
                row[col] = json.dumps(row[col], separators=(",", ":"), default=str)
        row.setdefault("db_server_addr", self.label)
        row.update(status="PENDING", retry_count=0, callback_attempts=0)
        cols = [c for c in row if c in TYPES]
        server = {"next_attempt_at": "@now", "received_at": "@now",
                  "created_at": "@now", "updated_at": "@now"}
        names = cols + list(server)
        values = [f"@{c}" for c in cols] + list(server.values())
        self.query(f"INSERT INTO {self.t} ({', '.join(names)}) VALUES ({', '.join(values)})",
                   {c: row[c] for c in cols}, {c: TYPES[c] for c in cols})
        self.query(f"""INSERT INTO {self.e} (event_id, request_id, request_date, old_status, new_status, retry_count,
                                             actor, event_at)
                       VALUES (@eid, @rid, @d, NULL, 'PENDING', 0, 'il', @now)""",
                   {"eid": uuid.uuid4().hex, "rid": row["request_id"], "d": row["request_date"]}, {"d": "DATE"})
        return row | {"storage_backend": "bigquery"}

    def get(self, request_id: str) -> dict | None:
        _, rows = self.query(f"SELECT * FROM {self.t} WHERE request_id = @rid", {"rid": str(request_id)})
        return _decode(rows[0]) if rows else None

    def list(self, *, source_app: str | None = None, workflow: str | None = None, status: str | None = None,
             q: str | None = None, since: datetime | None = None, limit: int = 100) -> list[dict]:
        conds, params = ["TRUE"], {"lim": limit}
        for col, name, value in (("source_app", "src", source_app), ("workflow_name", "wf", workflow),
                                 ("status", "st", status)):
            if value:
                conds.append(f"{col} = @{name}")
                params[name] = value
        if since:
            conds.append("received_at >= @since")
            params["since"] = since
        if q:
            conds.append("(request_id LIKE @q OR correlation_id LIKE @q OR idempotency_key LIKE @q OR input_json LIKE @q)")
            params["q"] = f"%{q}%"
        _, rows = self.query(f"SELECT * FROM {self.t} WHERE {' AND '.join(conds)} ORDER BY received_at DESC LIMIT @lim",
                             params)
        return [_decode(r) for r in rows]

    def events(self, request_id: str) -> list[dict]:
        _, rows = self.query(f"SELECT * FROM {self.e} WHERE request_id = @rid ORDER BY event_at, event_id",
                             {"rid": str(request_id)})
        return [dict(r.items()) for r in rows]

    def recent_events(self, limit: int = 80) -> list[dict]:
        _, rows = self.query(f"SELECT * FROM {self.e} ORDER BY event_at DESC LIMIT @lim", {"lim": limit})
        return [dict(r.items()) for r in rows]

    def status_counts(self) -> dict[str, int]:
        _, rows = self.query(f"SELECT status, COUNT(*) AS n FROM {self.t} GROUP BY status")
        return {r["status"]: r["n"] for r in rows}

    def recent(self, since: datetime, limit: int = 20000) -> list[dict]:
        """Light-weight rows (no payloads) for dashboards and telemetry."""
        _, rows = self.query(
            f"""SELECT request_id, workflow_name, source_app, status, retry_count, received_at, picked_at,
                       workflow_completed_at, processing_duration_ms, callback_status, callback_delivered_at
                FROM {self.t} WHERE received_at >= @since ORDER BY received_at DESC LIMIT @lim""",
            {"since": since, "lim": limit})
        return [dict(r.items()) | {"storage_backend": "bigquery"} for r in rows]

    def summary(self) -> dict:
        """Status counts, oldest pending request and callback counts (all time)."""
        _, rows = self.query(
            f"""SELECT status, callback_status, COUNT(*) AS n,
                       MIN(IF(status = 'PENDING', received_at, NULL)) AS oldest_pending
                FROM {self.t} GROUP BY status, callback_status""")
        counts, callbacks, oldest = {}, {}, None
        for r in rows:
            counts[r["status"]] = counts.get(r["status"], 0) + r["n"]
            if r["callback_status"]:
                callbacks[r["callback_status"]] = callbacks.get(r["callback_status"], 0) + r["n"]
            if r["oldest_pending"] and (oldest is None or r["oldest_pending"] < oldest):
                oldest = r["oldest_pending"]
        return {"counts": counts, "callbacks": callbacks, "oldest_pending": oldest}

    def changed_since(self, since: datetime, limit: int = 5000) -> list[dict]:
        _, rows = self.query(f"SELECT * FROM {self.t} WHERE updated_at > @since ORDER BY updated_at LIMIT @lim",
                             {"since": since, "lim": limit})
        return [dict(r.items()) for r in rows]

    # ------------------------------------------------------------------ consumer side
    def _any(self, where: str, params: dict) -> bool:
        """Cheap existence check so idle pollers don't issue UPDATEs (DML is the expensive part)."""
        _, rows = self.query(f"SELECT 1 AS x FROM {self.t} WHERE {where} LIMIT 1", params)
        return bool(rows)

    def claim(self, worker_id: str, worker_host: str, target_app: str, limit: int, lease_seconds: int) -> list[dict]:
        if not self._any("status = 'PENDING' AND target_app = @t AND next_attempt_at <= @now", {"t": target_app}):
            return []
        token = uuid.uuid4().hex
        n = self._update(
            f"""UPDATE {self.t}
                   SET status = 'IN_PROGRESS', locked_by = @w, claim_token = @tok, op_token = @op,
                       locked_until = TIMESTAMP_ADD(@now, INTERVAL @lease SECOND),
                       consumer_worker_id = @w, consumer_worker_host = @host,
                       picked_at = @now, updated_at = @now
                 WHERE request_id IN (
                       SELECT request_id FROM {self.t}
                       WHERE status = 'PENDING' AND target_app = @t AND next_attempt_at <= @now
                       ORDER BY priority DESC, next_attempt_at LIMIT @n)""",
            {"w": worker_id, "tok": token, "lease": lease_seconds, "host": worker_host, "t": target_app, "n": limit})
        if not n:
            return []
        self._event("", "PENDING", worker_id, "claim_token = @tok", {"tok": token})
        _, rows = self.query(f"SELECT * FROM {self.t} WHERE claim_token = @tok AND status = 'IN_PROGRESS'", {"tok": token})
        return [_decode(r) for r in rows]

    def heartbeat(self, request_id: str, worker_id: str, lease_seconds: int) -> bool:
        return bool(self._update(
            f"""UPDATE {self.t} SET locked_until = TIMESTAMP_ADD(@now, INTERVAL @lease SECOND), op_token = @op
                WHERE request_id = @rid AND status = 'IN_PROGRESS' AND locked_by = @w""",
            {"lease": lease_seconds, "rid": str(request_id), "w": worker_id}))

    def complete(self, request_id: str, worker_id: str, output: dict, workflow_status: str = "COMPLETED") -> bool:
        n = self._update(
            f"""UPDATE {self.t}
                   SET status = 'COMPLETED', success_flag = TRUE, output_json = @out, workflow_status = @ws,
                       workflow_completed_at = @now,
                       processing_duration_ms = TIMESTAMP_DIFF(@now, picked_at, MILLISECOND),
                       error_code = NULL, error_message = NULL, locked_by = NULL, locked_until = NULL,
                       callback_status = IF(callback_url IS NULL, NULL, 'PENDING'), callback_attempts = 0,
                       callback_next_attempt_at = IF(callback_url IS NULL, NULL, @now),
                       callback_locked_until = NULL, callback_last_error = NULL, callback_delivered_at = NULL,
                       op_token = @op, updated_at = @now
                 WHERE request_id = @rid AND status = 'IN_PROGRESS' AND locked_by = @w""",
            {"out": json.dumps(output, separators=(",", ":"), default=str), "ws": workflow_status,
             "rid": str(request_id), "w": worker_id})
        if n:
            self._event(str(request_id), "IN_PROGRESS", worker_id)
        return bool(n)

    def fail(self, request_id: str, worker_id: str, code: str, message: str, retryable: bool,
             output: dict | None = None, workflow_status: str = "FAILED") -> str | None:
        retry = "@retry AND retry_count < max_retries"
        n = self._update(
            f"""UPDATE {self.t}
                   SET status = IF({retry}, 'PENDING', 'FAILED'),
                       success_flag = IF({retry}, NULL, FALSE),
                       retry_count = IF({retry}, retry_count + 1, retry_count),
                       next_attempt_at = TIMESTAMP_ADD(@now,
                           INTERVAL CAST(LEAST(3600, 30 * POW(2, retry_count)) AS INT64) SECOND),
                       workflow_status = IF({retry}, workflow_status, @ws),
                       workflow_completed_at = IF({retry}, NULL, @now),
                       processing_duration_ms = TIMESTAMP_DIFF(@now, picked_at, MILLISECOND),
                       output_json = COALESCE(@out, output_json),
                       error_code = @code, error_message = @msg, locked_by = NULL, locked_until = NULL,
                       callback_status = IF({retry} OR callback_url IS NULL, callback_status, 'PENDING'),
                       callback_attempts = IF({retry} OR callback_url IS NULL, callback_attempts, 0),
                       callback_next_attempt_at = IF({retry} OR callback_url IS NULL, callback_next_attempt_at, @now),
                       op_token = @op, updated_at = @now
                 WHERE request_id = @rid AND status = 'IN_PROGRESS' AND locked_by = @w""",
            {"retry": retryable, "ws": workflow_status, "code": code, "msg": message[:4000],
             "out": json.dumps(output, default=str) if output is not None else None, "rid": str(request_id), "w": worker_id},
            {"out": "STRING"})
        if not n:
            return None
        self._event(str(request_id), "IN_PROGRESS", worker_id)
        return self.get(request_id)["status"]

    # ------------------------------------------------------------------ maintenance / admin
    def reap(self) -> int:
        """Re-queues (or fails) IN_PROGRESS requests whose lease expired."""
        if not self._any("status = 'IN_PROGRESS' AND locked_until < @now", {}):
            return 0
        token = "reap-" + uuid.uuid4().hex
        n = self._update(
            f"""UPDATE {self.t}
                   SET status = IF(retry_count < max_retries, 'PENDING', 'FAILED'),
                       success_flag = IF(retry_count < max_retries, NULL, FALSE),
                       workflow_completed_at = IF(retry_count < max_retries, NULL, @now),
                       retry_count = IF(retry_count < max_retries, retry_count + 1, retry_count),
                       next_attempt_at = @now, error_code = 'LEASE_EXPIRED',
                       error_message = CONCAT('Worker ', IFNULL(locked_by, '?'), ' did not finish before its lease expired'),
                       locked_by = NULL, locked_until = NULL, claim_token = @tok, op_token = @op, updated_at = @now
                 WHERE status = 'IN_PROGRESS' AND locked_until < @now""", {"tok": token})
        if n:
            self._event("", "IN_PROGRESS", "reaper", "claim_token = @tok", {"tok": token})
        return n

    def cancel(self, request_id: str, reason: str | None = None) -> bool:
        n = self._update(
            f"""UPDATE {self.t} SET status = 'CANCELLED', success_flag = FALSE, error_code = 'CANCELLED',
                       error_message = @reason, workflow_completed_at = @now,
                       callback_status = IF(callback_url IS NULL, NULL, 'PENDING'), callback_attempts = 0,
                       callback_next_attempt_at = IF(callback_url IS NULL, NULL, @now),
                       op_token = @op, updated_at = @now
                 WHERE request_id = @rid AND status = 'PENDING'""", {"reason": reason, "rid": str(request_id)},
            {"reason": "STRING"})
        if n:
            self._event(str(request_id), "PENDING", "il")
        return bool(n)

    def replay(self, request_id: str) -> bool:
        n = self._update(
            f"""UPDATE {self.t} SET status = 'PENDING', success_flag = NULL, retry_count = 0,
                       next_attempt_at = @now, workflow_completed_at = NULL,
                       error_code = NULL, error_message = NULL, op_token = @op, updated_at = @now
                 WHERE request_id = @rid AND status = 'FAILED'""", {"rid": str(request_id)})
        if n:
            self._event(str(request_id), "FAILED", "admin-replay")
        return bool(n)

    # ------------------------------------------------------------------ callbacks (IL dispatcher)
    def claim_callbacks(self, limit: int, lease_seconds: int) -> list[dict]:
        if not self._any("callback_status = 'PENDING' AND callback_next_attempt_at <= @now"
                         " AND (callback_locked_until IS NULL OR callback_locked_until < @now)", {}):
            return []
        token = uuid.uuid4().hex
        n = self._update(
            f"""UPDATE {self.t}
                   SET callback_locked_until = TIMESTAMP_ADD(@now, INTERVAL @lease SECOND),
                       callback_attempts = callback_attempts + 1, callback_claim_token = @tok, op_token = @op
                 WHERE request_id IN (
                       SELECT request_id FROM {self.t}
                       WHERE callback_status = 'PENDING' AND callback_next_attempt_at <= @now
                         AND (callback_locked_until IS NULL OR callback_locked_until < @now)
                       ORDER BY callback_next_attempt_at LIMIT @n)""",
            {"lease": lease_seconds, "tok": token, "n": limit})
        if not n:
            return []
        _, rows = self.query(f"SELECT * FROM {self.t} WHERE callback_claim_token = @tok", {"tok": token})
        return [_decode(r) for r in rows]

    def record_callback_result(self, request_id: str, delivered: bool, error: str | None, max_attempts: int) -> str | None:
        self._update(
            f"""UPDATE {self.t}
                   SET callback_status = IF(@ok, 'DELIVERED', IF(callback_attempts >= @max, 'FAILED', 'PENDING')),
                       callback_delivered_at = IF(@ok, @now, NULL),
                       callback_next_attempt_at = IF(@ok, NULL, TIMESTAMP_ADD(@now,
                           INTERVAL CAST(LEAST(1800, 10 * POW(2, callback_attempts - 1)) AS INT64) SECOND)),
                       callback_last_error = IF(@ok, NULL, @err), callback_locked_until = NULL,
                       op_token = @op, updated_at = @now
                 WHERE request_id = @rid AND callback_status = 'PENDING'""",
            {"ok": delivered, "max": max_attempts, "err": (error or "")[:2000], "rid": str(request_id)})
        row = self.get(request_id)
        return row["callback_status"] if row else None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
