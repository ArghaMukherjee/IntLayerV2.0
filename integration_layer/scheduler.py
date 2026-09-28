"""Job runner (runs as its own container).

Two kinds of jobs, both scheduled from the database so the console can create,
trigger, pause, resume and reschedule them:

* System jobs (console.system_jobs): maintenance for both data planes (lease reapers,
  partitions, warehouse loads from PostgreSQL and BigQuery). The schedule is in the
  table; what each job does is defined here.
* Traffic jobs (console.jobs): send requests to the Integration Layer API built
  from a JSON payload template, recording every HTTP status and response body.

Due jobs are claimed with FOR UPDATE SKIP LOCKED, so running more than one
scheduler never runs a job twice.
"""
import logging
import signal
import threading
import time

import httpx
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic_settings import BaseSettings, SettingsConfigDict

from ilcommon.bigquery_store import BigQuerySettings, BigQueryStore
from ilcommon.templating import render

log = logging.getLogger("scheduler")

SYSTEM_JOB_SQL = {
    "reap_expired_leases": "SELECT integration.reap_expired_leases()::text AS result",
    "ensure_partitions": "SELECT string_agg(integration.ensure_month_partition("
                         "(current_date + make_interval(months => m))::date), ', ') AS result "
                         "FROM generate_series(0, 2) AS m",
    "dwh_load_incremental": "CALL dwh.load_incremental()",
}

STAGE_DDL = """CREATE TEMP TABLE _chg (
    request_id uuid, correlation_id uuid, request_date date, source_app text, target_app text, workflow_name text,
    il_server_hostname text, consumer_worker_host text, status text, success_flag boolean, workflow_status text,
    error_code text, retry_count integer, picked_at timestamptz, received_at timestamptz,
    processing_duration_ms bigint, workflow_completed_at timestamptz, input_json text, output_json text,
    updated_at timestamptz) ON COMMIT DROP"""
STAGE_COLUMNS = ["request_id", "correlation_id", "request_date", "source_app", "target_app", "workflow_name",
                 "il_server_hostname", "consumer_worker_host", "status", "success_flag", "workflow_status", "error_code",
                 "retry_count", "picked_at", "received_at", "processing_duration_ms", "workflow_completed_at",
                 "input_json", "output_json", "updated_at"]


def reap_bigquery(conn: psycopg.Connection, bq: BigQueryStore | None) -> str:
    if bq is None:
        return "BigQuery disabled"
    return str(bq.reap())


def dwh_load_bigquery(conn: psycopg.Connection, bq: BigQueryStore | None) -> str:
    """Copies BigQuery requests changed since the watermark into the (PostgreSQL) warehouse."""
    if bq is None:
        return "BigQuery disabled"
    since = conn.execute(
        """SELECT CASE WHEN last_loaded_at = '-infinity' THEN timestamptz '1970-01-01'
                       ELSE last_loaded_at - interval '5 minutes' END AS since
           FROM dwh.etl_watermark WHERE job_name = 'fact_request_bigquery' FOR UPDATE""").fetchone()["since"]
    rows = bq.changed_since(since)
    conn.execute(STAGE_DDL)
    with conn.cursor() as cur:
        cur.executemany(f"INSERT INTO _chg ({', '.join(STAGE_COLUMNS)}) VALUES ({', '.join(['%s'] * len(STAGE_COLUMNS))})",
                        [[r.get(c) for c in STAGE_COLUMNS] for r in rows])
    conn.execute("CALL dwh.merge_changes('bigquery', 'fact_request_bigquery')")
    conn.execute("DROP TABLE _chg")
    return f"{len(rows)} changed rows"


SYSTEM_JOB_PY = {"reap_expired_leases_bigquery": reap_bigquery, "dwh_load_bigquery": dwh_load_bigquery}


class SchedulerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SCHEDULER_", extra="ignore")

    database_url: str = "postgresql://il_scheduler_user:il_scheduler@localhost:5432/integration"
    tick_seconds: float = 2
    # Traffic jobs call the Integration Layer API as the job's source app
    il_base_url: str = "http://localhost:8000"
    source_keys: str = ""        # "WWMG_Source:<key>,MyCareHub:<key>"
    callback_urls: str = ""      # "WWMG_Source:http://wwmg-source:8002/hooks/il,MyCareHub:http://mycarehub:8003/hooks/il"
    http_timeout_seconds: float = 30

    @staticmethod
    def _pairs(text: str) -> dict[str, str]:
        return {k.strip(): v.strip() for k, v in (p.split(":", 1) for p in text.split(",") if ":" in p)}

    def key_for(self, source_app: str) -> str:
        return self._pairs(self.source_keys).get(source_app, "")

    def callback_for(self, source_app: str) -> str | None:
        return self._pairs(self.callback_urls).get(source_app)


# ----------------------------------------------------------------------------- system jobs
def run_system_jobs(conn: psycopg.Connection, bq: BigQueryStore | None = None) -> int:
    with conn.transaction():
        due = conn.execute(
            """SELECT job_name, interval_seconds, run_requested FROM console.system_jobs
               WHERE run_requested OR (NOT is_paused AND next_run_at <= now())
               FOR UPDATE SKIP LOCKED""").fetchall()
        for job in due:
            name = job["job_name"]
            trigger = "manual" if job["run_requested"] else "schedule"
            started, t0 = time.time(), time.perf_counter()
            status, result = "OK", None
            sql, fn = SYSTEM_JOB_SQL.get(name), SYSTEM_JOB_PY.get(name)
            try:
                if sql is None and fn is None:
                    raise ValueError(f"no implementation for system job '{name}'")
                with conn.transaction():  # savepoint: a failing job doesn't abort the others
                    if fn is not None:
                        result = fn(conn, bq)
                    else:
                        cur = conn.execute(sql)
                        row = cur.fetchone() if cur.description else None
                        result = str(row["result"]) if row and row["result"] is not None else "done"
            except Exception as exc:
                status, result = "ERROR", f"{type(exc).__name__}: {exc}"[:1000]
            duration = int((time.perf_counter() - t0) * 1000)
            conn.execute(
                """UPDATE console.system_jobs
                      SET last_started_at = to_timestamp(%(started)s), last_finished_at = clock_timestamp(),
                          last_status = %(status)s, last_result = %(result)s, last_duration_ms = %(duration)s,
                          run_count = run_count + 1, run_requested = false,
                          next_run_at = clock_timestamp() + make_interval(secs => interval_seconds)
                    WHERE job_name = %(name)s""",
                {"started": started, "status": status, "result": result, "duration": duration, "name": name})
            conn.execute(
                """INSERT INTO console.system_job_runs (job_name, trigger, started_at, duration_ms, status, result)
                   VALUES (%s, %s, to_timestamp(%s), %s, %s, %s)""",
                (name, trigger, started, duration, status, result))
            log.log(logging.INFO if status == "OK" else logging.ERROR,
                    "system job=%s trigger=%s status=%s result=%s duration_ms=%d", name, trigger, status, result, duration)
    return len(due)


# ----------------------------------------------------------------------------- traffic jobs
def submit(http: httpx.Client, settings: SchedulerSettings, job: dict, payload) -> tuple[int | None, object, int]:
    source = job.get("source_app") or "WWMG_Source"
    body = {"workflow_name": job["workflow_name"], "payload": payload}
    if job["use_callback"] and settings.callback_for(source):
        body["callback_url"] = settings.callback_for(source)
    t0 = time.perf_counter()
    try:
        response = http.post(f"{settings.il_base_url}/v1/requests", json=body,
                             headers={"X-API-Key": settings.key_for(source)})
        try:
            data = response.json()
        except ValueError:
            data = {"text": response.text[:2000]}
        return response.status_code, data, int((time.perf_counter() - t0) * 1000)
    except httpx.HTTPError as exc:
        return None, {"error": f"{type(exc).__name__}: {exc}"}, int((time.perf_counter() - t0) * 1000)


def run_traffic_job(conn: psycopg.Connection, http: httpx.Client, settings: SchedulerSettings, job: dict) -> None:
    trigger = "manual" if job["run_requested"] else "schedule"
    run_id = conn.execute(
        """INSERT INTO console.job_runs (job_id, trigger, requested, started_at)
           VALUES (%s, %s, %s, clock_timestamp()) RETURNING run_id""",
        (job["job_id"], trigger, job["requests_per_run"])).fetchone()["run_id"]
    template = job["payload_template"] if job["payload_template"] is not None else job["sample_payload"]
    counts = {"accepted": 0, "rejected": 0, "errors": 0}
    run_error = None
    if template is None:
        run_error = "Job has no payload: its sample was deleted and no template is set"
        counts["errors"] = job["requests_per_run"]
    else:
        for i in range(job["requests_per_run"]):
            seq = job["total_submitted"] + i + 1
            payload = render(template, {"seq": seq, "job": job["name"]})
            status, data, latency = submit(http, settings, job, payload)
            bucket = "accepted" if status and status < 300 else "rejected" if status and status < 500 else "errors"
            counts[bucket] += 1
            request_id = data.get("request_id") if isinstance(data, dict) and bucket == "accepted" else None
            conn.execute(
                """INSERT INTO console.job_submissions
                       (run_id, job_id, seq, request_id, http_status, payload, response_body, latency_ms, submitted_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, clock_timestamp())""",
                (run_id, job["job_id"], seq, request_id, status, Jsonb(payload), Jsonb(data), latency))

    conn.execute(
        """UPDATE console.job_runs SET finished_at = clock_timestamp(), accepted = %(accepted)s, rejected = %(rejected)s,
                  errors = %(errors)s, error = %(error)s WHERE run_id = %(run_id)s""",
        counts | {"error": run_error, "run_id": run_id})
    conn.execute(
        """UPDATE console.jobs
              SET run_count = run_count + 1,
                  total_submitted = total_submitted + %(n)s,
                  last_run_at = clock_timestamp(),
                  run_requested = false,
                  next_run_at = CASE WHEN interval_seconds IS NULL THEN NULL
                                     ELSE clock_timestamp() + make_interval(secs => interval_seconds) END,
                  status = CASE WHEN max_runs IS NOT NULL AND run_count + 1 >= max_runs THEN 'COMPLETED'
                                ELSE status END
            WHERE job_id = %(job_id)s""",
        {"n": job["requests_per_run"], "job_id": job["job_id"]})
    log.info("traffic job=%r trigger=%s accepted=%d rejected=%d errors=%d",
             job["name"], trigger, counts["accepted"], counts["rejected"], counts["errors"])


def run_traffic_jobs(conn: psycopg.Connection, http: httpx.Client, settings: SchedulerSettings) -> int:
    with conn.transaction():
        due = conn.execute(
            """SELECT j.*, s.payload AS sample_payload
               FROM console.jobs j LEFT JOIN console.payload_samples s ON s.sample_id = j.sample_id
               WHERE j.run_requested
                  OR (j.status = 'ACTIVE' AND j.interval_seconds IS NOT NULL
                      AND coalesce(j.next_run_at, now()) <= now())
               ORDER BY j.job_id
               FOR UPDATE OF j SKIP LOCKED""").fetchall()
        for job in due:
            with conn.transaction():
                run_traffic_job(conn, http, settings, job)
    return len(due)


# ----------------------------------------------------------------------------- main loop
def _loop(name: str, settings: SchedulerSettings, stop: threading.Event, cycle) -> None:
    """Runs cycle(conn) every tick with its own connection; reconnects on database errors."""
    conn = None
    while not stop.is_set():
        try:
            if conn is None or conn.closed:
                conn = psycopg.connect(settings.database_url, row_factory=dict_row)
                log.info("%s loop connected; polling every %ss", name, settings.tick_seconds)
            cycle(conn)
        except psycopg.OperationalError:
            log.exception("%s loop: database connection problem; retrying", name)
            conn = None
        except Exception:
            log.exception("%s loop: cycle failed", name)
            if conn is not None and not conn.closed:
                conn.rollback()
        stop.wait(settings.tick_seconds)
    if conn is not None:
        conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    settings = SchedulerSettings()
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    bq_settings = BigQuerySettings()
    bq = BigQueryStore(bq_settings) if bq_settings.enabled else None
    with httpx.Client(timeout=settings.http_timeout_seconds) as http:
        # System and traffic jobs run in separate threads so a slow warehouse load never delays traffic.
        loops = [threading.Thread(target=_loop, name=n, args=(n, settings, stop, fn), daemon=True) for n, fn in (
            ("system-jobs", lambda conn: run_system_jobs(conn, bq)),
            ("traffic-jobs", lambda conn: run_traffic_jobs(conn, http, settings)))]
        for t in loops:
            t.start()
        while not stop.is_set():
            stop.wait(1)
        for t in loops:
            t.join(timeout=30)
    log.info("scheduler stopped")


if __name__ == "__main__":
    main()
