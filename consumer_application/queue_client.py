"""ConsumerApplication's queue clients: PostgreSQL (four SQL functions) and BigQuery (ilcommon store).

Both expose the same interface (claim / heartbeat / complete / fail / output_validator) so one
Worker class drives either queue. Output schemas always come from integration.workflow_registry
in PostgreSQL, the control plane.
"""
import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import date
from uuid import UUID

from jsonschema import Draft202012Validator, FormatChecker
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from ilcommon.bigquery_store import BigQueryStore

log = logging.getLogger(__name__)


@dataclass
class Job:
    request_id: UUID
    request_date: date
    correlation_id: UUID
    workflow_name: str
    input_json: dict
    retry_count: int


class SchemaCache:
    """Output JSON Schemas per workflow, cached for a short TTL."""

    def __init__(self, pool: AsyncConnectionPool, ttl_seconds: float = 60):
        self._pool = pool
        self._ttl = ttl_seconds
        self._schemas: dict[str, tuple[float, Draft202012Validator | None]] = {}

    async def validator(self, workflow_name: str) -> Draft202012Validator | None:
        cached = self._schemas.get(workflow_name)
        if cached and time.monotonic() - cached[0] < self._ttl:
            return cached[1]
        async with self._pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT output_schema FROM integration.workflow_registry WHERE workflow_name = %s",
                (workflow_name,))).fetchone()
        schema = row["output_schema"] if row else None
        validator = Draft202012Validator(schema, format_checker=FormatChecker()) if schema else None
        self._schemas[workflow_name] = (time.monotonic(), validator)
        return validator


class PostgresQueueClient:
    backend = "postgres"

    def __init__(self, pool: AsyncConnectionPool, worker_id: str, worker_host: str,
                 target_app: str, lease_seconds: int, schemas: SchemaCache | None = None):
        self._pool = pool
        self.worker_id = worker_id
        self._worker_host = worker_host
        self._target_app = target_app
        self._lease = lease_seconds
        self._schemas = schemas or SchemaCache(pool)

    async def claim(self, limit: int) -> list[Job]:
        async with self._pool.connection() as conn:
            rows = await (await conn.execute(
                "SELECT * FROM integration.claim_requests(%s, %s, %s, %s, %s)",
                (self.worker_id, self._worker_host, self._target_app, limit, self._lease))).fetchall()
        return [Job(**row) for row in rows]

    async def heartbeat(self, job: Job) -> bool:
        async with self._pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT integration.heartbeat(%s, %s, %s) AS ok",
                (job.request_id, self.worker_id, self._lease))).fetchone()
        return bool(row["ok"])

    async def complete(self, job: Job, output: dict, workflow_status: str) -> bool:
        async with self._pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT integration.complete_request(%s, %s, %s, %s) AS ok",
                (job.request_id, self.worker_id, Jsonb(output), workflow_status))).fetchone()
        return bool(row["ok"])

    async def fail(self, job: Job, code: str, message: str, retryable: bool,
                   output: dict | None = None, workflow_status: str = "FAILED") -> str | None:
        async with self._pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT integration.fail_request(%s, %s, %s, %s, %s, %s, %s) AS status",
                (job.request_id, self.worker_id, code, message[:4000], retryable,
                 Jsonb(output) if output is not None else None, workflow_status))).fetchone()
        return row["status"]

    async def output_validator(self, workflow_name: str) -> Draft202012Validator | None:
        return await self._schemas.validator(workflow_name)


class BigQueryQueueClient:
    """Same interface, backed by BigQuery. After a final state it sends NOTIFY request_done through
    PostgreSQL so the Integration Layer delivers the callback without waiting for its poll."""

    backend = "bigquery"

    def __init__(self, store: BigQueryStore, pool: AsyncConnectionPool, worker_id: str, worker_host: str,
                 target_app: str, lease_seconds: int, schemas: SchemaCache | None = None):
        self._store = store
        self._pool = pool
        self.worker_id = worker_id
        self._worker_host = worker_host
        self._target_app = target_app
        self._lease = lease_seconds
        self._schemas = schemas or SchemaCache(pool)

    async def _run(self, method: str, *args):
        return await asyncio.to_thread(getattr(self._store, method), *args)

    async def _notify_done(self, job: Job, status: str) -> None:
        try:
            async with self._pool.connection() as conn:
                await conn.execute("SELECT pg_notify('request_done', %s)", (json.dumps(
                    {"request_id": str(job.request_id), "status": status, "storage_backend": "bigquery"}),))
        except Exception:
            log.exception("could not send request_done notification")

    async def claim(self, limit: int) -> list[Job]:
        rows = await self._run("claim", self.worker_id, self._worker_host, self._target_app, limit, self._lease)
        return [Job(request_id=UUID(r["request_id"]), request_date=r["request_date"],
                    correlation_id=UUID(r["correlation_id"]), workflow_name=r["workflow_name"],
                    input_json=r["input_json"], retry_count=r["retry_count"]) for r in rows]

    async def heartbeat(self, job: Job) -> bool:
        return await self._run("heartbeat", str(job.request_id), self.worker_id, self._lease)

    async def complete(self, job: Job, output: dict, workflow_status: str) -> bool:
        ok = await self._run("complete", str(job.request_id), self.worker_id, output, workflow_status)
        if ok:
            await self._notify_done(job, "COMPLETED")
        return ok

    async def fail(self, job: Job, code: str, message: str, retryable: bool,
                   output: dict | None = None, workflow_status: str = "FAILED") -> str | None:
        status = await self._run("fail", str(job.request_id), self.worker_id, code, message, retryable, output,
                                 workflow_status)
        if status == "FAILED":
            await self._notify_done(job, status)
        return status

    async def output_validator(self, workflow_name: str) -> Draft202012Validator | None:
        return await self._schemas.validator(workflow_name)
