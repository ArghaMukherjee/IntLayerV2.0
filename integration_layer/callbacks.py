"""Delivers final results to the source app's callback_url, for requests stored in
PostgreSQL or BigQuery.

Wakes on NOTIFY request_done (sent by PostgreSQL triggers and by ConsumerApplication after a
BigQuery update) and also polls (covers missed notifications, retries with backoff and IL
restarts). Claims are exclusive on both backends (SKIP LOCKED / claim tokens), so several IL
replicas never deliver the same callback at the same time.
"""
import asyncio
import hashlib
import hmac
import json
import logging
import time

import httpx
import psycopg
from psycopg_pool import AsyncConnectionPool

from .api_log import ApiCallLogger
from .config import Settings
from .stores import bq_call

log = logging.getLogger(__name__)

EVENT_BY_STATUS = {"COMPLETED": "request.completed", "FAILED": "request.failed",
                   "CANCELLED": "request.cancelled"}


def sign(secret: str, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def build_payload(row: dict) -> dict:
    error = None
    if row["error_code"]:
        error = {"code": row["error_code"], "message": row["error_message"]}
    return {
        "event": EVENT_BY_STATUS.get(row["status"], "request.updated"),
        "request_id": str(row["request_id"]),
        "correlation_id": str(row["correlation_id"]),
        "workflow_name": row["workflow_name"],
        "status": row["status"],
        "success_flag": row["success_flag"],
        "workflow_status": row["workflow_status"],
        "output_json": row["output_json"],
        "error": error,
        "storage_backend": row.get("storage_backend", "postgres"),
        "received_at": row["received_at"].isoformat(),
        "completed_at": row["workflow_completed_at"].isoformat() if row["workflow_completed_at"] else None,
        "attempt": row["callback_attempts"],
    }


class CallbackDispatcher:
    def __init__(self, settings: Settings, pool: AsyncConnectionPool, api_logger: ApiCallLogger, bq=None):
        self._settings = settings
        self._pool = pool
        self._bq = bq
        self._last_bq_poll = 0.0
        self._api_logger = api_logger
        self._wake = asyncio.Event()
        self._stopping = False
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self._tasks = [asyncio.create_task(self._run(), name="callback-dispatcher"),
                       asyncio.create_task(self._listen(), name="callback-listener")]

    async def stop(self) -> None:
        self._stopping = True
        self._wake.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _listen(self) -> None:
        while not self._stopping:
            try:
                async with await psycopg.AsyncConnection.connect(
                        self._settings.database_url, autocommit=True) as conn:
                    await conn.execute("LISTEN request_done")
                    self._wake.set()  # catch up on anything finished while disconnected
                    async for note in conn.notifies():
                        if '"bigquery"' in (note.payload or ""):
                            self._last_bq_poll = 0.0  # a BigQuery request finished: check it now
                        self._wake.set()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("LISTEN request_done connection lost; reconnecting in 5s")
                await asyncio.sleep(5)

    async def _run(self) -> None:
        async with httpx.AsyncClient(timeout=self._settings.callback_timeout_seconds) as client:
            while not self._stopping:
                self._wake.clear()
                try:
                    delivered = await self.dispatch_due(client)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("Callback dispatch cycle failed")
                    delivered = 0
                if delivered >= self._settings.callback_batch_size:
                    continue  # more work is probably waiting
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self._settings.callback_poll_seconds)
                except TimeoutError:
                    pass

    async def dispatch_due(self, client: httpx.AsyncClient) -> int:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT * FROM integration.claim_callbacks(%s, %s)",
                (self._settings.callback_batch_size, self._settings.callback_lease_seconds))
            rows = [r | {"storage_backend": "postgres"} for r in await cur.fetchall()]
        if self._bq is not None and time.monotonic() - self._last_bq_poll >= self._settings.bq_callback_poll_seconds:
            self._last_bq_poll = time.monotonic()
            try:
                rows += await bq_call(self._bq, "claim_callbacks", self._settings.callback_batch_size,
                                      self._settings.callback_lease_seconds)
            except Exception:
                log.exception("BigQuery callback claim failed")
        await asyncio.gather(*(self._deliver(client, row) for row in rows))
        return len(rows)

    async def _deliver(self, client: httpx.AsyncClient, row: dict) -> None:
        body = json.dumps(build_payload(row), separators=(",", ":"), default=str).encode()
        timestamp = str(int(time.time()))
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "integration-layer-callback/1.0",
            "X-IL-Request-Id": str(row["request_id"]),
            "X-Correlation-Id": str(row["correlation_id"]),
            "X-IL-Attempt": str(row["callback_attempts"]),
            "X-IL-Timestamp": timestamp,
        }
        if self._settings.callback_signing_secret:
            headers["X-IL-Signature"] = sign(self._settings.callback_signing_secret, timestamp, body)

        started = time.perf_counter()
        status_code, error, response_body = None, None, None
        try:
            response = await client.post(row["callback_url"], content=body, headers=headers)
            status_code = response.status_code
            if not response.is_success:
                error = f"HTTP {status_code}: {response.text[:500]}"
                response_body = {"text": response.text[:2000]}
        except httpx.HTTPError as exc:
            error = f"{type(exc).__name__}: {exc}"

        if row["storage_backend"] == "bigquery":
            new_status = await bq_call(self._bq, "record_callback_result", str(row["request_id"]), error is None,
                                       error, self._settings.callback_max_attempts)
        else:
            async with self._pool.connection() as conn:
                cur = await conn.execute(
                    "SELECT integration.record_callback_result(%s, %s, %s, %s) AS callback_status",
                    (row["request_id"], error is None, error, self._settings.callback_max_attempts))
                new_status = (await cur.fetchone())["callback_status"]

        if error:
            log.warning("Callback %s attempt %s failed (%s): %s",
                        row["request_id"], row["callback_attempts"], new_status, error)
        self._api_logger.submit(
            request_id=row["request_id"], correlation_id=row["correlation_id"], direction="OUTBOUND",
            http_method="POST", api_url=row["callback_url"], request_headers=headers,
            request_body_bytes=len(body), response_status_code=status_code,
            response_body=response_body or ({"error": error} if error else None),
            latency_ms=int((time.perf_counter() - started) * 1000))
