"""Pulls requests from one queue (PostgreSQL or BigQuery), runs the matching handler and reports the result."""
import asyncio
import logging
from collections import Counter
from datetime import datetime, timezone

import psycopg

from .processors import HANDLERS, Handler, PermanentError, RetryableError
from .queue_client import BigQueryQueueClient, Job, PostgresQueueClient

log = logging.getLogger(__name__)


def _json_path(path) -> str:
    return "$" + "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in path)


class Worker:
    def __init__(self, client: PostgresQueueClient | BigQueryQueueClient, database_url: str, max_concurrency: int,
                 lease_seconds: int, poll_seconds: float, handlers: dict[str, Handler] | None = None):
        self._client = client
        self._database_url = database_url
        self._max_concurrency = max_concurrency
        self._heartbeat_every = max(lease_seconds / 3, 1)
        self._poll_seconds = poll_seconds
        self._handlers = HANDLERS if handlers is None else handlers
        self._wake = asyncio.Event()
        self._stopping = False
        self._inflight: set[asyncio.Task] = set()
        self._tasks: list[asyncio.Task] = []
        self.stats: Counter = Counter()
        self.started_at: datetime | None = None
        self.paused = False
        self.backend = getattr(client, "backend", "postgres")

    def pause(self) -> None:
        """Stop claiming new requests; in-flight requests still finish."""
        self.paused = True
        log.info("%s worker %s paused", self.backend, self._client.worker_id)

    def resume(self) -> None:
        self.paused = False
        self._wake.set()
        log.info("%s worker %s resumed", self.backend, self._client.worker_id)

    @property
    def running(self) -> bool:
        return bool(self._tasks) and not self._stopping

    @property
    def inflight(self) -> int:
        return len(self._inflight)

    async def start(self) -> None:
        self.started_at = datetime.now(timezone.utc)
        self._tasks = [asyncio.create_task(self._run(), name="app2-worker"),
                       asyncio.create_task(self._listen(), name="app2-listener")]
        log.info("%s worker %s started", self.backend, self._client.worker_id)

    async def stop(self, timeout: float = 60) -> None:
        """Stop claiming new work and let in-flight requests finish."""
        self._stopping = True
        self._wake.set()
        if self._inflight:
            await asyncio.wait(self._inflight, timeout=timeout)
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        log.info("%s worker %s stopped", self.backend, self._client.worker_id)

    async def _listen(self) -> None:
        while not self._stopping:
            try:
                async with await psycopg.AsyncConnection.connect(self._database_url, autocommit=True) as conn:
                    await conn.execute("LISTEN request_new")
                    self._wake.set()
                    async for _ in conn.notifies():
                        self._wake.set()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("LISTEN request_new connection lost; reconnecting in 5s")
                await asyncio.sleep(5)

    async def _run(self) -> None:
        while not self._stopping:
            self._wake.clear()
            free = 0 if self.paused else self._max_concurrency - len(self._inflight)
            claimed = 0
            if free > 0:
                try:
                    jobs = await self._client.claim(free)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("claim failed")
                    jobs = []
                claimed = len(jobs)
                for job in jobs:
                    task = asyncio.create_task(self._process(job))
                    self._inflight.add(task)
                    task.add_done_callback(self._on_done)
            if claimed and claimed == free:
                continue  # queue may have more; loop straight away once a slot frees up
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._poll_seconds)
            except TimeoutError:
                pass

    def _on_done(self, task: asyncio.Task) -> None:
        self._inflight.discard(task)
        self._wake.set()  # a slot is free

    async def _heartbeat(self, job: Job) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_every)
            try:
                if not await self._client.heartbeat(job):
                    log.warning("request %s: lease lost during processing", job.request_id)
                    return
            except Exception:
                log.exception("request %s: heartbeat failed", job.request_id)

    async def _process(self, job: Job) -> None:
        self.stats["claimed"] += 1
        log.info("[%s] request %s (%s) claimed, attempt %d", self.backend, job.request_id, job.workflow_name, job.retry_count + 1)
        heartbeat = asyncio.create_task(self._heartbeat(job))
        try:
            await self._execute(job)
        except Exception:
            # Reporting itself failed (e.g. DB down); the reaper will re-queue after the lease expires.
            self.stats["report_errors"] += 1
            log.exception("request %s: could not report result", job.request_id)
        finally:
            heartbeat.cancel()

    async def _execute(self, job: Job) -> None:
        handler = self._handlers.get(job.workflow_name)
        if handler is None:
            await self._fail(job, "NO_HANDLER", f"App2 has no handler for workflow '{job.workflow_name}'", False)
            return
        try:
            result = await handler(job.input_json)
        except PermanentError as exc:
            await self._fail(job, exc.code, str(exc), False)
            return
        except RetryableError as exc:
            await self._fail(job, exc.code, str(exc), True)
            return
        except Exception as exc:
            await self._fail(job, "UNHANDLED_ERROR", f"{type(exc).__name__}: {exc}", True)
            return

        validator = await self._client.output_validator(job.workflow_name)
        if validator is not None:
            errors = [f"{_json_path(e.absolute_path)}: {e.message}" for e in validator.iter_errors(result.output)]
            if errors:
                await self._fail(job, "OUTPUT_SCHEMA_INVALID",
                                 "Output does not match output_schema: " + "; ".join(errors[:20]),
                                 False, output=result.output)
                return

        if await self._client.complete(job, result.output, result.workflow_status):
            self.stats["completed"] += 1
            log.info("request %s completed (%s)", job.request_id, result.workflow_status)
        else:
            self.stats["lease_lost"] += 1
            log.warning("request %s: result discarded, lease no longer held", job.request_id)

    async def _fail(self, job: Job, code: str, message: str, retryable: bool, output: dict | None = None) -> None:
        new_status = await self._client.fail(job, code, message, retryable, output=output)
        if new_status is None:
            self.stats["lease_lost"] += 1
            log.warning("request %s: failure not recorded, lease no longer held", job.request_id)
        elif new_status == "PENDING":
            self.stats["retried"] += 1
            log.warning("request %s: %s (%s), will retry", job.request_id, code, message)
        else:
            self.stats["failed"] += 1
            log.error("request %s: %s (%s), failed permanently", job.request_id, code, message)
