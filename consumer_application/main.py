"""ConsumerApplication: processes integration requests from the PostgreSQL and BigQuery queues."""
import asyncio
import logging
from collections import Counter
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from ilcommon.bigquery_store import BigQuerySettings, BigQueryStore

from .config import Settings
from .processors import HANDLERS
from .queue_client import BigQueryQueueClient, PostgresQueueClient, SchemaCache
from .worker import Worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("consumer")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = AsyncConnectionPool(settings.database_url, min_size=1, max_size=settings.db_pool_max,
                                   open=False, kwargs={"autocommit": True, "row_factory": dict_row})
        await pool.open(wait=True, timeout=30)
        schemas = SchemaCache(pool, settings.schema_cache_seconds)
        workers: dict[str, Worker] = {}
        backends = settings.backend_list()
        if "postgres" in backends:
            client = PostgresQueueClient(pool, settings.worker_id, settings.worker_host, settings.target_app,
                                         settings.lease_seconds, schemas)
            workers["postgres"] = Worker(client, settings.database_url, settings.max_concurrency,
                                         settings.lease_seconds, settings.poll_seconds)
        bq_settings = BigQuerySettings()
        app.state.bq = None
        if "bigquery" in backends and bq_settings.enabled:
            app.state.bq = BigQueryStore(bq_settings)
            client = BigQueryQueueClient(app.state.bq, pool, settings.worker_id, settings.worker_host,
                                         settings.target_app, settings.lease_seconds, schemas)
            workers["bigquery"] = Worker(client, settings.database_url, settings.max_concurrency,
                                         settings.lease_seconds, settings.bq_poll_seconds)
        app.state.pool, app.state.workers = pool, workers
        if settings.worker_enabled:
            for w in workers.values():
                await w.start()
        try:
            yield
        finally:
            await asyncio.gather(*(w.stop(timeout=settings.lease_seconds) for w in workers.values()))
            await pool.close()

    app = FastAPI(title="ConsumerApplication", version="2.0.0", lifespan=lifespan,
                  description="Claims requests from the PostgreSQL and BigQuery queues, runs the workflow handler, "
                              "validates the output against the workflow's output schema and reports the result.")
    app.state.settings = settings

    def workers(request: Request) -> dict[str, Worker]:
        return request.app.state.workers

    @app.get("/health", tags=["health"])
    async def liveness(request: Request):
        return {"status": "ok", "worker_id": settings.worker_id,
                "workers": {b: w.running for b, w in workers(request).items()}}

    @app.get("/health/ready", tags=["health"])
    async def readiness(request: Request):
        """PostgreSQL must be reachable; BigQuery is reported."""
        try:
            async with request.app.state.pool.connection(timeout=3) as conn:
                await conn.execute("SELECT 1")
        except Exception as exc:
            return JSONResponse(status_code=503, content={"status": "unavailable", "database": str(exc)})
        bigquery = None
        if request.app.state.bq is not None:
            try:
                bigquery = await asyncio.wait_for(asyncio.to_thread(request.app.state.bq.cached_ping), timeout=3)
            except Exception as exc:
                bigquery = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:300]}
        return {"status": "ready", "database": "ok", "bigquery": bigquery}

    @app.get("/stats", tags=["worker"])
    async def stats(request: Request):
        """Totals across backends, plus per-backend detail."""
        ws = workers(request)
        total = Counter()
        for w in ws.values():
            total.update(w.stats)
        return {
            "worker_id": settings.worker_id,
            "running": any(w.running for w in ws.values()),
            "paused": bool(ws) and all(w.paused for w in ws.values()),
            "inflight": sum(w.inflight for w in ws.values()),
            "max_concurrency": settings.max_concurrency * len(ws),
            "counters": dict(total),
            "workflows": sorted(HANDLERS),
            "backends": {b: {"running": w.running, "paused": w.paused, "inflight": w.inflight,
                             "started_at": w.started_at, "counters": dict(w.stats)} for b, w in ws.items()},
        }

    @app.post("/worker/pause", tags=["worker"])
    async def pause(request: Request, backend: str | None = None):
        """Stop claiming new requests (all backends, or ?backend=postgres|bigquery). In-flight work finishes."""
        for name, w in workers(request).items():
            if backend in (None, name):
                w.pause()
        return {"paused": True, "backend": backend or "all"}

    @app.post("/worker/resume", tags=["worker"])
    async def resume(request: Request, backend: str | None = None):
        for name, w in workers(request).items():
            if backend in (None, name):
                w.resume()
        return {"paused": False, "backend": backend or "all"}

    return app


app = create_app()
