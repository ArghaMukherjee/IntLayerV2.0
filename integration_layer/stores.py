"""Access to the two data planes: PostgreSQL (SQL functions) and BigQuery (ilcommon.bigquery_store)."""
import asyncio
import json
import logging
from typing import Any

from ilcommon.bigquery_store import BigQuerySettings, BigQueryStore

log = logging.getLogger(__name__)


def create_bigquery_store() -> BigQueryStore | None:
    settings = BigQuerySettings()
    if not settings.enabled:
        log.info("BigQuery backend disabled (BQ_ENABLED=false)")
        return None
    try:
        store = BigQueryStore(settings)
    except Exception:
        # e.g. no Google credentials: keep serving PostgreSQL workflows; BigQuery ones get 503
        log.exception("BigQuery client could not be created; BigQuery workflows will return 503")
        return None
    log.info("BigQuery backend: %s", store.label)
    return store


async def bq_call(store: BigQueryStore, method: str, *args, **kwargs) -> Any:
    """Runs a (blocking) BigQueryStore method in a worker thread."""
    return await asyncio.to_thread(getattr(store, method), *args, **kwargs)


async def notify(pool, channel: str, payload: dict) -> None:
    """Wakes listeners (ConsumerApplication, callback dispatcher) through PostgreSQL NOTIFY."""
    try:
        async with pool.connection() as conn:
            await conn.execute("SELECT pg_notify(%s, %s)", (channel, json.dumps(payload, default=str)))
    except Exception:
        log.exception("pg_notify(%s) failed", channel)
