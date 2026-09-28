"""Writes every inbound GET/POST (and outbound callback) to integration.api_call_log."""
import asyncio
import logging
import time
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from starlette.requests import Request

from . import server_info

log = logging.getLogger(__name__)

SENSITIVE_HEADERS = {"authorization", "proxy-authorization", "x-api-key", "cookie", "set-cookie"}
UNLOGGED_PATHS = {"/health", "/health/ready", "/docs", "/redoc", "/openapi.json", "/favicon.ico"}

INSERT_SQL = """
INSERT INTO integration.api_call_log (
    request_id, correlation_id, direction, http_method, api_url, endpoint, query_params,
    request_headers, request_body_bytes, response_status_code, response_body, latency_ms,
    client_ip, user_agent, server_hostname, server_ip, instance_id)
VALUES (
    %(request_id)s, %(correlation_id)s, %(direction)s, %(http_method)s, %(api_url)s, %(endpoint)s,
    %(query_params)s, %(request_headers)s, %(request_body_bytes)s, %(response_status_code)s,
    %(response_body)s, %(latency_ms)s, %(client_ip)s::inet, %(user_agent)s, %(server_hostname)s,
    %(server_ip)s::inet, %(instance_id)s)
"""


def mask_headers(headers) -> dict[str, str]:
    return {k: ("***" if k.lower() in SENSITIVE_HEADERS else v) for k, v in headers.items()}


def _as_uuid(value: Any) -> UUID | None:
    try:
        return value if isinstance(value, UUID) else UUID(str(value))
    except (TypeError, ValueError):
        return None


class ApiCallLogger:
    """Fire-and-forget writer so logging never slows down or breaks an API response."""

    def __init__(self, pool: AsyncConnectionPool, instance_id: str):
        self._pool = pool
        self._instance_id = instance_id
        self._tasks: set[asyncio.Task] = set()

    def submit(self, **record: Any) -> None:
        record.setdefault("request_id", None)
        record.setdefault("correlation_id", None)
        record.setdefault("endpoint", None)
        record.setdefault("query_params", None)
        record.setdefault("request_headers", None)
        record.setdefault("request_body_bytes", None)
        record.setdefault("response_body", None)
        record.setdefault("client_ip", None)
        record.setdefault("user_agent", None)
        record["request_id"] = _as_uuid(record["request_id"])
        record["correlation_id"] = _as_uuid(record["correlation_id"])
        for key in ("query_params", "request_headers", "response_body"):
            if record[key] is not None:
                record[key] = Jsonb(record[key])
        record.update(server_hostname=server_info.hostname(), server_ip=server_info.server_ip(),
                      instance_id=self._instance_id)
        task = asyncio.create_task(self._write(record))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _write(self, record: dict) -> None:
        try:
            async with self._pool.connection() as conn:
                await conn.execute(INSERT_SQL, record)
        except Exception:
            log.exception("Failed to write api_call_log for %s %s", record["http_method"], record["api_url"])

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)


async def api_call_log_middleware(request: Request, call_next):
    if request.url.path in UNLOGGED_PATHS:
        return await call_next(request)

    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        path_params = request.scope.get("path_params") or {}
        body_bytes = request.headers.get("content-length")
        request.app.state.api_logger.submit(
            request_id=getattr(request.state, "request_id", None) or path_params.get("request_id"),
            correlation_id=getattr(request.state, "correlation_id", None),
            direction="INBOUND",
            http_method=request.method,
            api_url=str(request.url),
            endpoint=getattr(route, "path", None),
            query_params=dict(request.query_params) or None,
            request_headers=mask_headers(request.headers),
            request_body_bytes=int(body_bytes) if body_bytes and body_bytes.isdigit() else None,
            response_status_code=status_code,
            response_body=getattr(request.state, "error_body", None) or getattr(request.state, "response_body", None),
            latency_ms=int((time.perf_counter() - started) * 1000),
            client_ip=server_info.client_ip(request),
            user_agent=request.headers.get("user-agent"),
        )
