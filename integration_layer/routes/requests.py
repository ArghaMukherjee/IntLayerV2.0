"""Request API. Each workflow's storage_backend decides where a new request is stored and queued:
PostgreSQL (SQL functions in the integration schema) or BigQuery (ilcommon.bigquery_store).
Look-ups, listing, cancel and replay work across both."""
import uuid
from datetime import date, datetime, timezone
from urllib.parse import urlsplit
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from psycopg.types.json import Jsonb

from .. import server_info
from ..api_log import mask_headers
from ..auth import require_admin, require_app
from ..models import RequestStatus, RequestSummary, SubmitRequest, SubmitResponse
from ..stores import bq_call, notify

router = APIRouter(prefix="/v1/requests", tags=["requests"])
admin_router = APIRouter(prefix="/v1/admin/requests", tags=["admin"])

SUBMIT_SQL = """
SELECT * FROM integration.submit_request(
    p_source_app           => %(source_app)s,
    p_workflow_name        => %(workflow_name)s,
    p_input_json           => %(input_json)s,
    p_input_schema_version => %(schema_version)s::integer,
    p_api_url              => %(api_url)s,
    p_http_method          => %(http_method)s,
    p_request_headers      => %(request_headers)s,
    p_client_ip            => %(client_ip)s::inet,
    p_il_server_hostname   => %(hostname)s,
    p_il_server_ip         => %(server_ip)s::inet,
    p_il_instance_id       => %(instance_id)s,
    p_idempotency_key      => %(idempotency_key)s,
    p_correlation_id       => %(correlation_id)s::uuid,
    p_callback_url         => %(callback_url)s,
    p_priority             => %(priority)s::smallint)
"""
STATUSES = "^(PENDING|IN_PROGRESS|COMPLETED|FAILED|CANCELLED)$"


def _error(code: int, error_code: str, message: str, **extra) -> HTTPException:
    return HTTPException(code, detail={"code": error_code, "message": message, **extra})


def _bq(request: Request):
    store = request.app.state.bq
    if store is None:
        raise _error(503, "BACKEND_UNAVAILABLE", "The BigQuery backend is not enabled on this Integration Layer")
    return store


def _check_callback_host(request: Request, url: str) -> None:
    allowed = request.app.state.settings.allowed_callback_hosts()
    host = (urlsplit(url).hostname or "").lower()
    if allowed and host not in allowed:
        raise _error(422, "CALLBACK_HOST_NOT_ALLOWED", f"callback_url host '{host}' is not allowed")


async def _find(request: Request, request_id: UUID, source_app: str | None) -> dict | None:
    """Finds a request in PostgreSQL, else (via request_routes) in BigQuery. source_app=None: any owner."""
    async with request.app.state.pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT * FROM integration.v_request_status WHERE request_id = %s AND (%s::text IS NULL OR source_app = %s)",
            (request_id, source_app, source_app))).fetchone()
        if row:
            return row | {"storage_backend": "postgres"}
        route = await (await conn.execute(
            "SELECT storage_backend FROM integration.request_routes WHERE request_id = %s"
            " AND (%s::text IS NULL OR source_app = %s)", (request_id, source_app, source_app))).fetchone()
    if route and route["storage_backend"] == "bigquery":
        return await bq_call(_bq(request), "get", str(request_id))
    return None


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=SubmitResponse,
             responses={200: {"description": "Duplicate idempotency_key: original request returned"},
                        413: {"description": "Payload too large"},
                        422: {"description": "Payload failed JSON Schema validation"},
                        503: {"description": "The workflow's storage backend is unavailable"}})
async def submit_request(body: SubmitRequest, request: Request, response: Response,
                         source_app: str = Depends(require_app)):
    """Validates the payload against the workflow's input schema and stores it in the workflow's backend."""
    settings = request.app.state.settings
    size = request.headers.get("content-length")
    if size and size.isdigit() and int(size) > settings.max_payload_bytes:
        raise _error(413, "PAYLOAD_TOO_LARGE", f"Request body exceeds {settings.max_payload_bytes} bytes")

    workflow = await request.app.state.catalog.get(body.workflow_name)
    if workflow is None or not workflow.is_active:
        raise _error(422, "UNKNOWN_WORKFLOW", f"Workflow '{body.workflow_name}' is not registered or inactive")
    errors = workflow.validate_input(body.payload)
    if errors:
        raise _error(422, "INPUT_SCHEMA_INVALID", "payload does not match the workflow input schema",
                     workflow_name=workflow.workflow_name, schema_version=workflow.schema_version, errors=errors)
    callback_url = str(body.callback_url) if body.callback_url else None
    if callback_url:
        _check_callback_host(request, callback_url)

    meta = {
        "api_url": str(request.url), "http_method": request.method,
        "request_headers": mask_headers(request.headers), "client_ip": server_info.client_ip(request),
        "hostname": server_info.hostname(), "server_ip": server_info.server_ip(), "instance_id": settings.instance_id,
    }
    if workflow.storage_backend == "bigquery":
        result = await _submit_bigquery(request, body, workflow, source_app, callback_url, meta)
    else:
        result = await _submit_postgres(request, body, workflow, source_app, callback_url, meta)

    request.state.request_id = result.request_id
    request.state.correlation_id = result.correlation_id
    response.headers["Location"] = result.status_url
    if result.is_duplicate:
        response.status_code = status.HTTP_200_OK
    request.state.response_body = result.model_dump(mode="json")  # recorded in api_call_log
    return result


def _response(request_id, correlation_id, status_, duplicate, backend) -> SubmitResponse:
    return SubmitResponse(request_id=request_id, correlation_id=correlation_id, status=status_, is_duplicate=duplicate,
                          storage_backend=backend, status_url=f"/v1/requests/{request_id}")


async def _submit_postgres(request, body, workflow, source_app, callback_url, meta) -> SubmitResponse:
    params = {"source_app": source_app, "workflow_name": workflow.workflow_name, "input_json": Jsonb(body.payload),
              "schema_version": workflow.schema_version, "idempotency_key": body.idempotency_key,
              "correlation_id": body.correlation_id, "callback_url": callback_url, "priority": body.priority,
              **meta, "request_headers": Jsonb(meta["request_headers"])}
    try:
        async with request.app.state.pool.connection() as conn:
            row = await (await conn.execute(SUBMIT_SQL, params)).fetchone()
    except psycopg.errors.NoDataFound:  # workflow deactivated after it was cached
        request.app.state.catalog.invalidate(workflow.workflow_name)
        raise _error(422, "UNKNOWN_WORKFLOW", f"Workflow '{workflow.workflow_name}' is not registered or inactive")
    if row["is_duplicate"] and row["storage_backend"] == "bigquery":
        original = await bq_call(_bq(request), "get", str(row["request_id"]))
        return _response(row["request_id"], original["correlation_id"], original["status"], True, "bigquery")
    return _response(row["request_id"], row["correlation_id"], row["status"], row["is_duplicate"], row["storage_backend"])


async def _submit_bigquery(request, body, workflow, source_app, callback_url, meta) -> SubmitResponse:
    store = _bq(request)
    new_id = uuid.uuid4()
    async with request.app.state.pool.connection() as conn:
        reg = await (await conn.execute("SELECT * FROM integration.register_bq_request(%s, %s, %s, %s)",
                                        (new_id, source_app, workflow.workflow_name, body.idempotency_key))).fetchone()
    if reg["is_duplicate"]:
        original = await _find(request, reg["request_id"], source_app)
        return _response(original["request_id"], original["correlation_id"], original["status"], True,
                         original["storage_backend"])
    fields = {
        "request_id": str(new_id), "request_date": datetime.now(timezone.utc).date(),
        "correlation_id": str(body.correlation_id or uuid.uuid4()), "idempotency_key": body.idempotency_key,
        "source_app": source_app, "target_app": workflow.target_app, "workflow_name": workflow.workflow_name,
        "priority": body.priority, "input_json": body.payload, "input_schema_version": workflow.schema_version,
        "api_url": meta["api_url"], "http_method": meta["http_method"], "request_headers": meta["request_headers"],
        "callback_url": callback_url, "client_ip": meta["client_ip"], "il_server_hostname": meta["hostname"],
        "il_server_ip": meta["server_ip"], "il_instance_id": meta["instance_id"], "max_retries": workflow.max_retries,
    }
    try:
        row = await bq_call(store, "submit", fields)
    except Exception as exc:
        async with request.app.state.pool.connection() as conn:
            await conn.execute("SELECT integration.unregister_bq_request(%s)", (new_id,))
        raise _error(503, "BACKEND_UNAVAILABLE", f"BigQuery insert failed: {type(exc).__name__}")
    await notify(request.app.state.pool, "request_new",
                 {"request_id": str(new_id), "target_app": workflow.target_app, "storage_backend": "bigquery"})
    return _response(new_id, fields["correlation_id"], "PENDING", False, "bigquery")


@router.get("/{request_id}", response_model=RequestStatus)
async def get_request(request_id: UUID, request: Request,
                      include_input: bool = Query(False, description="Also return input_json"),
                      source_app: str = Depends(require_app)):
    """Status, result and callback state of one of the caller's requests (polling)."""
    row = await _find(request, request_id, source_app)
    if row is None:
        raise _error(404, "NOT_FOUND", f"Request {request_id} not found")
    request.state.correlation_id = row["correlation_id"]
    if not include_input:
        row["input_json"] = None
    return row


@router.get("", response_model=list[RequestSummary])
async def list_requests(request: Request,
                        status_filter: str | None = Query(None, alias="status", pattern=STATUSES),
                        workflow_name: str | None = None,
                        storage_backend: str | None = Query(None, pattern="^(postgres|bigquery)$"),
                        from_date: date | None = None,
                        to_date: date | None = None,
                        include_payload: bool = Query(False, description="Include input_json and output_json"),
                        limit: int = Query(50, ge=1, le=200),
                        source_app: str = Depends(require_app)):
    """The caller's requests from both backends, newest first."""
    rows: list[dict] = []
    if storage_backend in (None, "postgres"):
        sql = ["SELECT * FROM integration.v_request_status WHERE source_app = %(source_app)s"]
        params = {"source_app": source_app, "limit": limit}
        for cond, key, value in (("status = %(status)s::integration.request_status", "status", status_filter),
                                 ("workflow_name = %(workflow_name)s", "workflow_name", workflow_name),
                                 ("request_date >= %(from_date)s", "from_date", from_date),
                                 ("request_date <= %(to_date)s", "to_date", to_date)):
            if value:
                sql.append("AND " + cond)
                params[key] = value
        sql.append("ORDER BY received_at DESC LIMIT %(limit)s")
        async with request.app.state.pool.connection() as conn:
            rows += [r | {"storage_backend": "postgres"} for r in await (await conn.execute(" ".join(sql), params)).fetchall()]
    if storage_backend in (None, "bigquery") and request.app.state.bq is not None:
        since = datetime.combine(from_date, datetime.min.time(), timezone.utc) if from_date else None
        bq_rows = await bq_call(request.app.state.bq, "list", source_app=source_app, workflow=workflow_name,
                                status=status_filter, since=since, limit=limit)
        rows += [r for r in bq_rows if not to_date or r["request_date"] <= to_date]
    rows.sort(key=lambda r: r["received_at"], reverse=True)
    rows = rows[:limit]
    if not include_payload:
        for r in rows:
            r["input_json"] = r["output_json"] = None
    return rows


@router.post("/{request_id}/cancel", response_model=RequestStatus)
async def cancel_request(request_id: UUID, request: Request, reason: str | None = None,
                         source_app: str = Depends(require_app)):
    """Cancels a request that ConsumerApplication has not picked up yet."""
    row = await _find(request, request_id, source_app)
    if row is None:
        raise _error(404, "NOT_FOUND", f"Request {request_id} not found")
    if row["storage_backend"] == "bigquery":
        ok = await bq_call(_bq(request), "cancel", str(request_id), reason)
    else:
        async with request.app.state.pool.connection() as conn:
            ok = (await (await conn.execute("SELECT integration.cancel_request(%s, %s) AS ok",
                                            (request_id, reason))).fetchone())["ok"]
    if not ok:
        raise _error(409, "NOT_CANCELLABLE", "Only PENDING requests can be cancelled")
    if row["storage_backend"] == "bigquery":
        await notify(request.app.state.pool, "request_done", {"request_id": str(request_id), "storage_backend": "bigquery"})
    row = await _find(request, request_id, source_app)
    row["input_json"] = None
    return row


@admin_router.post("/{request_id}/replay", response_model=RequestStatus)
async def replay_request(request_id: UUID, request: Request, _: str = Depends(require_admin)):
    """Re-queues a FAILED request (any source app)."""
    row = await _find(request, request_id, None)
    if row is None:
        raise _error(404, "NOT_FOUND", f"Request {request_id} not found")
    if row["storage_backend"] == "bigquery":
        ok = await bq_call(_bq(request), "replay", str(request_id))
    else:
        async with request.app.state.pool.connection() as conn:
            ok = (await (await conn.execute("SELECT integration.replay_request(%s) AS ok", (request_id,))).fetchone())["ok"]
    if not ok:
        raise _error(409, "NOT_REPLAYABLE", "Only FAILED requests can be replayed")
    await notify(request.app.state.pool, "request_new", {"request_id": str(request_id), "storage_backend": row["storage_backend"]})
    row = await _find(request, request_id, None)
    row["input_json"] = None
    return row
