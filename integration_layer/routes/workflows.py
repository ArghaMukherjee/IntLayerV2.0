from fastapi import APIRouter, Depends, HTTPException, Path, Request, Response, status
from jsonschema import SchemaError
from psycopg.types.json import Jsonb

from ..auth import require_admin, require_app
from ..models import WORKFLOW_NAME_PATTERN, WorkflowIn, WorkflowOut
from ..validation import build_validator

router = APIRouter(prefix="/v1/workflows", tags=["workflows"])
admin_router = APIRouter(prefix="/v1/admin/workflows", tags=["admin"])

UPSERT_SQL = """
INSERT INTO integration.workflow_registry AS w
    (workflow_name, target_app, description, input_schema, output_schema, max_retries, storage_backend, is_active)
VALUES (%(workflow_name)s, %(target_app)s, %(description)s, %(input_schema)s, %(output_schema)s,
        %(max_retries)s, %(storage_backend)s, %(is_active)s)
ON CONFLICT (workflow_name) DO UPDATE SET
    target_app     = EXCLUDED.target_app,
    description    = EXCLUDED.description,
    max_retries    = EXCLUDED.max_retries,
    storage_backend = EXCLUDED.storage_backend,
    is_active      = EXCLUDED.is_active,
    input_schema   = EXCLUDED.input_schema,
    output_schema  = EXCLUDED.output_schema,
    schema_version = w.schema_version
        + CASE WHEN w.input_schema  IS DISTINCT FROM EXCLUDED.input_schema
                 OR w.output_schema IS DISTINCT FROM EXCLUDED.output_schema THEN 1 ELSE 0 END
RETURNING *, (xmax = 0) AS created
"""


@router.get("", response_model=list[WorkflowOut])
async def list_workflows(request: Request, _: str = Depends(require_app)):
    async with request.app.state.pool.connection() as conn:
        return await (await conn.execute(
            "SELECT * FROM integration.workflow_registry WHERE is_active ORDER BY workflow_name")).fetchall()


@router.get("/{workflow_name}", response_model=WorkflowOut)
async def get_workflow(workflow_name: str, request: Request, _: str = Depends(require_app)):
    async with request.app.state.pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT * FROM integration.workflow_registry WHERE workflow_name = %s", (workflow_name,))).fetchone()
    if row is None:
        raise HTTPException(404, detail={"code": "NOT_FOUND", "message": f"Workflow '{workflow_name}' not found"})
    return row


@admin_router.put("/{workflow_name}", response_model=WorkflowOut,
                  responses={201: {"description": "Workflow created"}})
async def upsert_workflow(body: WorkflowIn, request: Request, response: Response,
                          workflow_name: str = Path(pattern=WORKFLOW_NAME_PATTERN),
                          _: str = Depends(require_admin)):
    for field in ("input_schema", "output_schema"):
        schema = getattr(body, field)
        if schema is None:
            continue
        try:
            build_validator(schema)
        except SchemaError as exc:
            raise HTTPException(422, detail={"code": "INVALID_JSON_SCHEMA",
                                             "message": f"{field} is not a valid JSON Schema: {exc.message}"})

    params = body.model_dump()
    params["workflow_name"] = workflow_name
    params["input_schema"] = Jsonb(body.input_schema)
    params["output_schema"] = Jsonb(body.output_schema) if body.output_schema is not None else None
    async with request.app.state.pool.connection() as conn:
        row = await (await conn.execute(UPSERT_SQL, params)).fetchone()
    request.app.state.catalog.invalidate(workflow_name)
    if row["created"]:
        response.status_code = status.HTTP_201_CREATED
    return row
