"""JSON Schema validation of request payloads against integration.workflow_registry."""
import time
from dataclasses import dataclass, field
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from psycopg_pool import AsyncConnectionPool

MAX_REPORTED_ERRORS = 50


def json_path(path) -> str:
    return "$" + "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in path)


def validate_against(validator: Draft202012Validator, instance: Any) -> list[dict]:
    """Return a list of {path, message, rule} dicts; empty when valid."""
    errors = sorted(validator.iter_errors(instance), key=lambda e: list(map(str, e.absolute_path)))
    return [
        {"path": json_path(e.absolute_path), "message": e.message, "rule": e.validator}
        for e in errors[:MAX_REPORTED_ERRORS]
    ]


def build_validator(schema: dict) -> Draft202012Validator:
    """Raises jsonschema.SchemaError if the schema itself is invalid."""
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


@dataclass
class Workflow:
    workflow_name: str
    target_app: str
    description: str | None
    input_schema: dict
    output_schema: dict | None
    schema_version: int
    max_retries: int
    is_active: bool
    storage_backend: str
    input_validator: Draft202012Validator = field(repr=False)

    def validate_input(self, payload: Any) -> list[dict]:
        return validate_against(self.input_validator, payload)


class WorkflowCatalog:
    """Caches workflow definitions for a short TTL so every request doesn't hit the registry."""

    def __init__(self, pool: AsyncConnectionPool, ttl_seconds: float):
        self._pool = pool
        self._ttl = ttl_seconds
        self._cache: dict[str, tuple[float, Workflow | None]] = {}

    async def get(self, name: str) -> Workflow | None:
        cached = self._cache.get(name)
        if cached and time.monotonic() - cached[0] < self._ttl:
            return cached[1]
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT * FROM integration.workflow_registry WHERE workflow_name = %s", (name,))
            row = await cur.fetchone()
        wf = None
        if row:
            wf = Workflow(
                workflow_name=row["workflow_name"], target_app=row["target_app"],
                description=row["description"], input_schema=row["input_schema"],
                output_schema=row["output_schema"], schema_version=row["schema_version"],
                max_retries=row["max_retries"], is_active=row["is_active"],
                storage_backend=row["storage_backend"],
                input_validator=build_validator(row["input_schema"]))
        self._cache[name] = (time.monotonic(), wf)
        return wf

    def invalidate(self, name: str) -> None:
        self._cache.pop(name, None)
