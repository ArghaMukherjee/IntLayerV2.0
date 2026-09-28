from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field

WORKFLOW_NAME_PATTERN = r"^[a-z][a-z0-9_]{1,62}$"


class SubmitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_name: str = Field(pattern=WORKFLOW_NAME_PATTERN, examples=["order_validation"])
    payload: dict[str, Any] = Field(description="Business JSON; validated against the workflow's input_schema")
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200,
                                        description="Resubmitting the same key returns the original request")
    callback_url: AnyHttpUrl | None = Field(default=None, description="Result is POSTed here when processing ends")
    correlation_id: UUID | None = None
    priority: int = Field(default=0, ge=0, le=9)


class SubmitResponse(BaseModel):
    request_id: UUID
    correlation_id: UUID
    status: str
    is_duplicate: bool
    storage_backend: Literal["postgres", "bigquery"]
    status_url: str


class RequestStatus(BaseModel):
    request_id: UUID
    correlation_id: UUID
    idempotency_key: str | None
    source_app: str
    target_app: str
    workflow_name: str
    input_schema_version: int | None
    input_json: dict[str, Any] | None = None
    status: str
    success_flag: bool | None
    workflow_status: str | None
    output_json: dict[str, Any] | None
    error_code: str | None
    error_message: str | None
    retry_count: int
    max_retries: int
    received_at: datetime
    picked_at: datetime | None
    workflow_completed_at: datetime | None
    processing_duration_ms: int | None
    callback_url: str | None
    callback_status: str | None
    callback_attempts: int
    callback_delivered_at: datetime | None
    callback_last_error: str | None
    storage_backend: Literal["postgres", "bigquery"]


class RequestSummary(BaseModel):
    request_id: UUID
    correlation_id: UUID
    storage_backend: Literal["postgres", "bigquery"]
    source_app: str
    workflow_name: str
    status: str
    success_flag: bool | None
    workflow_status: str | None
    error_code: str | None
    retry_count: int
    received_at: datetime
    workflow_completed_at: datetime | None
    input_json: dict[str, Any] | None = None     # only with ?include_payload=true
    output_json: dict[str, Any] | None = None    # only with ?include_payload=true


class WorkflowIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_app: str = Field(min_length=1, max_length=64)
    description: str | None = None
    input_schema: dict[str, Any]
    output_schema: dict[str, Any] | None = None
    max_retries: int = Field(default=3, ge=0, le=20)
    storage_backend: Literal["postgres", "bigquery"] = "postgres"
    is_active: bool = True


class WorkflowOut(BaseModel):
    workflow_name: str
    target_app: str
    description: str | None
    input_schema: dict[str, Any]
    output_schema: dict[str, Any] | None
    schema_version: int
    max_retries: int
    storage_backend: str
    is_active: bool
    updated_at: datetime
