import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from .api_log import ApiCallLogger, api_call_log_middleware
from .callbacks import CallbackDispatcher
from .config import Settings
from .db import create_pool
from .routes import health, requests, workflows
from .stores import create_bigquery_store
from .validation import WorkflowCatalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = create_pool(settings)
        await pool.open(wait=True, timeout=30)
        app.state.pool = pool
        app.state.catalog = WorkflowCatalog(pool, settings.schema_cache_seconds)
        app.state.api_logger = ApiCallLogger(pool, settings.instance_id)
        app.state.bq = create_bigquery_store()
        dispatcher = None
        if settings.callbacks_enabled:
            dispatcher = CallbackDispatcher(settings, pool, app.state.api_logger, app.state.bq)
            await dispatcher.start()
        try:
            yield
        finally:
            if dispatcher:
                await dispatcher.stop()
            await app.state.api_logger.drain()
            await pool.close()

    app = FastAPI(
        title="Integration Layer",
        version="2.0.0",
        description="Accepts requests from source applications (WWMG_Source, MyCareHub), validates them against the "
                    "workflow's JSON Schema, stores them in the workflow's backend (PostgreSQL or BigQuery) for "
                    "ConsumerApplication, and returns results by polling or signed callback.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.add_middleware(BaseHTTPMiddleware, dispatch=api_call_log_middleware)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException):
        detail = exc.detail if isinstance(exc.detail, dict) else {"code": "HTTP_ERROR", "message": exc.detail}
        request.state.error_body = detail
        return JSONResponse(status_code=exc.status_code, content={"error": detail}, headers=exc.headers)

    @app.exception_handler(RequestValidationError)
    async def request_invalid(request: Request, exc: RequestValidationError):
        errors = [{"path": ".".join(str(p) for p in e["loc"]), "message": e["msg"], "rule": e["type"]}
                  for e in exc.errors()]
        detail = {"code": "REQUEST_INVALID", "message": "Request envelope is invalid",
                  "errors": jsonable_encoder(errors)}
        request.state.error_body = detail
        return JSONResponse(status_code=422, content={"error": detail})

    app.include_router(health.router)
    app.include_router(requests.router)
    app.include_router(workflows.router)
    app.include_router(requests.admin_router)
    app.include_router(workflows.admin_router)
    return app


app = create_app()
