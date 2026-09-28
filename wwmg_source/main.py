"""WWMG_Source: the source system that submits orders to the Integration Layer and receives callbacks."""
import json
import logging
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from pydantic_settings import BaseSettings, SettingsConfigDict

from ilcommon.il_client import IntegrationLayerClient, IntegrationLayerError, verify_signature

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("wwmg_source")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WWMG_", extra="ignore")

    il_base_url: str = "http://localhost:8000"
    il_api_key: str = ""
    callback_url: str = "http://localhost:8002/hooks/il"   # how the IL reaches this service
    callback_signing_secret: str = ""


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.il = IntegrationLayerClient(settings.il_base_url, settings.il_api_key)
        yield
        await app.state.il.aclose()

    app = FastAPI(title="WWMG_Source", version="2.0.0", lifespan=lifespan,
                  description="Source system: submits orders (workflow order_validation) and receives signed callbacks.")
    app.state.received = deque(maxlen=500)

    async def call_il(coro):
        try:
            return await coro
        except IntegrationLayerError as exc:
            raise HTTPException(exc.status_code, detail=exc.error)

    @app.post("/orders", tags=["demo"], status_code=202)
    async def submit_order(order: dict, request: Request, idempotency_key: str | None = None,
                           use_callback: bool = True):
        """Forward an order to the Integration Layer (workflow: order_validation)."""
        return await call_il(request.app.state.il.submit(
            "order_validation", order, idempotency_key=idempotency_key,
            callback_url=settings.callback_url if use_callback else None))

    @app.get("/orders/{request_id}", tags=["demo"])
    async def order_status(request_id: str, request: Request):
        """Polling mode: ask the Integration Layer for the current status."""
        return await call_il(request.app.state.il.get(request_id))

    @app.post("/hooks/il", tags=["callbacks"])
    async def receive_callback(request: Request):
        """Callback mode: the Integration Layer POSTs the final result here."""
        body = await request.body()
        if settings.callback_signing_secret and not verify_signature(
                settings.callback_signing_secret, request.headers.get("x-il-timestamp", ""),
                body, request.headers.get("x-il-signature", "")):
            raise HTTPException(401, detail="Invalid callback signature")
        event = json.loads(body)
        event["_received_at"] = datetime.now(timezone.utc).isoformat()
        request.app.state.received.appendleft(event)
        log.info("callback %s for request %s: %s / %s", event["event"], event["request_id"],
                 event["status"], event["workflow_status"])
        return {"received": True}

    @app.get("/hooks/il/received", tags=["callbacks"])
    async def received_callbacks(request: Request, request_id: str | None = None):
        events = list(request.app.state.received)
        return [e for e in events if e["request_id"] == request_id] if request_id else events

    @app.get("/health", tags=["health"])
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
