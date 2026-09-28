"""MyCareHub: member-facing care portal connected to the Integration Layer.

Members submit care requests (workflow care_request, stored in BigQuery by default) and follow
them to a result. MyCareHub talks to the Integration Layer server-side with its own API key,
receives signed callbacks, and turns them into member notifications. Demo data is synthetic.
"""
import json
import logging
from collections import deque
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Literal

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from ilcommon.il_client import IntegrationLayerClient, IntegrationLayerError, verify_signature

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("mycarehub")
STATIC = Path(__file__).parent / "static"
WORKFLOW = "care_request"

MEMBERS = [  # synthetic demo members
    {"member_id": "M-1001", "name": "Alex Rivera", "plan": "Gold PPO"},
    {"member_id": "M-1002", "name": "Priya Nair", "plan": "Silver HMO"},
    {"member_id": "M-1003", "name": "Jordan Lee", "plan": "Gold PPO"},
    {"member_id": "M-1004", "name": "Sam Okafor", "plan": "Bronze EPO"},
    {"member_id": "M-1005", "name": "Mei Chen", "plan": "Silver HMO"},
    {"member_id": "M-1006", "name": "Luis Moreno", "plan": "Gold PPO"},
]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MYCAREHUB_", extra="ignore")

    il_base_url: str = "http://localhost:8000"
    il_api_key: str = ""
    callback_url: str = "http://localhost:8003/hooks/il"   # how the IL reaches MyCareHub
    callback_signing_secret: str = ""


class CareRequestIn(BaseModel):
    member_id: str
    request_type: Literal["appointment", "prescription_refill", "referral", "lab_results"]
    urgency: Literal["routine", "soon", "urgent"] = "routine"
    preferred_date: date | None = None
    notes: str | None = Field(default=None, max_length=500)


def view(r: dict) -> dict:
    """Flattens an Integration Layer request into what the portal shows."""
    inp, out = r.get("input_json") or {}, r.get("output_json") or {}
    return {
        "request_id": r["request_id"], "status": r["status"], "workflow_status": r.get("workflow_status"),
        "request_type": inp.get("request_type"), "urgency": inp.get("urgency"), "notes": inp.get("notes"),
        "preferred_date": inp.get("preferred_date"), "member_id": inp.get("member_id"),
        "decision": out.get("decision"), "message": out.get("message"), "reference": out.get("reference"),
        "scheduled_for": out.get("scheduled_for"), "ready_by": out.get("ready_by"),
        "error_code": r.get("error_code"), "retry_count": r.get("retry_count"),
        "received_at": r["received_at"], "completed_at": r.get("workflow_completed_at"),
        "storage_backend": r.get("storage_backend"),
    }


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.il = IntegrationLayerClient(settings.il_base_url, settings.il_api_key)
        yield
        await app.state.il.aclose()

    app = FastAPI(title="MyCareHub", version="2.0.0", lifespan=lifespan,
                  description="Member-facing care portal. Submits care requests to the Integration Layer "
                              "(workflow care_request) and receives the results by signed callback.")
    app.state.notifications = deque(maxlen=500)

    async def il_call(coro):
        try:
            return await coro
        except IntegrationLayerError as exc:
            raise HTTPException(exc.status_code, detail=exc.error)

    @app.get("/api/members", tags=["members"])
    async def members():
        return MEMBERS

    @app.post("/api/care-requests", tags=["care requests"])
    async def create_care_request(body: CareRequestIn):
        """Submits a care request through the Integration Layer. Returns its HTTP status and body."""
        payload = body.model_dump(mode="json", exclude_none=True)
        try:
            result = await app.state.il.submit(WORKFLOW, payload, callback_url=settings.callback_url)
            return {"http_status": 202, "response": result}
        except IntegrationLayerError as exc:
            return JSONResponse(status_code=200, content={"http_status": exc.status_code, "response": {"error": exc.error}})
        except httpx.HTTPError as exc:
            return JSONResponse(status_code=200, content={"http_status": 504, "response": {"error": {
                "code": "IL_UNREACHABLE", "message": f"The Integration Layer did not answer: {type(exc).__name__}"}}})

    @app.get("/api/care-requests", tags=["care requests"])
    async def list_care_requests(member_id: str = Query(pattern=r"^M-\d{4}$")):
        rows = await il_call(app.state.il.list(workflow_name=WORKFLOW, include_payload="true", limit=200))
        return [view(r) for r in rows if (r.get("input_json") or {}).get("member_id") == member_id]

    @app.get("/api/care-requests/{request_id}", tags=["care requests"])
    async def care_request(request_id: str):
        return view(await il_call(app.state.il.get(request_id, include_input=True)))

    @app.post("/api/care-requests/{request_id}/cancel", tags=["care requests"])
    async def cancel(request_id: str):
        return view(await il_call(app.state.il.cancel(request_id, reason="Cancelled by member in MyCareHub")))

    @app.post("/hooks/il", tags=["callbacks"])
    async def receive_callback(request: Request):
        """Signed callback from the Integration Layer when a care request finishes."""
        body = await request.body()
        if settings.callback_signing_secret and not verify_signature(
                settings.callback_signing_secret, request.headers.get("x-il-timestamp", ""),
                body, request.headers.get("x-il-signature", "")):
            raise HTTPException(401, detail="Invalid callback signature")
        event = json.loads(body)
        member_id = (event.get("output_json") or {}).get("member_id")
        if member_id is None:  # failures carry no output: look the member up
            try:
                member_id = (await app.state.il.get(event["request_id"], include_input=True))["input_json"]["member_id"]
            except Exception:
                member_id = None
        app.state.notifications.appendleft({**event, "member_id": member_id,
                                            "received_at": datetime.now(timezone.utc).isoformat()})
        log.info("callback %s for %s: %s / %s", event["event"], member_id, event["status"], event["workflow_status"])
        return {"received": True}

    @app.get("/api/notifications", tags=["callbacks"])
    async def notifications(member_id: str | None = None):
        items = list(app.state.notifications)
        return [n for n in items if member_id in (None, n["member_id"])][:50]

    @app.get("/health", tags=["health"])
    async def health():
        return {"status": "ok"}

    app.mount("/static", StaticFiles(directory=STATIC), name="static")

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(STATIC / "index.html")

    return app


app = create_app()
