import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])


@router.get("/health")
async def liveness(request: Request):
    return {"status": "ok", "instance_id": request.app.state.settings.instance_id}


@router.get("/health/ready")
async def readiness(request: Request):
    """PostgreSQL must be reachable; BigQuery is reported (requests for BigQuery workflows fail with 503 without it)."""
    try:
        async with request.app.state.pool.connection(timeout=3) as conn:
            await conn.execute("SELECT 1")
    except Exception as exc:
        return JSONResponse(status_code=503, content={"status": "unavailable", "database": str(exc)})
    bq = request.app.state.bq
    if bq is None:
        bigquery = {"enabled": False}
    else:
        try:
            bigquery = {"enabled": True, **await asyncio.wait_for(asyncio.to_thread(bq.cached_ping), timeout=3)}
        except Exception as exc:
            bigquery = {"enabled": True, "ok": False, "error": f"{type(exc).__name__}: {exc}"[:300]}
    return {"status": "ready", "database": "ok", "bigquery": bigquery}
