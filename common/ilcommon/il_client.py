"""Minimal client a source application (WWMG_Source, MyCareHub) uses to talk to the Integration Layer."""
import asyncio
import hashlib
import hmac
import time
from typing import Any

import httpx

TERMINAL_STATUSES = {"COMPLETED", "FAILED", "CANCELLED"}


class IntegrationLayerError(Exception):
    def __init__(self, status_code: int, error: dict):
        super().__init__(f"{status_code} {error.get('code')}: {error.get('message')}")
        self.status_code = status_code
        self.error = error


class IntegrationLayerClient:
    def __init__(self, base_url: str, api_key: str, timeout: float = 30):
        self._http = httpx.AsyncClient(base_url=base_url, headers={"X-API-Key": api_key}, timeout=timeout)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _call(self, method: str, url: str, **kwargs) -> dict:
        response = await self._http.request(method, url, **kwargs)
        if response.is_error:
            try:
                error = response.json().get("error", {})
            except ValueError:
                error = {"code": "HTTP_ERROR", "message": response.text}
            raise IntegrationLayerError(response.status_code, error)
        return response.json()

    async def submit(self, workflow_name: str, payload: dict, *, idempotency_key: str | None = None,
                     callback_url: str | None = None, correlation_id: str | None = None,
                     priority: int = 0) -> dict:
        body: dict[str, Any] = {"workflow_name": workflow_name, "payload": payload, "priority": priority}
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
        if callback_url:
            body["callback_url"] = callback_url
        if correlation_id:
            body["correlation_id"] = correlation_id
        return await self._call("POST", "/v1/requests", json=body)

    async def list(self, **params) -> list:
        return await self._call("GET", "/v1/requests", params={k: v for k, v in params.items() if v is not None})

    async def get(self, request_id: str, include_input: bool = False) -> dict:
        return await self._call("GET", f"/v1/requests/{request_id}",
                                params={"include_input": str(include_input).lower()})

    async def cancel(self, request_id: str, reason: str | None = None) -> dict:
        return await self._call("POST", f"/v1/requests/{request_id}/cancel",
                                params={"reason": reason} if reason else None)

    async def wait_for_result(self, request_id: str, timeout: float = 60, interval: float = 1) -> dict:
        """Polling mode: GET the request until it reaches a terminal status."""
        deadline = time.monotonic() + timeout
        while True:
            result = await self.get(request_id)
            if result["status"] in TERMINAL_STATUSES:
                return result
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Request {request_id} still {result['status']} after {timeout}s")
            await asyncio.sleep(interval)


def verify_signature(secret: str, timestamp: str, body: bytes, signature: str, max_age_seconds: int = 300) -> bool:
    """Checks the X-IL-Signature header of a callback."""
    if not (secret and timestamp and signature) or not timestamp.isdigit():
        return False
    if abs(time.time() - int(timestamp)) > max_age_seconds:
        return False
    expected = "sha256=" + hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)
