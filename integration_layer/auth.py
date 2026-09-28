import secrets

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_app(request: Request, api_key: str | None = Depends(api_key_header)) -> str:
    """Authenticate the calling application; returns its source_app code."""
    if api_key:
        for key, app in request.app.state.settings.api_key_map().items():
            if secrets.compare_digest(key, api_key):
                request.state.source_app = app
                return app
    raise HTTPException(status.HTTP_401_UNAUTHORIZED,
                        detail={"code": "UNAUTHORIZED", "message": "Invalid or missing X-API-Key"})


def require_admin(request: Request, api_key: str | None = Depends(api_key_header)) -> str:
    admin_key = request.app.state.settings.admin_api_key
    if admin_key and api_key and secrets.compare_digest(admin_key, api_key):
        request.state.source_app = "admin"
        return "admin"
    raise HTTPException(status.HTTP_401_UNAUTHORIZED,
                        detail={"code": "UNAUTHORIZED", "message": "Admin API key required"})
