import ipaddress
import socket
from functools import lru_cache

from starlette.requests import Request


@lru_cache
def hostname() -> str:
    return socket.gethostname()


@lru_cache
def server_ip() -> str | None:
    try:
        return socket.gethostbyname(hostname())
    except OSError:
        return None


def _valid_ip(value: str | None) -> str | None:
    try:
        return str(ipaddress.ip_address(value.strip())) if value else None
    except ValueError:
        return None


def client_ip(request: Request) -> str | None:
    """Caller's IP (first X-Forwarded-For hop when behind a proxy); None if not a valid address."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return _valid_ip(forwarded.split(",")[0])
    return _valid_ip(request.client.host if request.client else None)
