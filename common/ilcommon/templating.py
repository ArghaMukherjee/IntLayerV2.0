"""Fills {{placeholders}} in JSON payload templates (used by traffic jobs and the console).

A string that is exactly one placeholder takes the placeholder's type, so
"quantity": "{{rand_int:1:40}}" becomes an integer. Placeholders inside longer
strings are substituted as text. Unknown placeholders are left unchanged.

    {{seq}}                 sequence number supplied by the caller
    {{uuid}}                random UUID
    {{short_id}}            6 random upper-case hex characters
    {{rand_int:MIN:MAX}}    random integer, inclusive
    {{rand_float:MIN:MAX}}  random number with 2 decimals
    {{choice:A|B|C}}        one of the options
    {{now}} / {{date}}      current UTC timestamp / date (ISO 8601)
    {{job}}                 job name supplied by the caller
"""
import random
import re
import secrets
import uuid
from datetime import datetime, timezone
from typing import Any

PLACEHOLDER = re.compile(r"\{\{\s*([a-z_]+)(?::([^}]*))?\s*\}\}")


def _value(name: str, arg: str | None, ctx: dict) -> Any:
    if name == "seq":
        return ctx.get("seq", 1)
    if name == "uuid":
        return str(uuid.uuid4())
    if name == "short_id":
        return secrets.token_hex(3).upper()
    if name == "rand_int" and arg:
        low, high = (int(x) for x in arg.split(":", 1))
        return random.randint(low, high)
    if name == "rand_float" and arg:
        low, high = (float(x) for x in arg.split(":", 1))
        return round(random.uniform(low, high), 2)
    if name == "choice" and arg:
        return random.choice(arg.split("|"))
    if name == "now":
        return datetime.now(timezone.utc).isoformat(timespec="seconds")
    if name == "date":
        return datetime.now(timezone.utc).date().isoformat()
    if name == "job":
        return ctx.get("job", "")
    raise KeyError(name)


def _render_str(text: str, ctx: dict) -> Any:
    whole = PLACEHOLDER.fullmatch(text.strip())
    if whole:
        try:
            return _value(whole.group(1), whole.group(2), ctx)
        except (KeyError, ValueError):
            return text

    def substitute(m: re.Match) -> str:
        try:
            return str(_value(m.group(1), m.group(2), ctx))
        except (KeyError, ValueError):
            return m.group(0)

    return PLACEHOLDER.sub(substitute, text)


def render(template: Any, ctx: dict | None = None) -> Any:
    ctx = ctx or {}
    if isinstance(template, dict):
        return {k: render(v, ctx) for k, v in template.items()}
    if isinstance(template, list):
        return [render(v, ctx) for v in template]
    if isinstance(template, str):
        return _render_str(template, ctx)
    return template


def has_placeholders(template: Any) -> bool:
    if isinstance(template, dict):
        return any(has_placeholders(v) for v in template.values())
    if isinstance(template, list):
        return any(has_placeholders(v) for v in template)
    return isinstance(template, str) and bool(PLACEHOLDER.search(template))
