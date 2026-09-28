"""Business handlers, one per workflow_name.

A handler receives the validated input_json and returns a Result. Raise
RetryableError for temporary problems (retried with backoff) and PermanentError
for problems that retrying will not fix. Any other exception is treated as retryable.
"""
import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal


class RetryableError(Exception):
    def __init__(self, message: str, code: str = "RETRYABLE_ERROR"):
        super().__init__(message)
        self.code = code


class PermanentError(Exception):
    def __init__(self, message: str, code: str = "PERMANENT_ERROR"):
        super().__init__(message)
        self.code = code


@dataclass
class Result:
    output: dict
    workflow_status: str = "COMPLETED"


Handler = Callable[[dict], Awaitable[Result]]
HANDLERS: dict[str, Handler] = {}


def handler(workflow_name: str):
    def register(fn: Handler) -> Handler:
        HANDLERS[workflow_name] = fn
        return fn
    return register


# --- order_validation ---------------------------------------------------------

CREDIT_LIMIT = Decimal("50000")
MAX_QUANTITY_PER_LINE = 1000
# Demo hook: simulates the downstream credit service being unavailable
UNAVAILABLE_CUSTOMER = "CUST-DOWNSTREAM-DOWN"


@handler("order_validation")
async def order_validation(order: dict) -> Result:
    if order["customer_id"] == UNAVAILABLE_CUSTOMER:
        raise RetryableError("Credit check service unavailable", code="DOWNSTREAM_UNAVAILABLE")

    total = sum(Decimal(str(i["unit_price"])) * i["quantity"] for i in order["items"])
    reasons = []
    if total > CREDIT_LIMIT:
        reasons.append(f"Order total {total} exceeds credit limit {CREDIT_LIMIT}")
    reasons += [f"Quantity {i['quantity']} for SKU {i['sku']} exceeds {MAX_QUANTITY_PER_LINE}"
                for i in order["items"] if i["quantity"] > MAX_QUANTITY_PER_LINE]

    decision = "REJECTED" if reasons else "APPROVED"
    return Result(
        output={"order_id": order["order_id"], "decision": decision,
                "total_amount": float(total), "currency": order["currency"], "reasons": reasons},
        workflow_status=decision,
    )


# --- care_request (MyCareHub) ----------------------------------------------------------
# Synthetic triage rules for the demo: nothing here is clinical advice.

OUTAGE_MARKER = "SIMULATE_OUTAGE"   # demo hook: the scheduling system is unavailable


def _reference(member_id: str, request_type: str, notes: str) -> str:
    digest = hashlib.sha1(f"{member_id}|{request_type}|{notes}|{datetime.now(timezone.utc).isoformat()}".encode())
    return "CR-" + digest.hexdigest()[:8].upper()


def _next_business_day(start: date) -> date:
    day = start
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return day


@handler("care_request")
async def care_request(req: dict) -> Result:
    notes = req.get("notes") or ""
    if OUTAGE_MARKER in notes.upper():
        raise RetryableError("Scheduling system unavailable", code="SCHEDULING_UNAVAILABLE")

    member, kind, urgency = req["member_id"], req["request_type"], req["urgency"]
    out = {"member_id": member, "request_type": kind, "reference": _reference(member, kind, notes)}

    if urgency == "urgent":
        out.update(decision="ESCALATED",
                   message="Marked urgent: a nurse from your care team will call you within 2 hours. "
                           "If this is an emergency, call your local emergency number.")
    elif kind == "appointment":
        earliest = date.today() + timedelta(days=1 if urgency == "soon" else 2)
        preferred = date.fromisoformat(req["preferred_date"]) if req.get("preferred_date") else earliest
        day = _next_business_day(max(preferred, earliest))
        slot_minutes = int(hashlib.sha1(member.encode()).hexdigest(), 16) % 14 * 30      # 09:00-15:30
        when = datetime.combine(day, time(9, 0)) + timedelta(minutes=slot_minutes)
        out.update(decision="SCHEDULED", scheduled_for=when.isoformat(timespec="minutes"),
                   message=f"Appointment booked for {when:%a %d %b at %H:%M}.")
    elif kind == "prescription_refill":
        ready = datetime.now(timezone.utc) + timedelta(hours=4 if urgency == "soon" else 24)
        out.update(decision="APPROVED", ready_by=ready.isoformat(timespec="minutes"),
                   message="Refill approved and sent to your pharmacy.")
    elif kind == "referral":
        out.update(decision="SUBMITTED",
                   message="Referral submitted; the specialist office will contact you within 3 business days.")
    else:
        out.update(decision="RELEASED",
                   message="Your results are in your chart. Your care team will contact you if follow-up is needed.")
    return Result(output=out, workflow_status=out["decision"])
