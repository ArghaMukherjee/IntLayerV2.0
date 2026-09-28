"""care_request business rules (ConsumerApplication, synthetic triage)."""
import asyncio
from datetime import date, datetime, timedelta

import pytest

from consumer_application.processors import RetryableError, care_request


def run(**req):
    return asyncio.run(care_request({"member_id": "M-1001", "request_type": "appointment", "urgency": "routine", **req}))


def test_urgent_requests_are_escalated():
    r = run(urgency="urgent", request_type="prescription_refill")
    assert r.workflow_status == "ESCALATED" and "emergency" in r.output["message"]


def test_appointments_are_booked_on_a_business_day_after_the_preferred_date():
    preferred = date.today() + timedelta(days=10)
    r = run(preferred_date=preferred.isoformat())
    when = datetime.fromisoformat(r.output["scheduled_for"])
    assert r.workflow_status == "SCHEDULED" and when.date() >= preferred and when.weekday() < 5
    assert 9 <= when.hour <= 15


def test_other_request_types():
    assert run(request_type="prescription_refill").output["decision"] == "APPROVED"
    assert run(request_type="referral").output["decision"] == "SUBMITTED"
    assert run(request_type="lab_results").output["decision"] == "RELEASED"
    assert run().output["reference"].startswith("CR-")


def test_simulated_outage_is_retryable():
    with pytest.raises(RetryableError) as exc:
        run(notes="please SIMULATE_OUTAGE")
    assert exc.value.code == "SCHEDULING_UNAVAILABLE"
