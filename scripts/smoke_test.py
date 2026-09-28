"""End-to-end smoke test for a running V2 stack (docker compose or local processes).

    python scripts/smoke_test.py            # reads keys from .env when present

Environment (defaults match docker-compose.yml):
    IL_URL, CONSUMER_URL, WWMG_URL, MYCAREHUB_URL, CONSOLE_URL, WWMG_API_KEY, MYCAREHUB_API_KEY, ADMIN_API_KEY
"""
import os
import sys
import time
import uuid
from pathlib import Path

import httpx

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
if ENV_FILE.exists():
    for line in ENV_FILE.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

IL_URL = os.getenv("IL_URL", "http://localhost:8000")
CONSUMER_URL = os.getenv("CONSUMER_URL", "http://localhost:8001")
WWMG_URL = os.getenv("WWMG_URL", "http://localhost:8002")
MYCAREHUB_URL = os.getenv("MYCAREHUB_URL", "http://localhost:8003")
CONSOLE_URL = os.getenv("CONSOLE_URL", "http://localhost:8080")
WWMG_KEY = os.getenv("WWMG_API_KEY", "wwmg-dev-key")
MCH_KEY = os.getenv("MYCAREHUB_API_KEY", "mycarehub-dev-key")
ADMIN_KEY = os.getenv("ADMIN_API_KEY", "admin-dev-key")

wwmg = httpx.Client(base_url=IL_URL, headers={"X-API-Key": WWMG_KEY}, timeout=30)
mch = httpx.Client(base_url=IL_URL, headers={"X-API-Key": MCH_KEY}, timeout=30)
failures = 0


def check(name: str, condition: bool, detail: object = "") -> None:
    global failures
    print(f"  [{'PASS' if condition else 'FAIL'}] {name}" + ("" if condition else f"  -> {str(detail)[:300]}"))
    failures += 0 if condition else 1


def wait_until(fn, timeout: float = 60, interval: float = 1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except httpx.HTTPError:
            pass
        time.sleep(interval)
    return None


def order(customer: str = "CUST-001", qty: int = 2, price: float = 99.5) -> dict:
    return {"order_id": f"ORD-{uuid.uuid4().hex[:8]}", "customer_id": customer, "currency": "EUR",
            "items": [{"sku": "SKU-1", "quantity": qty, "unit_price": price}]}


print("1. Health")
for name, url in (("Integration Layer", IL_URL), ("ConsumerApplication", CONSUMER_URL), ("WWMG_Source", WWMG_URL),
                  ("MyCareHub", MYCAREHUB_URL), ("Console", CONSOLE_URL)):
    check(f"{name} healthy", httpx.get(f"{url}/health", timeout=10).status_code == 200)
ready = httpx.get(f"{IL_URL}/health/ready", timeout=15).json()
check("PostgreSQL ready", ready.get("database") == "ok", ready)
check("BigQuery reachable from the IL", (ready.get("bigquery") or {}).get("ok") is True, ready)
workers = httpx.get(f"{CONSUMER_URL}/health", timeout=10).json().get("workers", {})
check("ConsumerApplication runs a PostgreSQL and a BigQuery worker", workers == {"postgres": True, "bigquery": True}, workers)

print("2. Authentication and validation")
r = httpx.post(f"{IL_URL}/v1/requests", json={"workflow_name": "order_validation", "payload": {}})
check("missing API key -> 401", r.status_code == 401, r.text)
r = wwmg.post("/v1/requests", json={"workflow_name": "order_validation",
                                    "payload": {"order_id": "X", "currency": "euro", "items": []}})
check("invalid order -> 422 INPUT_SCHEMA_INVALID", r.status_code == 422 and r.json()["error"]["code"] == "INPUT_SCHEMA_INVALID", r.text)

print("3. WWMG_Source -> PostgreSQL -> ConsumerApplication -> callback")
r = httpx.post(f"{WWMG_URL}/orders", json=order(), timeout=30)
check("WWMG_Source submit accepted (202)", r.status_code == 202, r.text)
rid = r.json().get("request_id")
check("stored in PostgreSQL", r.json().get("storage_backend") == "postgres", r.json())
events = wait_until(lambda: httpx.get(f"{WWMG_URL}/hooks/il/received", params={"request_id": rid}).json())
check("signed callback received by WWMG_Source", bool(events), "no callback")
if events:
    check("callback: COMPLETED / APPROVED", events[0]["status"] == "COMPLETED" and events[0]["workflow_status"] == "APPROVED", events[0])

print("4. MyCareHub -> BigQuery -> ConsumerApplication -> callback")
r = httpx.post(f"{MYCAREHUB_URL}/api/care-requests", timeout=30,
               json={"member_id": "M-1001", "request_type": "appointment", "urgency": "routine", "notes": "smoke test"})
body = r.json()
check("MyCareHub submit accepted (202)", body.get("http_status") == 202, body)
cid = (body.get("response") or {}).get("request_id")
check("stored in BigQuery", (body.get("response") or {}).get("storage_backend") == "bigquery", body)
notes = wait_until(lambda: [n for n in httpx.get(f"{MYCAREHUB_URL}/api/notifications", params={"member_id": "M-1001"}).json()
                            if n["request_id"] == cid], timeout=90, interval=2)
check("MyCareHub notified by signed callback", bool(notes), "no notification")
if notes:
    check("care request SCHEDULED", notes[0]["workflow_status"] == "SCHEDULED", notes[0])
status = mch.get(f"/v1/requests/{cid}").json()
check("IL record: BigQuery, COMPLETED, callback DELIVERED",
      (status.get("storage_backend"), status.get("status"), status.get("callback_status")) == ("bigquery", "COMPLETED", "DELIVERED"), status)
r = httpx.post(f"{MYCAREHUB_URL}/api/care-requests", json={"member_id": "M-1001", "request_type": "appointment",
                                                           "urgency": "routine", "notes": "x" * 600}, timeout=30)
check("MyCareHub rejects invalid input before the IL (422)", r.status_code == 422, r.text)
check("WWMG_Source cannot read MyCareHub's request (404)", wwmg.get(f"/v1/requests/{cid}").status_code == 404)

print("5. Polling, idempotency and retries")
key = f"idem-{uuid.uuid4().hex[:8]}"
r1 = wwmg.post("/v1/requests", json={"workflow_name": "order_validation", "payload": order(qty=1200, price=80), "idempotency_key": key})
r2 = wwmg.post("/v1/requests", json={"workflow_name": "order_validation", "payload": order(), "idempotency_key": key})
check("same idempotency_key -> 200 with original id", r2.status_code == 200 and r2.json()["request_id"] == r1.json()["request_id"], r2.text)
final = wait_until(lambda: (s := wwmg.get(f"/v1/requests/{r1.json()['request_id']}").json())["status"] == "COMPLETED" and s)
check("polled result COMPLETED / REJECTED", bool(final) and final["workflow_status"] == "REJECTED", final)
r = mch.post("/v1/requests", json={"workflow_name": "care_request",
                                   "payload": {"member_id": "M-1004", "request_type": "appointment", "urgency": "routine",
                                               "notes": "SIMULATE_OUTAGE"}})
oid = r.json()["request_id"]
retried = wait_until(lambda: (s := mch.get(f"/v1/requests/{oid}").json())["retry_count"] >= 1 and s, timeout=90, interval=2)
check("BigQuery request retried after SCHEDULING_UNAVAILABLE",
      bool(retried) and retried["error_code"] == "SCHEDULING_UNAVAILABLE" and retried["status"] == "PENDING", retried)

print("6. Choosing the backend per workflow")
admin = httpx.Client(base_url=IL_URL, headers={"X-API-Key": ADMIN_KEY}, timeout=30)
wf = wwmg.get("/v1/workflows/order_validation").json()
spec = {k: wf[k] for k in ("target_app", "description", "input_schema", "output_schema", "max_retries", "is_active")}
r = admin.put("/v1/admin/workflows/order_validation", json=spec | {"storage_backend": "bigquery"})
check("switch order_validation to BigQuery", r.status_code == 200 and r.json()["storage_backend"] == "bigquery", r.text)
time.sleep(1)
r = wwmg.post("/v1/requests", json={"workflow_name": "order_validation", "payload": order()})
check("new order stored in BigQuery", r.json().get("storage_backend") == "bigquery", r.text)
done = wait_until(lambda: (s := wwmg.get(f"/v1/requests/{r.json()['request_id']}").json())["status"] == "COMPLETED" and s,
                  timeout=90, interval=2)
check("...and processed by ConsumerApplication", bool(done) and done["workflow_status"] == "APPROVED", done)
r = admin.put("/v1/admin/workflows/order_validation", json=spec | {"storage_backend": "postgres"})
check("switch order_validation back to PostgreSQL", r.status_code == 200 and r.json()["storage_backend"] == "postgres", r.text)

print("7. Console")
o = httpx.get(f"{CONSOLE_URL}/api/overview", timeout=30).json()
check("console sees requests in both backends", o["backend_24h"].get("postgres", 0) > 0 and o["backend_24h"].get("bigquery", 0) > 0, o["backend_24h"])
check("console reports all services healthy", all(s["ok"] for s in o["services"]), [(s["name"], s["ok"]) for s in o["services"]])
stats = httpx.get(f"{CONSUMER_URL}/stats").json()
print(f"\nConsumerApplication counters: {stats['counters']}")
print("RESULT:", "ALL PASSED" if failures == 0 else f"{failures} FAILED")
sys.exit(1 if failures else 0)
