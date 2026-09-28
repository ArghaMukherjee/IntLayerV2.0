"""BigQuery data plane against the emulator (skipped unless TEST_BQ_EMULATOR is set)."""
import asyncio
import threading
import time
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from consumer_application.processors import Result, RetryableError
from consumer_application.queue_client import BigQueryQueueClient
from consumer_application.worker import Worker
from ilcommon.bigquery_store import BigQuerySettings, BigQueryStore
from integration_layer.config import Settings
from integration_layer.main import create_app
from integration_layer.scheduler import run_system_jobs

from .conftest import BQ_EMULATOR

pytestmark = pytest.mark.skipif(not BQ_EMULATOR, reason="TEST_BQ_EMULATOR is not set")


@pytest.fixture(scope="module")
def bq():
    store = BigQueryStore(BigQuerySettings())
    store.ensure_schema()
    return store


def new_request(bq, target, **extra):
    rid = str(uuid.uuid4())
    bq.submit({"request_id": rid, "request_date": time.strftime("%Y-%m-%d"), "correlation_id": str(uuid.uuid4()),
               "source_app": "WWMG_Source", "target_app": target, "workflow_name": "care_request", "priority": 0,
               "input_json": {"member_id": "M-1001"}, "input_schema_version": 1, "max_retries": 1, **extra})
    return rid


def test_submit_claim_complete_with_callback(bq):
    target = f"t_{uuid.uuid4().hex[:6]}"
    rid = new_request(bq, target, callback_url="http://mycarehub/hooks/il")
    assert bq.get(rid)["status"] == "PENDING"
    jobs = bq.claim("w1", "host", target, 5, 60)
    assert [j["request_id"] for j in jobs] == [rid] and jobs[0]["status"] == "IN_PROGRESS"
    assert bq.heartbeat(rid, "w1", 60) and not bq.heartbeat(rid, "intruder", 60)
    assert not bq.complete(rid, "intruder", {"x": 1})
    assert bq.complete(rid, "w1", {"ok": True}, "DONE")
    row = bq.get(rid)
    assert (row["status"], row["success_flag"], row["workflow_status"], row["output_json"]) == ("COMPLETED", True, "DONE", {"ok": True})
    assert row["callback_status"] == "PENDING"
    due = bq.claim_callbacks(50, 60)
    assert rid in [d["request_id"] for d in due]
    assert bq.record_callback_result(rid, False, "HTTP 500", 6) == "PENDING"
    assert rid not in [d["request_id"] for d in bq.claim_callbacks(50, 60)]  # backoff
    assert [e["new_status"] for e in bq.events(rid)] == ["PENDING", "IN_PROGRESS", "COMPLETED"]


def test_retry_then_permanent_failure_and_replay(bq):
    target = f"t_{uuid.uuid4().hex[:6]}"
    rid = new_request(bq, target)
    bq.claim("w1", "h", target, 1, 60)
    assert bq.fail(rid, "w1", "E_TEMP", "temporary", True) == "PENDING"
    assert bq.claim("w1", "h", target, 1, 60) == []          # backoff not yet due
    bq.query(f"UPDATE {bq.t} SET next_attempt_at = CURRENT_TIMESTAMP() WHERE request_id = @rid", {"rid": rid})
    assert len(bq.claim("w1", "h", target, 1, 60)) == 1
    assert bq.fail(rid, "w1", "E_TEMP", "again", True) == "FAILED"   # max_retries = 1
    assert bq.replay(rid) and bq.get(rid)["status"] == "PENDING"


def test_concurrent_claims_are_exclusive(bq):
    target = f"t_{uuid.uuid4().hex[:6]}"
    ids = {new_request(bq, target) for _ in range(12)}
    got, errors = [], []

    def worker(n):
        try:
            for _ in range(4):
                got.extend(j["request_id"] for j in bq.claim(f"w{n}", "h", target, 2, 60))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(3)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors
    assert len(got) == len(set(got)) and set(got) <= ids


def test_reaper_and_cancel(bq):
    target = f"t_{uuid.uuid4().hex[:6]}"
    rid, other = new_request(bq, target), new_request(bq, target)
    bq.claim("w1", "h", target, 1, 1)
    claimed = rid if bq.get(rid)["status"] == "IN_PROGRESS" else other
    bq.query(f"UPDATE {bq.t} SET locked_until = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 SECOND) WHERE request_id = @rid",
             {"rid": claimed})
    assert bq.reap() >= 1
    assert bq.get(claimed)["error_code"] == "LEASE_EXPIRED"
    pending = other if claimed == rid else rid
    assert bq.cancel(pending, "no longer needed") and bq.get(pending)["status"] == "CANCELLED"


@pytest.fixture(scope="module")
def il_client(db_urls, bq):
    with psycopg.connect(db_urls["admin"], autocommit=True) as conn:
        conn.execute("UPDATE integration.workflow_registry SET storage_backend = 'bigquery' WHERE workflow_name = 'care_request'")
    settings = Settings(database_url=db_urls["il"], api_keys="MyCareHub:k2", admin_api_key="adm",
                        callbacks_enabled=False, schema_cache_seconds=0)
    with TestClient(create_app(settings)) as c:
        yield c


def test_il_routes_bigquery_workflows(il_client, db_urls):
    body = {"workflow_name": "care_request", "idempotency_key": f"k-{uuid.uuid4()}",
            "payload": {"member_id": "M-1002", "request_type": "referral", "urgency": "routine"}}
    r = il_client.post("/v1/requests", headers={"X-API-Key": "k2"}, json=body)
    assert r.status_code == 202 and r.json()["storage_backend"] == "bigquery"
    rid = r.json()["request_id"]
    dup = il_client.post("/v1/requests", headers={"X-API-Key": "k2"}, json=body)
    assert dup.status_code == 200 and dup.json()["request_id"] == rid
    got = il_client.get(f"/v1/requests/{rid}", headers={"X-API-Key": "k2"}).json()
    assert got["status"] == "PENDING" and got["storage_backend"] == "bigquery"
    listed = il_client.get("/v1/requests", headers={"X-API-Key": "k2"}, params={"include_payload": True}).json()
    assert any(x["request_id"] == rid and x["input_json"]["member_id"] == "M-1002" for x in listed)
    with psycopg.connect(db_urls["admin"]) as conn:
        assert conn.execute("SELECT storage_backend FROM integration.request_routes WHERE request_id = %s",
                            (rid,)).fetchone() == ("bigquery",)
    cancelled = il_client.post(f"/v1/requests/{rid}/cancel", headers={"X-API-Key": "k2"})
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "CANCELLED"


def test_consumer_worker_on_bigquery(db_urls, bq):
    target = f"t_{uuid.uuid4().hex[:6]}"
    ok, retry = new_request(bq, target), new_request(bq, target, input_json={"member_id": "M-1001", "retry": True})

    async def handler(payload):
        if payload.get("retry"):
            raise RetryableError("busy", code="E_BUSY")
        return Result({"member_id": payload["member_id"], "request_type": "x", "decision": "APPROVED",
                       "message": "ok", "reference": "CR-1"}, "APPROVED")

    async def run():
        pool = AsyncConnectionPool(db_urls["consumer"], min_size=1, max_size=4, open=False,
                                   kwargs={"autocommit": True, "row_factory": dict_row})
        await pool.open()
        client = BigQueryQueueClient(bq, pool, "bq-worker", "host", target, 30)
        worker = Worker(client, db_urls["consumer"], max_concurrency=2, lease_seconds=30, poll_seconds=0.5,
                        handlers={"care_request": handler})
        await worker.start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and worker.stats["completed"] + worker.stats["retried"] < 2:
            await asyncio.sleep(0.2)
        await worker.stop()
        await pool.close()
        return worker.stats

    stats = asyncio.run(run())
    assert stats["completed"] == 1 and stats["retried"] == 1
    assert bq.get(ok)["workflow_status"] == "APPROVED"
    assert (bq.get(retry)["status"], bq.get(retry)["error_code"]) == ("PENDING", "E_BUSY")


def test_warehouse_loads_bigquery_rows(db_urls, bq, admin):
    with psycopg.connect(db_urls["scheduler"], row_factory=dict_row) as sched:
        admin.execute("UPDATE console.system_jobs SET run_requested = true WHERE job_name = 'dwh_load_bigquery'")
        run_system_jobs(sched, bq)
    status, result = admin.execute("SELECT last_status, last_result FROM console.system_jobs"
                                   " WHERE job_name = 'dwh_load_bigquery'").fetchone()
    assert status == "OK", result
    n = admin.execute("SELECT count(*) FROM dwh.fact_request WHERE storage_backend = 'bigquery'").fetchone()[0]
    assert n >= 1
