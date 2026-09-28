"""ConsumerApplication worker against the real queue functions, plus handler rules."""
import asyncio
import time

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from ilcommon.il_client import verify_signature
from consumer_application.processors import PermanentError, Result, RetryableError, care_request, order_validation
from consumer_application.queue_client import PostgresQueueClient
from consumer_application.worker import Worker
from integration_layer.callbacks import sign

from .conftest import submit_sql


def test_order_validation_rules():
    order = {"order_id": "O", "customer_id": "C", "currency": "EUR",
             "items": [{"sku": "A", "quantity": 2, "unit_price": 10.25}]}
    ok = asyncio.run(order_validation(order))
    assert ok.workflow_status == "APPROVED" and ok.output["total_amount"] == 20.5
    big = order | {"items": [{"sku": "A", "quantity": 1001, "unit_price": 100}]}
    rejected = asyncio.run(order_validation(big))
    assert rejected.workflow_status == "REJECTED" and len(rejected.output["reasons"]) == 2


def test_callback_signature_roundtrip():
    ts = str(int(time.time()))
    signature = sign("secret", ts, b'{"a":1}')
    assert verify_signature("secret", ts, b'{"a":1}', signature)
    assert not verify_signature("secret", ts, b'{"a":2}', signature)
    assert not verify_signature("other", ts, b'{"a":1}', signature)
    assert not verify_signature("secret", str(int(time.time()) - 3600), b'{"a":1}',
                                sign("secret", str(int(time.time()) - 3600), b'{"a":1}'))


def test_worker_outcomes(db_urls, make_workflow, admin):
    output_schema = {"type": "object", "required": ["ok"], "properties": {"ok": {"type": "boolean"}}}
    wf, target = make_workflow(output_schema=output_schema)
    wf_no_handler, _ = make_workflow(target_app=target)

    async def handle(payload):
        match payload["case"]:
            case "ok":
                return Result({"ok": True}, "DONE")
            case "bad_output":
                return Result({"ok": "yes"})
            case "retry":
                raise RetryableError("downstream timeout", code="E_TIMEOUT")
            case "permanent":
                raise PermanentError("cannot process", code="E_BUSINESS")
            case "crash":
                raise ValueError("boom")

    with psycopg.connect(db_urls["il"], autocommit=True) as il:
        ids = {case: submit_sql(il, wf, {"case": case})[0]
               for case in ("ok", "bad_output", "retry", "permanent", "crash")}
        ids["no_handler"] = submit_sql(il, wf_no_handler, {})[0]

    async def run():
        pool = AsyncConnectionPool(db_urls["consumer"], min_size=1, max_size=5, open=False,
                                   kwargs={"autocommit": True, "row_factory": dict_row})
        await pool.open()
        client = PostgresQueueClient(pool, "test-worker", "test-host", target, lease_seconds=30)
        worker = Worker(client, db_urls["consumer"], max_concurrency=2, lease_seconds=30,
                        poll_seconds=0.2, handlers={wf: handle})
        await worker.start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and sum(worker.stats[k] for k in ("completed", "failed", "retried")) < 6:
            await asyncio.sleep(0.1)
        await worker.stop()
        await pool.close()
        return worker.stats

    stats = asyncio.run(run())
    assert stats["claimed"] == 6

    def row(case):
        return admin.execute("SELECT status, success_flag, workflow_status, error_code, retry_count, consumer_worker_id"
                             " FROM integration.request_metadata WHERE request_id = %s", (ids[case],)).fetchone()

    assert row("ok") == ("COMPLETED", True, "DONE", None, 0, "test-worker")
    assert row("bad_output")[:4] == ("FAILED", False, "FAILED", "OUTPUT_SCHEMA_INVALID")
    assert row("retry")[:5] == ("PENDING", None, None, "E_TIMEOUT", 1)
    assert row("permanent")[:4] == ("FAILED", False, "FAILED", "E_BUSINESS")
    assert row("crash")[:5] == ("PENDING", None, None, "UNHANDLED_ERROR", 1)
    assert row("no_handler")[:4] == ("FAILED", False, "FAILED", "NO_HANDLER")
