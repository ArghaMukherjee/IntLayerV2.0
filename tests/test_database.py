"""State machine, concurrency and permissions of the integration schema."""
import threading

import psycopg
import pytest
from psycopg.types.json import Jsonb

from .conftest import submit_sql


@pytest.fixture
def il(db_urls):
    with psycopg.connect(db_urls["il"], autocommit=True) as conn:
        yield conn


@pytest.fixture
def consumer(db_urls):
    with psycopg.connect(db_urls["consumer"], autocommit=True) as conn:
        yield conn


def claim(conn, target, worker="w1", n=10, lease=300):
    return conn.execute("SELECT * FROM integration.claim_requests(%s, 'host', %s, %s, %s)",
                        (worker, target, n, lease)).fetchall()


def status_of(admin, request_id):
    return admin.execute(
        "SELECT status, success_flag, retry_count, error_code, callback_status"
        " FROM integration.request_metadata WHERE request_id = %s", (request_id,)).fetchone()


def test_unknown_workflow_is_rejected(il):
    with pytest.raises(psycopg.errors.NoDataFound):
        submit_sql(il, "does_not_exist", {})


def test_idempotency_returns_original(il, make_workflow):
    wf, _ = make_workflow()
    first = submit_sql(il, wf, {"a": 1}, idempotency_key="k-1")
    second = submit_sql(il, wf, {"a": 2}, idempotency_key="k-1")
    assert second[0] == first[0] and second[4] is True
    assert submit_sql(il, wf, {"a": 1}, idempotency_key="k-1", source_app="other_app")[0] != first[0]


def test_concurrent_workers_never_claim_the_same_request(db_urls, il, make_workflow):
    wf, target = make_workflow()
    ids = {submit_sql(il, wf, {"i": i})[0] for i in range(40)}
    claimed: list = []

    def worker(n):
        with psycopg.connect(db_urls["consumer"], autocommit=True) as conn:
            claimed.extend(r[0] for r in claim(conn, target, worker=f"w{n}", n=15))

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(claimed) == len(set(claimed)) == 40
    assert set(claimed) == ids


def test_complete_sets_output_and_success(consumer, il, admin, make_workflow):
    wf, target = make_workflow()
    rid = submit_sql(il, wf, {})[0]
    claim(consumer, target)
    assert consumer.execute("SELECT integration.complete_request(%s, 'w1', %s, 'APPROVED')",
                        (rid, Jsonb({"ok": True}))).fetchone()[0] is True
    row = admin.execute("SELECT status, success_flag, output_json, workflow_status, workflow_completed_at,"
                        " processing_duration_ms FROM integration.request_metadata WHERE request_id = %s",
                        (rid,)).fetchone()
    assert row[:4] == ("COMPLETED", True, {"ok": True}, "APPROVED")
    assert row[4] is not None and row[5] is not None


def test_only_the_lease_holder_can_report(consumer, il, make_workflow):
    wf, target = make_workflow()
    rid = submit_sql(il, wf, {})[0]
    claim(consumer, target, worker="owner")
    assert consumer.execute("SELECT integration.complete_request(%s, 'intruder', '{}'::jsonb)", (rid,)).fetchone()[0] is False
    assert consumer.execute("SELECT integration.fail_request(%s, 'intruder', 'X', 'x')", (rid,)).fetchone()[0] is None


def test_retries_then_fails_permanently(consumer, il, admin, make_workflow):
    wf, target = make_workflow(max_retries=1)
    rid = submit_sql(il, wf, {})[0]
    claim(consumer, target)
    assert consumer.execute("SELECT integration.fail_request(%s, 'w1', 'E_TEMP', 'temp', true)", (rid,)).fetchone()[0] == "PENDING"
    assert claim(consumer, target) == []           # backoff: not yet due
    admin.execute("UPDATE integration.request_metadata SET next_attempt_at = now() WHERE request_id = %s", (rid,))
    assert len(claim(consumer, target)) == 1
    assert consumer.execute("SELECT integration.fail_request(%s, 'w1', 'E_TEMP', 'temp', true)", (rid,)).fetchone()[0] == "FAILED"
    assert status_of(admin, rid)[:4] == ("FAILED", False, 1, "E_TEMP")


def test_non_retryable_failure_is_final(consumer, il, admin, make_workflow):
    wf, target = make_workflow(max_retries=5)
    rid = submit_sql(il, wf, {})[0]
    claim(consumer, target)
    assert consumer.execute("SELECT integration.fail_request(%s, 'w1', 'E_BAD', 'bad', false)", (rid,)).fetchone()[0] == "FAILED"
    assert status_of(admin, rid)[:3] == ("FAILED", False, 0)


def test_reaper_recovers_expired_lease(db_urls, consumer, il, admin, make_workflow):
    wf, target = make_workflow()
    rid = submit_sql(il, wf, {})[0]
    claim(consumer, target, lease=1)
    admin.execute("UPDATE integration.request_metadata SET locked_until = now() - interval '1 s' WHERE request_id = %s", (rid,))
    with psycopg.connect(db_urls["scheduler"], autocommit=True) as sched:
        assert sched.execute("SELECT integration.reap_expired_leases()").fetchone()[0] >= 1
    assert status_of(admin, rid)[:4] == ("PENDING", None, 1, "LEASE_EXPIRED")


def test_callback_is_queued_retried_and_delivered(consumer, il, admin, make_workflow):
    wf, target = make_workflow()
    rid = submit_sql(il, wf, {}, callback_url="http://wwmg-source/hook")[0]
    assert status_of(admin, rid)[4] is None
    claim(consumer, target)
    consumer.execute("SELECT integration.complete_request(%s, 'w1', '{}'::jsonb)", (rid,))
    assert status_of(admin, rid)[4] == "PENDING"

    due = il.execute("SELECT request_id FROM integration.claim_callbacks(100, 60)").fetchall()
    assert (rid,) in due
    assert il.execute("SELECT integration.record_callback_result(%s, false, 'HTTP 500', 6)", (rid,)).fetchone()[0] == "PENDING"
    assert (rid,) not in il.execute("SELECT request_id FROM integration.claim_callbacks(100, 60)").fetchall()  # backoff
    admin.execute("UPDATE integration.request_metadata SET callback_next_attempt_at = now() WHERE request_id = %s", (rid,))
    assert (rid,) in il.execute("SELECT request_id FROM integration.claim_callbacks(100, 60)").fetchall()
    assert il.execute("SELECT integration.record_callback_result(%s, true)", (rid,)).fetchone()[0] == "DELIVERED"


def test_callback_gives_up_after_max_attempts(consumer, il, admin, make_workflow):
    wf, target = make_workflow()
    rid = submit_sql(il, wf, {}, callback_url="http://wwmg-source/hook")[0]
    claim(consumer, target)
    consumer.execute("SELECT integration.complete_request(%s, 'w1', '{}'::jsonb)", (rid,))
    for attempt in range(1, 3):
        admin.execute("UPDATE integration.request_metadata SET callback_next_attempt_at = now() WHERE request_id = %s", (rid,))
        il.execute("SELECT integration.claim_callbacks(100, 60)").fetchall()
        result = il.execute("SELECT integration.record_callback_result(%s, false, 'down', 2)", (rid,)).fetchone()[0]
    assert result == "FAILED"


def test_every_transition_is_audited(consumer, il, admin, make_workflow):
    wf, target = make_workflow()
    rid = submit_sql(il, wf, {})[0]
    claim(consumer, target, worker="w-audit")
    consumer.execute("SELECT integration.complete_request(%s, 'w-audit', '{}'::jsonb)", (rid,))
    events = admin.execute("SELECT old_status, new_status, actor FROM integration.request_event_log"
                           " WHERE request_id = %s ORDER BY event_id", (rid,)).fetchall()
    assert events == [(None, "PENDING", "il"), ("PENDING", "IN_PROGRESS", "w-audit"),
                      ("IN_PROGRESS", "COMPLETED", "w-audit")]


def test_roles_are_least_privilege(db_urls, consumer):
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        consumer.execute("SELECT * FROM integration.request_metadata LIMIT 1")
    with psycopg.connect(db_urls["bi"], autocommit=True) as bi:
        bi.execute("SELECT count(*) FROM dwh.fact_request")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            bi.execute("SELECT * FROM integration.request_metadata LIMIT 1")


def test_dwh_load_is_idempotent(db_urls, admin, il, make_workflow):
    wf, _ = make_workflow()
    submit_sql(il, wf, {})
    with psycopg.connect(db_urls["scheduler"], autocommit=True) as sched:
        sched.execute("CALL dwh.load_incremental()")
        first = admin.execute("SELECT count(*) FROM dwh.fact_request WHERE storage_backend = 'postgres'").fetchone()[0]
        sched.execute("CALL dwh.load_incremental()")
    assert first == admin.execute("SELECT count(*) FROM integration.request_metadata").fetchone()[0]
    assert admin.execute("SELECT count(*) FROM dwh.fact_request WHERE storage_backend = 'postgres'").fetchone()[0] == first
