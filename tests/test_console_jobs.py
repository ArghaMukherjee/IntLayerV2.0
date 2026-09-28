"""Scheduler job runners and console privileges against the real schema."""
import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from integration_layer.scheduler import SchedulerSettings, run_system_jobs, run_traffic_jobs


class FakeResponse:
    def __init__(self, status, body):
        self.status_code, self._body, self.text = status, body, str(body)

    def json(self):
        return self._body


class FakeHttp:
    """Accepts payloads that have an order_id, rejects others with 422."""
    def __init__(self):
        self.calls = []

    def post(self, url, json, headers):
        self.calls.append(json)
        if json["payload"].get("order_id"):
            return FakeResponse(202, {"request_id": "00000000-0000-0000-0000-%012d" % len(self.calls), "status": "PENDING"})
        return FakeResponse(422, {"error": {"code": "INPUT_SCHEMA_INVALID"}})


@pytest.fixture
def sched(db_urls):
    with psycopg.connect(db_urls["scheduler"], row_factory=dict_row) as conn:
        yield conn


@pytest.fixture
def ui(db_urls):
    with psycopg.connect(db_urls["console"], autocommit=True, row_factory=dict_row) as conn:
        yield conn


def test_system_jobs_run_record_and_respect_pause(sched, ui, admin):
    admin.execute("UPDATE console.system_jobs SET next_run_at = now(), is_paused = false, run_requested = false")
    ran = run_system_jobs(sched)
    assert ran == 5
    rows = {r[0]: r for r in admin.execute(
        "SELECT job_name, last_status, run_count, next_run_at > now() AS rescheduled FROM console.system_jobs").fetchall()}
    assert all(r[1] == "OK" and r[2] >= 1 and r[3] for r in rows.values())

    ui.execute("UPDATE console.system_jobs SET is_paused = true, next_run_at = now() WHERE job_name = 'reap_expired_leases'")
    assert run_system_jobs(sched) == 0                      # paused and nothing requested
    ui.execute("UPDATE console.system_jobs SET run_requested = true WHERE job_name = 'reap_expired_leases'")
    assert run_system_jobs(sched) == 1                      # manual trigger runs even while paused
    last = admin.execute("SELECT trigger, status FROM console.system_job_runs ORDER BY run_id DESC LIMIT 1").fetchone()
    assert last == ("manual", "OK")


def test_traffic_job_records_every_submission(sched, ui, admin):
    job = ui.execute(
        """INSERT INTO console.jobs (name, workflow_name, payload_template, interval_seconds, requests_per_run,
                                     max_runs, status, next_run_at)
           VALUES ('t-job', 'order_validation', %s, 30, 3, 2, 'ACTIVE', now()) RETURNING job_id""",
        (Jsonb({"order_id": "ORD-{{seq}}", "q": "{{rand_int:1:5}}"}),)).fetchone()["job_id"]
    bad = ui.execute(
        """INSERT INTO console.jobs (name, workflow_name, payload_template, requests_per_run, run_requested)
           VALUES ('t-bad', 'order_validation', %s, 2, true) RETURNING job_id""", (Jsonb({"x": 1}),)).fetchone()["job_id"]
    http = FakeHttp()
    assert run_traffic_jobs(sched, http, SchedulerSettings()) == 2
    assert [c["payload"]["order_id"] for c in http.calls[:3]] == ["ORD-1", "ORD-2", "ORD-3"]

    good_run = admin.execute("SELECT trigger, accepted, rejected FROM console.job_runs WHERE job_id = %s", (job,)).fetchone()
    assert good_run == ("schedule", 3, 0)
    bad_run = admin.execute("SELECT trigger, accepted, rejected FROM console.job_runs WHERE job_id = %s", (bad,)).fetchone()
    assert bad_run == ("manual", 0, 2)
    codes = admin.execute("SELECT http_status, response_body->'error'->>'code' FROM console.job_submissions"
                          " WHERE job_id = %s", (bad,)).fetchall()
    assert codes == [(422, "INPUT_SCHEMA_INVALID")] * 2

    state = admin.execute("SELECT status, run_count, total_submitted, next_run_at > now(), run_requested"
                          " FROM console.jobs WHERE job_id = %s", (job,)).fetchone()
    assert state == ("ACTIVE", 1, 3, True, False)
    admin.execute("UPDATE console.jobs SET next_run_at = now() WHERE job_id = %s", (job,))
    run_traffic_jobs(sched, http, SchedulerSettings())
    assert admin.execute("SELECT status, run_count FROM console.jobs WHERE job_id = %s", (job,)).fetchone() == ("COMPLETED", 2)
    assert run_traffic_jobs(sched, http, SchedulerSettings()) == 0   # completed and paused jobs don't run


def test_console_role_boundaries(ui):
    ui.execute("SELECT count(*) FROM integration.request_metadata")   # read everything
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        ui.execute("UPDATE integration.request_metadata SET priority = 1")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        ui.execute("UPDATE console.system_jobs SET last_status = 'OK'")  # only schedule columns
    ui.execute("UPDATE console.system_jobs SET interval_seconds = 120 WHERE job_name = 'ensure_partitions'")
