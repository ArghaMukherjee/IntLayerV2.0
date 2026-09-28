"""Tests run against a real PostgreSQL 16 server.

    TEST_PG_ADMIN_URL=postgresql://postgres:postgres@localhost:5432/postgres pytest

A throwaway database and throwaway login users (test_*) are created per session and
dropped afterwards; the real service users are never touched.
"""
import os
import uuid

import psycopg
import pytest
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from integration_layer.migrate import MigrateSettings, run

ADMIN_URL = os.getenv("TEST_PG_ADMIN_URL")
# BigQuery tests need the emulator: TEST_BQ_EMULATOR=http://localhost:9050 (a fresh dataset is used per session)
BQ_EMULATOR = os.getenv("TEST_BQ_EMULATOR")
BQ_DATASET = f"it_{uuid.uuid4().hex[:8]}"
if BQ_EMULATOR:
    os.environ.update(BQ_ENABLED="true", BQ_EMULATOR_HOST=BQ_EMULATOR, BQ_PROJECT=os.getenv("TEST_BQ_PROJECT", "intlayer-local"), BQ_DATASET=BQ_DATASET)
else:
    os.environ["BQ_ENABLED"] = "false"
PASSWORD = "test-password"
# fixture key -> group roles
TEST_USERS = {"il": ["il_service"], "consumer": ["consumer_worker"],
              "scheduler": ["il_scheduler", "dwh_etl"], "bi": ["dwh_reader"], "console": ["ops_viewer", "console_app"]}


@pytest.fixture(scope="session")
def db_urls():
    if not ADMIN_URL:
        pytest.skip("TEST_PG_ADMIN_URL is not set")
    suffix = uuid.uuid4().hex[:8]
    name = f"il_test_{suffix}"
    users = {key: f"test_{key}_{suffix}" for key in TEST_USERS}
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    base = make_conninfo(ADMIN_URL, dbname=name)
    run(MigrateSettings(database_url=base))  # no passwords -> service users are left alone
    with psycopg.connect(base, autocommit=True) as conn:
        for key, roles in TEST_USERS.items():
            conn.execute(f"CREATE ROLE {users[key]} LOGIN PASSWORD '{PASSWORD}' IN ROLE {', '.join(roles)}")
    try:
        yield {"admin": base} | {key: make_conninfo(base, user=user, password=PASSWORD)
                                 for key, user in users.items()}
    finally:
        with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
            conn.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
            for user in users.values():
                conn.execute(f"DROP ROLE IF EXISTS {user}")


@pytest.fixture
def admin(db_urls):
    with psycopg.connect(db_urls["admin"], autocommit=True) as conn:
        yield conn


@pytest.fixture
def make_workflow(admin):
    """Registers an isolated workflow (own target_app) so tests don't see each other's rows."""
    def _make(max_retries: int = 3, output_schema: dict | None = None,
              input_schema: dict | None = None, target_app: str | None = None) -> tuple[str, str]:
        name = f"t_{uuid.uuid4().hex[:10]}"
        target = target_app or f"consumer_{name}"
        admin.execute(
            "INSERT INTO integration.workflow_registry"
            " (workflow_name, target_app, input_schema, output_schema, max_retries)"
            " VALUES (%s, %s, %s, %s, %s)",
            (name, target, Jsonb(input_schema or {"type": "object"}),
             Jsonb(output_schema) if output_schema else None, max_retries))
        return name, target
    return _make


def submit_sql(conn, workflow: str, payload: dict, **kwargs):
    params = {"wf": workflow, "payload": Jsonb(payload), "key": kwargs.get("idempotency_key"),
              "cb": kwargs.get("callback_url"), "src": kwargs.get("source_app", "WWMG_Source")}
    return conn.execute(
        "SELECT * FROM integration.submit_request(p_source_app => %(src)s, p_workflow_name => %(wf)s,"
        " p_input_json => %(payload)s, p_input_schema_version => 1, p_api_url => 'http://test/v1/requests',"
        " p_idempotency_key => %(key)s, p_callback_url => %(cb)s)", params).fetchone()
