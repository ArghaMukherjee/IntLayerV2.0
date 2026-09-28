"""Integration Layer HTTP API (callback dispatcher disabled; see test_database for callbacks)."""
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient

from integration_layer.config import Settings
from integration_layer.main import create_app

APP1, APP3, ADMIN = {"X-API-Key": "k1"}, {"X-API-Key": "k3"}, {"X-API-Key": "adm"}


def valid_order(**overrides):
    order = {"order_id": "ORD-1", "customer_id": "C-1", "currency": "EUR",
             "items": [{"sku": "A", "quantity": 1, "unit_price": 10}]}
    return order | overrides


@pytest.fixture(scope="module")
def client(db_urls):
    settings = Settings(database_url=db_urls["il"], api_keys="WWMG_Source:k1,MyCareHub:k3", admin_api_key="adm",
                        callbacks_enabled=False, callback_allowed_hosts="wwmg.internal",
                        schema_cache_seconds=0)
    with TestClient(create_app(settings)) as c:
        yield c


def submit(client, payload=None, headers=APP1, **body):
    return client.post("/v1/requests", headers=headers,
                       json={"workflow_name": "order_validation", "payload": payload or valid_order(), **body})


def test_requires_api_key(client):
    assert submit(client, headers={}).status_code == 401
    assert submit(client, headers={"X-API-Key": "wrong"}).status_code == 401


def test_submit_and_poll(client):
    r = submit(client)
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "PENDING" and r.headers["location"] == body["status_url"]

    status = client.get(body["status_url"], headers=APP1).json()
    assert status["status"] == "PENDING" and status["source_app"] == "WWMG_Source"
    assert status["target_app"] == "ConsumerApplication" and status["input_schema_version"] == 1
    assert status["input_json"] is None
    with_input = client.get(body["status_url"], headers=APP1, params={"include_input": True}).json()
    assert with_input["input_json"]["order_id"] == "ORD-1"


def test_garbage_forwarded_for_header_is_ignored(client):
    r = submit(client, headers=APP1 | {"X-Forwarded-For": "not-an-ip, 10.0.0.1"})
    assert r.status_code == 202


def test_other_apps_cannot_see_the_request(client):
    url = submit(client).json()["status_url"]
    assert client.get(url, headers=APP3).status_code == 404


def test_payload_schema_errors_are_reported_by_path(client):
    r = submit(client, payload={"order_id": "", "customer_id": "C", "currency": "eur",
                                "items": [{"sku": "A", "quantity": 0, "unit_price": -1}], "x": 1})
    assert r.status_code == 422
    error = r.json()["error"]
    assert error["code"] == "INPUT_SCHEMA_INVALID" and error["schema_version"] == 1
    paths = {e["path"] for e in error["errors"]}
    assert {"$", "$.order_id", "$.currency", "$.items[0].quantity", "$.items[0].unit_price"} <= paths


def test_envelope_errors(client):
    r = client.post("/v1/requests", headers=APP1, json={"workflow_name": "order_validation", "payload": {}, "bogus": 1})
    assert r.status_code == 422 and r.json()["error"]["code"] == "REQUEST_INVALID"
    r = submit(client, workflow_name="unknown_flow")
    assert r.status_code == 422 and r.json()["error"]["code"] == "UNKNOWN_WORKFLOW"


def test_callback_host_allowlist(client):
    assert submit(client, callback_url="https://evil.example/hook").json()["error"]["code"] == "CALLBACK_HOST_NOT_ALLOWED"
    assert submit(client, callback_url="https://wwmg.internal/hook").status_code == 202


def test_idempotent_resubmit(client):
    key = f"idem-{uuid.uuid4()}"
    first = submit(client, idempotency_key=key)
    second = submit(client, idempotency_key=key)
    assert first.status_code == 202 and second.status_code == 200
    assert second.json()["request_id"] == first.json()["request_id"] and second.json()["is_duplicate"]


def test_cancel(client):
    rid = submit(client).json()["request_id"]
    assert client.post(f"/v1/requests/{rid}/cancel", headers=APP3).status_code == 404
    r = client.post(f"/v1/requests/{rid}/cancel", headers=APP1, params={"reason": "customer withdrew"})
    assert r.status_code == 200 and r.json()["status"] == "CANCELLED" and r.json()["success_flag"] is False
    assert client.post(f"/v1/requests/{rid}/cancel", headers=APP1).status_code == 409


def test_list_filters(client):
    rid = submit(client).json()["request_id"]
    listed = client.get("/v1/requests", headers=APP1, params={"status": "PENDING"}).json()
    assert rid in {x["request_id"] for x in listed}
    assert all(x["status"] == "PENDING" for x in listed)
    assert client.get("/v1/requests", headers=APP1, params={"status": "BOGUS"}).status_code == 422


def test_replay_requires_admin_and_failed_status(client):
    rid = submit(client).json()["request_id"]
    assert client.post(f"/v1/admin/requests/{rid}/replay", headers=APP1).status_code == 401
    assert client.post(f"/v1/admin/requests/{rid}/replay", headers=ADMIN).status_code == 409


def test_workflow_registration_and_versioning(client):
    name = f"wf_{uuid.uuid4().hex[:8]}"
    schema = {"type": "object", "required": ["id"], "properties": {"id": {"type": "integer"}}}
    body = {"target_app": "ConsumerApplication", "input_schema": schema}
    assert client.put(f"/v1/admin/workflows/{name}", headers=APP1, json=body).status_code == 401

    r = client.put(f"/v1/admin/workflows/{name}", headers=ADMIN, json=body)
    assert r.status_code == 201 and r.json()["schema_version"] == 1
    r = client.put(f"/v1/admin/workflows/{name}", headers=ADMIN, json=body | {"description": "same schema"})
    assert r.status_code == 200 and r.json()["schema_version"] == 1
    schema["properties"]["id"]["minimum"] = 1
    assert client.put(f"/v1/admin/workflows/{name}", headers=ADMIN, json=body).json()["schema_version"] == 2

    bad = client.put(f"/v1/admin/workflows/{name}", headers=ADMIN,
                     json={"target_app": "ConsumerApplication", "input_schema": {"type": "not-a-type"}})
    assert bad.status_code == 422 and bad.json()["error"]["code"] == "INVALID_JSON_SCHEMA"

    ok = client.post("/v1/requests", headers=APP1, json={"workflow_name": name, "payload": {"id": 5}})
    assert ok.status_code == 202
    rejected = client.post("/v1/requests", headers=APP1, json={"workflow_name": name, "payload": {"id": 0}})
    assert rejected.status_code == 422 and rejected.json()["error"]["schema_version"] == 2
    assert client.get(f"/v1/workflows/{name}", headers=APP1).json()["schema_version"] == 2


def test_api_calls_are_logged_with_masked_secrets(client, db_urls):
    rid = submit(client).json()["request_id"]
    client.get(f"/v1/requests/{rid}", headers=APP1)
    client.portal.call(client.app.state.api_logger.drain)  # log writes are async
    with psycopg.connect(db_urls["admin"]) as conn:
        rows = conn.execute("SELECT http_method, endpoint, response_status_code, request_headers->>'x-api-key',"
                            " server_hostname IS NOT NULL FROM integration.api_call_log"
                            " WHERE request_id = %s ORDER BY call_id", (rid,)).fetchall()
    assert rows == [("POST", "/v1/requests", 202, "***", True),
                    ("GET", "/v1/requests/{request_id}", 200, "***", True)]
