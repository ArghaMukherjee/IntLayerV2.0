# Integration Layer V2.0: Implementation Status and Roadmap

## Requirements for V2 and where they are met

| # | Requirement | Delivered |
|---|---|---|
| 1 | Updated documents and code in a new folder `V2.0` | This repository; V1 is unchanged in its own repository |
| 2 | Keep PostgreSQL, add BigQuery; each workflow chooses its source | `workflow_registry.storage_backend`; `ilcommon/bigquery_store.py`; routing in the IL; per-backend workers; console editor |
| 3 | Rename app1 to **WWMG_Source** | `wwmg_source/` service, API key and source_app `WWMG_Source` |
| 4 | Rename app2 to **ConsumerApplication** | `consumer_application/`, role `consumer_worker`, target_app `ConsumerApplication` |
| 5 | Connect a new front-facing application **MyCareHub** | `mycarehub/` portal: own API key, `care_request` workflow, signed callbacks, member notifications |
| 6 | Document with step-by-step launch commands and API documentation | [RUNBOOK.md](RUNBOOK.md), [API_REFERENCE.md](API_REFERENCE.md), `docs/openapi/*.json`, PDF versions |
| 7 | Toggle between the black and red theme and a light theme | Console top-right toggle (remembered per browser); MyCareHub has the same toggle |
| 8 | Rebuild everything and push to a new repository **IntLayerV2.0** | This repository |

## Verification

| Check | Result |
|---|---|
| Automated tests (PostgreSQL 16 + BigQuery emulator) | 46 passed: database functions, IL API, BigQuery store (lifecycle, retries, exclusive concurrent claims, reaper, callbacks), IL routing to BigQuery, BigQuery worker, BigQuery → warehouse, jobs, templating, care_request rules |
| End-to-end smoke test (`scripts/smoke_test.py`) | All checks passed with every service running (both sources, both backends, callbacks, retries, idempotency, live backend switch, console) |
| UI review | Console (dark and light), MyCareHub (light, dark, mobile) rendered in Chrome with no console errors |

## Design decisions

- **PostgreSQL stays the control plane** (registry, routing, idempotency, API log, console, warehouse,
  NOTIFY). BigQuery is an additional data plane chosen per workflow. This gives BigQuery workflows the
  same guarantees (idempotency, ownership, callbacks, audit) without distributed transactions.
- **BigQuery as a queue** uses token-stamped UPDATEs. BigQuery serialises conflicting DML on a table,
  so claims are exclusive; it suits the stated volume (500–1,000 requests/day) but not high-throughput queues.
- **Local BigQuery** runs on the open-source emulator; `docker-compose.gcp.yml` switches to real BigQuery.

## Roadmap

| Priority | Item |
|---|---|
| High | Run the stack on real BigQuery in a GCP project and tune poll intervals for cost |
| High | TLS termination in front of il-api and MyCareHub; authentication for the console and MyCareHub (SSO) |
| Medium | Secrets from a vault (instead of `.env`); rotate API keys |
| Medium | Metrics endpoint (Prometheus) and alerts: oldest PENDING, failure rate, callback failures, BigQuery latency |
| Medium | Backups: PostgreSQL PITR; BigQuery table snapshots |
| Low | Move the warehouse to BigQuery (and load PostgreSQL requests into it) if analytics grows |
| Low | CI pipeline running the test suite with postgres and emulator service containers |
