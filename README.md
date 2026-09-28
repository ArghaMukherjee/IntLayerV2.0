# Integration Layer V2.0

**WWMG_Source** and **MyCareHub** → **Integration Layer** → **PostgreSQL or BigQuery** (chosen per workflow)
→ **ConsumerApplication** → results by polling or signed callback, with a **data warehouse**, an
**operations console** (black and red / light theme) and a member-facing **MyCareHub** portal.

| Document | Contents |
|---|---|
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | **Start here**: step-by-step commands to launch, verify, operate and stop everything |
| [docs/API_REFERENCE.md](docs/API_REFERENCE.md) | Every API: Integration Layer, ConsumerApplication, WWMG_Source, MyCareHub, console backend |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Architecture, PostgreSQL vs BigQuery, data model, security, deployment |
| [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md) | Requirements traceability, verification, roadmap |
| `docs/*.pdf` | PDF versions of the architecture and of the runbook with the API reference |
| `docs/openapi/*.json` | OpenAPI specs of all five services |

## Quick start

```bash
cp .env.example .env              # set every password and key (see docs/RUNBOOK.md §2)
docker compose up -d --build      # 9 containers incl. the BigQuery emulator
python3 scripts/smoke_test.py     # end-to-end check
```

| Application | URL |
|---|---|
| Console | http://localhost:8080 |
| MyCareHub | http://localhost:8003 |
| Integration Layer API docs | http://localhost:8000/docs |

## Services

| Service | Port | Code |
|---|---|---|
| postgres | 5432 (localhost) | `db/` migrations |
| bigquery (emulator) | 9050 (localhost) | dataset created by `migrate` |
| il-api | 8000 | `integration_layer/` |
| il-scheduler | – | `integration_layer/scheduler.py` |
| consumer-application | 8001 | `consumer_application/` |
| wwmg-source | 8002 | `wwmg_source/` |
| mycarehub | 8003 | `mycarehub/` |
| console | 8080 (localhost) | `console/` |
| migrate (one-shot) | – | `integration_layer/migrate.py` |

Shared code lives in `common/ilcommon/` (BigQuery store, Integration Layer client, payload templating).
Real Google BigQuery: see `docker-compose.gcp.yml` and RUNBOOK §8.
