# Integration Layer V2.0: Runbook

Step-by-step commands to launch, verify, operate and stop every service. All commands run from the
repository root unless stated otherwise.

## 1. Prerequisites

| Requirement | Check | Notes |
|---|---|---|
| Docker Desktop 4.x (Engine 24+) with Compose v2 | `docker compose version` | Docker must be running |
| **At least 15 GB free disk** | `df -h ~` | Docker makes its disk read-only when the Mac is nearly full (see §10) |
| Free ports | `lsof -i :8000,8001,8002,8003,8080,5432,9050` | stop V1 first: `cd ../ && docker compose down` |
| Git | `git --version` | |
| Python 3.12+ (optional) | `python3 --version` | only for the smoke test and the automated tests |

## 2. Get the code and configure

```bash
git clone https://github.com/ArghaMukherjee/IntLayerV2.0.git
cd IntLayerV2.0

cp .env.example .env
# Replace every value. Quick way to generate secrets:
for k in POSTGRES_PASSWORD IL_API_DB_PASSWORD CONSUMER_DB_PASSWORD SCHEDULER_DB_PASSWORD \
         BI_READER_DB_PASSWORD CONSOLE_DB_PASSWORD CALLBACK_SIGNING_SECRET; do
  sed -i.bak "s/^$k=.*/$k=$(openssl rand -hex 16)/" .env
done
for k in WWMG_API_KEY MYCAREHUB_API_KEY ADMIN_API_KEY; do
  sed -i.bak "s/^$k=.*/$k=key-$(openssl rand -hex 16)/" .env
done
rm -f .env.bak
```

## 3. Build and launch

```bash
docker compose build                 # builds the 5 service images
docker compose up -d                 # starts postgres + bigquery, runs migrate once, then the services
docker compose ps                    # wait until everything is healthy / running
```

Start-up order is enforced: `postgres` (healthy) and `bigquery` → `migrate` (exits 0) → `il-api`,
`il-scheduler`, `consumer-application` → `wwmg-source`, `mycarehub`, `console` (after il-api is healthy).

Expected `docker compose ps`:

| Service | State |
|---|---|
| postgres | running (healthy) |
| bigquery | running |
| migrate | exited (0) |
| il-api, consumer-application, console | running (healthy) |
| il-scheduler, wwmg-source, mycarehub | running |

## 4. Verify

```bash
# 4.1 Health
curl -s localhost:8000/health/ready     # {"status":"ready","database":"ok","bigquery":{"ok":true,...}}
curl -s localhost:8001/health           # workers: {"postgres":true,"bigquery":true}
curl -s localhost:8002/health; curl -s localhost:8003/health; curl -s localhost:8080/health

# 4.2 Migrations and BigQuery bootstrap
docker compose logs migrate | tail -12  # "migrations complete (4 new)" and "BigQuery ... ready"

# 4.3 End-to-end smoke test (both sources, both backends, callbacks, retries, backend switch)
python3 -m pip install httpx
python3 scripts/smoke_test.py           # reads keys from .env; prints "RESULT: ALL PASSED"
```

## 5. Open the applications

| Application | URL | What to do |
|---|---|---|
| **Console** | http://localhost:8080 | Dashboard, Analytics, Jobs, Payloads & send, Requests, Telemetry, Workflows. Theme toggle top-right. |
| **MyCareHub** | http://localhost:8003 | Pick a member, request care, watch the status and notifications |
| Integration Layer API docs | http://localhost:8000/docs | Swagger UI (use the X-API-Key "Authorize" button) |
| ConsumerApplication API docs | http://localhost:8001/docs | |
| WWMG_Source API docs | http://localhost:8002/docs | |
| MyCareHub API docs | http://localhost:8003/docs | |
| PostgreSQL | `localhost:5432`, db `integration` | e.g. `docker compose exec postgres psql -U postgres -d integration` |
| BigQuery emulator | http://localhost:9050 | e.g. `bq --api http://localhost:9050 query ...` or the Python client |

## 6. Try it (5 minutes)

```bash
KEY=$(grep ^WWMG_API_KEY .env | cut -d= -f2)
# 6.1 Send an order as WWMG_Source (PostgreSQL workflow) and poll it
curl -s -X POST localhost:8000/v1/requests -H "X-API-Key: $KEY" -H 'Content-Type: application/json' -d '{
  "workflow_name":"order_validation","callback_url":"http://wwmg-source:8002/hooks/il",
  "payload":{"order_id":"ORD-1","customer_id":"C-1","currency":"EUR","items":[{"sku":"A","quantity":2,"unit_price":10}]}}'
curl -s localhost:8000/v1/requests/<request_id> -H "X-API-Key: $KEY"

# 6.2 A MyCareHub member request (BigQuery workflow)
curl -s -X POST localhost:8003/api/care-requests -H 'Content-Type: application/json' \
  -d '{"member_id":"M-1001","request_type":"appointment","urgency":"routine"}'
curl -s 'localhost:8003/api/notifications?member_id=M-1001'
```

In the console:

- **Jobs**: start "Steady order traffic" (PostgreSQL) and "MyCareHub member traffic" (BigQuery) with
  **Resume**; use **Run now** on the manual jobs; pause/resume each ConsumerApplication queue and watch
  PENDING build up and drain.
- **Payloads & send**: pick a sample, **Validate**, choose **Send as** WWMG_Source or MyCareHub, **Send now**;
  the response panel shows the status code, the response body, the backend and live progress.
- **Workflows**: **Edit schemas** → change **Storage backend** → save; new requests go to the other store.
- **Analytics**: filter by backend; **Refresh warehouse now** loads both backends immediately.

## 7. Day-2 operations

```bash
docker compose logs -f il-api consumer-application il-scheduler      # follow logs
docker compose restart consumer-application                          # restart one service
docker compose up -d --build console                                 # rebuild + redeploy one service after a code change
docker compose up -d --scale consumer-application=3                  # more consumers (remove its "8001:8001" port mapping first)
docker compose exec postgres psql -U postgres -d integration -c \
  "SELECT status, count(*) FROM integration.request_metadata GROUP BY 1"      # PostgreSQL queue by status
```

Pause or resume consumption from the command line:

```bash
curl -s -X POST 'localhost:8001/worker/pause?backend=bigquery'      # or postgres; omit for both
curl -s -X POST localhost:8001/worker/resume
```

Switch a workflow's backend from the command line:

```bash
ADMIN=$(grep ^ADMIN_API_KEY .env | cut -d= -f2)
curl -s localhost:8000/v1/workflows/order_validation -H "X-API-Key: $KEY" > wf.json
python3 - <<'EOF'
import json; w=json.load(open("wf.json"))
body={k:w[k] for k in ("target_app","description","input_schema","output_schema","max_retries","is_active")}
body["storage_backend"]="bigquery"; json.dump(body, open("wf.json","w"))
EOF
curl -s -X PUT localhost:8000/v1/admin/workflows/order_validation -H "X-API-Key: $ADMIN" -H 'Content-Type: application/json' -d @wf.json
```

## 8. Use real Google BigQuery instead of the emulator

1. In a GCP project: enable the **BigQuery API**.
2. Create a service account with **BigQuery Data Editor** and **BigQuery Job User** on the project
   (or Data Editor on the dataset only), and download a JSON key.
3. Save the key as `secrets/gcp-service-account.json` (the `secrets/` folder is git-ignored).
4. In `.env`: `BQ_PROJECT=<your-project-id>` and optionally `BQ_DATASET`, `BQ_LOCATION`.
5. Launch with the override:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.gcp.yml up -d --build
   docker compose logs migrate | tail -3    # creates the dataset + partitioned, clustered tables
   ```
6. Real BigQuery bills per query. The override polls every 20 s (`CONSUMER_BQ_POLL_SECONDS`), and idle
   pollers only run a cheap `SELECT … LIMIT 1` before any DML.

## 9. Automated tests (optional)

```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt
docker compose up -d postgres bigquery
TEST_PG_ADMIN_URL=postgresql://postgres:$(grep ^POSTGRES_PASSWORD .env | cut -d= -f2)@localhost:5432/postgres \
TEST_BQ_EMULATOR=http://localhost:9050 pytest            # 46 tests; without TEST_BQ_EMULATOR the BigQuery tests are skipped
```

The tests create and drop their own database, login users and BigQuery dataset.

## 10. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `read-only file system` from Docker; containers turn unhealthy | Mac disk almost full, Docker's virtual disk cannot grow | Free ≥ 15 GB (Trash, `~/Library/Caches`, Xcode `DerivedData`, `docker system prune`), then quit and reopen Docker Desktop |
| `bind: address already in use` | V1 stack or another app on the port | `docker compose down` in the V1 folder, or change the host port in `docker-compose.yml` |
| `migrate` exits 1 | database not ready, or a migration file edited after it was applied | `docker compose logs migrate`; migrations are checksummed, so add a new file instead of editing |
| il-api unhealthy | PostgreSQL unreachable or wrong password | check `.env`, `docker compose logs il-api` |
| BigQuery requests return 503 BACKEND_UNAVAILABLE | emulator not running / GCP credentials invalid | `docker compose ps bigquery`; `curl -s localhost:8000/health/ready` |
| BigQuery pages feel slow | the emulator executes queries one at a time (SQLite) | expected locally (about 1–3 s per query); real BigQuery is similar per statement but runs statements in parallel |
| Callbacks not arriving | callback host not allow-listed | `CALLBACK_ALLOWED_HOSTS=wwmg-source,mycarehub` |
| 401 from the IL | wrong key | keys are in `.env` (`WWMG_API_KEY`, `MYCAREHUB_API_KEY`, `ADMIN_API_KEY`) |

## 11. Stop and reset

```bash
docker compose stop          # stop, keep data
docker compose down          # remove containers, keep volumes (pgdata, bqdata)
docker compose down -v       # remove containers AND data (fresh start next time)
```
