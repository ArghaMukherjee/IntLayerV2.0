-- =============================================================================
-- Integration Layer: operational schema (PostgreSQL 16+)
--
-- Source apps (WWMG_Source, MyCareHub) --HTTP--> Integration Layer --> PostgreSQL | BigQuery <-- ConsumerApplication
-- This schema is the control plane (registry, routing, idempotency, API log) and the PostgreSQL data plane.
-- All state transitions go through the functions in this file, which enforce
-- the lifecycle PENDING -> IN_PROGRESS -> COMPLETED | FAILED (| CANCELLED).
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()

CREATE SCHEMA IF NOT EXISTS integration;

-- -----------------------------------------------------------------------------
-- Roles (group roles; attach real LOGIN users to them at deploy time)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'il_service')  THEN CREATE ROLE il_service  NOLOGIN; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'consumer_worker') THEN CREATE ROLE consumer_worker NOLOGIN; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dwh_etl')     THEN CREATE ROLE dwh_etl     NOLOGIN; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dwh_reader')  THEN CREATE ROLE dwh_reader  NOLOGIN; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'il_scheduler') THEN CREATE ROLE il_scheduler NOLOGIN; END IF;
END $$;

-- -----------------------------------------------------------------------------
-- Types
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
                   WHERE t.typname = 'request_status' AND n.nspname = 'integration') THEN
        CREATE TYPE integration.request_status AS ENUM
            ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'CANCELLED');
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 0. workflow_registry: allowed workflows and their JSON Schemas.
--    The IL validates the source app's input_json against input_schema;
--    ConsumerApplication validates its output_json against output_schema.
--    storage_backend decides where new requests of the workflow are stored and queued.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration.workflow_registry (
    workflow_name   text        PRIMARY KEY CHECK (workflow_name ~ '^[a-z][a-z0-9_]{1,62}$'),
    target_app      text        NOT NULL,
    description     text,
    input_schema    jsonb       NOT NULL,             -- JSON Schema (draft 2020-12)
    output_schema   jsonb,                            -- NULL = output not validated
    schema_version  integer     NOT NULL DEFAULT 1,   -- bumped whenever a schema changes
    max_retries     integer     NOT NULL DEFAULT 3 CHECK (max_retries BETWEEN 0 AND 20),
    storage_backend text        NOT NULL DEFAULT 'postgres' CHECK (storage_backend IN ('postgres', 'bigquery')),
    is_active       boolean     NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- 1. request_metadata: one row per request (the "metadata table")
--    Partitioned monthly by request_date.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration.request_metadata (
    request_id              uuid          NOT NULL DEFAULT gen_random_uuid(),
    request_date            date          NOT NULL DEFAULT current_date,
    correlation_id          uuid          NOT NULL DEFAULT gen_random_uuid(),
    idempotency_key         text,

    -- routing
    source_app              text          NOT NULL,              -- 'WWMG_Source', 'MyCareHub'
    target_app              text          NOT NULL,              -- 'ConsumerApplication'
    workflow_name           text          NOT NULL REFERENCES integration.workflow_registry,
    priority                smallint      NOT NULL DEFAULT 0,

    -- payloads
    input_json              jsonb         NOT NULL,
    input_schema_version    integer,                             -- registry version validated against
    output_json             jsonb,

    -- API call that created the request (all GET/POST calls are in api_call_log)
    api_url                 text          NOT NULL,
    http_method             text          NOT NULL CHECK (http_method IN ('GET','POST','PUT','PATCH','DELETE')),
    request_headers         jsonb,                               -- sensitive headers masked by IL
    callback_url            text,

    -- server-level details
    client_ip               inet,                                -- source app caller
    il_server_hostname      text,
    il_server_ip            inet,
    il_instance_id          text,                                -- pod / container id
    consumer_worker_host        text,
    consumer_worker_id          text,
    db_server_addr          inet          DEFAULT inet_server_addr(),

    -- status and workflow completion
    status                  integration.request_status NOT NULL DEFAULT 'PENDING',
    success_flag            boolean,                             -- NULL until terminal
    workflow_status         text,                                -- ConsumerApplication's completion status
    error_code              text,
    error_message           text,

    -- retry / lease
    retry_count             integer       NOT NULL DEFAULT 0,
    max_retries             integer       NOT NULL DEFAULT 3,
    next_attempt_at         timestamptz   NOT NULL DEFAULT now(),
    locked_by               text,
    locked_until            timestamptz,

    -- timestamps
    received_at             timestamptz   NOT NULL DEFAULT now(),
    picked_at               timestamptz,
    workflow_completed_at   timestamptz,
    processing_duration_ms  bigint,

    -- callback delivery to the source app (only when callback_url is set)
    callback_status         text          CHECK (callback_status IN ('PENDING','DELIVERED','FAILED')),
    callback_attempts       integer       NOT NULL DEFAULT 0,
    callback_next_attempt_at timestamptz,
    callback_locked_until   timestamptz,
    callback_last_error     text,
    callback_delivered_at   timestamptz,

    created_at             timestamptz   NOT NULL DEFAULT now(),
    updated_at              timestamptz   NOT NULL DEFAULT now(),

    PRIMARY KEY (request_id, request_date),
    CONSTRAINT chk_success_flag CHECK (
        (status IN ('PENDING','IN_PROGRESS') AND success_flag IS NULL)
     OR (status = 'COMPLETED' AND success_flag = true)
     OR (status IN ('FAILED','CANCELLED') AND success_flag = false)
    )
) PARTITION BY RANGE (request_date);

COMMENT ON TABLE  integration.request_metadata IS 'One row per integration request from a source app to ConsumerApplication: payloads, API/server metadata and lifecycle status.';
COMMENT ON COLUMN integration.request_metadata.success_flag    IS 'TRUE = COMPLETED, FALSE = FAILED/CANCELLED, NULL = still in flight.';
COMMENT ON COLUMN integration.request_metadata.workflow_status IS 'Workflow completion status reported by ConsumerApplication (free text, e.g. APPROVED/REJECTED).';

CREATE TABLE IF NOT EXISTS integration.request_metadata_default
    PARTITION OF integration.request_metadata DEFAULT;

-- Queue pick-up index (ConsumerApplication claim query)
CREATE INDEX IF NOT EXISTS ix_rm_pending
    ON integration.request_metadata (target_app, priority DESC, next_attempt_at)
    WHERE status = 'PENDING';
-- Reaper index
CREATE INDEX IF NOT EXISTS ix_rm_inprogress_lease
    ON integration.request_metadata (locked_until)
    WHERE status = 'IN_PROGRESS';
-- Callback dispatcher index
CREATE INDEX IF NOT EXISTS ix_rm_callback_due
    ON integration.request_metadata (callback_next_attempt_at)
    WHERE callback_status = 'PENDING';
CREATE INDEX IF NOT EXISTS ix_rm_request_id     ON integration.request_metadata (request_id);
CREATE INDEX IF NOT EXISTS ix_rm_correlation_id ON integration.request_metadata (correlation_id);
CREATE INDEX IF NOT EXISTS ix_rm_updated_at     ON integration.request_metadata (updated_at);  -- DWH incremental load
CREATE INDEX IF NOT EXISTS ix_rm_status_date    ON integration.request_metadata (status, request_date);
CREATE INDEX IF NOT EXISTS ix_rm_input_gin      ON integration.request_metadata USING gin (input_json jsonb_path_ops);

-- Monthly partition helper
CREATE OR REPLACE FUNCTION integration.ensure_month_partition(p_month date)
RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
DECLARE
    v_start date := date_trunc('month', p_month)::date;
    v_end   date := (date_trunc('month', p_month) + interval '1 month')::date;
    v_name  text := format('request_metadata_%s', to_char(v_start, 'YYYY_MM'));
BEGIN
    IF to_regclass('integration.' || v_name) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE integration.%I PARTITION OF integration.request_metadata FOR VALUES FROM (%L) TO (%L)',
            v_name, v_start, v_end);
    END IF;
    RETURN v_name;
END $$;

-- Current month plus the next 2 months (schedule this monthly via pg_cron)
SELECT integration.ensure_month_partition((current_date + make_interval(months => m))::date)
FROM generate_series(0, 2) AS m;

-- -----------------------------------------------------------------------------
-- 2. idempotency_keys: global uniqueness (partitioned tables can't enforce
--    uniqueness on columns that exclude the partition key)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration.idempotency_keys (
    source_app       text        NOT NULL,
    idempotency_key  text        NOT NULL,
    request_id       uuid        NOT NULL,
    request_date     date        NOT NULL,
    storage_backend  text        NOT NULL DEFAULT 'postgres',
    created_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_app, idempotency_key)
);

-- -----------------------------------------------------------------------------
-- 2b. request_routes: where each BigQuery-stored request lives, so it can be
--     found by id and owned by its source app without scanning BigQuery.
--     (PostgreSQL-stored requests are found in request_metadata directly.)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration.request_routes (
    request_id       uuid        PRIMARY KEY,
    storage_backend  text        NOT NULL CHECK (storage_backend IN ('postgres', 'bigquery')),
    source_app       text        NOT NULL,
    workflow_name    text        NOT NULL,
    received_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_routes_source ON integration.request_routes (source_app, received_at DESC);

-- -----------------------------------------------------------------------------
-- 3. api_call_log: every GET / POST handled by (or made by) the IL
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration.api_call_log (
    call_id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_id           uuid,                          -- NULL if rejected before insert
    correlation_id       uuid,
    direction            text        NOT NULL CHECK (direction IN ('INBOUND','OUTBOUND')),
    http_method          text        NOT NULL,
    api_url              text        NOT NULL,
    endpoint             text,                          -- route template, e.g. /v1/requests/{id}
    query_params         jsonb,
    request_headers      jsonb,
    request_body_bytes   integer,
    response_status_code integer,
    response_body        jsonb,                         -- optional; error bodies at minimum
    latency_ms           integer,
    client_ip            inet,
    user_agent           text,
    server_hostname      text,
    server_ip            inet,
    instance_id          text,
    called_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_acl_request_id ON integration.api_call_log (request_id);
CREATE INDEX IF NOT EXISTS ix_acl_called_at  ON integration.api_call_log USING brin (called_at);

-- -----------------------------------------------------------------------------
-- 4. request_event_log: audit trail of every status transition (trigger-fed)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration.request_event_log (
    event_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_id      uuid        NOT NULL,
    request_date    date        NOT NULL,
    old_status      integration.request_status,
    new_status      integration.request_status NOT NULL,
    retry_count     integer,
    actor           text,                               -- worker id / 'il' / 'reaper'
    error_code      text,
    error_message   text,
    event_at        timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (request_id, request_date)
        REFERENCES integration.request_metadata (request_id, request_date) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_rel_request ON integration.request_event_log (request_id, event_at);

-- -----------------------------------------------------------------------------
-- Triggers
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION integration.trg_set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS set_updated_at ON integration.request_metadata;
CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON integration.request_metadata
    FOR EACH ROW EXECUTE FUNCTION integration.trg_set_updated_at();

DROP TRIGGER IF EXISTS set_updated_at ON integration.workflow_registry;
CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON integration.workflow_registry
    FOR EACH ROW EXECUTE FUNCTION integration.trg_set_updated_at();

-- Queue a callback to the source app whenever a request reaches a terminal state
CREATE OR REPLACE FUNCTION integration.trg_schedule_callback()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
       AND NEW.status IS DISTINCT FROM OLD.status
       AND NEW.callback_url IS NOT NULL THEN
        NEW.callback_status          := 'PENDING';
        NEW.callback_attempts        := 0;
        NEW.callback_next_attempt_at := now();
        NEW.callback_locked_until    := NULL;
        NEW.callback_last_error      := NULL;
        NEW.callback_delivered_at    := NULL;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS schedule_callback ON integration.request_metadata;
CREATE TRIGGER schedule_callback
    BEFORE UPDATE OF status ON integration.request_metadata
    FOR EACH ROW EXECUTE FUNCTION integration.trg_schedule_callback();

-- Audit + notifications on insert / status change
CREATE OR REPLACE FUNCTION integration.trg_status_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' OR NEW.status IS DISTINCT FROM OLD.status THEN
        INSERT INTO integration.request_event_log
            (request_id, request_date, old_status, new_status, retry_count, actor, error_code, error_message)
        VALUES
            (NEW.request_id, NEW.request_date,
             CASE WHEN TG_OP = 'UPDATE' THEN OLD.status END,
             NEW.status, NEW.retry_count,
             coalesce(nullif(current_setting('integration.actor', true), ''), NEW.locked_by, 'il'),
             NEW.error_code, NEW.error_message);

        IF NEW.status = 'PENDING' THEN
            PERFORM pg_notify('request_new', json_build_object(
                'request_id', NEW.request_id, 'target_app', NEW.target_app)::text);
        ELSIF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN
            PERFORM pg_notify('request_done', json_build_object(
                'request_id', NEW.request_id, 'status', NEW.status,
                'callback_url', NEW.callback_url)::text);
        END IF;
    END IF;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS status_change ON integration.request_metadata;
CREATE TRIGGER status_change
    AFTER INSERT OR UPDATE OF status ON integration.request_metadata
    FOR EACH ROW EXECUTE FUNCTION integration.trg_status_change();

-- -----------------------------------------------------------------------------
-- API functions (the only way state changes)
-- -----------------------------------------------------------------------------

-- IL: store a new request from a source app (idempotent).
-- target_app and max_retries come from workflow_registry; the IL has already
-- validated p_input_json against the schema version it passes in.
CREATE OR REPLACE FUNCTION integration.submit_request(
    p_source_app            text,
    p_workflow_name         text,
    p_input_json            jsonb,
    p_input_schema_version  integer,
    p_api_url               text,
    p_http_method           text     DEFAULT 'POST',
    p_request_headers       jsonb    DEFAULT NULL,
    p_client_ip             inet     DEFAULT NULL,
    p_il_server_hostname    text     DEFAULT NULL,
    p_il_server_ip          inet     DEFAULT NULL,
    p_il_instance_id        text     DEFAULT NULL,
    p_idempotency_key       text     DEFAULT NULL,
    p_correlation_id        uuid     DEFAULT NULL,
    p_callback_url          text     DEFAULT NULL,
    p_priority              smallint DEFAULT 0)
RETURNS TABLE (request_id uuid, request_date date, correlation_id uuid,
               status integration.request_status, is_duplicate boolean, storage_backend text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
#variable_conflict use_column
DECLARE
    v_id    uuid := gen_random_uuid();
    v_date  date := current_date;
    v_corr  uuid := coalesce(p_correlation_id, gen_random_uuid());
    v_wf    integration.workflow_registry%ROWTYPE;
    v_found uuid;
BEGIN
    SELECT * INTO v_wf FROM integration.workflow_registry w
    WHERE w.workflow_name = p_workflow_name AND w.is_active;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown or inactive workflow: %', p_workflow_name
            USING ERRCODE = 'no_data_found';
    END IF;

    IF p_idempotency_key IS NOT NULL THEN
        INSERT INTO integration.idempotency_keys (source_app, idempotency_key, request_id, request_date)
        VALUES (p_source_app, p_idempotency_key, v_id, v_date)
        ON CONFLICT (source_app, idempotency_key) DO NOTHING
        RETURNING idempotency_keys.request_id INTO v_found;

        IF v_found IS NULL THEN   -- duplicate: return the original (it may live in BigQuery)
            RETURN QUERY
            SELECT k.request_id, k.request_date, r.correlation_id, r.status, true, k.storage_backend
            FROM integration.idempotency_keys k
            LEFT JOIN integration.request_metadata r
              ON r.request_id = k.request_id AND r.request_date = k.request_date
            WHERE k.source_app = p_source_app AND k.idempotency_key = p_idempotency_key;
            RETURN;
        END IF;
    END IF;

    PERFORM set_config('integration.actor', 'il', true);

    INSERT INTO integration.request_metadata (
        request_id, request_date, correlation_id, idempotency_key,
        source_app, target_app, workflow_name, priority,
        input_json, input_schema_version, api_url, http_method, request_headers, callback_url,
        client_ip, il_server_hostname, il_server_ip, il_instance_id, max_retries)
    VALUES (
        v_id, v_date, v_corr, p_idempotency_key,
        p_source_app, v_wf.target_app, p_workflow_name, p_priority,
        p_input_json, p_input_schema_version, p_api_url, upper(p_http_method), p_request_headers,
        p_callback_url, p_client_ip, p_il_server_hostname, p_il_server_ip, p_il_instance_id,
        v_wf.max_retries);

    RETURN QUERY SELECT v_id, v_date, v_corr, 'PENDING'::integration.request_status, false, 'postgres'::text;
END $$;

-- IL: register a request that will be stored in BigQuery (idempotency + routing).
-- Returns the id to use; when is_duplicate, the id and backend of the original.
CREATE OR REPLACE FUNCTION integration.register_bq_request(
    p_request_id       uuid,
    p_source_app       text,
    p_workflow_name    text,
    p_idempotency_key  text DEFAULT NULL)
RETURNS TABLE (request_id uuid, is_duplicate boolean, storage_backend text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
#variable_conflict use_column
DECLARE
    v_found uuid;
BEGIN
    IF p_idempotency_key IS NOT NULL THEN
        INSERT INTO integration.idempotency_keys (source_app, idempotency_key, request_id, request_date, storage_backend)
        VALUES (p_source_app, p_idempotency_key, p_request_id, current_date, 'bigquery')
        ON CONFLICT (source_app, idempotency_key) DO NOTHING
        RETURNING idempotency_keys.request_id INTO v_found;
        IF v_found IS NULL THEN
            RETURN QUERY SELECT k.request_id, true, k.storage_backend FROM integration.idempotency_keys k
                         WHERE k.source_app = p_source_app AND k.idempotency_key = p_idempotency_key;
            RETURN;
        END IF;
    END IF;
    INSERT INTO integration.request_routes (request_id, storage_backend, source_app, workflow_name)
    VALUES (p_request_id, 'bigquery', p_source_app, p_workflow_name);
    RETURN QUERY SELECT p_request_id, false, 'bigquery'::text;
END $$;

-- IL: undo register_bq_request when the BigQuery insert failed
CREATE OR REPLACE FUNCTION integration.unregister_bq_request(p_request_id uuid)
RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
    DELETE FROM integration.idempotency_keys WHERE request_id = p_request_id AND storage_backend = 'bigquery';
    DELETE FROM integration.request_routes WHERE request_id = p_request_id;
$$;

-- ConsumerApplication: claim up to N pending requests (safe for many concurrent workers)
CREATE OR REPLACE FUNCTION integration.claim_requests(
    p_worker_id      text,
    p_worker_host    text,
    p_target_app     text    DEFAULT 'ConsumerApplication',
    p_batch_size     integer DEFAULT 10,
    p_lease_seconds  integer DEFAULT 300)
RETURNS TABLE (request_id uuid, request_date date, correlation_id uuid,
               workflow_name text, input_json jsonb, retry_count integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
#variable_conflict use_column
BEGIN
    PERFORM set_config('integration.actor', p_worker_id, true);

    RETURN QUERY
    WITH picked AS (
        SELECT r.request_id, r.request_date
        FROM integration.request_metadata r
        WHERE r.status = 'PENDING'
          AND r.target_app = p_target_app
          AND r.next_attempt_at <= now()
        ORDER BY r.priority DESC, r.next_attempt_at
        LIMIT p_batch_size
        FOR UPDATE SKIP LOCKED
    )
    UPDATE integration.request_metadata r
       SET status           = 'IN_PROGRESS',
           locked_by        = p_worker_id,
           locked_until     = now() + make_interval(secs => p_lease_seconds),
           consumer_worker_id   = p_worker_id,
           consumer_worker_host = p_worker_host,
           picked_at        = now()
      FROM picked p
     WHERE r.request_id = p.request_id AND r.request_date = p.request_date
    RETURNING r.request_id, r.request_date, r.correlation_id,
              r.workflow_name, r.input_json, r.retry_count;
END $$;

-- ConsumerApplication: extend the lease on a long-running job
CREATE OR REPLACE FUNCTION integration.heartbeat(
    p_request_id uuid, p_worker_id text, p_lease_seconds integer DEFAULT 300)
RETURNS boolean
LANGUAGE sql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
    UPDATE integration.request_metadata
       SET locked_until = now() + make_interval(secs => p_lease_seconds)
     WHERE request_id = p_request_id AND status = 'IN_PROGRESS' AND locked_by = p_worker_id
    RETURNING true;
$$;

-- ConsumerApplication: report successful workflow completion with output JSON
CREATE OR REPLACE FUNCTION integration.complete_request(
    p_request_id      uuid,
    p_worker_id       text,
    p_output_json     jsonb,
    p_workflow_status text DEFAULT 'COMPLETED')
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
BEGIN
    PERFORM set_config('integration.actor', p_worker_id, true);

    UPDATE integration.request_metadata
       SET status                 = 'COMPLETED',
           success_flag           = true,
           output_json            = p_output_json,
           workflow_status        = p_workflow_status,
           workflow_completed_at  = now(),
           processing_duration_ms = (extract(epoch FROM now() - picked_at) * 1000)::bigint,
           error_code             = NULL,
           error_message          = NULL,
           locked_by              = NULL,
           locked_until           = NULL
     WHERE request_id = p_request_id AND status = 'IN_PROGRESS' AND locked_by = p_worker_id;

    RETURN FOUND;   -- false => lease lost (reaped / claimed by someone else)
END $$;

-- ConsumerApplication: report failure; retries with exponential backoff while allowed
CREATE OR REPLACE FUNCTION integration.fail_request(
    p_request_id      uuid,
    p_worker_id       text,
    p_error_code      text,
    p_error_message   text,
    p_retryable       boolean DEFAULT true,
    p_output_json     jsonb   DEFAULT NULL,
    p_workflow_status text    DEFAULT 'FAILED')
RETURNS integration.request_status     -- new status, or NULL if lease was lost
LANGUAGE plpgsql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
DECLARE
    v_new integration.request_status;
BEGIN
    PERFORM set_config('integration.actor', p_worker_id, true);

    UPDATE integration.request_metadata r
       SET status        = CASE WHEN p_retryable AND r.retry_count < r.max_retries
                                THEN 'PENDING' ELSE 'FAILED' END::integration.request_status,
           success_flag  = CASE WHEN p_retryable AND r.retry_count < r.max_retries
                                THEN NULL ELSE false END,
           retry_count   = CASE WHEN p_retryable AND r.retry_count < r.max_retries
                                THEN r.retry_count + 1 ELSE r.retry_count END,
           next_attempt_at = now() + least(interval '1 hour',
                                           interval '30 seconds' * power(2, r.retry_count)),
           workflow_status = CASE WHEN p_retryable AND r.retry_count < r.max_retries
                                  THEN r.workflow_status ELSE p_workflow_status END,
           workflow_completed_at = CASE WHEN p_retryable AND r.retry_count < r.max_retries
                                        THEN NULL ELSE now() END,
           processing_duration_ms = (extract(epoch FROM now() - r.picked_at) * 1000)::bigint,
           output_json   = coalesce(p_output_json, r.output_json),
           error_code    = p_error_code,
           error_message = p_error_message,
           locked_by     = NULL,
           locked_until  = NULL
     WHERE r.request_id = p_request_id AND r.status = 'IN_PROGRESS' AND r.locked_by = p_worker_id
    RETURNING r.status INTO v_new;

    RETURN v_new;
END $$;

-- Scheduler (pg_cron, every minute): recover requests whose worker died
CREATE OR REPLACE FUNCTION integration.reap_expired_leases()
RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
DECLARE
    v_count integer;
BEGIN
    PERFORM set_config('integration.actor', 'reaper', true);

    UPDATE integration.request_metadata r
       SET status        = CASE WHEN r.retry_count < r.max_retries
                                THEN 'PENDING' ELSE 'FAILED' END::integration.request_status,
           success_flag  = CASE WHEN r.retry_count < r.max_retries THEN NULL ELSE false END,
           retry_count   = CASE WHEN r.retry_count < r.max_retries
                                THEN r.retry_count + 1 ELSE r.retry_count END,
           workflow_completed_at = CASE WHEN r.retry_count < r.max_retries THEN NULL ELSE now() END,
           next_attempt_at = now(),
           error_code    = 'LEASE_EXPIRED',
           error_message = format('Worker %s did not finish before %s', r.locked_by, r.locked_until),
           locked_by     = NULL,
           locked_until  = NULL
     WHERE r.status = 'IN_PROGRESS' AND r.locked_until < now();

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END $$;

-- IL/admin: cancel a request that has not been picked up yet
CREATE OR REPLACE FUNCTION integration.cancel_request(p_request_id uuid, p_reason text DEFAULT NULL)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
BEGIN
    PERFORM set_config('integration.actor', 'il', true);
    UPDATE integration.request_metadata
       SET status = 'CANCELLED', success_flag = false,
           error_code = 'CANCELLED', error_message = p_reason
     WHERE request_id = p_request_id AND status = 'PENDING';
    RETURN FOUND;
END $$;

-- Admin: re-queue a FAILED request
CREATE OR REPLACE FUNCTION integration.replay_request(p_request_id uuid)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
BEGIN
    PERFORM set_config('integration.actor', 'admin-replay', true);
    UPDATE integration.request_metadata
       SET status = 'PENDING', success_flag = NULL, retry_count = 0,
           next_attempt_at = now(), workflow_completed_at = NULL,
           error_code = NULL, error_message = NULL
     WHERE request_id = p_request_id AND status = 'FAILED';
    RETURN FOUND;
END $$;

-- IL callback dispatcher: claim callbacks that are due (safe across IL replicas)
CREATE OR REPLACE FUNCTION integration.claim_callbacks(
    p_batch_size     integer DEFAULT 20,
    p_lease_seconds  integer DEFAULT 60)
RETURNS TABLE (request_id uuid, correlation_id uuid, source_app text, workflow_name text,
               callback_url text, callback_attempts integer,
               status integration.request_status, success_flag boolean, workflow_status text,
               output_json jsonb, error_code text, error_message text,
               received_at timestamptz, workflow_completed_at timestamptz)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
#variable_conflict use_column
BEGIN
    RETURN QUERY
    WITH due AS (
        SELECT r.request_id, r.request_date
        FROM integration.request_metadata r
        WHERE r.callback_status = 'PENDING'
          AND r.callback_next_attempt_at <= now()
          AND (r.callback_locked_until IS NULL OR r.callback_locked_until < now())
        ORDER BY r.callback_next_attempt_at
        LIMIT p_batch_size
        FOR UPDATE SKIP LOCKED
    )
    UPDATE integration.request_metadata r
       SET callback_locked_until = now() + make_interval(secs => p_lease_seconds),
           callback_attempts     = r.callback_attempts + 1
      FROM due d
     WHERE r.request_id = d.request_id AND r.request_date = d.request_date
    RETURNING r.request_id, r.correlation_id, r.source_app, r.workflow_name,
              r.callback_url, r.callback_attempts,
              r.status, r.success_flag, r.workflow_status, r.output_json,
              r.error_code, r.error_message, r.received_at, r.workflow_completed_at;
END $$;

-- IL callback dispatcher: record the outcome of one delivery attempt
CREATE OR REPLACE FUNCTION integration.record_callback_result(
    p_request_id    uuid,
    p_delivered     boolean,
    p_error         text    DEFAULT NULL,
    p_max_attempts  integer DEFAULT 6)
RETURNS text                                   -- new callback_status
LANGUAGE plpgsql SECURITY DEFINER SET search_path = integration, pg_temp AS $$
DECLARE
    v_status text;
BEGIN
    UPDATE integration.request_metadata r
       SET callback_status = CASE
               WHEN p_delivered                          THEN 'DELIVERED'
               WHEN r.callback_attempts >= p_max_attempts THEN 'FAILED'
               ELSE 'PENDING' END,
           callback_delivered_at    = CASE WHEN p_delivered THEN now() END,
           callback_next_attempt_at = CASE WHEN p_delivered THEN NULL
               ELSE now() + least(interval '30 minutes',
                                  interval '10 seconds' * power(2, r.callback_attempts - 1)) END,
           callback_last_error      = CASE WHEN p_delivered THEN NULL ELSE left(p_error, 2000) END,
           callback_locked_until    = NULL
     WHERE r.request_id = p_request_id AND r.callback_status = 'PENDING'
    RETURNING r.callback_status INTO v_status;
    RETURN v_status;
END $$;

-- Read model used by GET /v1/requests/{id}
CREATE OR REPLACE VIEW integration.v_request_status AS
SELECT request_id, request_date, correlation_id, idempotency_key, source_app, target_app,
       workflow_name, input_schema_version, input_json,
       status, success_flag, workflow_status, output_json, error_code, error_message,
       retry_count, max_retries, received_at, picked_at, workflow_completed_at,
       processing_duration_ms, callback_url, callback_status, callback_attempts,
       callback_delivered_at, callback_last_error
FROM integration.request_metadata;

-- -----------------------------------------------------------------------------
-- Privileges
-- -----------------------------------------------------------------------------
REVOKE ALL ON SCHEMA integration FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA integration FROM PUBLIC;

GRANT USAGE ON SCHEMA integration TO il_service, consumer_worker, dwh_etl, il_scheduler;

-- IL service
GRANT EXECUTE ON FUNCTION
    integration.submit_request(text,text,jsonb,integer,text,text,jsonb,inet,text,inet,text,text,uuid,text,smallint),
    integration.register_bq_request(uuid,text,text,text),
    integration.unregister_bq_request(uuid),
    integration.cancel_request(uuid,text),
    integration.replay_request(uuid),
    integration.claim_callbacks(integer,integer),
    integration.record_callback_result(uuid,boolean,text,integer)
  TO il_service;
GRANT SELECT ON integration.v_request_status, integration.request_routes TO il_service;
GRANT SELECT, INSERT, UPDATE ON integration.workflow_registry TO il_service;
GRANT INSERT, SELECT ON integration.api_call_log TO il_service;

-- ConsumerApplication: execute-only on the queue, read-only on workflow schemas (output validation)
GRANT EXECUTE ON FUNCTION
    integration.claim_requests(text,text,text,integer,integer),
    integration.heartbeat(uuid,text,integer),
    integration.complete_request(uuid,text,jsonb,text),
    integration.fail_request(uuid,text,text,text,boolean,jsonb,text)
  TO consumer_worker;
GRANT SELECT (workflow_name, target_app, output_schema, schema_version, is_active, storage_backend)
    ON integration.workflow_registry TO consumer_worker;

-- Scheduler: maintenance jobs
GRANT EXECUTE ON FUNCTION
    integration.reap_expired_leases(),
    integration.ensure_month_partition(date)
  TO il_scheduler;

-- DWH ETL: read-only on operational tables
GRANT SELECT ON ALL TABLES IN SCHEMA integration TO dwh_etl;
