-- =============================================================================
-- Data warehouse layer: star schema fed from the integration schema
-- Grain of fact_request: one row per integration request (upserted until final).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS dwh;

-- -----------------------------------------------------------------------------
-- Dimensions
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key      integer PRIMARY KEY,          -- YYYYMMDD
    full_date     date    NOT NULL UNIQUE,
    year          smallint NOT NULL,
    quarter       smallint NOT NULL,
    month         smallint NOT NULL,
    month_name    text    NOT NULL,
    day_of_month  smallint NOT NULL,
    day_of_week   smallint NOT NULL,            -- ISO: 1 = Monday
    day_name      text    NOT NULL,
    iso_week      smallint NOT NULL,
    is_weekend    boolean NOT NULL
);

INSERT INTO dwh.dim_date
SELECT to_char(d, 'YYYYMMDD')::int, d,
       extract(year FROM d), extract(quarter FROM d), extract(month FROM d),
       trim(to_char(d, 'Month')), extract(day FROM d), extract(isodow FROM d),
       trim(to_char(d, 'Day')), extract(week FROM d), extract(isodow FROM d) IN (6, 7)
FROM generate_series(date '2024-01-01', date '2035-12-31', interval '1 day') AS g(d)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS dwh.dim_application (
    app_key   integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    app_code  text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_workflow (
    workflow_key   integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workflow_name  text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_server (
    server_key   integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    server_role  text NOT NULL,                  -- 'IL' | 'CONSUMER_WORKER'
    hostname     text NOT NULL,
    UNIQUE (server_role, hostname)
);

CREATE TABLE IF NOT EXISTS dwh.dim_status (
    status_key   smallint PRIMARY KEY,
    status_code  text    NOT NULL UNIQUE,
    is_terminal  boolean NOT NULL,
    is_success   boolean
);
INSERT INTO dwh.dim_status VALUES
    (1, 'PENDING',     false, NULL),
    (2, 'IN_PROGRESS', false, NULL),
    (3, 'COMPLETED',   true,  true),
    (4, 'FAILED',      true,  false),
    (5, 'CANCELLED',   true,  false)
ON CONFLICT DO NOTHING;

-- Unknown members so the fact table never needs NULL foreign keys
INSERT INTO dwh.dim_server (server_role, hostname) VALUES ('UNKNOWN', 'unknown')
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- Fact
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.fact_request (
    request_id          uuid     PRIMARY KEY,
    correlation_id      uuid     NOT NULL,
    date_key            integer  NOT NULL REFERENCES dwh.dim_date,
    source_app_key      integer  NOT NULL REFERENCES dwh.dim_application,
    target_app_key      integer  NOT NULL REFERENCES dwh.dim_application,
    workflow_key        integer  NOT NULL REFERENCES dwh.dim_workflow,
    il_server_key       integer  NOT NULL REFERENCES dwh.dim_server,
    worker_server_key   integer  NOT NULL REFERENCES dwh.dim_server,
    status_key          smallint NOT NULL REFERENCES dwh.dim_status,
    storage_backend     text     NOT NULL DEFAULT 'postgres',  -- where the request was stored and queued

    success_flag        boolean,
    workflow_status     text,
    error_code          text,
    retry_count         integer  NOT NULL,
    api_call_count      integer  NOT NULL DEFAULT 0,

    queue_wait_ms       bigint,                  -- received -> picked
    processing_ms       bigint,                  -- picked   -> completed
    end_to_end_ms       bigint,                  -- received -> completed
    input_bytes         integer,
    output_bytes        integer,

    received_at         timestamptz NOT NULL,
    completed_at        timestamptz,
    source_updated_at   timestamptz NOT NULL,
    etl_loaded_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fr_date     ON dwh.fact_request (date_key);
CREATE INDEX IF NOT EXISTS ix_fr_workflow ON dwh.fact_request (workflow_key, date_key);
CREATE INDEX IF NOT EXISTS ix_fr_status   ON dwh.fact_request (status_key);
CREATE INDEX IF NOT EXISTS ix_fr_backend  ON dwh.fact_request (storage_backend, received_at);

-- -----------------------------------------------------------------------------
-- ETL control
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.etl_watermark (
    job_name        text PRIMARY KEY,
    last_loaded_at  timestamptz NOT NULL,
    last_run_at     timestamptz,
    rows_loaded     integer
);
INSERT INTO dwh.etl_watermark (job_name, last_loaded_at)
VALUES ('fact_request', '-infinity'), ('fact_request_bigquery', '-infinity')
ON CONFLICT DO NOTHING;

-- Merges the rows in the temp table _chg (request_metadata-shaped) into the star schema.
-- Callers: dwh.load_incremental() for PostgreSQL, and il-scheduler's BigQuery loader, which
-- fills _chg from BigQuery (status and JSON columns may arrive as text; both work here).
-- Idempotent (upserts); advances the watermark of p_job to the newest updated_at merged.
CREATE OR REPLACE PROCEDURE dwh.merge_changes(p_backend text, p_job text)
LANGUAGE plpgsql AS $$
DECLARE
    v_to    timestamptz;
    v_rows  integer;
BEGIN
    SELECT max(updated_at) INTO v_to FROM _chg;

    INSERT INTO dwh.dim_application (app_code)
    SELECT source_app FROM _chg UNION SELECT target_app FROM _chg
    ON CONFLICT (app_code) DO NOTHING;

    INSERT INTO dwh.dim_workflow (workflow_name)
    SELECT DISTINCT workflow_name FROM _chg
    ON CONFLICT (workflow_name) DO NOTHING;

    INSERT INTO dwh.dim_server (server_role, hostname)
    SELECT 'IL', il_server_hostname FROM _chg WHERE il_server_hostname IS NOT NULL
    UNION
    SELECT 'CONSUMER_WORKER', consumer_worker_host FROM _chg WHERE consumer_worker_host IS NOT NULL
    ON CONFLICT (server_role, hostname) DO NOTHING;

    INSERT INTO dwh.fact_request AS f (
        request_id, correlation_id, date_key, source_app_key, target_app_key, workflow_key,
        il_server_key, worker_server_key, status_key, storage_backend, success_flag, workflow_status,
        error_code, retry_count, api_call_count, queue_wait_ms, processing_ms, end_to_end_ms,
        input_bytes, output_bytes, received_at, completed_at, source_updated_at)
    SELECT c.request_id, c.correlation_id,
           to_char(c.request_date, 'YYYYMMDD')::int,
           sa.app_key, ta.app_key, w.workflow_key,
           coalesce(ils.server_key, unk.server_key),
           coalesce(wks.server_key, unk.server_key),
           st.status_key, p_backend, c.success_flag, c.workflow_status, c.error_code, c.retry_count,
           (SELECT count(*) FROM integration.api_call_log a WHERE a.request_id = c.request_id),
           (extract(epoch FROM c.picked_at - c.received_at) * 1000)::bigint,
           c.processing_duration_ms,
           (extract(epoch FROM c.workflow_completed_at - c.received_at) * 1000)::bigint,
           octet_length(c.input_json::text),
           octet_length(c.output_json::text),
           c.received_at, c.workflow_completed_at, c.updated_at
    FROM _chg c
    JOIN dwh.dim_application sa ON sa.app_code = c.source_app
    JOIN dwh.dim_application ta ON ta.app_code = c.target_app
    JOIN dwh.dim_workflow    w  ON w.workflow_name = c.workflow_name
    JOIN dwh.dim_status      st ON st.status_code = c.status::text
    JOIN dwh.dim_server      unk ON unk.server_role = 'UNKNOWN'
    LEFT JOIN dwh.dim_server ils ON ils.server_role = 'IL'              AND ils.hostname = c.il_server_hostname
    LEFT JOIN dwh.dim_server wks ON wks.server_role = 'CONSUMER_WORKER' AND wks.hostname = c.consumer_worker_host
    ON CONFLICT (request_id) DO UPDATE SET
        worker_server_key = EXCLUDED.worker_server_key,
        status_key        = EXCLUDED.status_key,
        storage_backend   = EXCLUDED.storage_backend,
        success_flag      = EXCLUDED.success_flag,
        workflow_status   = EXCLUDED.workflow_status,
        error_code        = EXCLUDED.error_code,
        retry_count       = EXCLUDED.retry_count,
        api_call_count    = EXCLUDED.api_call_count,
        queue_wait_ms     = EXCLUDED.queue_wait_ms,
        processing_ms     = EXCLUDED.processing_ms,
        end_to_end_ms     = EXCLUDED.end_to_end_ms,
        output_bytes      = EXCLUDED.output_bytes,
        completed_at      = EXCLUDED.completed_at,
        source_updated_at = EXCLUDED.source_updated_at,
        etl_loaded_at     = now()
    WHERE f.source_updated_at < EXCLUDED.source_updated_at;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    UPDATE dwh.etl_watermark
       SET last_loaded_at = coalesce(v_to, last_loaded_at),
           last_run_at    = now(),
           rows_loaded    = v_rows
     WHERE job_name = p_job;
END $$;

-- Incremental load of PostgreSQL-stored requests. A 5-minute overlap on the watermark
-- picks up rows from transactions that committed late with an earlier updated_at.
CREATE OR REPLACE PROCEDURE dwh.load_incremental()
LANGUAGE plpgsql AS $$
DECLARE
    v_from timestamptz;
BEGIN
    SELECT last_loaded_at - interval '5 minutes' INTO v_from
    FROM dwh.etl_watermark WHERE job_name = 'fact_request' FOR UPDATE;

    CREATE TEMP TABLE _chg ON COMMIT DROP AS
    SELECT * FROM integration.request_metadata WHERE updated_at > v_from;

    CALL dwh.merge_changes('postgres', 'fact_request');
    DROP TABLE _chg;   -- the BigQuery loader may stage its own _chg in the same transaction
END $$;

-- -----------------------------------------------------------------------------
-- Reporting views
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW dwh.v_daily_workflow_summary AS
SELECT d.full_date,
       w.workflow_name,
       count(*)                                                     AS total_requests,
       count(*) FILTER (WHERE s.status_code = 'COMPLETED')          AS completed,
       count(*) FILTER (WHERE s.status_code = 'FAILED')             AS failed,
       count(*) FILTER (WHERE NOT s.is_terminal)                    AS in_flight,
       round(100.0 * count(*) FILTER (WHERE f.success_flag)
             / nullif(count(*) FILTER (WHERE s.is_terminal), 0), 2) AS success_rate_pct,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY f.end_to_end_ms) AS p50_end_to_end_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY f.end_to_end_ms) AS p95_end_to_end_ms,
       sum(f.retry_count)                                           AS total_retries
FROM dwh.fact_request f
JOIN dwh.dim_date     d ON d.date_key = f.date_key
JOIN dwh.dim_workflow w ON w.workflow_key = f.workflow_key
JOIN dwh.dim_status   s ON s.status_key = f.status_key
GROUP BY d.full_date, w.workflow_name;

-- -----------------------------------------------------------------------------
-- Privileges
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA dwh TO dwh_etl, dwh_reader;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA dwh TO dwh_etl;
GRANT EXECUTE ON PROCEDURE dwh.load_incremental(), dwh.merge_changes(text, text) TO dwh_etl;
GRANT SELECT ON ALL TABLES IN SCHEMA dwh TO dwh_reader;

-- The scheduler container runs dwh.load_incremental() as il_scheduler
GRANT dwh_etl TO il_scheduler;
