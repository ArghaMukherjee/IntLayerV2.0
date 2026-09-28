"""Applies db/*.sql in order, creates the service login users, and creates the BigQuery
dataset and tables (when BQ_ENABLED).

Runs once per deployment (the `migrate` container) with a superuser connection.
Each file is recorded in public.schema_migrations with its checksum; a file that
changes after it was applied stops the migration instead of being silently re-run.
"""
import hashlib
import logging
import sys
import time
from pathlib import Path

import psycopg
from psycopg import sql
from pydantic_settings import BaseSettings, SettingsConfigDict

from ilcommon.bigquery_store import BigQuerySettings, BigQueryStore

log = logging.getLogger("migrate")

# login user -> (password setting, group roles defined in db/*.sql)
LOGIN_USERS = {
    "il_api": ("il_api_password", ("il_service",)),
    "consumer_app": ("consumer_password", ("consumer_worker",)),
    "il_scheduler_user": ("scheduler_password", ("il_scheduler",)),
    "bi_reader": ("bi_reader_password", ("dwh_reader",)),
    "console_user": ("console_password", ("ops_viewer", "console_app")),
}


class MigrateSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MIGRATE_", extra="ignore")

    database_url: str = "postgresql://postgres:postgres@localhost:5432/integration"
    migrations_dir: Path = Path(__file__).resolve().parent.parent / "db"
    il_api_password: str | None = None
    consumer_password: str | None = None
    scheduler_password: str | None = None
    bi_reader_password: str | None = None
    console_password: str | None = None


def apply_migrations(conn: psycopg.Connection, migrations_dir: Path) -> list[str]:
    conn.execute("""CREATE TABLE IF NOT EXISTS public.schema_migrations (
                        filename text PRIMARY KEY, checksum text NOT NULL,
                        applied_at timestamptz NOT NULL DEFAULT now())""")
    applied_now = []
    for path in sorted(migrations_dir.glob("*.sql")):
        script = path.read_text()
        checksum = hashlib.sha256(script.encode()).hexdigest()
        row = conn.execute("SELECT checksum FROM public.schema_migrations WHERE filename = %s",
                           (path.name,)).fetchone()
        if row:
            if row[0] != checksum:
                raise RuntimeError(f"{path.name} changed after it was applied; add a new migration instead")
            continue
        with conn.transaction():
            conn.execute(script)
            conn.execute("INSERT INTO public.schema_migrations (filename, checksum) VALUES (%s, %s)",
                         (path.name, checksum))
        log.info("applied %s", path.name)
        applied_now.append(path.name)
    return applied_now


def ensure_login_users(conn: psycopg.Connection, settings: MigrateSettings) -> None:
    for user, (password_field, group_roles) in LOGIN_USERS.items():
        password = getattr(settings, password_field)
        if not password:
            log.info("skipping login user %s (no password configured)", user)
            continue
        exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,)).fetchone()
        verb = "ALTER" if exists else "CREATE"
        conn.execute(sql.SQL(verb + " ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(user), sql.Literal(password)))
        for role in group_roles:
            conn.execute(sql.SQL("GRANT {} TO {}").format(sql.Identifier(role), sql.Identifier(user)))
        log.info("login user %s ready (member of %s)", user, ", ".join(group_roles))


def run(settings: MigrateSettings) -> None:
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(hashtext('integration_layer_migrate'))")
        try:
            applied = apply_migrations(conn, settings.migrations_dir)
            ensure_login_users(conn, settings)
        finally:
            conn.execute("SELECT pg_advisory_unlock(hashtext('integration_layer_migrate'))")
    log.info("migrations complete (%d new)", len(applied))


def bootstrap_bigquery() -> None:
    settings = BigQuerySettings()
    if not settings.enabled:
        log.info("BigQuery disabled; skipping dataset bootstrap")
        return
    store = BigQueryStore(settings)
    for attempt in range(30):  # the emulator may still be starting
        try:
            created = store.ensure_schema()
            log.info("BigQuery %s ready (created: %s)", store.label, ", ".join(created) or "nothing, already present")
            return
        except Exception as exc:
            if attempt == 29:
                raise
            log.info("waiting for BigQuery (%s)", type(exc).__name__)
            time.sleep(2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        run(MigrateSettings())
        bootstrap_bigquery()
    except Exception:
        log.exception("migration failed")
        sys.exit(1)
