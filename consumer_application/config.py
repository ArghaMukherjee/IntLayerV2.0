import os
import socket

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """ConsumerApplication settings, read from CONSUMER_* environment variables (BigQuery: BQ_*)."""

    model_config = SettingsConfigDict(env_prefix="CONSUMER_", extra="ignore")

    database_url: str = "postgresql://consumer_app:consumer_app@localhost:5432/integration"
    db_pool_max: int = 10
    target_app: str = "ConsumerApplication"
    backends: str = "postgres,bigquery"   # which queues to consume
    worker_id: str = Field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    worker_host: str = Field(default_factory=socket.gethostname)
    worker_enabled: bool = True
    max_concurrency: int = 5              # requests processed in parallel, per backend
    lease_seconds: int = 120              # extended by heartbeats while processing
    poll_seconds: float = 5               # PostgreSQL fallback poll when no NOTIFY arrives
    bq_poll_seconds: float = 5            # BigQuery poll (use 15-30 s against real BigQuery to limit query cost)
    schema_cache_seconds: float = 60

    def backend_list(self) -> list[str]:
        return [b.strip() for b in self.backends.split(",") if b.strip()]
