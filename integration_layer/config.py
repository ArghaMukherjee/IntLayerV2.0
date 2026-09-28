import socket

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Integration Layer settings, read from IL_* environment variables (BigQuery: BQ_*, see ilcommon)."""

    model_config = SettingsConfigDict(env_prefix="IL_", extra="ignore")

    database_url: str = "postgresql://il_api:il_api@localhost:5432/integration"
    db_pool_min: int = 2
    db_pool_max: int = 10

    # Comma-separated "<source_app>:<api_key>" pairs, e.g. "WWMG_Source:s3cret,MyCareHub:0ther"
    api_keys: str = ""
    admin_api_key: str = ""

    instance_id: str = Field(default_factory=socket.gethostname)
    max_payload_bytes: int = 1_000_000
    schema_cache_seconds: float = 30

    # Callback delivery to App1
    callbacks_enabled: bool = True
    callback_signing_secret: str = ""
    callback_allowed_hosts: str = ""          # comma-separated; empty = any host
    callback_timeout_seconds: float = 10
    callback_max_attempts: int = 6
    callback_poll_seconds: float = 5
    callback_batch_size: int = 20
    callback_lease_seconds: int = 60
    # BigQuery has no NOTIFY: due callbacks there are also polled at this interval
    bq_callback_poll_seconds: float = 5

    def api_key_map(self) -> dict[str, str]:
        """Return {api_key: source_app}."""
        pairs = (item.split(":", 1) for item in self.api_keys.split(",") if ":" in item)
        return {key.strip(): app.strip() for app, key in pairs if key.strip() and app.strip()}

    def allowed_callback_hosts(self) -> set[str]:
        return {h.strip().lower() for h in self.callback_allowed_hosts.split(",") if h.strip()}
