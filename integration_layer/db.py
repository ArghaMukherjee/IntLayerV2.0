from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .config import Settings


def create_pool(settings: Settings) -> AsyncConnectionPool:
    # autocommit: every call is a single SQL function call and its own transaction
    return AsyncConnectionPool(
        settings.database_url,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
