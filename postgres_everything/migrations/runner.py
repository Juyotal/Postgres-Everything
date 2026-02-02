from __future__ import annotations

import logging
from pathlib import Path

from postgres_everything.connection import ConnectionPool
from postgres_everything.exceptions import MigrationError

logger = logging.getLogger("postgres_everything.migrations")

_MIGRATIONS_DIR = Path(__file__).parent

_MODULE_MAP: dict[str, str] = {
    "documents": "001_documents.sql",
    "queue": "002_queue.sql",
    "search": "003_search.sql",
    "vectors": "004_vectors.sql",
    "cache": "005_cache.sql",
}

_TRACKING_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pg_everything_migrations (
    name       TEXT        PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def run_migrations(pool: ConnectionPool, modules: list[str] | None = None) -> None:
    """Run SQL migration files in order, skipping already-applied ones.

    Args:
        pool: An open connection pool.
        modules: List of module names to migrate (e.g. ``["documents", "cache"]``).
            When ``None``, all modules are migrated.

    Raises:
        MigrationError: If a SQL file cannot be read or a statement fails.
        ValueError: If an unknown module name is given.
    """
    if modules is None:
        files = list(_MODULE_MAP.items())
    else:
        unknown = set(modules) - _MODULE_MAP.keys()
        if unknown:
            raise ValueError(f"Unknown module(s): {', '.join(sorted(unknown))}")
        files = [(m, _MODULE_MAP[m]) for m in modules]

    with pool.connection() as conn:
        conn.execute(_TRACKING_TABLE_SQL)

    for module_name, filename in files:
        migration_name = filename
        already_applied = _is_applied(pool, migration_name)
        if already_applied:
            logger.debug("Migration %s already applied, skipping", migration_name)
            continue

        sql_path = _MIGRATIONS_DIR / filename
        try:
            sql = sql_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise MigrationError(f"Cannot read migration file {sql_path}: {exc}") from exc

        logger.info("Applying migration %s", migration_name)
        try:
            with pool.connection() as conn:
                conn.execute(sql)
                conn.execute(
                    "INSERT INTO pg_everything_migrations (name) VALUES (%s) ON CONFLICT DO NOTHING",
                    (migration_name,),
                )
        except Exception as exc:
            raise MigrationError(f"Migration {migration_name} failed: {exc}") from exc

        logger.info("Migration %s applied", migration_name)


def _is_applied(pool: ConnectionPool, name: str) -> bool:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_everything_migrations WHERE name = %s",
                (name,),
            )
            return cur.fetchone() is not None
