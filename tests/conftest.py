from __future__ import annotations

import os

import pytest

TEST_DSN = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/pg_everything_test",
)

# Modules that do not require pgvector.
BASE_MODULES = ["documents", "queue", "search", "cache"]


def _pgvector_available(dsn: str) -> bool:
    """Return True if the pgvector extension is available on the server."""
    try:
        import psycopg

        with psycopg.connect(dsn) as conn:
            row = conn.execute(
                "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'"
            ).fetchone()
            return row is not None
    except Exception:
        return False


PGVECTOR_AVAILABLE = _pgvector_available(TEST_DSN)


@pytest.fixture(scope="session")
def pg():
    from postgres_everything import PostgresEverything

    client = PostgresEverything(TEST_DSN)
    modules = None if PGVECTOR_AVAILABLE else BASE_MODULES
    client.init(modules=modules)
    yield client
    client.close()


@pytest.fixture(autouse=True)
def clean_tables(pg):
    """Truncate all feature tables between tests for isolation."""
    tables = ["pg_documents", "pg_jobs", "pg_search_documents", "pg_cache"]
    if PGVECTOR_AVAILABLE:
        tables.append("pg_vectors")

    with pg._pool.connection() as conn:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"TRUNCATE {table} RESTART IDENTITY CASCADE")  # noqa: S608
    yield
