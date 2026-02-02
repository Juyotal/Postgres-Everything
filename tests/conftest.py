from __future__ import annotations

import os

import pytest

TEST_DSN = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/pg_everything_test",
)


@pytest.fixture(scope="session")
def pg():
    from postgres_everything import PostgresEverything

    client = PostgresEverything(TEST_DSN)
    client.init()
    yield client
    client.close()


@pytest.fixture(autouse=True)
def clean_tables(pg):
    """Truncate all feature tables between tests for isolation."""
    with pg._pool.connection() as conn:
        with conn.cursor() as cur:
            for table in [
                "pg_documents",
                "pg_jobs",
                "pg_search_documents",
                "pg_vectors",
                "pg_cache",
            ]:
                cur.execute(f"TRUNCATE {table} RESTART IDENTITY CASCADE")  # noqa: S608
    yield
