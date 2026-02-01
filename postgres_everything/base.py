from __future__ import annotations

import logging
from typing import Any

from psycopg.rows import dict_row

from postgres_everything.connection import ConnectionPool

logger = logging.getLogger("postgres_everything.base")


class PostgresModule:
    """Base class all feature modules inherit from.

    Accepts either a shared ``ConnectionPool`` instance or a ``dsn`` string
    (in which case it creates and owns its own pool). This lets modules be
    used standalone without the unified client.

    Args:
        pool: Shared connection pool (preferred for the unified client).
        dsn: PostgreSQL DSN; a private pool is created when this is used.
    """

    def __init__(
        self,
        pool: ConnectionPool | None = None,
        dsn: str | None = None,
    ) -> None:
        if pool is not None:
            self._pool = pool
            self._owns_pool = False
        elif dsn is not None:
            self._pool = ConnectionPool(dsn)
            self._owns_pool = True
        else:
            raise ValueError("Either pool or dsn must be provided")

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _execute(self, query: Any, params: Any = None) -> int:
        """Execute a statement and return the number of affected rows.

        Args:
            query: SQL string or psycopg.sql.Composable.
            params: Query parameters.

        Returns:
            Number of rows affected (cursor.rowcount).
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.rowcount

    def _fetch_one(self, query: Any, params: Any = None) -> dict | None:
        """Execute a query and return the first row as a dict, or None.

        Args:
            query: SQL string or psycopg.sql.Composable.
            params: Query parameters.

        Returns:
            A dict with column-name keys, or None if no rows matched.
        """
        with self._pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params)
                return cur.fetchone()

    def _fetch_all(self, query: Any, params: Any = None) -> list[dict]:
        """Execute a query and return all rows as a list of dicts.

        Args:
            query: SQL string or psycopg.sql.Composable.
            params: Query parameters.

        Returns:
            List of dicts, one per row. Empty list if no rows matched.
        """
        with self._pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params)
                return cur.fetchall()

    def _fetch_scalar(self, query: Any, params: Any = None) -> Any:
        """Execute a query and return the first column of the first row.

        Args:
            query: SQL string or psycopg.sql.Composable.
            params: Query parameters.

        Returns:
            The raw scalar value, or None if no rows matched.
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                return row[0] if row is not None else None

    def close(self) -> None:
        """Close the pool if this module owns it."""
        if self._owns_pool:
            self._pool.close()
