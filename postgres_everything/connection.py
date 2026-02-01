from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

import psycopg
import psycopg_pool

logger = logging.getLogger("postgres_everything.connection")


def _configure_connection(conn: psycopg.Connection) -> None:
    """Register pgvector type adapters on every new connection from the pool."""
    try:
        from pgvector.psycopg import register_vector

        register_vector(conn)
    except ImportError:
        logger.debug("pgvector not installed; vector adapter not registered")


class ConnectionPool:
    """Wraps psycopg_pool.ConnectionPool with sensible defaults.

    Args:
        dsn: PostgreSQL connection string.
        min_size: Minimum number of connections kept open.
        max_size: Maximum number of connections allowed.
    """

    def __init__(self, dsn: str, min_size: int = 2, max_size: int = 10) -> None:
        self._dsn = dsn
        self._pool = psycopg_pool.ConnectionPool(
            dsn,
            min_size=min_size,
            max_size=max_size,
            configure=_configure_connection,
            open=True,
        )
        logger.info("Connection pool opened (min=%d, max=%d)", min_size, max_size)

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        """Context manager that yields a pooled connection.

        Commits on clean exit, rolls back on exception.
        """
        with self._pool.connection() as conn:
            yield conn

    def raw_connection(self, *, autocommit: bool = False) -> psycopg.Connection:
        """Open a dedicated connection outside the pool.

        Callers are responsible for closing it. Useful for LISTEN/NOTIFY
        which needs to hold a connection open indefinitely.

        Args:
            autocommit: If True, the connection runs in autocommit mode.
        """
        return psycopg.connect(self._dsn, autocommit=autocommit)

    def close(self) -> None:
        """Close the connection pool and release all connections."""
        self._pool.close()
        logger.info("Connection pool closed")
