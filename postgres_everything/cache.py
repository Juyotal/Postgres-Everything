from __future__ import annotations

import logging
from typing import Any, Callable

from psycopg.types.json import Jsonb

from postgres_everything.base import PostgresModule
from postgres_everything.connection import ConnectionPool

logger = logging.getLogger("postgres_everything.cache")


class Cache(PostgresModule):
    """Redis-style key/value cache backed by PostgreSQL.

    Values are stored as JSONB, so any JSON-serialisable Python object is
    supported.  Expiry is lazy (checked on read) plus an explicit
    ``cleanup_expired()`` method for periodic sweeps.

    Args:
        pool: Shared connection pool.
        dsn: PostgreSQL DSN (creates a private pool when pool is omitted).
    """

    def __init__(
        self,
        pool: ConnectionPool | None = None,
        dsn: str | None = None,
    ) -> None:
        super().__init__(pool=pool, dsn=dsn)

    def get(self, key: str) -> Any | None:
        """Return the value for ``key``, or ``None`` if missing or expired.

        Expired entries are deleted lazily during this call.

        Args:
            key: Cache key.

        Returns:
            The stored value, or None.
        """
        row = self._fetch_one(
            """
            SELECT value, expires_at
            FROM pg_cache
            WHERE key = %s
            """,
            (key,),
        )
        if row is None:
            return None

        if row["expires_at"] is not None:
            # Check expiry using a DB-side query to avoid clock skew.
            expired = self._fetch_scalar(
                "SELECT %s < NOW()",
                (row["expires_at"],),
            )
            if expired:
                self.delete(key)
                return None

        # psycopg3 automatically deserialises JSONB columns to Python objects.
        return row["value"]

    def set(self, key: str, value: Any, *, ttl: int | None = None) -> None:
        """Store ``value`` under ``key``, optionally with a TTL.

        Args:
            key: Cache key.
            value: Any JSON-serialisable object.
            ttl: Time-to-live in seconds. ``None`` means no expiration.
        """
        expires_at_expr = (
            "NOW() + %s * interval '1 second'" if ttl is not None else "NULL"
        )
        params: tuple
        if ttl is not None:
            params = (key, Jsonb(value), ttl)
        else:
            params = (key, Jsonb(value))

        self._execute(
            f"""
            INSERT INTO pg_cache (key, value, expires_at)
            VALUES (%s, %s, {expires_at_expr})
            ON CONFLICT (key) DO UPDATE
                SET value      = EXCLUDED.value,
                    expires_at = EXCLUDED.expires_at,
                    created_at = NOW()
            """,  # noqa: S608
            params,
        )
        logger.debug("Cache set key='%s' ttl=%s", key, ttl)

    def delete(self, key: str) -> bool:
        """Remove a key from the cache.

        Args:
            key: Cache key to remove.

        Returns:
            True if the key existed and was deleted, False otherwise.
        """
        return self._execute("DELETE FROM pg_cache WHERE key = %s", (key,)) > 0

    def exists(self, key: str) -> bool:
        """Return True if ``key`` exists and has not expired.

        Args:
            key: Cache key.
        """
        result = self._fetch_scalar(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_cache
                WHERE key = %s
                  AND (expires_at IS NULL OR expires_at > NOW())
            )
            """,
            (key,),
        )
        return bool(result)

    def clear(self) -> int:
        """Delete every entry in the cache.

        Returns:
            Number of entries deleted.
        """
        return self._execute("DELETE FROM pg_cache")

    def cleanup_expired(self) -> int:
        """Delete all expired entries.  Meant to be run periodically.

        Returns:
            Number of entries deleted.
        """
        count = self._execute(
            "DELETE FROM pg_cache WHERE expires_at IS NOT NULL AND expires_at <= NOW()"
        )
        logger.info("Cleaned up %d expired cache entries", count)
        return count

    def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        *,
        ttl: int | None = None,
    ) -> Any:
        """Return cached value, or call ``factory`` to compute and store it.

        Args:
            key: Cache key.
            factory: Callable that returns the value when the cache misses.
            ttl: TTL in seconds for the newly stored entry.

        Returns:
            The cached or freshly computed value.
        """
        value = self.get(key)
        if value is None:
            value = factory()
            self.set(key, value, ttl=ttl)
        return value

    def incr(self, key: str, amount: int = 1) -> int:
        """Atomically increment a counter, initialising to 0 if absent.

        Args:
            key: Cache key for the counter.
            amount: Amount to add (can be negative for decrement).

        Returns:
            The new integer value.
        """
        row = self._fetch_one(
            """
            INSERT INTO pg_cache (key, value)
            VALUES (%s, to_jsonb(%s::int))
            ON CONFLICT (key) DO UPDATE
                SET value = to_jsonb((pg_cache.value::text::int + %s))
            RETURNING value
            """,
            (key, amount, amount),
        )
        assert row is not None
        # psycopg3 auto-deserialises JSONB; value is already an int.
        return int(row["value"])
