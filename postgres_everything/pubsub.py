from __future__ import annotations

import json
import logging
import signal
from collections.abc import Iterator
from typing import Any, Callable

import psycopg
from psycopg import sql

from postgres_everything.base import PostgresModule
from postgres_everything.connection import ConnectionPool

logger = logging.getLogger("postgres_everything.pubsub")


class PubSub(PostgresModule):
    """Redis Pub/Sub–style messaging backed by PostgreSQL LISTEN/NOTIFY.

    NOTIFY delivers messages to all currently-listening connections on the
    channel.  Messages are transient — if no consumer is listening when
    NOTIFY fires, the message is lost (unlike a queue).

    ``publish`` uses a pooled connection (fire and forget).
    ``subscribe`` and ``listen`` use a *dedicated* connection held open for
    the duration because LISTEN requires the connection to stay alive.

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

    # ------------------------------------------------------------------
    # Publisher
    # ------------------------------------------------------------------

    def publish(self, channel: str, message: str | dict) -> None:
        """Send a notification on ``channel``.

        Args:
            channel: PostgreSQL NOTIFY channel name.
            message: String payload, or a dict (auto-serialised to JSON).
        """
        payload = json.dumps(message) if isinstance(message, dict) else str(message)
        notify_sql = sql.SQL("NOTIFY {channel}, {payload}").format(
            channel=sql.Identifier(channel),
            payload=sql.Literal(payload),
        )
        self._execute(notify_sql)
        logger.debug("Published to channel '%s'", channel)

    # ------------------------------------------------------------------
    # Subscriber — blocking loop
    # ------------------------------------------------------------------

    def subscribe(
        self,
        channels: list[str],
        callback: Callable[[str, str], None],
        *,
        timeout: float | None = None,
    ) -> None:
        """Block and call ``callback`` for every notification received.

        Uses a dedicated connection so the pool is not depleted.
        Handles SIGTERM and SIGINT for graceful shutdown.

        Args:
            channels: List of channel names to subscribe to.
            callback: Called with ``(channel, payload)`` for each notification.
            timeout: Stop after this many seconds without a notification.
                ``None`` blocks indefinitely until a signal is received.
        """
        running = True

        def _stop(signum: int, frame: Any) -> None:
            nonlocal running
            logger.info("PubSub received signal %d, shutting down…", signum)
            running = False

        old_sigterm = signal.signal(signal.SIGTERM, _stop)
        old_sigint = signal.signal(signal.SIGINT, _stop)

        try:
            with self._pool.raw_connection(autocommit=True) as conn:
                self._listen_channels(conn, channels)
                poll_secs = min(timeout, 1.0) if timeout is not None else 1.0
                elapsed = 0.0

                while running:
                    for notification in conn.notifies(timeout=poll_secs):
                        callback(notification.channel, notification.payload)
                    elapsed += poll_secs
                    if timeout is not None and elapsed >= timeout:
                        break
        finally:
            signal.signal(signal.SIGTERM, old_sigterm)
            signal.signal(signal.SIGINT, old_sigint)

    # ------------------------------------------------------------------
    # Subscriber — generator
    # ------------------------------------------------------------------

    def listen(self, channels: list[str]) -> Iterator[tuple[str, str]]:
        """Generator that yields ``(channel, payload)`` tuples indefinitely.

        Holds a dedicated connection open for the lifetime of the generator.

        Args:
            channels: Channel names to subscribe to.

        Yields:
            ``(channel, payload)`` tuples as notifications arrive.
        """
        with self._pool.raw_connection(autocommit=True) as conn:
            self._listen_channels(conn, channels)
            for notification in conn.notifies():
                yield notification.channel, notification.payload

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _listen_channels(conn: psycopg.Connection, channels: list[str]) -> None:
        for channel in channels:
            conn.execute(
                sql.SQL("LISTEN {}").format(sql.Identifier(channel))
            )
        logger.debug("Subscribed to channels: %s", channels)
