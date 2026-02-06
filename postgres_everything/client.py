from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from postgres_everything.connection import ConnectionPool
from postgres_everything.migrations.runner import run_migrations

if TYPE_CHECKING:
    from postgres_everything.cache import Cache
    from postgres_everything.documents import DocumentStore
    from postgres_everything.embeddings.base import EmbeddingProvider
    from postgres_everything.pubsub import PubSub
    from postgres_everything.queue import TaskQueue
    from postgres_everything.search import SearchEngine
    from postgres_everything.vectors import VectorStore

logger = logging.getLogger("postgres_everything.client")


class PostgresEverything:
    """Unified entry point that composes all postgres_everything modules.

    Modules are instantiated lazily on first property access and share a
    single :class:`~postgres_everything.connection.ConnectionPool`.

    Example::

        pg = PostgresEverything("postgresql://user:pass@localhost/mydb")
        pg.init()   # run migrations once

        pg.documents.insert("users", {"name": "Alice"})
        pg.cache.set("greeting", "hello", ttl=60)

    Args:
        dsn: PostgreSQL connection string.
        embedding_provider: Optional provider passed to :class:`VectorStore`.
        pool_size: Maximum connections in the shared pool.
    """

    def __init__(
        self,
        dsn: str,
        *,
        embedding_provider: EmbeddingProvider | None = None,
        pool_size: int = 10,
    ) -> None:
        self._pool = ConnectionPool(dsn, min_size=2, max_size=pool_size)
        self._embedding_provider = embedding_provider

        # Lazy-loaded module instances.
        self._documents: DocumentStore | None = None
        self._queue: TaskQueue | None = None
        self._search: SearchEngine | None = None
        self._vectors: VectorStore | None = None
        self._cache: Cache | None = None
        self._pubsub: PubSub | None = None

    # ------------------------------------------------------------------
    # Module properties (lazy initialisation)
    # ------------------------------------------------------------------

    @property
    def documents(self) -> DocumentStore:
        """Access the :class:`~postgres_everything.documents.DocumentStore`."""
        if self._documents is None:
            from postgres_everything.documents import DocumentStore

            self._documents = DocumentStore(pool=self._pool)
        return self._documents

    @property
    def queue(self) -> TaskQueue:
        """Access the :class:`~postgres_everything.queue.TaskQueue`."""
        if self._queue is None:
            from postgres_everything.queue import TaskQueue

            self._queue = TaskQueue(pool=self._pool)
        return self._queue

    @property
    def search(self) -> SearchEngine:
        """Access the :class:`~postgres_everything.search.SearchEngine`."""
        if self._search is None:
            from postgres_everything.search import SearchEngine

            self._search = SearchEngine(pool=self._pool)
        return self._search

    @property
    def vectors(self) -> VectorStore:
        """Access the :class:`~postgres_everything.vectors.VectorStore`."""
        if self._vectors is None:
            from postgres_everything.vectors import VectorStore

            self._vectors = VectorStore(
                pool=self._pool,
                embedding_provider=self._embedding_provider,
            )
        return self._vectors

    @property
    def cache(self) -> Cache:
        """Access the :class:`~postgres_everything.cache.Cache`."""
        if self._cache is None:
            from postgres_everything.cache import Cache

            self._cache = Cache(pool=self._pool)
        return self._cache

    @property
    def pubsub(self) -> PubSub:
        """Access the :class:`~postgres_everything.pubsub.PubSub`."""
        if self._pubsub is None:
            from postgres_everything.pubsub import PubSub

            self._pubsub = PubSub(pool=self._pool)
        return self._pubsub

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def init(self, modules: list[str] | None = None) -> None:
        """Run schema migrations.  Safe to call multiple times (idempotent).

        Args:
            modules: List of module names to migrate, e.g.
                ``["documents", "cache"]``.  ``None`` migrates all modules.
        """
        run_migrations(self._pool, modules=modules)
        logger.info("Migrations complete (modules=%s)", modules or "all")

    def close(self) -> None:
        """Close the connection pool and release all resources."""
        self._pool.close()

    def __enter__(self) -> PostgresEverything:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
