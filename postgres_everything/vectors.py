from __future__ import annotations

import logging
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from postgres_everything.base import PostgresModule
from postgres_everything.connection import ConnectionPool
from postgres_everything.embeddings.base import EmbeddingProvider
from postgres_everything.exceptions import ConfigurationError, EmbeddingProviderError

logger = logging.getLogger("postgres_everything.vectors")


class VectorStore(PostgresModule):
    """Pinecone-style vector store backed by pgvector.

    The killer advantage over a dedicated vector DB: pgvector queries live
    inside PostgreSQL, so you can combine ``<=>`` distance with ordinary SQL
    ``WHERE`` filters in a single round-trip — no client-side post-filtering.

    An HNSW index is optional but highly recommended for large collections.
    Call :meth:`create_hnsw_index` once you know the embedding dimension;
    without it the store falls back to an exact sequential scan.

    Args:
        pool: Shared connection pool.
        dsn: PostgreSQL DSN (creates a private pool when pool is omitted).
        embedding_provider: Provider used to auto-embed text inputs.
            When ``None`` callers must supply pre-computed vectors.
    """

    def __init__(
        self,
        pool: ConnectionPool | None = None,
        dsn: str | None = None,
        embedding_provider: EmbeddingProvider | None = None,
    ) -> None:
        super().__init__(pool=pool, dsn=dsn)
        self._provider = embedding_provider

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------

    def create_hnsw_index(
        self,
        dimensions: int,
        *,
        distance: str = "cosine",
        m: int = 16,
        ef_construction: int = 64,
    ) -> None:
        """Create an HNSW approximate-nearest-neighbour index.

        Must be called once after inserting data with known dimensions.
        Safe to call multiple times (uses ``IF NOT EXISTS``).

        Args:
            dimensions: Embedding vector length.
            distance: Distance metric — ``"cosine"``, ``"l2"``, or ``"ip"``
                (inner product).
            m: HNSW ``m`` parameter (connections per layer).
            ef_construction: HNSW build-time search depth.
        """
        ops_map = {"cosine": "vector_cosine_ops", "l2": "vector_l2_ops", "ip": "vector_ip_ops"}
        if distance not in ops_map:
            raise ConfigurationError(
                f"Unknown distance metric '{distance}'. Choose from: {', '.join(ops_map)}"
            )
        ops = ops_map[distance]

        # Recreate the column with an explicit dimension so the index can be built.
        with self._pool.connection() as conn:
            conn.execute(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes
                        WHERE tablename = 'pg_vectors'
                          AND indexname = 'idx_pg_vectors_hnsw'
                    ) THEN
                        ALTER TABLE pg_vectors
                            ALTER COLUMN embedding TYPE vector({dimensions});
                        CREATE INDEX idx_pg_vectors_hnsw
                            ON pg_vectors
                            USING hnsw (embedding {ops})
                            WITH (m = {m}, ef_construction = {ef_construction});
                    END IF;
                END
                $$;
                """  # noqa: S608
            )
        logger.info(
            "HNSW index created (dim=%d, distance=%s, m=%d, ef_construction=%d)",
            dimensions,
            distance,
            m,
            ef_construction,
        )

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def add(
        self,
        content: str,
        *,
        collection: str = "default",
        metadata: dict | None = None,
        embedding: list[float] | None = None,
    ) -> int:
        """Insert a document and its embedding vector.

        Args:
            content: Text content to store (also used for auto-embedding).
            collection: Logical namespace.
            metadata: Optional metadata dict (supports ``@>`` filtering).
            embedding: Pre-computed vector.  When ``None`` the configured
                provider generates the embedding.

        Returns:
            Integer row ID.

        Raises:
            ConfigurationError: If no embedding is given and no provider set.
        """
        vector = self._resolve_embedding(content, embedding)
        row = self._fetch_one(
            """
            INSERT INTO pg_vectors (collection, content, embedding, metadata)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (collection, content, vector, Jsonb(metadata or {})),
        )
        assert row is not None
        return int(row["id"])

    def add_many(self, items: list[dict]) -> list[int]:
        """Insert multiple vectors in a single transaction.

        Each item dict should have::

            {
                "content": str,
                "collection": str,       # optional, default "default"
                "metadata": dict,        # optional
                "embedding": list[float] # optional, auto-embedded if absent
            }

        Args:
            items: List of item dicts.

        Returns:
            List of integer IDs in insertion order.
        """
        ids: list[int] = []
        with self._pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                for item in items:
                    content = item["content"]
                    collection = item.get("collection", "default")
                    metadata = item.get("metadata") or {}
                    embedding = item.get("embedding")
                    vector = self._resolve_embedding(content, embedding)

                    cur.execute(
                        """
                        INSERT INTO pg_vectors (collection, content, embedding, metadata)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id
                        """,
                        (collection, content, vector, Jsonb(metadata)),
                    )
                    row = cur.fetchone()
                    assert row is not None
                    ids.append(int(row["id"]))
        return ids

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def search(
        self,
        query: str | list[float],
        *,
        limit: int = 10,
        collection: str | None = None,
        where: dict | None = None,
        since_days: int | None = None,
    ) -> list[dict]:
        """Semantic nearest-neighbour search with optional SQL filters.

        This is the core killer feature: combine vector similarity with
        relational filters (metadata, recency) in a single PostgreSQL query.

        Args:
            query: Text (auto-embedded) or pre-computed float vector.
            limit: Maximum results.
            collection: Restrict to this collection.
            where: JSONB metadata filter using ``@>`` containment.
            since_days: Only include vectors inserted in the last N days.

        Returns:
            List of result dicts with ``id``, ``collection``, ``content``,
            ``metadata``, ``created_at``, and ``similarity`` (1 − distance).
        """
        vector = self._resolve_query_vector(query)

        conditions: list[str] = []
        params: list[Any] = [vector, vector]

        if collection:
            conditions.append("collection = %s")
            params.append(collection)
        if where:
            conditions.append("metadata @> %s")
            params.append(Jsonb(where))
        if since_days:
            conditions.append("created_at >= NOW() - %s * interval '1 day'")
            params.append(since_days)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit)

        sql = f"""
            SELECT
                id, collection, content, metadata, created_at,
                1 - (embedding <=> %s) AS similarity
            FROM pg_vectors
            {where_clause}
            ORDER BY embedding <=> %s
            LIMIT %s
        """  # noqa: S608

        return self._fetch_all(sql, params)

    def hybrid_search(
        self,
        query: str,
        *,
        limit: int = 10,
        collection: str | None = None,
        semantic_weight: float = 0.7,
    ) -> list[dict]:
        """Combine vector similarity with PostgreSQL full-text search.

        Uses a CTE to compute both scores independently then blends them.
        The text score is normalised to [0,1] via ``ts_rank``.

        Args:
            query: Natural-language query (used for both embedding and FTS).
            limit: Maximum results.
            collection: Restrict to this collection.
            semantic_weight: Weight for the semantic score (0–1).  The text
                score receives ``1 - semantic_weight``.

        Returns:
            List of result dicts with ``id``, ``collection``, ``content``,
            ``metadata``, ``created_at``, and ``score``.
        """
        text_weight = 1.0 - semantic_weight
        vector = self._resolve_query_vector(query)

        collection_clause = "AND collection = %s" if collection else ""
        coll_params = [collection] if collection else []

        sql = f"""
            WITH semantic AS (
                SELECT
                    id,
                    1 - (embedding <=> %s) AS sem_score
                FROM pg_vectors
                WHERE 1=1 {collection_clause}
                ORDER BY embedding <=> %s
                LIMIT %s
            ),
            text_scores AS (
                SELECT
                    id,
                    ts_rank(
                        to_tsvector('english', content),
                        plainto_tsquery('english', %s)
                    ) AS txt_score
                FROM pg_vectors
                WHERE to_tsvector('english', content) @@
                      plainto_tsquery('english', %s)
                  {collection_clause}
            )
            SELECT
                v.id, v.collection, v.content, v.metadata, v.created_at,
                COALESCE(s.sem_score, 0) * %s +
                COALESCE(t.txt_score, 0) * %s AS score
            FROM pg_vectors v
            JOIN semantic s ON v.id = s.id
            LEFT JOIN text_scores t ON v.id = t.id
            ORDER BY score DESC
            LIMIT %s
        """  # noqa: S608

        params = (
            [vector]
            + coll_params
            + [vector, limit * 2, query, query]
            + coll_params
            + [semantic_weight, text_weight, limit]
        )
        return self._fetch_all(sql, params)

    def delete(self, vector_id: int) -> bool:
        """Delete a single vector by ID.

        Args:
            vector_id: Row ID to remove.

        Returns:
            True if a row was deleted.
        """
        return self._execute("DELETE FROM pg_vectors WHERE id = %s", (vector_id,)) > 0

    def delete_by_metadata(self, where: dict) -> int:
        """Delete all vectors whose metadata matches the filter.

        Args:
            where: JSONB containment filter.

        Returns:
            Number of rows deleted.
        """
        return self._execute(
            "DELETE FROM pg_vectors WHERE metadata @> %s", (Jsonb(where),)
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_embedding(
        self, content: str, embedding: list[float] | None
    ) -> list[float]:
        if embedding is not None:
            return embedding
        if self._provider is None:
            raise ConfigurationError(
                "No embedding_provider configured and no pre-computed embedding supplied. "
                "Pass embedding=... or set embedding_provider on VectorStore."
            )
        return self._provider.embed(content)

    def _resolve_query_vector(self, query: str | list[float]) -> list[float]:
        if isinstance(query, list):
            return query
        if self._provider is None:
            raise ConfigurationError(
                "query is a string but no embedding_provider is configured. "
                "Pass a pre-computed vector or set embedding_provider on VectorStore."
            )
        return self._provider.embed(query)
