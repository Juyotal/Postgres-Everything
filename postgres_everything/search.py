from __future__ import annotations

import logging
from typing import Any

from psycopg.types.json import Jsonb

from postgres_everything.base import PostgresModule
from postgres_everything.connection import ConnectionPool

logger = logging.getLogger("postgres_everything.search")


class SearchEngine(PostgresModule):
    """Elasticsearch-style full-text search backed by PostgreSQL tsvector + pg_trgm.

    - :meth:`search` uses ``websearch_to_tsquery`` (forgives bad input) with
      ts_headline snippet extraction.
    - :meth:`fuzzy_search` uses trigram similarity for typo-tolerance.
    - :meth:`hybrid_search` combines both scores.
    - :meth:`autocomplete` does prefix matching on the title column.

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
    # Writes
    # ------------------------------------------------------------------

    def index(
        self,
        title: str,
        body: str = "",
        *,
        collection: str = "default",
        metadata: dict | None = None,
    ) -> int:
        """Add a document to the search index.

        Args:
            title: Document title (weighted higher than body in ranking).
            body: Document body text.
            collection: Logical collection for scoping searches.
            metadata: Arbitrary metadata stored alongside the document.

        Returns:
            Integer document ID.
        """
        row = self._fetch_one(
            """
            INSERT INTO pg_search_documents (collection, title, body, metadata)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (collection, title, body, Jsonb(metadata or {})),
        )
        assert row is not None
        return int(row["id"])

    def update(
        self,
        doc_id: int,
        *,
        title: str | None = None,
        body: str | None = None,
        metadata: dict | None = None,
    ) -> bool:
        """Update a search document.  Only non-None fields are changed.

        Args:
            doc_id: ID of the document to update.
            title: New title, or None to leave unchanged.
            body: New body, or None to leave unchanged.
            metadata: New metadata dict (full replace), or None to leave unchanged.

        Returns:
            True if the document was found and updated.
        """
        if title is None and body is None and metadata is None:
            return False

        parts: list[str] = []
        params: list[Any] = []

        if title is not None:
            parts.append("title = %s")
            params.append(title)
        if body is not None:
            parts.append("body = %s")
            params.append(body)
        if metadata is not None:
            parts.append("metadata = %s")
            params.append(Jsonb(metadata))

        params.append(doc_id)
        sql = f"UPDATE pg_search_documents SET {', '.join(parts)} WHERE id = %s"  # noqa: S608
        return self._execute(sql, params) > 0

    def delete(self, doc_id: int) -> bool:
        """Remove a document from the index.

        Args:
            doc_id: ID returned by :meth:`index`.

        Returns:
            True if the document existed and was removed.
        """
        return self._execute(
            "DELETE FROM pg_search_documents WHERE id = %s", (doc_id,)
        ) > 0

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        *,
        collection: str | None = None,
        limit: int = 10,
        snippet: bool = True,
    ) -> list[dict]:
        """Full-text search ranked by ts_rank.

        Uses ``websearch_to_tsquery`` so raw user input (e.g. "quick brown")
        works without quoting.

        Args:
            query: Search query string.
            collection: Restrict search to this collection, or None for all.
            limit: Maximum number of results.
            snippet: When True, include a ``snippet`` field with highlighted
                context from the body via ``ts_headline``.

        Returns:
            List of result dicts with ``id``, ``title``, ``body``,
            ``metadata``, ``rank``, and optionally ``snippet``.
        """
        snippet_expr = (
            "ts_headline('english', body, websearch_to_tsquery('english', %s),"
            " 'MaxWords=30, MinWords=10') AS snippet"
            if snippet
            else ""
        )
        snippet_params = [query] if snippet else []

        collection_clause = "AND collection = %s" if collection else ""
        collection_params = [collection] if collection else []

        sql = f"""
            SELECT
                id, title, body, metadata, collection,
                ts_rank(search_vector, websearch_to_tsquery('english', %s)) AS rank
                {', ' + snippet_expr if snippet else ''}
            FROM pg_search_documents
            WHERE search_vector @@ websearch_to_tsquery('english', %s)
              {collection_clause}
            ORDER BY rank DESC
            LIMIT %s
        """  # noqa: S608

        params = [query] + snippet_params + [query] + collection_params + [limit]
        return self._fetch_all(sql, params)

    def fuzzy_search(
        self,
        query: str,
        *,
        collection: str | None = None,
        limit: int = 10,
        threshold: float = 0.3,
    ) -> list[dict]:
        """Trigram-based typo-tolerant search.

        Args:
            query: Search string (typos tolerated).
            collection: Restrict to this collection, or None for all.
            limit: Maximum results.
            threshold: Minimum similarity score (0–1).

        Returns:
            List of result dicts with ``id``, ``title``, ``body``,
            ``metadata``, ``collection``, and ``similarity``.
        """
        collection_clause = "AND collection = %s" if collection else ""
        collection_params = [collection] if collection else []

        # word_similarity compares the query against the best-matching substring of the
        # title, so a short misspelled word still scores well against a multi-word title.
        sql = f"""
            SELECT
                id, title, body, metadata, collection,
                word_similarity(%s, title) AS similarity
            FROM pg_search_documents
            WHERE word_similarity(%s, title) >= %s
              {collection_clause}
            ORDER BY similarity DESC
            LIMIT %s
        """  # noqa: S608

        params = [query, query, threshold] + collection_params + [limit]
        return self._fetch_all(sql, params)

    def hybrid_search(
        self,
        query: str,
        *,
        collection: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Combine full-text rank and trigram similarity into a single score.

        Full-text rank is normalised to [0,1] and weighted 0.7; trigram
        similarity is weighted 0.3.

        Args:
            query: Search query.
            collection: Restrict to this collection, or None for all.
            limit: Maximum results.

        Returns:
            List of result dicts with ``id``, ``title``, ``body``,
            ``metadata``, ``collection``, and ``score``.
        """
        collection_clause = "AND collection = %s" if collection else ""
        collection_params = [collection] if collection else []

        sql = f"""
            WITH ft AS (
                SELECT
                    id,
                    ts_rank(search_vector, websearch_to_tsquery('english', %s)) AS ft_rank
                FROM pg_search_documents
                WHERE search_vector @@ websearch_to_tsquery('english', %s)
                  {collection_clause}
            ),
            trgm AS (
                SELECT
                    id,
                    similarity(title, %s) AS trgm_score
                FROM pg_search_documents
                WHERE similarity(title, %s) > 0.1
                  {collection_clause}
            ),
            combined AS (
                SELECT
                    d.id, d.title, d.body, d.metadata, d.collection,
                    COALESCE(ft.ft_rank, 0) * 0.7 +
                    COALESCE(trgm.trgm_score, 0) * 0.3 AS score
                FROM pg_search_documents d
                LEFT JOIN ft    ON d.id = ft.id
                LEFT JOIN trgm  ON d.id = trgm.id
                WHERE ft.id IS NOT NULL OR trgm.id IS NOT NULL
                  {collection_clause}
            )
            SELECT * FROM combined
            ORDER BY score DESC
            LIMIT %s
        """  # noqa: S608

        params = (
            [query, query]
            + collection_params
            + [query, query]
            + collection_params
            + collection_params
            + [limit]
        )
        return self._fetch_all(sql, params)

    def autocomplete(
        self,
        prefix: str,
        *,
        collection: str | None = None,
        limit: int = 5,
    ) -> list[dict]:
        """Prefix-based title autocomplete using trigram index.

        Args:
            prefix: Partial string to complete.
            collection: Restrict to this collection, or None for all.
            limit: Maximum suggestions.

        Returns:
            List of dicts with ``id``, ``title``, and ``collection``.
        """
        collection_clause = "AND collection = %s" if collection else ""
        collection_params = [collection] if collection else []

        sql = f"""
            SELECT id, title, collection
            FROM pg_search_documents
            WHERE title ILIKE %s
              {collection_clause}
            ORDER BY title
            LIMIT %s
        """  # noqa: S608

        params = [f"{prefix}%"] + collection_params + [limit]
        return self._fetch_all(sql, params)
