from __future__ import annotations

import logging
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from postgres_everything.base import PostgresModule
from postgres_everything.connection import ConnectionPool
from postgres_everything.exceptions import DocumentNotFoundError

logger = logging.getLogger("postgres_everything.documents")


class DocumentStore(PostgresModule):
    """MongoDB-style document storage backed by PostgreSQL JSONB.

    Queries use the ``@>`` (contains) operator which is accelerated by the
    GIN index, so even deeply nested field lookups are fast.

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

    def insert(self, collection: str, doc: dict) -> str:
        """Insert a document and return its generated UUID.

        Args:
            collection: Logical collection name (analogous to a Mongo collection).
            doc: Arbitrary JSON-serialisable dict.

        Returns:
            The new document's UUID as a string.
        """
        row = self._fetch_one(
            """
            INSERT INTO pg_documents (collection, data)
            VALUES (%s, %s)
            RETURNING id::text
            """,
            (collection, Jsonb(doc)),
        )
        assert row is not None
        logger.debug("Inserted document %s into collection '%s'", row["id"], collection)
        return row["id"]

    def insert_many(self, collection: str, docs: list[dict]) -> list[str]:
        """Insert multiple documents in a single transaction.

        Args:
            collection: Target collection name.
            docs: List of JSON-serialisable dicts.

        Returns:
            List of new document UUIDs in insertion order.
        """
        ids: list[str] = []
        with self._pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                for doc in docs:
                    cur.execute(
                        """
                        INSERT INTO pg_documents (collection, data)
                        VALUES (%s, %s)
                        RETURNING id::text
                        """,
                        (collection, Jsonb(doc)),
                    )
                    row = cur.fetchone()
                    assert row is not None
                    ids.append(row["id"])
        logger.debug("Inserted %d documents into collection '%s'", len(ids), collection)
        return ids

    def update(self, collection: str, query: dict, updates: dict) -> int:
        """Merge ``updates`` into all documents matching ``query``.

        Uses the JSONB ``||`` operator so existing keys are overwritten and
        new keys are added — nested objects are not deep-merged.

        Args:
            collection: Collection to search within.
            query: Filter dict matched with ``@>``.
            updates: Partial document merged into matching docs.

        Returns:
            Number of documents updated.
        """
        return self._execute(
            """
            UPDATE pg_documents
            SET data = data || %s,
                updated_at = NOW()
            WHERE collection = %s AND data @> %s
            """,
            (Jsonb(updates), collection, Jsonb(query)),
        )

    def update_by_id(self, doc_id: str, updates: dict) -> bool:
        """Merge ``updates`` into a single document identified by UUID.

        Args:
            doc_id: UUID string of the target document.
            updates: Partial document to merge in.

        Returns:
            True if the document was found and updated, False otherwise.
        """
        count = self._execute(
            """
            UPDATE pg_documents
            SET data = data || %s,
                updated_at = NOW()
            WHERE id = %s::uuid
            """,
            (Jsonb(updates), doc_id),
        )
        return count > 0

    def delete(self, collection: str, query: dict) -> int:
        """Delete all documents matching ``query`` in the collection.

        Args:
            collection: Collection to search within.
            query: Filter dict matched with ``@>``.

        Returns:
            Number of documents deleted.
        """
        return self._execute(
            "DELETE FROM pg_documents WHERE collection = %s AND data @> %s",
            (collection, Jsonb(query)),
        )

    def delete_by_id(self, doc_id: str) -> bool:
        """Delete a single document by UUID.

        Args:
            doc_id: UUID string of the document to remove.

        Returns:
            True if a document was deleted, False if it did not exist.
        """
        return self._execute(
            "DELETE FROM pg_documents WHERE id = %s::uuid",
            (doc_id,),
        ) > 0

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def find(
        self,
        collection: str,
        query: dict | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Return documents from a collection, optionally filtered.

        Args:
            collection: Collection to search within.
            query: Optional filter dict matched with ``@>``. When ``None``
                all documents in the collection are returned.
            limit: Maximum number of results.

        Returns:
            List of document dicts including ``id``, ``collection``,
            ``data``, ``created_at``, and ``updated_at``.
        """
        if query:
            return self._fetch_all(
                """
                SELECT id::text, collection, data, created_at, updated_at
                FROM pg_documents
                WHERE collection = %s AND data @> %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (collection, Jsonb(query), limit),
            )
        return self._fetch_all(
            """
            SELECT id::text, collection, data, created_at, updated_at
            FROM pg_documents
            WHERE collection = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (collection, limit),
        )

    def find_one(self, collection: str, query: dict) -> dict | None:
        """Return the first document matching ``query``, or None.

        Args:
            collection: Collection to search within.
            query: Filter dict matched with ``@>``.

        Returns:
            Document dict or None if no match exists.
        """
        return self._fetch_one(
            """
            SELECT id::text, collection, data, created_at, updated_at
            FROM pg_documents
            WHERE collection = %s AND data @> %s
            LIMIT 1
            """,
            (collection, Jsonb(query)),
        )

    def find_by_id(self, doc_id: str) -> dict | None:
        """Return a document by its UUID.

        Args:
            doc_id: UUID string.

        Returns:
            Document dict or None if the UUID does not exist.
        """
        return self._fetch_one(
            """
            SELECT id::text, collection, data, created_at, updated_at
            FROM pg_documents
            WHERE id = %s::uuid
            """,
            (doc_id,),
        )

    def count(self, collection: str, query: dict | None = None) -> int:
        """Return the number of documents in a collection.

        Args:
            collection: Collection to count within.
            query: Optional filter. When ``None`` counts all documents.

        Returns:
            Integer count.
        """
        if query:
            result = self._fetch_scalar(
                "SELECT COUNT(*) FROM pg_documents WHERE collection = %s AND data @> %s",
                (collection, Jsonb(query)),
            )
        else:
            result = self._fetch_scalar(
                "SELECT COUNT(*) FROM pg_documents WHERE collection = %s",
                (collection,),
            )
        return int(result or 0)
