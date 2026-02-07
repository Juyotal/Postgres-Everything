"""Minimal RAG (Retrieval-Augmented Generation) demo.

Shows how VectorStore, Cache, and DocumentStore work together.

Usage:
    OPENAI_API_KEY=sk-... python examples/rag_app.py

Without an OpenAI key, pass pre-computed vectors via the ``--demo`` flag
which uses a tiny deterministic mock provider.
"""
from __future__ import annotations

import argparse
import math
import os

from postgres_everything import PostgresEverything
from postgres_everything.embeddings.base import EmbeddingProvider

DSN = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pg_everything")


# ---------------------------------------------------------------------------
# Optional mock provider (avoids needing a real API key for quick demos)
# ---------------------------------------------------------------------------


class ToyEmbeddings(EmbeddingProvider):
    """4-dim deterministic embeddings for demo purposes only."""

    @property
    def dimensions(self) -> int:
        return 4

    def embed(self, text: str) -> list[float]:
        return self.embed_many([text])[0]

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        result = []
        for text in texts:
            h = hash(text) % (10**6)
            raw = [math.sin(h * 0.001), math.cos(h * 0.002), math.sin(h * 0.003), math.cos(h * 0.004)]
            length = math.sqrt(sum(x**2 for x in raw)) or 1.0
            result.append([x / length for x in raw])
        return result


def get_provider(use_demo: bool) -> EmbeddingProvider:
    if use_demo:
        return ToyEmbeddings()
    from postgres_everything.embeddings.openai import OpenAIEmbeddings
    return OpenAIEmbeddings()


# ---------------------------------------------------------------------------
# RAG helpers
# ---------------------------------------------------------------------------

CORPUS = [
    {
        "user_id": "alice",
        "source": "docs",
        "chunk": "PostgreSQL supports full-text search via tsvector and GIN indexes.",
    },
    {
        "user_id": "alice",
        "source": "docs",
        "chunk": "The pgvector extension adds vector similarity search to PostgreSQL.",
    },
    {
        "user_id": "alice",
        "source": "wiki",
        "chunk": "HNSW is an approximate nearest-neighbour algorithm used by pgvector.",
    },
    {
        "user_id": "bob",
        "source": "docs",
        "chunk": "Redis is an in-memory data store used for caching and pub/sub.",
    },
    {
        "user_id": "bob",
        "source": "wiki",
        "chunk": "RabbitMQ is a message broker that supports task queues.",
    },
]


def index_corpus(pg: PostgresEverything) -> None:
    print("Indexing corpus…")
    items = [
        {
            "content": doc["chunk"],
            "metadata": {"user_id": doc["user_id"], "source": doc["source"]},
            "collection": "rag",
        }
        for doc in CORPUS
    ]
    ids = pg.vectors.add_many(items)
    print(f"  Indexed {len(ids)} chunks.")


def retrieve(pg: PostgresEverything, query: str, user_id: str, top_k: int = 3) -> list[dict]:
    """Retrieve top-k relevant chunks for this user, caching the query embedding."""
    cache_key = f"rag_embed:{query}"
    cached_vec = pg.cache.get(cache_key)

    if cached_vec is not None:
        print("  [cache hit] reusing cached query embedding")
        query_vec = cached_vec
    else:
        # The embedding_provider on pg.vectors generates this.
        query_vec = pg.vectors._provider.embed(query) if pg.vectors._provider else None
        if query_vec:
            pg.cache.set(cache_key, query_vec, ttl=300)
            print("  [cache miss] generated and cached query embedding")

    return pg.vectors.search(
        query_vec or query,
        collection="rag",
        where={"user_id": user_id},
        limit=top_k,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="RAG demo with postgres_everything")
    parser.add_argument("--demo", action="store_true", help="Use toy embeddings (no API key needed)")
    parser.add_argument("--user", default="alice", help="User to filter results for")
    parser.add_argument("--query", default="How does pgvector work?", help="Search query")
    args = parser.parse_args()

    provider = get_provider(args.demo)
    pg = PostgresEverything(DSN, embedding_provider=provider)
    pg.init(modules=["vectors", "cache"])

    # Clear and re-index for a clean demo run.
    with pg._pool.connection() as conn:
        conn.execute("TRUNCATE pg_vectors RESTART IDENTITY CASCADE")
        conn.execute("TRUNCATE pg_cache RESTART IDENTITY CASCADE")

    index_corpus(pg)

    print(f"\nQuery : {args.query!r}")
    print(f"User  : {args.user!r}")
    print()

    results = retrieve(pg, args.query, user_id=args.user)
    for i, r in enumerate(results, 1):
        sim = r.get("similarity", 0)
        print(f"  {i}. [{sim:.3f}] {r['content']}")
        print(f"       metadata: {r['metadata']}")
        print()

    # Second call shows cache hit.
    print("Repeating query (should hit embedding cache)…")
    retrieve(pg, args.query, user_id=args.user)

    pg.close()


if __name__ == "__main__":
    main()
