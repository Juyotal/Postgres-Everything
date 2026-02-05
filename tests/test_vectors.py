from __future__ import annotations

import math

import pytest

from postgres_everything.embeddings.base import EmbeddingProvider
from postgres_everything.exceptions import ConfigurationError


class DeterministicEmbeddings(EmbeddingProvider):
    """Mock provider that maps text → a deterministic 4-dimensional unit vector.

    Vectors are constructed from the hash of the text so identical strings
    always produce the same vector without any ML model.
    """

    @property
    def dimensions(self) -> int:
        return 4

    def embed(self, text: str) -> list[float]:
        return self.embed_many([text])[0]

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        results = []
        for text in texts:
            h = hash(text) % (10**6)
            # Build a 4-dim vector and normalise to unit length.
            raw = [
                math.sin(h * 0.001),
                math.cos(h * 0.002),
                math.sin(h * 0.003 + 1),
                math.cos(h * 0.004 + 2),
            ]
            length = math.sqrt(sum(x**2 for x in raw))
            results.append([x / length for x in raw])
        return results


@pytest.fixture
def vec(pg):
    """VectorStore with the deterministic mock provider."""
    from postgres_everything import VectorStore

    return VectorStore(pool=pg._pool, embedding_provider=DeterministicEmbeddings())


def test_add_with_raw_vector(pg):
    from postgres_everything import VectorStore

    store = VectorStore(pool=pg._pool)
    v_id = store.add("hello world", embedding=[0.1, 0.2, 0.3, 0.4])
    assert isinstance(v_id, int)


def test_add_with_provider(vec):
    v_id = vec.add("cats are great", collection="animals")
    assert isinstance(v_id, int)


def test_add_no_provider_no_vector_raises(pg):
    from postgres_everything import VectorStore

    store = VectorStore(pool=pg._pool)
    with pytest.raises(ConfigurationError):
        store.add("needs embedding")


def test_search_returns_results(vec):
    vec.add("the quick brown fox", collection="test")
    vec.add("a lazy dog", collection="test")
    vec.add("unrelated content about galaxies", collection="test")

    results = vec.search("quick fox", collection="test")
    assert len(results) > 0
    assert "similarity" in results[0]
    assert "content" in results[0]


def test_search_with_metadata_filter(vec):
    vec.add("user A document", metadata={"user_id": "alice"})
    vec.add("user B document", metadata={"user_id": "bob"})

    results = vec.search("document", where={"user_id": "alice"})
    assert all(r["metadata"]["user_id"] == "alice" for r in results)


def test_add_many(vec):
    items = [
        {"content": f"item {i}", "metadata": {"idx": i}}
        for i in range(5)
    ]
    ids = vec.add_many(items)
    assert len(ids) == 5
    assert all(isinstance(i, int) for i in ids)


def test_delete(vec):
    v_id = vec.add("to be deleted", embedding=[0.1, 0.0, 0.0, 0.0])
    assert vec.delete(v_id) is True
    # After deletion, searching for it should return nothing with that id.
    results = vec.search([0.1, 0.0, 0.0, 0.0])
    assert all(r["id"] != v_id for r in results)


def test_delete_by_metadata(vec):
    vec.add("a", metadata={"tag": "remove-me"}, embedding=[0.1, 0.2, 0.3, 0.4])
    vec.add("b", metadata={"tag": "remove-me"}, embedding=[0.4, 0.3, 0.2, 0.1])
    vec.add("c", metadata={"tag": "keep"}, embedding=[0.0, 0.0, 0.5, 0.5])

    removed = vec.delete_by_metadata({"tag": "remove-me"})
    assert removed == 2


def test_hybrid_search(vec):
    vec.add("PostgreSQL is powerful", collection="htest")
    vec.add("cats and dogs", collection="htest")

    results = vec.hybrid_search("PostgreSQL database", collection="htest")
    assert len(results) > 0
    assert "score" in results[0]


def test_search_since_days(vec):
    vec.add("recent entry", embedding=[1.0, 0.0, 0.0, 0.0])
    results = vec.search([1.0, 0.0, 0.0, 0.0], since_days=1)
    assert len(results) > 0

    results_old = vec.search([1.0, 0.0, 0.0, 0.0], since_days=0)
    # 0 days means only entries from the last 0 days — should be empty.
    assert results_old == [] or all(r["id"] != results[0]["id"] for r in results_old)
