from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def seed_docs(pg):
    """Index a small corpus once per test."""
    pg.search.index("Running in the Park", "A guide to daily jogging habits", collection="articles")
    pg.search.index("Python Programming", "Learn Python from scratch", collection="articles")
    pg.search.index("Database Design", "Relational and NoSQL databases compared", collection="books")
    pg.search.index("PostgreSQL Performance", "Tuning Postgres for production workloads", collection="articles")


def test_full_text_stemming(pg):
    # "run" should match "Running" via English stemming.
    results = pg.search.search("run", collection="articles")
    assert len(results) >= 1
    assert any("Running" in r["title"] for r in results)


def test_full_text_returns_rank(pg):
    results = pg.search.search("Postgres", collection="articles")
    assert len(results) >= 1
    assert "rank" in results[0]
    assert results[0]["rank"] > 0


def test_full_text_with_snippet(pg):
    results = pg.search.search("databases", snippet=True)
    assert any("snippet" in r for r in results)


def test_full_text_collection_filter(pg):
    results = pg.search.search("database", collection="books")
    assert all(r["collection"] == "books" for r in results)


def test_fuzzy_typo_tolerance(pg):
    # "Pyhton" is a common typo for "Python".
    results = pg.search.fuzzy_search("Pyhton", collection="articles", threshold=0.2)
    assert len(results) >= 1
    assert any("Python" in r["title"] for r in results)


def test_fuzzy_returns_similarity(pg):
    results = pg.search.fuzzy_search("Python", collection="articles")
    assert len(results) >= 1
    assert "similarity" in results[0]


def test_hybrid_search(pg):
    results = pg.search.hybrid_search("postgres database", collection="articles")
    assert len(results) >= 1
    assert "score" in results[0]
    # Highest scorer should relate to postgres/database topics.
    top = results[0]
    assert "score" in top


def test_autocomplete_prefix(pg):
    results = pg.search.autocomplete("Post", collection="articles")
    assert len(results) >= 1
    assert all(r["title"].startswith("Post") for r in results)


def test_autocomplete_no_match(pg):
    results = pg.search.autocomplete("ZZZZZ")
    assert results == []


def test_index_and_delete(pg):
    doc_id = pg.search.index("Temp Doc", "temporary content")
    results = pg.search.search("temporary")
    assert any(r["id"] == doc_id for r in results)

    deleted = pg.search.delete(doc_id)
    assert deleted is True
    results_after = pg.search.search("temporary")
    assert all(r["id"] != doc_id for r in results_after)


def test_update_document(pg):
    doc_id = pg.search.index("Old Title", "old body")
    ok = pg.search.update(doc_id, title="New Title", body="new body content")
    assert ok is True

    results = pg.search.search("new body")
    assert any(r["id"] == doc_id for r in results)
