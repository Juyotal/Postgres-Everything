from __future__ import annotations

import pytest


def test_insert_and_find_by_id(pg):
    doc_id = pg.documents.insert("users", {"name": "Alice", "age": 30})
    assert isinstance(doc_id, str) and len(doc_id) == 36  # UUID

    doc = pg.documents.find_by_id(doc_id)
    assert doc is not None
    assert doc["data"]["name"] == "Alice"
    assert doc["id"] == doc_id


def test_find_with_nested_query(pg):
    pg.documents.insert("products", {"name": "Widget", "meta": {"sku": "W-001"}})
    pg.documents.insert("products", {"name": "Gadget", "meta": {"sku": "G-002"}})

    results = pg.documents.find("products", {"meta": {"sku": "W-001"}})
    assert len(results) == 1
    assert results[0]["data"]["name"] == "Widget"


def test_find_all_in_collection(pg):
    pg.documents.insert_many("items", [{"x": 1}, {"x": 2}, {"x": 3}])
    results = pg.documents.find("items")
    assert len(results) == 3


def test_insert_many_returns_ids(pg):
    ids = pg.documents.insert_many("batch", [{"v": i} for i in range(5)])
    assert len(ids) == 5
    assert all(len(i) == 36 for i in ids)


def test_find_one(pg):
    pg.documents.insert("things", {"color": "red"})
    pg.documents.insert("things", {"color": "blue"})

    doc = pg.documents.find_one("things", {"color": "blue"})
    assert doc is not None
    assert doc["data"]["color"] == "blue"


def test_find_one_missing(pg):
    result = pg.documents.find_one("things", {"color": "invisible"})
    assert result is None


def test_update_merge(pg):
    doc_id = pg.documents.insert("docs", {"a": 1, "b": 2})
    count = pg.documents.update("docs", {"a": 1}, {"b": 99, "c": 3})
    assert count == 1

    doc = pg.documents.find_by_id(doc_id)
    assert doc is not None
    assert doc["data"]["a"] == 1   # unchanged
    assert doc["data"]["b"] == 99  # overwritten
    assert doc["data"]["c"] == 3   # added


def test_update_by_id(pg):
    doc_id = pg.documents.insert("docs", {"status": "new"})
    ok = pg.documents.update_by_id(doc_id, {"status": "done"})
    assert ok is True

    doc = pg.documents.find_by_id(doc_id)
    assert doc["data"]["status"] == "done"


def test_delete(pg):
    pg.documents.insert("trash", {"keep": False})
    pg.documents.insert("trash", {"keep": True})

    deleted = pg.documents.delete("trash", {"keep": False})
    assert deleted == 1
    assert pg.documents.count("trash") == 1


def test_delete_by_id(pg):
    doc_id = pg.documents.insert("gone", {"x": 1})
    assert pg.documents.delete_by_id(doc_id) is True
    assert pg.documents.find_by_id(doc_id) is None


def test_count(pg):
    pg.documents.insert_many("counted", [{"n": i} for i in range(7)])
    assert pg.documents.count("counted") == 7
    assert pg.documents.count("counted", {"n": 3}) == 1
    assert pg.documents.count("counted", {"n": 999}) == 0
