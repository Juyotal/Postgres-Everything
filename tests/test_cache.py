from __future__ import annotations

import time


def test_set_and_get(pg):
    pg.cache.set("greeting", "hello")
    assert pg.cache.get("greeting") == "hello"


def test_get_missing(pg):
    assert pg.cache.get("no_such_key") is None


def test_overwrite(pg):
    pg.cache.set("k", 1)
    pg.cache.set("k", 2)
    assert pg.cache.get("k") == 2


def test_delete(pg):
    pg.cache.set("tmp", "x")
    assert pg.cache.delete("tmp") is True
    assert pg.cache.get("tmp") is None
    assert pg.cache.delete("tmp") is False


def test_exists(pg):
    assert pg.cache.exists("missing") is False
    pg.cache.set("present", True)
    assert pg.cache.exists("present") is True


def test_ttl_expiration(pg):
    pg.cache.set("expiring", "bye", ttl=1)
    assert pg.cache.get("expiring") == "bye"  # still alive
    time.sleep(1.1)
    assert pg.cache.get("expiring") is None  # expired


def test_exists_expired(pg):
    pg.cache.set("exp_key", "v", ttl=1)
    time.sleep(1.1)
    assert pg.cache.exists("exp_key") is False


def test_get_or_set(pg):
    calls = []

    def factory():
        calls.append(1)
        return {"computed": True}

    val = pg.cache.get_or_set("lazy", factory)
    assert val == {"computed": True}
    assert len(calls) == 1

    # Second call should hit cache, not call factory again.
    val2 = pg.cache.get_or_set("lazy", factory)
    assert val2 == {"computed": True}
    assert len(calls) == 1


def test_incr(pg):
    assert pg.cache.incr("counter") == 1
    assert pg.cache.incr("counter") == 1 + 1
    assert pg.cache.incr("counter", 10) == 12


def test_clear(pg):
    pg.cache.set("a", 1)
    pg.cache.set("b", 2)
    count = pg.cache.clear()
    assert count == 2
    assert pg.cache.get("a") is None


def test_cleanup_expired(pg):
    pg.cache.set("e1", 1, ttl=1)
    pg.cache.set("e2", 2, ttl=1)
    pg.cache.set("keep", 3)
    time.sleep(1.1)
    removed = pg.cache.cleanup_expired()
    assert removed == 2
    assert pg.cache.get("keep") == 3


def test_complex_value(pg):
    data = {"nested": {"list": [1, 2, 3], "bool": True, "null": None}}
    pg.cache.set("complex", data)
    assert pg.cache.get("complex") == data
