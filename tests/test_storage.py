"""Tests for common.storage.KVStore: get/put/delete and LWW enforcement."""

import threading

import pytest

from common.storage import KVStore


@pytest.fixture
def store():
    return KVStore()


def test_get_missing_key_returns_none(store):
    assert store.get("missing") is None


def test_put_then_get_roundtrips_value(store):
    applied = store.put("k", "v1", timestamp=1.0, node_id="node-1")

    assert applied is True
    entry = store.get("k")
    assert entry.value == "v1"
    assert entry.timestamp == 1.0
    assert entry.node_id == "node-1"


def test_put_with_newer_timestamp_overwrites(store):
    store.put("k", "v1", timestamp=1.0, node_id="node-1")
    applied = store.put("k", "v2", timestamp=2.0, node_id="node-1")

    assert applied is True
    assert store.get("k").value == "v2"


def test_put_with_older_timestamp_is_rejected_as_noop(store):
    store.put("k", "v1", timestamp=2.0, node_id="node-1")
    applied = store.put("k", "v_stale", timestamp=1.0, node_id="node-1")

    assert applied is False
    # original value is untouched
    assert store.get("k").value == "v1"
    assert store.get("k").timestamp == 2.0


def test_put_stale_write_does_not_raise(store):
    store.put("k", "v1", timestamp=2.0, node_id="node-1")
    # Should not raise, just return False.
    result = store.put("k", "v_stale", timestamp=1.0, node_id="node-1")
    assert result is False


def test_put_equal_timestamp_uses_node_id_tiebreak(store):
    store.put("k", "from-node-1", timestamp=1.0, node_id="node-1")
    applied = store.put("k", "from-node-2", timestamp=1.0, node_id="node-2")

    # node-2 > node-1, so this write should win the tie.
    assert applied is True
    assert store.get("k").value == "from-node-2"


def test_put_equal_timestamp_lower_node_id_loses(store):
    store.put("k", "from-node-2", timestamp=1.0, node_id="node-2")
    applied = store.put("k", "from-node-1", timestamp=1.0, node_id="node-1")

    assert applied is False
    assert store.get("k").value == "from-node-2"


def test_delete_existing_key_returns_true(store):
    store.put("k", "v1", timestamp=1.0, node_id="node-1")
    assert store.delete("k") is True
    assert store.get("k") is None


def test_delete_missing_key_returns_false(store):
    assert store.delete("missing") is False


def test_independent_keys_do_not_interfere(store):
    store.put("a", "va", timestamp=1.0, node_id="node-1")
    store.put("b", "vb", timestamp=1.0, node_id="node-1")

    assert store.get("a").value == "va"
    assert store.get("b").value == "vb"

    store.delete("a")
    assert store.get("a") is None
    assert store.get("b").value == "vb"


def test_concurrent_puts_are_thread_safe(store):
    # Fire many concurrent writes at the same key with strictly increasing
    # timestamps; under the lock, the highest timestamp must win regardless
    # of thread scheduling order.
    n = 50

    def writer(i):
        store.put("k", f"v{i}", timestamp=float(i), node_id="node-1")

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = store.get("k")
    assert final.value == f"v{n - 1}"
    assert final.timestamp == float(n - 1)
