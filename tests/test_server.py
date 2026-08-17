"""Tests for common.server.create_app: generic KV HTTP layer."""

import time

import pytest
from fastapi.testclient import TestClient

from common.server import create_app
from common.storage import KVStore


@pytest.fixture
def storage():
    return KVStore()


@pytest.fixture
def client(storage):
    app = create_app(storage, node_id="node-1")
    return TestClient(app)


def test_get_missing_key_returns_404(client):
    resp = client.get("/kv/missing")
    assert resp.status_code == 404


def test_put_then_get_roundtrips_value(client):
    put_resp = client.put("/kv/k", json={"value": "v1"})
    assert put_resp.status_code == 200
    assert put_resp.json() == {"applied": True}

    get_resp = client.get("/kv/k")
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["key"] == "k"
    assert body["value"] == "v1"
    assert body["node_id"] == "node-1"
    assert isinstance(body["timestamp"], float)


def test_put_ignores_client_supplied_timestamp_and_node_id(client):
    # PutRequest only has a `value` field -- extra fields like timestamp
    # or node_id are simply not part of the schema and are ignored.
    before = time.time()
    client.put("/kv/k", json={"value": "v1", "timestamp": 1.0, "node_id": "someone-else"})
    after = time.time()

    body = client.get("/kv/k").json()
    assert body["node_id"] == "node-1"
    assert before <= body["timestamp"] <= after


def test_put_overwrite_with_newer_write(client):
    client.put("/kv/k", json={"value": "v1"})
    resp = client.put("/kv/k", json={"value": "v2"})
    assert resp.json() == {"applied": True}
    assert client.get("/kv/k").json()["value"] == "v2"


def test_delete_existing_key(client):
    client.put("/kv/k", json={"value": "v1"})
    resp = client.delete("/kv/k")
    assert resp.status_code == 200
    assert resp.json() == {"deleted": True}
    assert client.get("/kv/k").status_code == 404


def test_delete_missing_key_returns_false_not_error(client):
    resp = client.delete("/kv/missing")
    assert resp.status_code == 200
    assert resp.json() == {"deleted": False}


def test_replicate_applies_newer_write(client, storage):
    resp = client.post(
        "/internal/replicate",
        json={"key": "k", "value": "from-peer", "timestamp": 5.0, "node_id": "node-2"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"applied": True}

    entry = storage.get("k")
    assert entry.value == "from-peer"
    assert entry.timestamp == 5.0
    assert entry.node_id == "node-2"


def test_replicate_preserves_original_timestamp_and_node_id(client):
    # Unlike PUT /kv, /internal/replicate must NOT stamp its own node_id
    # or generate a fresh timestamp -- it's relaying someone else's write.
    client.post(
        "/internal/replicate",
        json={"key": "k", "value": "v", "timestamp": 42.5, "node_id": "node-7"},
    )
    body = client.get("/kv/k").json()
    assert body["timestamp"] == 42.5
    assert body["node_id"] == "node-7"


def test_replicate_rejects_stale_write(client):
    client.post(
        "/internal/replicate",
        json={"key": "k", "value": "newer", "timestamp": 10.0, "node_id": "node-2"},
    )
    resp = client.post(
        "/internal/replicate",
        json={"key": "k", "value": "older", "timestamp": 1.0, "node_id": "node-2"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"applied": False}
    # original (newer) value is untouched
    assert client.get("/kv/k").json()["value"] == "newer"


def test_health_reports_node_id_and_key_count(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"node_id": "node-1", "key_count": 0}

    client.put("/kv/a", json={"value": "va"})
    client.put("/kv/b", json={"value": "vb"})

    resp = client.get("/health")
    assert resp.json() == {"node_id": "node-1", "key_count": 2}
