"""Shared FastAPI HTTP layer for KV store nodes.

Provides create_app(), a factory that wires a KVStore instance and a
node_id into a FastAPI app exposing the generic KV HTTP surface every
node needs: client-facing CRUD on /kv/{key}, an internal
/internal/replicate endpoint for node-to-node writes, and a /health
check.

This module is intentionally generic. It has no idea whether it's
running inside a leader, a follower, or a leaderless quorum node --
there's no forwarding, no ack-counting, no quorum logic here. That
behavior is layered on top by leader_follower/leader.py,
leader_follower/follower.py, and leaderless/node.py, which import
create_app() and build on it.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from common.storage import KVStore

logger = logging.getLogger(__name__)


# --- Request/response bodies -------------------------------------------


class PutRequest(BaseModel):
    """Body for PUT /kv/{key}.

    Only `value` is client-supplied. timestamp and node_id are generated
    server-side in create_app() -- a client-facing write is never trusted
    to supply its own version metadata.
    """

    value: Any


class PutResponse(BaseModel):
    """Response for PUT /kv/{key}.

    `applied` reports whether the write was actually stored. In normal
    operation this is always True for a client-facing PUT, since the
    server always stamps a fresh timestamp -- but storage.put() is LWW
    under the hood, so we surface the real result rather than assuming.

    `timestamp` is the timestamp this write was stamped with, echoed
    back so a caller that needs to know it (e.g. a load test recording
    ground truth for later staleness comparisons) doesn't have to
    immediately re-GET the key to find out -- a separate GET can race a
    *different*, unrelated concurrent write to the same key and pick up
    its value instead, misattributing it to this write. Always present,
    even when `applied` is False: it's still this write's own
    timestamp, just one that lost the LWW race to something already
    newer.
    """

    applied: bool
    timestamp: float


class KVResponse(BaseModel):
    """Response body for a successful read, returned by GET /kv/{key}."""

    key: str
    value: Any
    timestamp: float
    node_id: str


class DeleteResponse(BaseModel):
    """Response for DELETE /kv/{key}."""

    deleted: bool


class ReplicateRequest(BaseModel):
    """Body for POST /internal/replicate.

    Unlike PutRequest, this carries the full versioned entry -- timestamp
    and node_id are supplied by the caller and preserved as-is, since
    this endpoint is replicating someone else's write, not originating a
    new one.
    """

    key: str
    value: Any
    timestamp: float
    node_id: str


class ReplicateResponse(BaseModel):
    """Response for POST /internal/replicate."""

    applied: bool


class HealthResponse(BaseModel):
    """Response for GET /health."""

    node_id: str
    key_count: int


# --- App factory ---------------------------------------------------------


def create_app(storage: KVStore, node_id: str) -> FastAPI:
    """Build a FastAPI app wired to `storage`, identifying as `node_id`.

    Exposes the generic KV HTTP surface shared by every node regardless
    of replication strategy. Leader/follower- or leaderless-specific
    behavior (forwarding writes, counting acks, quorum reads/writes)
    does not belong here -- callers import this and layer that on top.
    """
    app = FastAPI(title=f"kv-node[{node_id}]")

    @app.get("/kv/{key}", response_model=KVResponse)
    def get_key(key: str, request: Request) -> KVResponse:
        logger.info("method=%s path=%s key=%s", request.method, request.url.path, key)
        entry = storage.get(key)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"key {key!r} not found")
        return KVResponse(
            key=key,
            value=entry.value,
            timestamp=entry.timestamp,
            node_id=entry.node_id,
        )

    @app.put("/kv/{key}", response_model=PutResponse)
    def put_key(key: str, body: PutRequest, request: Request) -> PutResponse:
        logger.info("method=%s path=%s key=%s", request.method, request.url.path, key)
        # Server generates the timestamp and stamps its own node_id --
        # never trust a client to supply these for a fresh write.
        timestamp = time.time()
        applied = storage.put(key, body.value, timestamp=timestamp, node_id=node_id)
        return PutResponse(applied=applied, timestamp=timestamp)

    @app.delete("/kv/{key}", response_model=DeleteResponse)
    def delete_key(key: str, request: Request) -> DeleteResponse:
        logger.info("method=%s path=%s key=%s", request.method, request.url.path, key)
        deleted = storage.delete(key)
        return DeleteResponse(deleted=deleted)

    @app.post("/internal/replicate", response_model=ReplicateResponse)
    def replicate(body: ReplicateRequest, request: Request) -> ReplicateResponse:
        logger.info(
            "method=%s path=%s key=%s", request.method, request.url.path, body.key
        )
        # Preserve the incoming timestamp/node_id as-is -- this call is
        # replicating someone else's write, not originating a new one.
        applied = storage.put(
            body.key, body.value, timestamp=body.timestamp, node_id=body.node_id
        )
        return ReplicateResponse(applied=applied)

    @app.get("/health", response_model=HealthResponse)
    def health(request: Request) -> HealthResponse:
        logger.info("method=%s path=%s", request.method, request.url.path)
        return HealthResponse(node_id=node_id, key_count=storage.key_count())

    return app
