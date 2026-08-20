"""Node process for the leaderless (quorum-based) replication strategy.

Wraps common/server.py's app like leader_follower/leader.py does, but
unlike that single-leader design, *every* node runs this same module --
there's no dedicated leader, so any node can act as coordinator for any
client request. This module overrides PUT /kv/{key} and GET /kv/{key}
with coordinator behavior; DELETE, /internal/replicate, and /health are
inherited from create_app() unchanged, same as a leader-follower node.

Because every node is a coordinator, this module also adds one route
create_app() doesn't have: GET /internal/kv/{key}, a raw local read with
no coordinator logic. This is necessary, not incidental: since every
node's public GET /kv/{key} *is* coordinator logic, a coordinator asking
a peer for its local view of a key can't use the peer's public GET --
that would recursively re-fan the read out to the peer's own peers,
multiplying a single client read across the whole cluster instead of
touching each node once. /internal/kv/{key} is the read-path equivalent
of /internal/replicate: it hands back exactly what that one node holds
locally, nothing more.

Write path, per PUT /kv/{key}:
  1. This node acts as coordinator. Generate a timestamp and stamp this
     node's own node_id -- same as a leader-follower leader's client-facing
     write.
  2. Write locally to this node's own storage first. This always counts
     as one of W, regardless of whether storage.put() reports the write
     as applied (a fresh timestamp should always win LWW; see
     common/server.py's PutResponse docstring for the same reasoning).
  3. Fan the write out to every peer concurrently (not sequentially) via
     POST /internal/replicate, carrying the *same* timestamp and node_id
     used for the local write, exactly like leader.py's Replicator.
  4. `w` (query param, optional) is the total acks required, including
     the local write from step 2 -- so W-1 more are needed from peers.
     Defaults to config.default_w if omitted. Must be between 1 and N
     (the full cluster size, self included).
  5. Wait until W total acks are reached or `timeout_seconds` elapses,
     whichever comes first. If W isn't reached in time: fail loudly (an
     error response), never partial success -- same policy as
     leader-follower.
  6. Peers that ack after the response threshold still get the write
     applied; their in-flight requests aren't cancelled, just handed off
     to run in the background once we stop waiting on them.

Read path, per GET /kv/{key}:
  1. This node acts as coordinator. Read its own storage locally first
     (that's 1 of R) and concurrently query every peer's
     /internal/kv/{key} for their view of the key.
  2. `r` (query param, optional) is the total responses to wait for,
     including the local read. Defaults to config.default_r if omitted.
     Same 1..N bounds as `w`.
  3. A peer responding 404 (no entry for the key) still counts as a
     response reached toward R -- it's just not a candidate for step 4.
     Only an unreachable/erroring peer fails to count, same as a failed
     ack on the write path.
  4. Once R responses are collected, the response with the highest
     timestamp wins (VersionedValue.is_newer_than: same LWW rule
     storage.put() uses, node_id as tiebreak). If none of the R
     responses had an entry for the key at all, that's a real 404, not
     a coordinator error.
  5. Same timeout/fail-loudly policy as writes if R responses aren't
     collected in time. Unlike a write, a peer read that arrives late
     has nothing further to do with it, so once R is reached (or the
     deadline passes) any still-outstanding peer reads are simply
     cancelled rather than left running in the background.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Query, Request

from common.models import VersionedValue
from common.server import KVResponse, PutRequest, PutResponse, create_app
from common.storage import KVStore

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/leaderless_cluster.yaml"


# --- Cluster config --------------------------------------------------


@dataclass(frozen=True, slots=True)
class Node:
    """A single cluster node's address, as read from the cluster config."""

    host: str
    port: int

    @property
    def replicate_url(self) -> str:
        return f"http://{self.host}:{self.port}/internal/replicate"

    def internal_read_url(self, key: str) -> str:
        return f"http://{self.host}:{self.port}/internal/kv/{key}"


@dataclass(frozen=True, slots=True)
class ClusterConfig:
    """A node's view of the cluster: every node (self included), and
    the default quorum sizes/timeout. See config/leaderless_cluster.yaml
    for the on-disk schema and the meaning of each field.
    """

    nodes: list[Node]
    default_w: int
    default_r: int
    timeout_seconds: float

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ClusterConfig":
        nodes = [
            Node(host=n["host"], port=int(n["port"])) for n in raw.get("nodes", [])
        ]
        default_w = int(raw.get("default_w", 1))
        default_r = int(raw.get("default_r", 1))
        timeout_seconds = float(raw.get("timeout_seconds", 2.0))

        cls._validate_quorum(default_w, "default_w", nodes)
        cls._validate_quorum(default_r, "default_r", nodes)
        if timeout_seconds <= 0:
            raise ValueError(
                f"timeout_seconds must be positive, got {timeout_seconds}"
            )

        return cls(
            nodes=nodes,
            default_w=default_w,
            default_r=default_r,
            timeout_seconds=timeout_seconds,
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> "ClusterConfig":
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        return cls.from_dict(raw)

    @staticmethod
    def _validate_quorum(value: int, name: str, nodes: list[Node]) -> None:
        n = len(nodes)
        if not 1 <= value <= n:
            raise ValueError(
                f"{name} ({value}) must be between 1 and the number of "
                f"nodes ({n})"
            )

    def with_default_w(self, default_w: int) -> "ClusterConfig":
        """Return a copy of this config with default_w overridden, e.g.
        by a --default-w CLI flag. Validated the same way as the
        YAML-sourced value (1 to N).
        """
        self._validate_quorum(default_w, "default_w", self.nodes)
        return dataclasses.replace(self, default_w=default_w)

    def with_default_r(self, default_r: int) -> "ClusterConfig":
        """Same as with_default_w, but for default_r."""
        self._validate_quorum(default_r, "default_r", self.nodes)
        return dataclasses.replace(self, default_r=default_r)

    def peers_excluding(self, own_port: int) -> list[Node]:
        """Every configured node except the one at `own_port`.

        Matched by port alone, not host: every node in this project's
        local/dev config runs on 127.0.0.1, so port is the only thing
        that distinguishes one node's entry in `nodes` from another's.
        """
        return [node for node in self.nodes if node.port != own_port]


def _validate_quorum_param(value: int, name: str, n: int) -> None:
    """Validate a client-supplied `w`/`r` query param against the live
    cluster size. Raises the FastAPI-native HTTPException (422) rather
    than ValueError, since this runs inside a request handler, not at
    config-load time (see ClusterConfig._validate_quorum for that case).
    """
    if not 1 <= value <= n:
        raise HTTPException(
            status_code=422,
            detail=f"{name} ({value}) must be between 1 and {n} (cluster size)",
        )


# --- Quorum coordination ------------------------------------------------


class QuorumCoordinator:
    """Fans a write or read out to every peer concurrently, and reports
    back once enough of them have responded (or the timeout fires).

    Shared machinery for both the write path (replicate_write) and the
    read path (read) -- both boil down to "run one coroutine per peer,
    collect results that count until `needed` are collected or time runs
    out". What differs is what "counts" as a response and what happens
    to peers still outstanding once we stop waiting (see _gather).
    """

    def __init__(
        self, peers: list[Node], client: httpx.AsyncClient, timeout_seconds: float
    ) -> None:
        self._peers = peers
        self._client = client
        self._timeout_seconds = timeout_seconds
        # Must keep a strong reference to background write tasks: asyncio
        # only holds a *weak* reference to scheduled tasks, so a task
        # with no other referent can be garbage-collected mid-flight.
        self._background: set[asyncio.Task] = set()

    async def replicate_write(
        self, key: str, value: Any, timestamp: float, node_id: str, needed: int
    ) -> int:
        """Replicate one write to every peer; return how many acked.

        `needed` is W-1 (the coordinator's own local write already
        covers 1 of W). If needed <= 0 (W=1), every peer still gets the
        write -- just not waited on, fired into the background
        immediately -- so a low-W write still lands everywhere
        eventually instead of only on the coordinator.
        """
        payload = {
            "key": key,
            "value": value,
            "timestamp": timestamp,
            "node_id": node_id,
        }
        coros = [self._replicate_outcome(peer, payload) for peer in self._peers]

        if needed <= 0:
            for coro in coros:
                task = asyncio.ensure_future(coro)
                self._background.add(task)
                task.add_done_callback(self._background.discard)
            return 0

        acked = await self._gather(coros, needed, cancel_remaining=False)
        return len(acked)

    async def read(self, key: str, needed: int) -> list[VersionedValue | None]:
        """Query every peer's local view of `key`; return the responses
        collected (each element is that peer's entry, or None if the
        peer responded but has no entry for the key -- see module
        docstring point 3).

        `needed` is R-1 (the coordinator's own local read already
        covers 1 of R). If needed <= 0 (R=1), no peer is even contacted
        -- unlike a write, an un-consulted peer here has no "eventual"
        follow-up to preserve, so there's nothing worth firing off.
        """
        if needed <= 0:
            return []
        coros = [self._read_outcome(peer, key) for peer in self._peers]
        return await self._gather(coros, needed, cancel_remaining=True)

    async def _gather(
        self,
        coros: list[Awaitable[tuple[bool, Any]]],
        needed: int,
        *,
        cancel_remaining: bool,
    ) -> list[Any]:
        """Run `coros` concurrently; each resolves to (counts, value).
        Collect `value` for every result whose `counts` flag is True,
        stopping once `needed` such results are in or the configured
        timeout elapses, whichever comes first.

        Tasks still outstanding when we stop are either cancelled
        (cancel_remaining=True -- reads, which have nothing left to do
        once the quorum is decided) or handed off to run in the
        background (cancel_remaining=False -- writes, which should
        still land wherever they can).
        """
        pending: set[asyncio.Task] = {asyncio.ensure_future(c) for c in coros}
        collected: list[Any] = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout_seconds

        while pending and len(collected) < needed:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            done, pending = await asyncio.wait(
                pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                break  # asyncio.wait's own timeout fired
            for task in done:
                counts, value = task.result()
                if counts:
                    collected.append(value)

        for task in pending:
            if cancel_remaining:
                task.cancel()
            else:
                self._background.add(task)
                task.add_done_callback(self._background.discard)

        return collected

    async def _replicate_outcome(
        self, peer: Node, payload: dict[str, Any]
    ) -> tuple[bool, None]:
        """An "ack" is a successful HTTP response from a peer's
        /internal/replicate -- not necessarily `applied: True`. Every
        replicated write here carries a timestamp fresh at the moment of
        the local write, so peers should always apply it; `applied:
        False` would mean a peer already holds something LWW-newer,
        worth logging as a real anomaly, but the peer is still reachable
        and processed the write, so it still counts as durability
        progress.
        """
        try:
            resp = await self._client.post(peer.replicate_url, json=payload)
            resp.raise_for_status()
        except httpx.HTTPError:
            logger.warning(
                "replicate to %s failed", peer.replicate_url, exc_info=True
            )
            return False, None
        body = resp.json()
        if not body.get("applied", True):
            logger.warning(
                "replicate to %s was received but not applied "
                "(peer already held a newer/equal write) key=%s",
                peer.replicate_url, payload["key"],
            )
        return True, None

    async def _read_outcome(
        self, peer: Node, key: str
    ) -> tuple[bool, VersionedValue | None]:
        """A "response" is the peer being reachable and answering, via
        /internal/kv/{key} -- a plain local read, not that peer's own
        coordinator logic (see module docstring). 200 -> that peer's
        entry; 404 -> the peer responded but has no entry (still a
        response, just with nothing to contribute to LWW comparison).
        Anything else (unreachable, error, unexpected status) doesn't
        count, same as an unreachable peer on the write path.
        """
        try:
            resp = await self._client.get(peer.internal_read_url(key))
        except httpx.HTTPError:
            logger.warning(
                "internal read from %s failed", peer.internal_read_url(key),
                exc_info=True,
            )
            return False, None
        if resp.status_code == 404:
            return True, None
        if resp.status_code != 200:
            logger.warning(
                "internal read from %s returned unexpected status %s",
                peer.internal_read_url(key), resp.status_code,
            )
            return False, None
        body = resp.json()
        entry = VersionedValue(
            value=body["value"], timestamp=body["timestamp"], node_id=body["node_id"]
        )
        return True, entry


# --- App factory --------------------------------------------------------


def build_app(node_id: str, own_port: int, config: ClusterConfig) -> FastAPI:
    """Build a leaderless node app: common.server's app with PUT and GET
    /kv/{key} replaced by coordinator logic (see module docstring), plus
    a new GET /internal/kv/{key} for peers' raw local reads. DELETE,
    /internal/replicate, and /health are left as-is.
    """
    storage = KVStore()
    app = create_app(storage, node_id)
    peers = config.peers_excluding(own_port)
    n_total = len(config.nodes)
    client = httpx.AsyncClient(timeout=config.timeout_seconds)
    coordinator = QuorumCoordinator(peers, client, config.timeout_seconds)
    app.router.add_event_handler("shutdown", client.aclose)

    # Drop create_app()'s client-facing GET/PUT routes so ours take over
    # -- Starlette matches routes in registration order, so simply adding
    # new GET/PUT /kv/{key} routes wouldn't shadow the originals.
    app.router.routes = [
        route
        for route in app.router.routes
        if not (
            getattr(route, "path", None) == "/kv/{key}"
            and ("PUT" in getattr(route, "methods", ()) or "GET" in getattr(route, "methods", ()))
        )
    ]

    @app.put("/kv/{key}", response_model=PutResponse)
    async def put_key_coordinator(
        key: str,
        body: PutRequest,
        request: Request,
        w: int | None = Query(default=None),
    ) -> PutResponse:
        logger.info("method=%s path=%s key=%s", request.method, request.url.path, key)
        w_required = w if w is not None else config.default_w
        _validate_quorum_param(w_required, "w", n_total)

        # 1-2. Stamp a fresh version, write locally first -- always
        # counts as 1 of W (see QuorumCoordinator.replicate_write).
        timestamp = time.time()
        local_applied = storage.put(
            key, body.value, timestamp=timestamp, node_id=node_id
        )

        # 3-5. Fan out to peers concurrently; wait for the remaining
        # W-1 acks or the configured timeout, whichever comes first.
        peers_needed = w_required - 1
        peer_acked = await coordinator.replicate_write(
            key, body.value, timestamp, node_id, peers_needed
        )
        total_acked = 1 + peer_acked

        if total_acked < w_required:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"only {total_acked}/{w_required} node acks (including "
                    f"self) within {config.timeout_seconds}s timeout"
                ),
            )

        return PutResponse(applied=local_applied)

    @app.get("/kv/{key}", response_model=KVResponse)
    async def get_key_coordinator(
        key: str, request: Request, r: int | None = Query(default=None)
    ) -> KVResponse:
        logger.info("method=%s path=%s key=%s", request.method, request.url.path, key)
        r_required = r if r is not None else config.default_r
        _validate_quorum_param(r_required, "r", n_total)

        # 1. Local read first -- always counts as 1 of R.
        local_entry = storage.get(key)

        # 2-3. Concurrently query peers; wait for the remaining R-1
        # responses or the configured timeout, whichever comes first.
        peers_needed = r_required - 1
        peer_entries = await coordinator.read(key, peers_needed)
        total_responses = 1 + len(peer_entries)

        if total_responses < r_required:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"only {total_responses}/{r_required} node responses "
                    f"(including self) within {config.timeout_seconds}s timeout"
                ),
            )

        # 4. Highest timestamp among the responses that actually had an
        # entry wins; a response with no entry just doesn't compete.
        candidates = [e for e in [local_entry, *peer_entries] if e is not None]
        if not candidates:
            raise HTTPException(status_code=404, detail=f"key {key!r} not found")
        winner = candidates[0]
        for entry in candidates[1:]:
            if entry.is_newer_than(winner):
                winner = entry

        return KVResponse(
            key=key, value=winner.value, timestamp=winner.timestamp,
            node_id=winner.node_id,
        )

    @app.get("/internal/kv/{key}", response_model=KVResponse)
    def internal_get_key(key: str, request: Request) -> KVResponse:
        """Raw local read, no coordinator logic. Used by peer
        coordinators during a quorum read -- see module docstring for
        why this can't just be the public GET /kv/{key}.
        """
        logger.info("method=%s path=%s key=%s", request.method, request.url.path, key)
        entry = storage.get(key)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"key {key!r} not found")
        return KVResponse(
            key=key, value=entry.value, timestamp=entry.timestamp,
            node_id=entry.node_id,
        )

    return app


# --- Process entrypoint --------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a leaderless (quorum-based) KV node."
    )
    parser.add_argument(
        "--node-id",
        default=os.environ.get("NODE_ID"),
        required=os.environ.get("NODE_ID") is None,
        help="Unique identifier for this node (env: NODE_ID).",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("HOST", "0.0.0.0"),
        help="Host/interface to bind (env: HOST, default 0.0.0.0).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("PORT", "8001")),
        help=(
            "Port to bind (env: PORT, default 8001). Also used to find "
            "this node's own entry in the cluster config, so its peers "
            "list excludes itself."
        ),
    )
    parser.add_argument(
        "--config",
        default=os.environ.get("CLUSTER_CONFIG", DEFAULT_CONFIG_PATH),
        help=(
            "Path to the cluster config YAML (env: CLUSTER_CONFIG, "
            f"default {DEFAULT_CONFIG_PATH})."
        ),
    )
    parser.add_argument(
        "--default-w",
        type=int,
        default=None,
        help=(
            "Default write quorum size, overriding default_w from the "
            "cluster config YAML. Must be between 1 and N (cluster "
            "size). Defaults to the value in the config YAML."
        ),
    )
    parser.add_argument(
        "--default-r",
        type=int,
        default=None,
        help=(
            "Default read quorum size, overriding default_r from the "
            "cluster config YAML. Must be between 1 and N (cluster "
            "size). Defaults to the value in the config YAML."
        ),
    )
    return parser.parse_args(argv)


def _resolve_config(args: argparse.Namespace) -> ClusterConfig:
    config = ClusterConfig.from_yaml(args.config)
    if args.default_w is not None:
        config = config.with_default_w(args.default_w)
    if args.default_r is not None:
        config = config.with_default_r(args.default_r)
    return config


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args(argv)
    config = _resolve_config(args)
    app = build_app(args.node_id, args.port, config)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
