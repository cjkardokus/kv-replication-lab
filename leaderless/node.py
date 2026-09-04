"""Node process for the leaderless (quorum-based) replication strategy.

Wraps common/server.py's app like leader_follower/leader.py does, but
unlike that single-leader design, *every* node runs this same module --
there's no dedicated leader, so any node can act as coordinator for any
client request. This module overrides PUT /kv/{key} and GET /kv/{key}
with coordinator behavior; DELETE and /health are inherited from
create_app() unchanged, same as a leader-follower node. /internal/replicate
is also inherited unchanged *unless* this node was started with
--fault-inject-delay-ms set (default: unset, meaning unchanged) -- see
"Fault injection" below.

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
  3. Fan the write out to *every* peer concurrently (not sequentially,
     and not just enough to satisfy W) via POST /internal/replicate,
     carrying the *same* timestamp and node_id used for the local write,
     exactly like leader.py's Replicator. Durability is deliberately
     decoupled from the ack-wait below: every write is attempted
     everywhere, regardless of W.
  4. `w` (query param, optional) is the total acks required, including
     the local write from step 2 -- so W-1 more are needed from peers,
     and only W-1 of the peers contacted in step 3 are actually waited
     on; the rest keep running in the background (see step 6). Defaults
     to config.default_w if omitted. Must be between 1 and N (the full
     cluster size, self included).
  5. Wait until W total acks are reached or `timeout_seconds` elapses,
     whichever comes first. If W isn't reached in time: fail loudly (an
     error response), never partial success -- same policy as
     leader-follower.
  6. Peers that ack after the response threshold still get the write
     applied; their in-flight requests aren't cancelled, just handed off
     to run in the background once we stop waiting on them.

Together, steps 3 and the read path's exactly-R contact below make the
classic W+R>N overlap guarantee literal, not approximate: at the moment
a write is acked, at least W nodes (any W, not a specific pre-chosen
subset -- see step 3) already hold it and can only gain more holders
over time, never lose one (LWW never regresses); a later read touches
exactly R nodes. Two subsets of the N nodes of size >=W and R must
intersect whenever W+R>N, by pigeonhole -- a plain fact about set sizes,
true regardless of *which* nodes ended up in each subset. When W+R<=N,
no such intersection is guaranteed, and a disjoint pair is a real,
reachable outcome, not just a theoretical one -- see
QuorumCoordinator.replicate_write and .read for exactly how each subset
is formed.

Read path, per GET /kv/{key}:
  1. This node acts as coordinator. Read its own storage locally first
     (that's 1 of R) and concurrently query exactly R-1 peers'
     /internal/kv/{key} for their view of the key -- no more, no fewer:
     unlike the write path, a read has no durability reason to touch
     extra peers, so R nodes total (including self) is exactly how many
     ever get contacted for a given read.
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
     collected in time. A peer read that arrives late has nothing
     further to do with its result, but once R is reached (or the
     deadline passes) any still-outstanding peer reads are left to
     finish in the background rather than cancelled -- see
     QuorumCoordinator._gather for why cancelling them turned out to be
     the wrong call.

Fault injection (--fault-inject-delay-ms, off by default):
  This node's /internal/replicate can be given an artificial delay,
  applied before it acknowledges a peer's replicated write, purely to
  make an otherwise-too-fast-to-observe race reproducible on localhost --
  see docs/results.md's "Demonstrating the W+R boundary case" section and
  docs/AUDIT_FINDINGS.md's §2 for why this exists: on loopback, an
  unconditional full-peer-flood write (see replicate_write above)
  normally lands everywhere in low single-digit milliseconds, far faster
  than this benchmark's real request-arrival rate, which makes a
  genuinely non-guaranteed config (W+R<=N, e.g. W=2,R=3 on this lab's
  5-node cluster) measure ~0% staleness locally even though nothing
  guarantees that. This flag has NO role in normal cluster behavior --
  it defaults to 0 (disabled), in which case /internal/replicate is
  exactly create_app()'s stock handler, unmodified -- and it is never
  driven by ClusterConfig/the cluster YAML, only by this process's own
  CLI flag/env var, so it can never be silently enabled by cluster
  config alone. See build_app() for where it's wired in.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import os
import random
import time
from collections.abc import Awaitable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import ValidationError

from common.cli import add_node_identity_args
from common.models import VersionedValue
from common.replication_client import build_replication_client
from common.server import (
    KVResponse,
    PutRequest,
    PutResponse,
    ReplicateRequest,
    ReplicateResponse,
    create_app,
    replace_route,
)
from common.storage import KVStore

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/leaderless_cluster.yaml"

# leader_follower/leader.py and leaderless/node.py both fan requests out
# to several peers concurrently (leaderless: reads too, not just
# writes), so both need an httpx.AsyncClient with a connection pool
# sized well above httpx's conservative defaults --
# common/replication_client.py's build_replication_client() builds that
# client for both; see that module for the pool-sizing rationale and the
# exact constants. What's below is leaderless-specific: how much of that
# shared headroom this strategy's own traffic pattern actually uses.
#
# Writes flood every peer regardless of W (see
# QuorumCoordinator.replicate_write), so per-write connection demand is
# the full peer count; reads contact exactly R-1 peers (see
# QuorumCoordinator.read), so per-read demand scales with R, not peer
# count. At this lab's cluster size (N=5) neither comes close to
# exhausting the shared pool's headroom even at full load.

# NOTE: there used to be a _FANOUT_MARGIN constant here -- writes and
# reads contacted `needed + margin` peers instead of exactly `needed`,
# as resilience against one slow/unreachable peer without contacting
# literally everyone. Removed: it made W/R mean something other than
# their literal definitions (a W-write's real coverage was
# min(W+margin, N), not W), and at this lab's cluster size it quietly
# rescued the classic W+R>N overlap rule even for configs deliberately
# chosen to sit *on* the W+R=N boundary to demonstrate it failing --
# see git history (leaderless/node.py, this constant) and
# docs/results.md for the full writeup that led to removing it. Writes
# now flood every peer unconditionally (see replicate_write) and reads
# contact exactly `needed` peers (see read/_select_read_peers), so W
# and R mean literally what they say, at the cost of the resilience
# margin used to provide: a single slow/unreachable peer among a read's
# exactly-`needed` selection, or among the acks a write waits on, now
# fails that request's quorum outright instead of being covered by a
# spare.


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
    def from_dict(cls, raw: dict[str, Any]) -> ClusterConfig:
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
    def from_yaml(cls, path: str | Path) -> ClusterConfig:
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

    def with_default_w(self, default_w: int) -> ClusterConfig:
        """Return a copy of this config with default_w overridden, e.g.
        by a --default-w CLI flag. Validated the same way as the
        YAML-sourced value (1 to N).
        """
        self._validate_quorum(default_w, "default_w", self.nodes)
        return dataclasses.replace(self, default_w=default_w)

    def with_default_r(self, default_r: int) -> ClusterConfig:
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
    """Fans a write or read out to a subset of peers concurrently, and
    reports back once enough of them have responded (or the timeout
    fires).

    Shared machinery for both the write path (replicate_write) and the
    read path (read) -- both boil down to "run one coroutine per
    contacted peer, collect results that count until `needed` are
    collected or time runs out". What differs is what "counts" as a
    response and what happens to peers still outstanding once we stop
    waiting (see _gather).
    """

    def __init__(
        self,
        peers: list[Node],
        client: httpx.AsyncClient,
        timeout_seconds: float,
    ) -> None:
        self._peers = peers
        self._client = client
        self._timeout_seconds = timeout_seconds
        # Must keep a strong reference to background write tasks: asyncio
        # only holds a *weak* reference to scheduled tasks, so a task
        # with no other referent can be garbage-collected mid-flight.
        self._background: set[asyncio.Task[Any]] = set()

    def _select_read_peers(self, needed: int) -> list[Node]:
        """Peers to contact for a quorum-bounded read: exactly `needed`,
        no more -- see module docstring's read-path step 1 for why reads
        don't get the write path's full-flood treatment. Chosen at
        random each call so which peers get read traffic doesn't settle
        into a fixed pattern.
        """
        if needed >= len(self._peers):
            return self._peers
        return random.sample(self._peers, needed)

    async def replicate_write(
        self, key: str, value: Any, timestamp: float, node_id: str, needed: int
    ) -> int:
        """Replicate one write to every peer; return how many acked.

        `needed` is W-1 (the coordinator's own local write already
        covers 1 of W). Every peer is contacted, unconditionally,
        regardless of `needed` -- this is about durability, not racing
        to satisfy `needed`: a write should eventually land everywhere,
        not just on however many nodes W requires an ack from. `_gather`
        below only *waits* for `needed` of them (0 is a valid `needed`,
        for W=1 -- see module docstring write-path step 4); whichever
        peers are still outstanding once that's satisfied (or once
        `needed` is already <= 0, meaning none are waited on at all) are
        handed off to finish in the background, same as ever.
        """
        payload = {
            "key": key,
            "value": value,
            "timestamp": timestamp,
            "node_id": node_id,
        }
        coros = [self._replicate_outcome(peer, payload) for peer in self._peers]
        acked = await self._gather(coros, needed)
        return len(acked)

    async def read(self, key: str, needed: int) -> list[VersionedValue | None]:
        """Query exactly `needed` peers' local view of `key`; return the
        responses collected (each element is that peer's entry, or None
        if the peer responded but has no entry for the key -- see
        module docstring point 3).

        `needed` is R-1 (the coordinator's own local read already
        covers 1 of R). If needed <= 0 (R=1), no peer is even contacted
        -- an un-consulted peer here has no "eventual" follow-up to
        preserve the way an un-waited-on write does, so there's nothing
        worth firing off.
        """
        if needed <= 0:
            return []
        coros = [self._read_outcome(peer, key) for peer in self._select_read_peers(needed)]
        return await self._gather(coros, needed)

    async def _gather(
        self, coros: Sequence[Awaitable[tuple[bool, Any]]], needed: int
    ) -> list[Any]:
        """Run `coros` concurrently; each resolves to (counts, value).
        Takes a Sequence, not a list, specifically so callers passing a
        list[Awaitable[tuple[bool, VersionedValue | None]]] (a more
        specific element type than tuple[bool, Any]) type-check --
        list's own type parameter is invariant, so a list of the more
        specific type isn't a list of the less specific one as far as
        the type checker is concerned, even though every real use here
        is read-only (Sequence is covariant, and _gather never mutates
        `coros`).

        Collect `value` for every result whose `counts` flag is True,
        stopping once `needed` such results are in or the configured
        timeout elapses, whichever comes first.

        Peers still outstanding when we stop are handed off to finish
        in the background rather than cancelled -- including for reads,
        which have no further use for the result once the quorum is
        decided. That might read as wasted work, but cancelling an
        in-flight httpx request turns out not to reliably release its
        connection: httpcore's cleanup for a request cancelled mid-read
        (httpcore/_async/connection_pool.py's PoolByteStream.aclose)
        shields only the stream close itself, not the connection-pool
        bookkeeping that follows it, so that follow-up can itself be
        cut short by the same ambient cancellation -- verified against
        this project's pinned httpx/httpcore versions by hammering a
        node with concurrent reads and watching its outbound sockets to
        peers via `ss`: cancelling left sockets stuck in CLOSE-WAIT
        indefinitely (no drain even after 30s idle), while letting them
        run to completion in the background did not. A peer read that
        loses the race still costs a socket and a small amount of peer
        CPU, but it deterministically finishes and releases cleanly --
        cheaper than a real leak.

        The hand-off runs in a `finally`, not after the loop, because
        this coroutine can itself be cancelled mid-`await` -- e.g. the
        client-facing request it's running under gets cancelled because
        that client disconnected. Without the `finally`, that
        cancellation would unwind straight out of the loop, skip the
        hand-off entirely, and drop the only reference to whatever
        peer tasks were still `pending` -- the exact leak this method
        exists to avoid, just reached by a different route.
        """
        pending: set[asyncio.Task[Any]] = {asyncio.ensure_future(c) for c in coros}
        collected: list[Any] = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout_seconds

        try:
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
        finally:
            for task in pending:
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
        # Validate against the real response schema rather than indexing
        # resp.json() as a raw dict -- a peer that responds 200 with a
        # non-JSON or incomplete body must not crash this coroutine with
        # an uncaught exception: json.JSONDecodeError ("not valid JSON")
        # and pydantic.ValidationError ("valid JSON, wrong/missing
        # fields") are handled identically here, the same as
        # httpx.HTTPError above -- a failed ack, not a raised exception
        # that would otherwise propagate out of _gather() and surface to
        # the client as a bare 500 instead of the usual "N/M acks" 503.
        try:
            parsed = ReplicateResponse.model_validate(resp.json())
        except (json.JSONDecodeError, ValidationError):
            logger.warning(
                "replicate to %s returned a malformed response body",
                peer.replicate_url, exc_info=True,
            )
            return False, None
        if not parsed.applied:
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
        # Validate against the real response schema rather than indexing
        # resp.json() as a raw dict -- see _replicate_outcome above for
        # why: a peer's 200 with a non-JSON or incomplete body must not
        # crash this coroutine with an uncaught exception (previously a
        # bare KeyError here), it's a failed response, same as an
        # unreachable peer.
        try:
            parsed = KVResponse.model_validate(resp.json())
        except (json.JSONDecodeError, ValidationError):
            logger.warning(
                "internal read from %s returned a malformed response body",
                peer.internal_read_url(key), exc_info=True,
            )
            return False, None
        entry = VersionedValue(
            value=parsed.value, timestamp=parsed.timestamp, node_id=parsed.node_id
        )
        return True, entry


# --- App factory --------------------------------------------------------


def build_app(
    node_id: str,
    own_port: int,
    config: ClusterConfig,
    *,
    fault_inject_delay_seconds: float = 0.0,
) -> FastAPI:
    """Build a leaderless node app: common.server's app with PUT and GET
    /kv/{key} replaced by coordinator logic (see module docstring), plus
    a new GET /internal/kv/{key} for peers' raw local reads. DELETE and
    /health are left as-is.

    `fault_inject_delay_seconds` is TEST/BENCHMARK-ONLY (see module
    docstring's "Fault injection" section) and defaults to 0.0, meaning
    completely disabled: /internal/replicate is then also left exactly
    as-is, unmodified from create_app()'s stock handler. Passing a
    positive value overrides that one route with a version that sleeps
    for the given duration before acknowledging -- nothing else about
    this node's behavior changes.
    """
    storage = KVStore()
    app = create_app(storage, node_id)
    peers = config.peers_excluding(own_port)
    n_total = len(config.nodes)
    client = build_replication_client(config.timeout_seconds)
    coordinator = QuorumCoordinator(peers, client, config.timeout_seconds)
    app.router.add_event_handler("shutdown", client.aclose)

    # Drop create_app()'s client-facing GET/PUT routes so ours take over
    # -- see replace_route()'s docstring for why this is necessary, not
    # just tidy.
    replace_route(app, "/kv/{key}", {"PUT", "GET"})

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

        return PutResponse(applied=local_applied, timestamp=timestamp)

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

    if fault_inject_delay_seconds > 0:
        # TEST/BENCHMARK ONLY -- see module docstring's "Fault injection"
        # section and build_app()'s own docstring. Only reached when a
        # caller explicitly opts in; otherwise this whole block doesn't
        # run and /internal/replicate stays create_app()'s stock,
        # unmodified handler.
        replace_route(app, "/internal/replicate", {"POST"})

        @app.post("/internal/replicate", response_model=ReplicateResponse)
        async def replicate_with_injected_delay(body: ReplicateRequest, request: Request) -> ReplicateResponse:
            logger.info(
                "method=%s path=%s key=%s [fault-injected delay=%.3fs]",
                request.method, request.url.path, body.key, fault_inject_delay_seconds,
            )
            # The delay runs *before* applying the write, so a peer that
            # hasn't woken up yet genuinely doesn't have it -- a read
            # landing on this node during the sleep sees the old value
            # (or 404), exactly the race this flag exists to widen. This
            # otherwise mirrors create_app()'s replicate() exactly.
            await asyncio.sleep(fault_inject_delay_seconds)
            applied = storage.put(
                body.key, body.value, timestamp=body.timestamp, node_id=body.node_id
            )
            return ReplicateResponse(applied=applied)

    return app


# --- Process entrypoint --------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a leaderless (quorum-based) KV node."
    )
    add_node_identity_args(
        parser,
        default_port=8001,
        port_help=(
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
    parser.add_argument(
        "--fault-inject-delay-ms",
        type=float,
        default=float(os.environ.get("FAULT_INJECT_REPLICATE_DELAY_MS", "0")),
        help=(
            "TEST/BENCHMARK ONLY: artificial delay in milliseconds, "
            "injected before this node acknowledges a peer's "
            "/internal/replicate call -- exists purely to make an "
            "otherwise-too-fast-to-observe replication race reproducible "
            "on localhost (see docs/results.md's 'Demonstrating the W+R "
            "boundary case' section). Has no role in normal cluster "
            "behavior. Defaults to 0 (disabled -- /internal/replicate is "
            "then create_app()'s stock, unmodified handler; env: "
            "FAULT_INJECT_REPLICATE_DELAY_MS)."
        ),
    )
    args = parser.parse_args(argv)
    if args.fault_inject_delay_ms < 0:
        parser.error("--fault-inject-delay-ms must be >= 0")
    return args


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
    app = build_app(
        args.node_id,
        args.port,
        config,
        fault_inject_delay_seconds=args.fault_inject_delay_ms / 1000.0,
    )
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
