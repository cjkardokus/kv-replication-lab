"""Leader node process for the leader-follower replication strategy.

Wraps common/server.py's app like follower.py does, but overrides
PUT /kv/{key}: the leader is the only node that originates client
writes needing replication, so it's the only place that needs to
know about followers, ack_required, and timeouts. GET, DELETE,
/internal/replicate, and /health are inherited from create_app()
unchanged -- a leader can also take direct replicate calls (e.g. from
an operator re-syncing a follower) and serve reads/deletes exactly
like any other node.

Write path, per PUT /kv/{key}:
  1. Write locally to this node's own storage first.
  2. Fan the write out to every configured follower concurrently (not
     sequentially) via POST /internal/replicate, carrying the *same*
     timestamp and node_id used for the local write -- so followers
     store the identical versioned entry, not a fresh one.
  3. Wait until at least `ack_required` followers have responded
     successfully, or `timeout_seconds` elapses, whichever comes first.
  4. If ack_required is met in time: return success. If the timeout
     fires first: fail loudly (an error response), never partial
     success -- the client should not be told "ok" for a write whose
     durability we couldn't confirm.
  5. Followers that ack after the response threshold still get the
     write applied; their in-flight requests are not cancelled, we
     just stop waiting on them once we've either got enough acks or
     run out of time.
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
from typing import Any

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Request

from common.server import PutRequest, PutResponse, create_app
from common.storage import KVStore

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/leader_follower_cluster.yaml"

# At ack_required>=1 the write path awaits at least one follower ack
# before returning, which naturally throttles how many replicate()
# calls can be in flight at once (bounded by however many client writes
# are concurrently in flight, e.g. ~50 under this lab's load test). At
# ack_required=0 (fire-and-forget) there is no such backpressure --
# every write immediately fires len(followers) background tasks with
# nothing waiting on them, so a sustained burst of writes can demand far
# more simultaneous outbound connections than httpx's conservative
# defaults (100 total / 20 keepalive) provide. Once that pool was
# exhausted, a queued replicate call used to wait out the full client
# timeout for a connection slot and then give up (httpx.PoolTimeout) --
# a write the client was told succeeded never actually reached that
# follower at all. Sizing the pool up (below) fixes exactly that: with
# ack_required 1..4, replicate delivery is 100% and the leader logs zero
# exceptions, confirmed via experiments/run_comparison.py.
#
# ack_required=0 used to be a different story, and *raising this pool
# size alone did not fix it* -- it only relocated where it broke. The
# original small pool was accidentally acting as admission control: it
# dropped excess replicate attempts fast (silent data loss -- ~87% of
# writes never reached any follower) but kept followers and the leader
# itself responsive (0 client-visible failures). With just the pool
# enlarged and no other change, more concurrent replicate attempts
# actually reached the followers, which have no concurrency limiting of
# their own (plain uvicorn, one event loop) -- under this lab's
# sustained, unthrottled load test they slowed down, which kept each
# replicate() call alive longer, which let ack_required=0's
# backpressure-free write path pile up yet more concurrent background
# tasks before the old ones drained. That feedback loop eventually
# starved the *leader's own* single-threaded event loop: at full
# load-test scale (before the fix below) this was observed causing 2,291
# client-visible timeouts on the leader's plain client-facing
# PUT /kv/{key} -- an in-memory dict write with no I/O of its own -- plus
# a ~3x elapsed-time blowup versus every other ack_required value.
#
# That was never a connection-pool problem -- it was the total absence
# of any bound on concurrent in-flight replicate fan-out, independent of
# pool size. `Replicator` now enforces that bound directly, via
# `_MAX_CONCURRENT_REPLICATE_CALLS` below (a semaphore around the
# outbound POST in `Replicator._send`, shared across every write this
# leader handles, not just one write's own fan-out) -- see that
# constant's comment for the sizing rationale. The pool size here and
# the semaphore below do different jobs: the pool exists so a call that
# *is* admitted never fails to find a connection; the semaphore exists
# so this leader never admits more concurrent follower calls than it or
# the followers can absorb, regardless of how fast writes arrive. See
# docs/results.md for the ack_required sweep this was confirmed against,
# both before and after the semaphore fix.
#
# _CLIENT_MAX_CONNECTIONS/_CLIENT_MAX_KEEPALIVE_CONNECTIONS below are a
# single module-level setting applied identically no matter what
# ack_required this leader is launched with -- there is no per-config
# pool sizing. That matters because Replicator never cancels a follower
# it stops waiting on (see Replicator.replicate below), so *every*
# ack_required value fans a write out to all 4 followers concurrently,
# not just the ones it waits on -- ack_required only changes how many of
# those 4 in-flight calls the write path blocks on before returning.
# Confirmed by isolation-testing this exact pool change at fixed
# ack_required values (same followers, same load, only the pool setting
# swapped, leader.py restored after): at ack_required=2, the old
# unset-Limits default (100/20, pre-dating this pool sizing) measured
# 0.00% staleness / 0 failures on a reduced run where the current 500/100
# pool measures 8.87% staleness on the identical run; at ack_required=3,
# 0.06% (old) vs 4.60% (new). In other words, the small pool wasn't only
# masking ack_required=0's silent drops -- it was quietly acting as
# admission control for every ack_required value, at a severity scaled to
# each one's own concurrency. Enlarging it for ack_required=0 removed
# that masking everywhere at once, which is why the ack_required=1..4
# staircase in docs/results.md only became measurable after this fix,
# not before it. The staircase is genuine replication lag, not an
# artifact of this fix or of the _MAX_CONCURRENT_REPLICATE_CALLS
# semaphore added later -- see the isolation-test evidence above -- but
# it's worth knowing that connection-pool size is currently one global
# knob that shapes every config's apparent staleness, not a per-config
# setting.
_CLIENT_MAX_CONNECTIONS = 500
_CLIENT_MAX_KEEPALIVE_CONNECTIONS = 100

# Fraction of `timeout_seconds` given to acquiring a connection from
# the pool, specifically, rather than the connect/read/write phases of
# a replicate call -- see build_app().
_POOL_TIMEOUT_FRACTION = 0.25

# Bounds how many replicate() calls to followers this leader ever has
# outstanding *at once, across every write it's handling* -- unlike
# _CLIENT_MAX_CONNECTIONS above, which only stops an admitted call from
# failing to find a connection, this is what actually stops
# ack_required=0 from admitting unbounded concurrent fan-out in the
# first place (see that constant's comment for the failure mode this
# closes: an unbounded pile-up of concurrent replicate() calls that
# eventually starved the leader's own event loop). Enforced in
# Replicator._send, around the outbound POST only -- not around response
# parsing, which is pure CPU and never worth gating.
#
# Sized well above what a backpressured config (ack_required>=1) could
# ever demand at this lab's normal load: every ack_required value fans a
# write out to all len(followers) followers concurrently regardless of
# what it waits on (see Replicator.replicate), so the true worst case is
# (concurrent client writes) x (followers), not just concurrent writes.
# At this lab's load test (NUM_WORKERS=50, so in the extreme all 50
# could be mid-write at once) x 4 followers = 200 is the theoretical
# ceiling; realistic concurrent write counts run far lower (~30% of 50
# workers are writes per the load test's read/write mix, i.e. roughly 15
# writers x 4 =~ 60 concurrent sends in practice). 256 leaves comfortable
# headroom above both numbers, so ack_required>=1 configs essentially
# never see this semaphore as a bottleneck, while still sitting well
# under _CLIENT_MAX_CONNECTIONS (500) -- so *this* is what actually runs
# out first for ack_required=0's unthrottled write path, not an
# httpx.PoolTimeout. That distinction matters: hitting this semaphore
# just means a coroutine waits in-process for a slot (cheap, no network
# resource held); hitting the connection pool's own limit means an
# httpx call that already holds a slot in line for a socket, which is
# the more expensive, harder-to-recover-from version of the same
# problem this constant exists to avoid.
_MAX_CONCURRENT_REPLICATE_CALLS = 256


# --- Cluster config --------------------------------------------------


@dataclass(frozen=True, slots=True)
class Follower:
    """A single follower's address, as read from the cluster config."""

    host: str
    port: int

    @property
    def replicate_url(self) -> str:
        return f"http://{self.host}:{self.port}/internal/replicate"


@dataclass(frozen=True, slots=True)
class ClusterConfig:
    """The leader's view of the cluster: who to replicate to, and how
    hard to wait for them. See config/leader_follower_cluster.yaml for
    the on-disk schema and the meaning of each field.
    """

    followers: list[Follower]
    ack_required: int
    timeout_seconds: float

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ClusterConfig":
        followers = [
            Follower(host=f["host"], port=int(f["port"]))
            for f in raw.get("followers", [])
        ]
        ack_required = int(raw.get("ack_required", 0))
        timeout_seconds = float(raw.get("timeout_seconds", 2.0))

        cls._validate_ack_required(ack_required, followers)
        if timeout_seconds <= 0:
            raise ValueError(
                f"timeout_seconds must be positive, got {timeout_seconds}"
            )

        return cls(
            followers=followers,
            ack_required=ack_required,
            timeout_seconds=timeout_seconds,
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> "ClusterConfig":
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        return cls.from_dict(raw)

    @staticmethod
    def _validate_ack_required(ack_required: int, followers: list[Follower]) -> None:
        if not 0 <= ack_required <= len(followers):
            raise ValueError(
                f"ack_required ({ack_required}) must be between 0 and "
                f"the number of followers ({len(followers)})"
            )

    def with_ack_required(self, ack_required: int) -> "ClusterConfig":
        """Return a copy of this config with ack_required overridden,
        e.g. by a --ack-required CLI flag. Validated the same way as
        the YAML-sourced value (0 to len(followers)).
        """
        self._validate_ack_required(ack_required, self.followers)
        return dataclasses.replace(self, ack_required=ack_required)


# --- Replication ------------------------------------------------------


class Replicator:
    """Fans a write out to all followers concurrently and reports back
    how many acked within the configured timeout.

    Followers that don't finish in time are *not* cancelled -- they're
    handed off to run in the background so the write still lands
    everywhere eventually, just without blocking the client on it.

    Concurrent in-flight replicate calls, across *every* write this
    Replicator handles (not just one write's own fan-out to
    len(followers) followers), are bounded by a shared semaphore sized
    at _MAX_CONCURRENT_REPLICATE_CALLS -- see that constant for the
    sizing rationale. This is what gives ack_required=0's
    fire-and-forget write path real backpressure: with nothing ever
    awaiting a follower, a sustained burst of writes previously had no
    limit on how many replicate() calls could pile up concurrently,
    which under sustained load eventually starved the leader's own event
    loop (see _CLIENT_MAX_CONNECTIONS' comment for the investigation
    that found this). ack_required>=1 configs are already naturally
    throttled by however many client writes are concurrently in flight,
    so this bound is sized to sit comfortably above that in normal
    operation and meaningfully engage only for ack_required=0.
    """

    def __init__(self, config: ClusterConfig, client: httpx.AsyncClient) -> None:
        self._config = config
        self._client = client
        self._semaphore = asyncio.Semaphore(_MAX_CONCURRENT_REPLICATE_CALLS)
        # Must keep a strong reference to background tasks: asyncio only
        # holds a *weak* reference to scheduled tasks, so a task with no
        # other referent can be garbage-collected mid-flight.
        self._background: set[asyncio.Task] = set()

    async def replicate(
        self, key: str, value: Any, timestamp: float, node_id: str
    ) -> int:
        """Replicate one write to every follower; return the ack count.

        An "ack" is a successful HTTP response from a follower's
        /internal/replicate -- not necessarily `applied: True`. In this
        single-leader design every replicated write carries a timestamp
        that's fresh at the moment of the local write, so followers
        should always apply it; a follower reporting `applied: False`
        would mean it already holds something LWW-newer, which signals
        a real anomaly worth logging, but the follower is still reachable
        and has processed the write attempt, so it still counts as
        durability progress.
        """
        payload = {
            "key": key,
            "value": value,
            "timestamp": timestamp,
            "node_id": node_id,
        }
        pending: set[asyncio.Task] = {
            asyncio.ensure_future(self._send(follower, payload))
            for follower in self._config.followers
        }

        acked = 0
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._config.timeout_seconds
        while pending and acked < self._config.ack_required:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            done, pending = await asyncio.wait(
                pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                break  # asyncio.wait's own timeout fired
            acked += sum(1 for task in done if task.result())

        # Anything still outstanding (either racing to finish, or a slow
        # follower) keeps running -- we just stop waiting on it here.
        for task in pending:
            self._background.add(task)
            task.add_done_callback(self._background.discard)

        return acked

    async def _send(self, follower: Follower, payload: dict[str, Any]) -> bool:
        if self._client.is_closed:
            # The leader is shutting down -- build_app()'s "shutdown"
            # event handler already closed self._client -- and this call
            # was still queued behind _semaphore when that happened. The
            # semaphore is what makes this common enough to guard against
            # explicitly: it can leave far more replicate() calls queued,
            # not yet even attempted, at a given moment than pre-semaphore
            # code ever did (see _MAX_CONCURRENT_REPLICATE_CALLS), so a
            # shutdown landing mid-flood (e.g. ack_required=0 under
            # sustained load) can strand a real number of them here.
            # Treat that the same as any other failed send rather than
            # letting httpx's "client has been closed" RuntimeError
            # surface as an unhandled exception in an unawaited
            # background task (asyncio logs those as "Task exception was
            # never retrieved" -- alarming, but harmless: nothing was
            # ever waiting on this task's result by the time it runs).
            # This narrows, but can't fully close, the race -- the client
            # could still close between this check and the POST below;
            # closing that completely would mean draining outstanding
            # background sends before build_app() closes the client at
            # all, which is more machinery than this guard needs.
            return False
        # Only the outbound POST itself is gated by the semaphore -- a
        # write queued behind it waits in-process for a slot rather than
        # ever reaching the network, which is the whole point (see
        # _MAX_CONCURRENT_REPLICATE_CALLS). Response parsing below is
        # pure CPU on an already-received response, so it's released
        # before that runs.
        async with self._semaphore:
            try:
                resp = await self._client.post(follower.replicate_url, json=payload)
                resp.raise_for_status()
            except httpx.HTTPError:
                logger.warning(
                    "replicate to %s failed", follower.replicate_url, exc_info=True
                )
                return False
        body = resp.json()
        if not body.get("applied", True):
            logger.warning(
                "replicate to %s was received but not applied "
                "(follower already held a newer/equal write) key=%s",
                follower.replicate_url, payload["key"],
            )
        return True


# --- App factory --------------------------------------------------------


def build_app(node_id: str, config: ClusterConfig) -> FastAPI:
    """Build a leader app: common.server's app with PUT /kv/{key}
    replaced by the replicate-then-ack-count logic described in this
    module's docstring. GET/DELETE/replicate/health are left as-is.
    """
    storage = KVStore()
    app = create_app(storage, node_id)
    # A bare float for `timeout=` applies to connect/read/write/pool
    # phases alike, so pool-acquire contention shares the exact same
    # budget as a slow-but-reachable follower. Give pool-acquire its own,
    # shorter timeout instead: a call that can't get a connection
    # quickly fails fast (see _POOL_TIMEOUT_FRACTION) rather than tying
    # up the whole ack-collection deadline waiting on a slot that was
    # never going to free up in time.
    client_timeout = httpx.Timeout(
        config.timeout_seconds,
        pool=config.timeout_seconds * _POOL_TIMEOUT_FRACTION,
    )
    client = httpx.AsyncClient(
        timeout=client_timeout,
        limits=httpx.Limits(
            max_connections=_CLIENT_MAX_CONNECTIONS,
            max_keepalive_connections=_CLIENT_MAX_KEEPALIVE_CONNECTIONS,
        ),
    )
    replicator = Replicator(config, client)
    app.router.add_event_handler("shutdown", client.aclose)

    # Drop create_app()'s client-facing PUT route so ours takes over --
    # Starlette matches routes in registration order, so simply adding a
    # new PUT /kv/{key} route wouldn't shadow the original.
    app.router.routes = [
        route
        for route in app.router.routes
        if not (
            getattr(route, "path", None) == "/kv/{key}"
            and "PUT" in getattr(route, "methods", ())
        )
    ]

    @app.put("/kv/{key}", response_model=PutResponse)
    async def put_key_leader(key: str, body: PutRequest, request: Request) -> PutResponse:
        logger.info("method=%s path=%s key=%s", request.method, request.url.path, key)

        # 1. Write locally first.
        timestamp = time.time()
        local_applied = storage.put(
            key, body.value, timestamp=timestamp, node_id=node_id
        )

        # 2-3. Fan out to followers concurrently; wait for ack_required
        # acks or the configured timeout, whichever comes first.
        acked = await replicator.replicate(key, body.value, timestamp, node_id)

        # 4. Fail loudly on an unmet quorum -- never partial success.
        if acked < config.ack_required:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"only {acked}/{config.ack_required} follower acks "
                    f"within {config.timeout_seconds}s timeout"
                ),
            )

        return PutResponse(applied=local_applied, timestamp=timestamp)

    return app


# --- Process entrypoint --------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a leader-follower KV leader node."
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
        default=int(os.environ.get("PORT", "8000")),
        help="Port to bind (env: PORT, default 8000).",
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
        "--ack-required",
        type=int,
        default=None,
        help=(
            "Number of follower acks required before a write succeeds, "
            "overriding ack_required from the cluster config YAML. Must "
            "be between 0 and the number of followers. Defaults to the "
            "value in the config YAML."
        ),
    )
    return parser.parse_args(argv)


def _resolve_config(args: argparse.Namespace) -> ClusterConfig:
    config = ClusterConfig.from_yaml(args.config)
    if args.ack_required is not None:
        config = config.with_ack_required(args.ack_required)
    return config


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args(argv)
    config = _resolve_config(args)
    app = build_app(args.node_id, config)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
