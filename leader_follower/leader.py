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
    """

    def __init__(self, config: ClusterConfig, client: httpx.AsyncClient) -> None:
        self._config = config
        self._client = client
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
    client = httpx.AsyncClient(timeout=config.timeout_seconds)
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

        return PutResponse(applied=local_applied)

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
