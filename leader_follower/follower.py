"""Follower node process for the leader-follower replication strategy.

A follower has no logic of its own beyond what common/server.py already
provides: GET /kv/{key} serves client reads directly (not leader-only,
so replication lag is observable), and POST /internal/replicate accepts
writes pushed by the leader. This module doesn't touch that behavior --
it just gives the app a process entrypoint, with a node_id and port
configurable via CLI args or environment variables so multiple follower
instances can be started as separate processes.
"""

from __future__ import annotations

import argparse
import logging

import uvicorn
from fastapi import FastAPI

from common.cli import add_node_identity_args
from common.server import create_app
from common.storage import KVStore

logger = logging.getLogger(__name__)


def build_app(node_id: str) -> FastAPI:
    """Build a follower app: common.server's app, unmodified.

    Each follower gets its own fresh, in-process KVStore -- there's no
    shared state between nodes beyond what's replicated over HTTP.
    """
    storage = KVStore()
    return create_app(storage, node_id)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a leader-follower KV follower node."
    )
    add_node_identity_args(parser, default_port=8001)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args(argv)
    app = build_app(args.node_id)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
