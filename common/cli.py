"""Shared CLI plumbing for every node process's entrypoint.

leader_follower/leader.py, leader_follower/follower.py, leaderless/
node.py, message_queue/producer.py, and message_queue/follower.py each
need the same --node-id/--host/--port handling: same env-var-fallback
pattern (NODE_ID/HOST/PORT), same trick for making --node-id
conditionally required (required only when NODE_ID isn't set in the
environment either), same host/port default shape -- independently
written five times over. add_node_identity_args() below is that shared
piece, extracted once each module's own strategy-specific flags
(--config, --ack-required, --default-w/--default-r,
--fault-inject-delay-ms, ...) are added by the caller afterward, same
pattern as common/replication_client.py's build_replication_client()
and common/server.py's replace_route() being pulled out once two (now
more) modules needed the identical thing.

Pure extraction: every caller's --node-id/--host/--port behavior,
defaults, and env var names are unchanged from before this existed --
see each call site for its own default_port and any help-text override.
"""

from __future__ import annotations

import argparse
import os

DEFAULT_NODE_ID_HELP = "Unique identifier for this node (env: NODE_ID)."
DEFAULT_HOST_HELP = "Host/interface to bind (env: HOST, default 0.0.0.0)."


def add_node_identity_args(
    parser: argparse.ArgumentParser,
    *,
    default_port: int,
    node_id_help: str = DEFAULT_NODE_ID_HELP,
    port_help: str | None = None,
) -> None:
    """Add --node-id/--host/--port to `parser`, matching what every node
    entrypoint already needs (see this module's docstring).

    `default_port` is this specific entrypoint's own default (e.g. 8000
    for the leader-follower leader, 8001 for a leader-follower follower
    or a leaderless node, 8200 for the MQ producer, 8201 for an MQ
    follower) -- env: PORT still overrides it at runtime, same as
    before this helper existed.

    `node_id_help`/`port_help` let a caller override the help text for
    those two flags specifically, when a plain "unique identifier"/"port
    to bind" description doesn't tell the whole story -- e.g.
    message_queue/follower.py's --node-id also documents that it derives
    this follower's Kafka consumer group id, and leaderless/node.py's
    --port also documents that it's used to find this node's own entry
    in the cluster config. Every other caller uses the plain defaults
    above. `port_help` defaults to None (not DEFAULT_HOST_HELP's sibling
    constant) because its default text embeds `default_port`, so it's
    built here rather than as a fixed module-level string.
    """
    parser.add_argument(
        "--node-id",
        default=os.environ.get("NODE_ID"),
        required=os.environ.get("NODE_ID") is None,
        help=node_id_help,
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("HOST", "0.0.0.0"),
        help=DEFAULT_HOST_HELP,
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("PORT", str(default_port))),
        help=port_help or f"Port to bind (env: PORT, default {default_port}).",
    )
