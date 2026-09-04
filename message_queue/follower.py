"""Follower node process for the message-queue replication strategy.

Serves the same generic KV HTTP surface as leader_follower's follower --
GET/PUT/DELETE/health, unmodified from common/server.py (see that
module for what each route does; PUT and /internal/replicate exist here
only because create_app() is reused wholesale rather than a
stripped-down variant, the same accepted quirk leader_follower's and
leaderless's own followers/nodes already have -- nothing in this
strategy's write path ever calls them). The one thing genuinely new
here: a background Kafka consumer task, started at app startup, that's
the *only* way this follower's KVStore gets populated in normal
operation -- see producer.py for the write path this consumes from.

Each follower joins its OWN Kafka consumer group
("{consumer_group_prefix}-{node_id}"), not a shared one. With exactly
one member, that group is assigned every partition of the topic, so
this follower consumes the FULL topic independently. This is
deliberate, not an oversight: this strategy models full replication --
every follower ends up holding a complete copy of every write -- the
same guarantee leader_follower's every-follower-gets-every-write fan-out
and leaderless's every-peer-gets-every-write flood both provide. A
shared, work-splitting consumer group (partitions divided up across
followers, each write landing on only one of them) would be sharding,
not replication -- a fundamentally different thing, and a deliberately
separate, out-of-scope future direction, not a variant of this.

Starts consuming from the earliest offset the first time a given
follower (by its node_id-derived group id) ever joins; a restart of the
*same* follower resumes from its last committed offset via Kafka's own
consumer-group offset tracking, rather than replaying the full topic
again -- ordinary Kafka consumer-group semantics, not anything this
module implements itself.

Applies each consumed message via KVStore.put() -- the same LWW-safe
write path /internal/replicate already uses for leader_follower/
leaderless, reused here directly rather than duplicated, since a
consumed Kafka message and an HTTP replicate call carry the exact same
payload shape (ReplicateRequest) and need the exact same handling. This
is also the safety net for out-of-order delivery: partitioning by key
(see producer.py) means writes to one key should always arrive at this
follower in the order they were produced, but KVStore.put()'s own LWW
comparison -- not this module -- is what actually guarantees a late/
out-of-order write can never overwrite something newer, the same
guarantee it already gives the other two strategies. This module never
assumes partitioning alone is enough and skips the check.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
from typing import Any

import uvicorn
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from pydantic import ValidationError

from common.server import ReplicateRequest, create_app
from common.storage import KVStore
from message_queue.config import MQConfig
from message_queue.topics import ensure_topic_exists

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/mq_cluster.yaml"


def _apply_message(raw: bytes, storage: KVStore, node_id: str) -> tuple[ReplicateRequest, bool] | None:
    """Parse one raw Kafka message value and apply it to `storage`.

    Factored out of _consume_loop below so message parsing and the
    LWW-safe apply are directly unit-testable against a plain KVStore,
    without a real consumer or broker.

    Returns:
        (parsed, applied) for a well-formed message -- `applied` is
        storage.put()'s own result (True if this write won LWW, False
        if it was rejected as stale).
        None if `raw` couldn't be parsed as a ReplicateRequest --
        logged here and otherwise ignored, not raised. Shouldn't happen
        given only producer.py ever publishes to this topic, but a
        follower crashing on one bad message would be worse than
        dropping it -- same "log and move on" policy leader.py's
        Replicator._send uses for a malformed HTTP response.
    """
    try:
        parsed = ReplicateRequest.model_validate_json(raw)
    except ValidationError:
        logger.warning("follower %s: skipping malformed message", node_id, exc_info=True)
        return None
    applied = storage.put(parsed.key, parsed.value, timestamp=parsed.timestamp, node_id=parsed.node_id)
    return parsed, applied


async def _consume_loop(consumer: AIOKafkaConsumer, storage: KVStore, node_id: str) -> None:
    """Apply every message this follower's consumer receives to `storage`,
    forever, until cancelled (app shutdown).
    """
    async for message in consumer:
        result = _apply_message(message.value, storage, node_id)
        if result is None:
            continue
        parsed, applied = result
        logger.info(
            "follower %s: consumed key=%s partition=%s offset=%s applied=%s",
            node_id, parsed.key, message.partition, message.offset, applied,
        )


def build_app(node_id: str, config: MQConfig) -> FastAPI:
    """Build a follower app: common.server's generic app (its own fresh
    KVStore, same as leader_follower/follower.py), plus a background
    Kafka consumer task wired to startup/shutdown that's the only writer
    to that KVStore in normal operation.
    """
    storage = KVStore()
    app = create_app(storage, node_id)
    # Holds the AIOKafkaConsumer and its background consume-loop task
    # once _on_startup runs -- can't build/start either before then, for
    # the same reason as producer.py's AIOKafkaProducer (see that
    # module's build_app for why).
    state: dict[str, Any] = {}

    async def _on_startup() -> None:
        await ensure_topic_exists(config)
        consumer = AIOKafkaConsumer(
            config.topic,
            bootstrap_servers=config.bootstrap_servers,
            group_id=f"{config.consumer_group_prefix}-{node_id}",
            auto_offset_reset="earliest",
        )
        await consumer.start()
        state["consumer"] = consumer
        state["task"] = asyncio.ensure_future(_consume_loop(consumer, storage, node_id))

    async def _on_shutdown() -> None:
        task = state.get("task")
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        consumer = state.get("consumer")
        if consumer is not None:
            await consumer.stop()

    app.router.add_event_handler("startup", _on_startup)
    app.router.add_event_handler("shutdown", _on_shutdown)
    return app


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a message-queue KV follower node.")
    parser.add_argument(
        "--node-id",
        default=os.environ.get("NODE_ID"),
        required=os.environ.get("NODE_ID") is None,
        help=(
            "Unique identifier for this node (env: NODE_ID). Also used "
            "to derive this follower's own Kafka consumer group id."
        ),
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("HOST", "0.0.0.0"),
        help="Host/interface to bind (env: HOST, default 0.0.0.0).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("PORT", "8201")),
        help="Port to bind (env: PORT, default 8201).",
    )
    parser.add_argument(
        "--config",
        default=os.environ.get("MQ_CONFIG", DEFAULT_CONFIG_PATH),
        help=f"Path to the MQ cluster config YAML (env: MQ_CONFIG, default {DEFAULT_CONFIG_PATH}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args(argv)
    config = MQConfig.from_yaml(args.config)
    app = build_app(args.node_id, config)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
