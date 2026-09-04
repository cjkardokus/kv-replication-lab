"""Follower node process for the message-queue replication strategy.

Serves the same generic KV HTTP surface as leader_follower's follower --
GET/PUT/DELETE/health, unmodified from common/server.py (see that
module for what each route does; PUT and /internal/replicate exist here
only because create_app() is reused wholesale rather than a
stripped-down variant, the same accepted quirk leader_follower's and
leaderless's own followers/nodes already have -- nothing in this
strategy's write path ever calls them). GET /kv/{key} is this
strategy's real read path: it serves whatever this follower's local
KVStore currently has, with no coordination and no waiting on the
consumer to "catch up" to anything -- there is no code here that could
add such a wait even if it wanted to, since create_app()'s generic
handler just does storage.get(). A read landing right after a write can
therefore legitimately see stale (or, before the first message ever
arrives, missing) data; that staleness, and how it correlates with
/internal/lag below, is the whole point of this strategy, not a gap in
its read path.

The one thing genuinely new here beyond common/server.py: a background
Kafka consumer task, started at app startup, that's the *only* way this
follower's KVStore gets populated in normal operation -- see producer.py
for the write path this consumes from.

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
module implements itself. See message_queue/topics.py's reset_topic()
for how a later branch's sweep gives every config a genuinely clean
slate instead (deleting the topic itself, not just resetting offsets).

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

The consumer commits its offset synchronously, right after applying
each message (enable_auto_commit=False; see _consume_loop) rather than
relying on aiokafka's default periodic auto-commit. This is what makes
GET /internal/lag below an accurate, real-time measure of "how many
applied writes is this follower missing" rather than one that's stale
by up to an auto-commit interval -- at the cost of a broker round trip
per message instead of a batched one. For this lab's scale and this
branch's purpose (making lag precisely measurable, so a later branch's
load test can correlate a stale read with the lag that caused it), that
trade is the right one.

Fault injection (--fault-inject-consume-delay-ms, off by default):
This follower's consume loop can be given an artificial per-message
delay, applied before applying/committing each message, purely to
manufacture visible consumer lag on demand for
experiments/mq_lag_demo.py -- the same role leaderless/node.py's
--fault-inject-delay-ms plays for its own boundary-case demo. Disabled
(0, the default), this loop is exactly as described above; it is never
driven by MQConfig/the cluster YAML, only by this process's own CLI
flag/env var, so it can never be silently enabled by cluster config
alone. See build_app() for where it's wired in.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
from typing import Any

import uvicorn
from aiokafka import AIOKafkaConsumer, TopicPartition
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ValidationError

from common.cli import add_node_identity_args
from common.server import ReplicateRequest, create_app
from common.storage import KVStore
from message_queue.config import MQConfig
from message_queue.topics import ensure_topic_exists

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/mq_cluster.yaml"


class PartitionLag(BaseModel):
    """This follower's consumer lag on one partition.

    `committed_offset` is None if this follower has never committed
    anything on this partition yet (a brand-new follower that hasn't
    consumed its first message here) -- `lag` still reports a real
    number in that case (the full end_offset), treating "never
    committed" as committed_offset=0, not as "unknown"/omitted.
    """

    partition: int
    end_offset: int
    committed_offset: int | None
    lag: int


class LagResponse(BaseModel):
    """Response for GET /internal/lag.

    `total_lag` is the plain sum of every partition's own lag -- fine
    as a single headline number (see this module's own docstring for
    why it's accurate in real time), but `partitions` keeps the
    breakdown available since it's cheap to compute alongside the total
    (see _compute_lag) and a later branch's load test may want it.
    """

    node_id: str
    total_lag: int
    partitions: list[PartitionLag]


async def _compute_lag(consumer: AIOKafkaConsumer) -> list[PartitionLag]:
    """Compute `consumer`'s real lag, per partition currently assigned
    to it.

    lag = end_offset (the broker's true log-end offset, via
    AIOKafkaConsumer.end_offsets()) minus this consumer's own last
    committed offset (via AIOKafkaConsumer.committed()) -- deliberately
    not aiokafka's own in-memory position(), which would also count
    messages already fetched-and-applied-but-not-yet-committed as
    "caught up". Since follower.py's consumer commits synchronously
    right after applying each message (see _consume_loop), committed()
    already reflects true applied progress in real time, so this stays
    an accurate, per-partition measure rather than one fuzzed by an
    auto-commit interval or by in-flight-but-uncommitted fetches.

    Factored out of the /internal/lag route below so it's directly
    testable against a real, freestanding AIOKafkaConsumer (e.g. one
    that never runs _consume_loop at all, standing in for a
    stalled/paused follower) without needing a full follower app.
    """
    partitions = consumer.assignment()
    if not partitions:
        return []

    end_offsets = await consumer.end_offsets(partitions)
    result = []
    for tp in sorted(partitions, key=lambda tp: tp.partition):
        committed = await consumer.committed(tp)
        end_offset = end_offsets[tp]
        result.append(
            PartitionLag(
                partition=tp.partition,
                end_offset=end_offset,
                committed_offset=committed,
                lag=end_offset - (committed or 0),
            )
        )
    return result


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


async def _consume_loop(
    consumer: AIOKafkaConsumer,
    storage: KVStore,
    node_id: str,
    *,
    fault_inject_delay_seconds: float = 0.0,
) -> None:
    """Apply every message this follower's consumer receives to `storage`,
    forever, until cancelled (app shutdown).

    `fault_inject_delay_seconds` is TEST/BENCHMARK-ONLY (see build_app's
    own docstring) and defaults to 0.0, meaning completely disabled: a
    positive value sleeps for that long, per message, *before* applying
    and committing it -- purely to manufacture visible consumer lag on
    demand for experiments/mq_lag_demo.py, the same role leaderless/
    node.py's --fault-inject-delay-ms plays for its own boundary-case
    demo. Nothing else about this loop's behavior changes: the delay
    runs before the same _apply_message/commit sequence below, not
    instead of it.

    Commits this message's offset immediately after applying it (the
    consumer is built with enable_auto_commit=False -- see build_app) --
    a malformed message is committed past too, same as an applied one,
    since either way this follower is done with it and shouldn't see it
    again on a restart. See this module's own docstring for why
    per-message synchronous commits are what makes GET /internal/lag
    accurate.
    """
    async for message in consumer:
        if fault_inject_delay_seconds > 0:
            await asyncio.sleep(fault_inject_delay_seconds)
        result = _apply_message(message.value, storage, node_id)
        if result is not None:
            parsed, applied = result
            logger.info(
                "follower %s: consumed key=%s partition=%s offset=%s applied=%s",
                node_id, parsed.key, message.partition, message.offset, applied,
            )
        tp = TopicPartition(message.topic, message.partition)
        await consumer.commit({tp: message.offset + 1})


def build_app(node_id: str, config: MQConfig, *, fault_inject_consume_delay_seconds: float = 0.0) -> FastAPI:
    """Build a follower app: common.server's generic app (its own fresh
    KVStore, same as leader_follower/follower.py), plus a background
    Kafka consumer task wired to startup/shutdown that's the only writer
    to that KVStore in normal operation.

    `fault_inject_consume_delay_seconds` is TEST/BENCHMARK-ONLY (see
    _consume_loop's own docstring) and defaults to 0.0 -- completely
    disabled, in which case this follower's consume loop behaves
    exactly as it always has. It is never driven by MQConfig/the
    cluster YAML, only by this process's own CLI flag/env var, so it
    can never be silently enabled by cluster config alone -- same
    guarantee leaderless/node.py's own fault-injection flag makes.
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
            # See this module's own docstring: commits happen
            # synchronously per message in _consume_loop instead, so
            # GET /internal/lag's committed()-based lag is accurate in
            # real time rather than stale by up to an auto-commit
            # interval.
            enable_auto_commit=False,
        )
        await consumer.start()
        state["consumer"] = consumer
        state["task"] = asyncio.ensure_future(
            _consume_loop(
                consumer, storage, node_id,
                fault_inject_delay_seconds=fault_inject_consume_delay_seconds,
            )
        )

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

    @app.get("/internal/lag", response_model=LagResponse)
    async def get_lag(request: Request) -> LagResponse:
        logger.info("method=%s path=%s", request.method, request.url.path)
        consumer = state.get("consumer")
        if consumer is None:
            # Startup hasn't run yet (or somehow never assigned a
            # consumer) -- fail loudly rather than report a fake 0 lag
            # that would look like "fully caught up."
            raise HTTPException(status_code=503, detail="Kafka consumer not started yet")
        partitions = await _compute_lag(consumer)
        return LagResponse(node_id=node_id, total_lag=sum(p.lag for p in partitions), partitions=partitions)

    return app


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a message-queue KV follower node.")
    add_node_identity_args(
        parser,
        default_port=8201,
        node_id_help=(
            "Unique identifier for this node (env: NODE_ID). Also used "
            "to derive this follower's own Kafka consumer group id."
        ),
    )
    parser.add_argument(
        "--config",
        default=os.environ.get("MQ_CONFIG", DEFAULT_CONFIG_PATH),
        help=f"Path to the MQ cluster config YAML (env: MQ_CONFIG, default {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--fault-inject-consume-delay-ms",
        type=float,
        default=float(os.environ.get("FAULT_INJECT_CONSUME_DELAY_MS", "0")),
        help=(
            "TEST/BENCHMARK ONLY: artificial delay (milliseconds) this "
            "follower's consume loop sleeps before applying/committing "
            "each message -- purely to manufacture visible consumer lag "
            "on demand (see experiments/mq_lag_demo.py). 0 (default) "
            "disables this entirely (env: FAULT_INJECT_CONSUME_DELAY_MS)."
        ),
    )
    args = parser.parse_args(argv)
    if args.fault_inject_consume_delay_ms < 0:
        parser.error("--fault-inject-consume-delay-ms must be >= 0")
    return args


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args(argv)
    config = MQConfig.from_yaml(args.config)
    app = build_app(
        args.node_id, config,
        fault_inject_consume_delay_seconds=args.fault_inject_consume_delay_ms / 1000.0,
    )
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
