"""Producer node process for the message-queue replication strategy --
the third replication strategy alongside leader_follower/ and
leaderless/.

Architecturally distinct from both of those: leader_follower's leader
and leaderless's coordinator each track per-follower/per-peer
acknowledgments *before* returning success to the client. Here, this
producer acks the client as soon as a write is durably logged by Kafka
(acks="all", see build_app below) -- replication to followers happens
entirely afterward, via follower.py's own Kafka consumer, decoupled from
this request. There is no ack-counting, no follower fan-out, no timeout
race here at all; contrast with leader_follower/leader.py's Replicator,
which exists entirely to implement that wait. That's the trade-off this
strategy exists to demonstrate: fast, predictable writes, at the cost of
replica staleness being bounded only by follower consumer throughput/lag
rather than by ack-wait timing.

Write path, per PUT /kv/{key}:
  1. Assign a timestamp and stamp this node's own node_id -- exactly
     like a leader-follower leader's local write -- so every follower
     that later consumes this message applies the identical versioned
     entry, not one stamped fresh per-follower.
  2. Publish that versioned write to Kafka, keyed by the KV key (hash-
     partitioned, aiokafka's default partitioner -- verified in
     experiments/kafka_smoke_test.py: every write to a given key lands
     in the same partition, so per-key ordering is preserved end to
     end).
  3. Return success to the client as soon as Kafka confirms the message
     is durably logged. Fail loudly (an error response) if it doesn't --
     never partial success, same policy as the other two strategies,
     applied to this strategy's own definition of "durable."

Message schema on the wire is common/server.py's ReplicateRequest
(key/value/timestamp/node_id), JSON-encoded -- the same shape
leader_follower's and leaderless's /internal/replicate both use over
HTTP, reused here as-is so LWW resolution (VersionedValue,
common/models.py) stays identical across all three strategies; only the
transport differs. See follower.py for the consumer side.

The producer holds no KV data of its own -- no KVStore, no GET/DELETE.
It only ever publishes to Kafka; reads (and the consumer-lag metric that
explains their staleness) are a follower's job -- see follower.py. A
single producer node is the design here (mirroring leader_follower's
single leader), not a pool of them -- see message_queue/config.py's
MQConfig, which has no notion of multiple producers.

Still out of scope (see the branch history for the full staged plan):
no load test, no run_comparison.py integration -- message_queue/topics.py's
reset_topic() is the primitive that integration will use to reset
between sweep configs, but nothing calls it yet.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from typing import Any

import uvicorn
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from common.cli import add_node_identity_args
from common.server import PutRequest, PutResponse, ReplicateRequest
from message_queue.config import MQConfig
from message_queue.topics import ensure_topic_exists

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/mq_cluster.yaml"


class ProducerHealthResponse(BaseModel):
    """Response for GET /health.

    No key_count field, unlike common/server.py's HealthResponse -- the
    producer has no KVStore, so there's no key count to report; see
    this module's own docstring for why.
    """

    node_id: str


def _build_message(key: str, value: Any, timestamp: float, node_id: str) -> tuple[bytes, bytes]:
    """Build the (kafka_key, kafka_value) bytes pair this producer
    publishes for one write.

    Factored out of the PUT /kv/{key} route handler below so the wire
    schema itself -- what this strategy's messages actually look like,
    independent of the Kafka I/O around sending one -- is directly
    unit-testable without a real broker. See this module's own
    docstring for why ReplicateRequest (not a new shape) is the value.
    """
    message = ReplicateRequest(key=key, value=value, timestamp=timestamp, node_id=node_id)
    return key.encode("utf-8"), message.model_dump_json().encode("utf-8")


def build_app(node_id: str, config: MQConfig) -> FastAPI:
    """Build the producer app: a client-facing PUT /kv/{key} that
    publishes to Kafka and acks on durable-write confirmation, plus a
    minimal /health. No GET/DELETE/replicate -- the producer holds no
    data, so none of those would mean anything here.
    """
    app = FastAPI(title=f"kv-mq-producer[{node_id}]")
    # Holds the AIOKafkaProducer once _on_startup runs. Can't build/start
    # it before then: AIOKafkaProducer.start() is a coroutine, and
    # build_app() itself runs before uvicorn's event loop exists (same
    # reason leader_follower/leader.py's httpx client, by contrast, gets
    # built synchronously right here -- httpx.AsyncClient's constructor
    # needs no event loop, aiokafka's producer does).
    state: dict[str, Any] = {}

    async def _on_startup() -> None:
        await ensure_topic_exists(config)
        producer = AIOKafkaProducer(
            bootstrap_servers=config.bootstrap_servers,
            # "all": wait for every in-sync replica to ack, not just the
            # partition leader -- the strongest durability confirmation
            # Kafka can give before this handler returns to the client.
            # No behavioral difference from the default (acks=1) on this
            # lab's single-node/replication-factor-1 broker today, but
            # it's the setting that actually matches this strategy's
            # "durably logged" claim once a real multi-broker cluster
            # exists.
            acks="all",
        )
        await producer.start()
        state["producer"] = producer

    async def _on_shutdown() -> None:
        producer = state.get("producer")
        if producer is not None:
            await producer.stop()

    app.router.add_event_handler("startup", _on_startup)
    app.router.add_event_handler("shutdown", _on_shutdown)

    @app.put("/kv/{key}", response_model=PutResponse)
    async def put_key(key: str, body: PutRequest, request: Request) -> PutResponse:
        logger.info("method=%s path=%s key=%s", request.method, request.url.path, key)

        # Assign version metadata here, at the producer -- exactly like
        # a leader-follower leader's local write -- so every follower
        # that later consumes this message applies the identical
        # versioned entry, not one stamped fresh per-follower.
        timestamp = time.time()
        kafka_key, kafka_value = _build_message(key, body.value, timestamp, node_id)

        producer: AIOKafkaProducer = state["producer"]
        try:
            await producer.send_and_wait(config.topic, key=kafka_key, value=kafka_value)
        except KafkaError as exc:
            # Fail loudly -- never tell the client a write succeeded
            # when Kafka never confirmed it durably logged the message.
            # Same policy as leader.py's unmet-ack_required 503, applied
            # to this strategy's own definition of "durable."
            logger.warning("publish to %s failed for key=%s", config.topic, key, exc_info=True)
            raise HTTPException(
                status_code=503, detail=f"Kafka did not confirm this write: {exc}"
            ) from exc

        # No local storage to check "applied" against here (see this
        # module's docstring) -- `applied` is always True once Kafka has
        # durably logged the message. A follower could still, in
        # principle, later reject the eventual apply as stale (see
        # follower.py) if it somehow observes writes to this key out of
        # order -- a real, accepted possibility this response can't see
        # or promise anything about, not a contradiction of `applied`
        # here.
        return PutResponse(applied=True, timestamp=timestamp)

    @app.get("/health", response_model=ProducerHealthResponse)
    def health(request: Request) -> ProducerHealthResponse:
        logger.info("method=%s path=%s", request.method, request.url.path)
        return ProducerHealthResponse(node_id=node_id)

    return app


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a message-queue KV producer node.")
    add_node_identity_args(parser, default_port=8200)
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
