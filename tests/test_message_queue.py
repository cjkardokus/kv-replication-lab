"""Tests for message_queue/: MQConfig, the producer/follower wire
schema, the real producer -> Kafka -> follower write path, follower
reads, topic reset (lifecycle), and consumer lag.

Testing approach
-----------------
Unlike tests/test_leader_follower.py and tests/test_leaderless.py, this
module never needs real uvicorn servers/sockets for the FastAPI layer:
producer.py and follower.py never call each other's HTTP endpoints at
all -- the only inter-node communication in this strategy is via Kafka,
which is exercised for real regardless of how the FastAPI app itself is
invoked. FastAPI's TestClient, used as a context manager
(`with TestClient(app) as client:`), runs the app's real startup/
shutdown lifecycle -- including _on_startup's real AIOKafkaProducer/
AIOKafkaConsumer construction and a follower's background consume-loop
task. That background task keeps running on a persistent event loop
across separate client calls within the same `with` block (verified
empirically: a task started at startup keeps advancing between calls,
not just during one request's own async context), so polling a
follower's GET between calls genuinely observes messages arriving from
a real broker in the background -- with none of the thread/socket
overhead the other two strategies' tests need for real inter-process
HTTP.

Integration tests (marked @pytest.mark.kafka_integration) need a real
Kafka broker reachable at KAFKA_BOOTSTRAP_SERVERS (default
localhost:9092, matching docker-compose.kafka.yml) -- the `require_kafka`
fixture below self-skips (not fails) any test that depends on it if no
broker is reachable, so the rest of this project's test suite is
unaffected by whether Kafka happens to be running locally. In CI,
.github/workflows/tests.yml runs these in their own job against a real
Kafka service container, deselected from the main "test" job -- see
that file for why.

Every integration test gets its own freshly generated topic name and
consumer-group prefix (the `mq_config` fixture), so tests never share a
topic -- and its accumulated messages/offsets -- with each other or
with a prior run. This branch doesn't implement fresh-topic-per-run
lifecycle management (a later branch's job; see
message_queue/producer.py's own docstring) -- per-test isolation here is
achieved by never reusing a topic name, plus best-effort topic deletion
at teardown, not by a real lifecycle policy.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from collections.abc import Callable

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from fastapi.testclient import TestClient

from common.server import ReplicateRequest
from common.storage import KVStore
from message_queue.config import MQConfig
from message_queue.follower import PartitionLag, _apply_message, _compute_lag
from message_queue.follower import build_app as build_follower_app
from message_queue.producer import _build_message
from message_queue.producer import build_app as build_producer_app
from message_queue.topics import ensure_topic_exists, reset_topic

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _kafka_reachable() -> bool:
    async def _check() -> bool:
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, request_timeout_ms=3000)
        try:
            await admin.start()
        except Exception:
            return False
        else:
            return True
        finally:
            with contextlib.suppress(Exception):
                await admin.close()

    return asyncio.run(_check())


def _wait_until(predicate: Callable[[], bool], timeout_s: float = 15.0, interval_s: float = 0.1) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"condition did not become true within {timeout_s}s")
        time.sleep(interval_s)


@pytest.fixture(scope="module")
def require_kafka() -> None:
    if not _kafka_reachable():
        pytest.skip(
            f"Kafka broker not reachable at {BOOTSTRAP_SERVERS} -- start it via "
            "`docker compose -f docker-compose.kafka.yml up -d` (see docs/kafka-setup.md)"
        )


@pytest.fixture
def mq_config(require_kafka: None):
    """A fresh MQConfig with a unique topic and consumer-group prefix,
    so this test never shares a topic with any other test or a prior
    run. Depends on require_kafka so an unreachable broker skips before
    this fixture's own (network-touching) teardown ever runs.
    """
    unique = uuid.uuid4().hex[:8]
    config = MQConfig(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=f"kv-mq-test-{unique}",
        num_partitions=4,
        consumer_group_prefix=f"kv-mq-test-group-{unique}",
    )
    yield config

    # Best-effort cleanup so repeated local runs don't leave an
    # ever-growing pile of test topics on a long-lived broker -- see
    # this module's own docstring for why this is just test hygiene,
    # not real topic lifecycle management.
    async def _delete() -> None:
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        await admin.start()
        try:
            await admin.delete_topics([config.topic])
        finally:
            await admin.close()

    with contextlib.suppress(Exception):
        asyncio.run(_delete())


# --- MQConfig parsing/validation -------------------------------------------


def test_mq_config_from_dict_parses_fields():
    config = MQConfig.from_dict(
        {
            "bootstrap_servers": "10.0.0.1:9092",
            "topic": "my-topic",
            "num_partitions": 8,
            "consumer_group_prefix": "my-group",
        }
    )
    assert config.bootstrap_servers == "10.0.0.1:9092"
    assert config.topic == "my-topic"
    assert config.num_partitions == 8
    assert config.consumer_group_prefix == "my-group"


@pytest.mark.parametrize("field", ["bootstrap_servers", "topic", "consumer_group_prefix"])
def test_mq_config_rejects_empty_required_strings(field):
    raw = {
        "bootstrap_servers": "localhost:9092",
        "topic": "t",
        "num_partitions": 1,
        "consumer_group_prefix": "g",
    }
    raw[field] = ""
    with pytest.raises(ValueError):
        MQConfig.from_dict(raw)


@pytest.mark.parametrize("num_partitions", [0, -1])
def test_mq_config_rejects_non_positive_num_partitions(num_partitions):
    with pytest.raises(ValueError):
        MQConfig.from_dict(
            {
                "bootstrap_servers": "localhost:9092",
                "topic": "t",
                "num_partitions": num_partitions,
                "consumer_group_prefix": "g",
            }
        )


def test_repo_mq_config_file_parses():
    # Regression test tying the test suite to the real, checked-in
    # config file -- catches schema drift between the YAML and the
    # dataclass that reads it.
    config = MQConfig.from_yaml("config/mq_cluster.yaml")
    assert config.bootstrap_servers
    assert config.topic
    assert config.num_partitions >= 1
    assert config.consumer_group_prefix


# --- Message schema ---------------------------------------------------------


def test_producer_build_message_schema():
    kafka_key, kafka_value = _build_message("k1", {"a": 1}, timestamp=123.5, node_id="producer-1")
    assert kafka_key == b"k1"

    # What's actually on the wire is a plain ReplicateRequest, JSON-
    # encoded -- the same shape leader_follower's and leaderless's
    # /internal/replicate both use over HTTP (see producer.py's module
    # docstring for why that's deliberate, not coincidental).
    parsed = ReplicateRequest.model_validate_json(kafka_value)
    assert parsed.key == "k1"
    assert parsed.value == {"a": 1}
    assert parsed.timestamp == 123.5
    assert parsed.node_id == "producer-1"


def test_follower_apply_message_applies_well_formed_message():
    storage = KVStore()
    raw = ReplicateRequest(key="k1", value="v1", timestamp=100.0, node_id="producer-1").model_dump_json().encode()

    result = _apply_message(raw, storage, node_id="follower-1")

    assert result is not None
    parsed, applied = result
    assert applied is True
    assert parsed.key == "k1"
    entry = storage.get("k1")
    assert entry is not None
    assert entry.value == "v1"
    assert entry.timestamp == 100.0
    # Preserved as-is from the message, not re-stamped with this
    # follower's own node_id -- same rule /internal/replicate follows.
    assert entry.node_id == "producer-1"


def test_follower_apply_message_rejects_stale_write_via_lww():
    storage = KVStore()
    newer = ReplicateRequest(key="k1", value="new", timestamp=200.0, node_id="producer-1").model_dump_json().encode()
    older = ReplicateRequest(key="k1", value="old", timestamp=100.0, node_id="producer-1").model_dump_json().encode()

    _apply_message(newer, storage, node_id="follower-1")
    result = _apply_message(older, storage, node_id="follower-1")

    assert result is not None
    _parsed, applied = result
    assert applied is False  # rejected as stale under LWW, not raised
    entry = storage.get("k1")
    assert entry is not None
    assert entry.value == "new"  # unchanged by the stale write


def test_follower_apply_message_skips_malformed_message_without_raising():
    storage = KVStore()

    result = _apply_message(b"not json", storage, node_id="follower-1")

    assert result is None
    assert storage.get("k1") is None  # nothing was touched


# --- Integration: real Kafka broker ----------------------------------------


@pytest.mark.kafka_integration
def test_ensure_topic_exists_tolerates_concurrent_create_race(mq_config):
    # producer.py and follower.py both call ensure_topic_exists() on
    # their own startup, with no ordering guarantee about which starts
    # first. Forces the actual race (not just "call it twice
    # sequentially") via asyncio.gather: both coroutines see the topic
    # missing and both attempt to create it -- neither call should
    # raise even though the broker only grants one of them the create.
    async def _run() -> None:
        await asyncio.gather(
            ensure_topic_exists(mq_config),
            ensure_topic_exists(mq_config),
        )

    asyncio.run(_run())  # must not raise


@pytest.mark.kafka_integration
def test_producer_write_is_replicated_to_follower(mq_config):
    producer_app = build_producer_app("producer-1", mq_config)
    follower_app = build_follower_app("follower-1", mq_config)

    with TestClient(producer_app) as producer_client, TestClient(follower_app) as follower_client:
        put_resp = producer_client.put("/kv/k1", json={"value": "v1"})
        assert put_resp.status_code == 200
        put_body = put_resp.json()
        assert put_body["applied"] is True

        def _follower_has_it() -> bool:
            resp = follower_client.get("/kv/k1")
            return resp.status_code == 200 and resp.json()["timestamp"] == put_body["timestamp"]

        _wait_until(_follower_has_it)

        entry = follower_client.get("/kv/k1").json()
        assert entry["value"] == "v1"
        # Preserved from the producer's write, not the follower's own
        # identity -- same rule as leader_follower's replicated writes.
        assert entry["node_id"] == "producer-1"


@pytest.mark.kafka_integration
def test_multiple_followers_each_get_full_replica_independently(mq_config):
    # This is the headline architectural claim under test: every
    # follower runs its OWN consumer group (see follower.py's module
    # docstring), so all three of these should end up with a complete
    # copy of every key -- not a work-split subset the way members of
    # one shared consumer group would.
    producer_app = build_producer_app("producer-1", mq_config)
    follower_apps = [build_follower_app(f"follower-{i}", mq_config) for i in range(3)]
    keys = ["a", "b", "c"]

    with contextlib.ExitStack() as stack, TestClient(producer_app) as producer_client:
        follower_clients = [stack.enter_context(TestClient(app)) for app in follower_apps]

        for key in keys:
            resp = producer_client.put(f"/kv/{key}", json={"value": f"v-{key}"})
            assert resp.status_code == 200

        def _all_followers_have_all_keys() -> bool:
            return all(
                client.get(f"/kv/{key}").status_code == 200 for client in follower_clients for key in keys
            )

        _wait_until(_all_followers_have_all_keys, timeout_s=20.0)

        for client in follower_clients:
            for key in keys:
                assert client.get(f"/kv/{key}").json()["value"] == f"v-{key}"


# --- Read path ---------------------------------------------------------


@pytest.mark.kafka_integration
def test_follower_read_returns_full_kv_response(mq_config):
    # A dedicated, pinned contract test for the read path (branch 3):
    # the other write-path tests above already incidentally exercise
    # GET, but this nails down the exact response shape -- the same
    # KVResponse shape (key/value/timestamp/node_id) leader_follower's
    # and leaderless's own GET endpoints return, per common/server.py.
    producer_app = build_producer_app("producer-1", mq_config)
    follower_app = build_follower_app("follower-1", mq_config)

    with TestClient(producer_app) as producer_client, TestClient(follower_app) as follower_client:
        put_resp = producer_client.put("/kv/k1", json={"value": {"nested": "v1"}})
        assert put_resp.status_code == 200
        put_body = put_resp.json()

        def _follower_has_it() -> bool:
            resp = follower_client.get("/kv/k1")
            return resp.status_code == 200 and resp.json()["timestamp"] == put_body["timestamp"]

        _wait_until(_follower_has_it)

        resp = follower_client.get("/kv/k1")
        assert resp.status_code == 200
        body = resp.json()
        assert set(body.keys()) == {"key", "value", "timestamp", "node_id"}
        assert body["key"] == "k1"
        assert body["value"] == {"nested": "v1"}
        assert body["timestamp"] == put_body["timestamp"]
        assert body["node_id"] == "producer-1"


@pytest.mark.kafka_integration
def test_follower_read_missing_key_returns_404(mq_config):
    follower_app = build_follower_app("follower-1", mq_config)
    with TestClient(follower_app) as follower_client:
        resp = follower_client.get("/kv/never-written")
        assert resp.status_code == 404


# --- Topic lifecycle (reset_topic) --------------------------------------


@pytest.mark.kafka_integration
def test_reset_topic_produces_clean_slate(mq_config):
    # Write something before the reset.
    producer_before = build_producer_app("producer-before", mq_config)
    with TestClient(producer_before) as client:
        resp = client.put("/kv/before-reset", json={"value": "old"})
        assert resp.status_code == 200
        put_body = resp.json()

    # Sanity check: a follower joining *before* the reset really does
    # see it -- otherwise a later 404 wouldn't prove anything about the
    # reset specifically (it could just mean nothing was ever written).
    follower_before = build_follower_app("follower-before", mq_config)
    with TestClient(follower_before) as client:

        def _has_it() -> bool:
            resp = client.get("/kv/before-reset")
            return resp.status_code == 200 and resp.json()["timestamp"] == put_body["timestamp"]

        _wait_until(_has_it)

    asyncio.run(reset_topic(mq_config))

    follower_after = build_follower_app("follower-after", mq_config)
    with TestClient(follower_after) as client:
        # There's no positive condition to poll *for* here -- "nothing
        # ever arrives" has no event to observe -- so this waits out a
        # fixed window instead, long enough for this follower's
        # consumer to have joined, been assigned every partition, and
        # (if the reset genuinely failed to clear anything) consumed
        # and applied the pre-reset write.
        time.sleep(3.0)
        resp = client.get("/kv/before-reset")
        assert resp.status_code == 404

        # The reset topic is genuinely usable afterward, not just
        # emptied forever -- a fresh write on it still replicates
        # normally.
        producer_after = build_producer_app("producer-after", mq_config)
        with TestClient(producer_after) as producer_client:
            put_resp = producer_client.put("/kv/after-reset", json={"value": "new"})
            assert put_resp.status_code == 200

        _wait_until(lambda: client.get("/kv/after-reset").status_code == 200)
        assert client.get("/kv/after-reset").json()["value"] == "new"


# --- Consumer lag --------------------------------------------------------


@pytest.mark.kafka_integration
def test_lag_reflects_unconsumed_backlog_accurately(mq_config):
    # A "paused" follower: a real consumer that joins this follower's
    # would-be group and gets a real partition assignment (so
    # end_offsets()/committed() reflect a real, live broker state), but
    # never runs a consume loop -- standing in for a follower that's
    # fallen behind or stalled. Lag must reflect the exact backlog size,
    # not just "some positive number."
    producer_app = build_producer_app("producer-1", mq_config)
    num_messages = 5

    async def _run() -> list[PartitionLag]:
        await ensure_topic_exists(mq_config)
        consumer = AIOKafkaConsumer(
            mq_config.topic,
            bootstrap_servers=mq_config.bootstrap_servers,
            group_id=f"{mq_config.consumer_group_prefix}-paused",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()
        try:
            deadline = time.monotonic() + 15.0
            while not consumer.assignment():
                if time.monotonic() >= deadline:
                    raise TimeoutError("paused consumer never got a partition assignment")
                await asyncio.sleep(0.1)

            # Produce while this consumer stays connected (assigned,
            # committed at 0) but reads nothing.
            with TestClient(producer_app) as client:
                for i in range(num_messages):
                    resp = client.put(f"/kv/k{i}", json={"value": i})
                    assert resp.status_code == 200

            return await _compute_lag(consumer)
        finally:
            await consumer.stop()

    partitions = asyncio.run(_run())
    assert sum(p.lag for p in partitions) == num_messages
    # Nothing was ever applied/committed by this consumer -- every
    # partition's own committed offset is still its starting point.
    assert all((p.committed_offset or 0) == 0 for p in partitions)
    # And every reported end_offset - committed_offset really is what
    # lag claims, per partition, not just in aggregate.
    assert all(p.lag == p.end_offset - (p.committed_offset or 0) for p in partitions)


@pytest.mark.kafka_integration
def test_lag_reaches_zero_once_follower_catches_up(mq_config):
    # Complements the paused-consumer test above with the healthy-path
    # case, through the real /internal/lag HTTP route (not just
    # _compute_lag directly): a follower that's actually running its
    # consume loop should see lag return to 0 once it's caught up.
    producer_app = build_producer_app("producer-1", mq_config)
    follower_app = build_follower_app("follower-1", mq_config)

    with TestClient(producer_app) as producer_client, TestClient(follower_app) as follower_client:
        for i in range(5):
            resp = producer_client.put(f"/kv/k{i}", json={"value": i})
            assert resp.status_code == 200

        def _lag_is_zero() -> bool:
            resp = follower_client.get("/internal/lag")
            return resp.status_code == 200 and resp.json()["total_lag"] == 0

        _wait_until(_lag_is_zero, timeout_s=20.0)

        body = follower_client.get("/internal/lag").json()
        assert body["node_id"] == "follower-1"
        assert body["total_lag"] == 0
        assert len(body["partitions"]) == mq_config.num_partitions
        assert all(p["lag"] == 0 for p in body["partitions"])
