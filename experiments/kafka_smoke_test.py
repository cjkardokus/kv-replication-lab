"""Standalone smoke test for the local Kafka broker (docker-compose.kafka.yml).

Not part of any real replication logic -- the message-queue replication
strategy itself (a message_queue/ package, mirroring leader_follower/ and
leaderless/) doesn't exist yet. This script's only job is to prove the
infrastructure and client library (aiokafka) actually behave the way the
rest of that strategy will be built to assume, empirically rather than
by reading aiokafka's docs:

  1. A topic can be deleted and recreated safely, even when a prior run
     of this same script left one behind -- so this script is safe to
     rerun freely, the same way run_comparison.py's per-config process
     isolation gives leader_follower/leaderless a clean slate each time.
  2. Producing with an explicit key routes deterministically by a hash
     of that key: the same key always lands on the same partition,
     never round-robin or random. This matters beyond this smoke test
     -- once real replication logic exists, correctness depends on
     every write to a given KV key landing in one strictly-ordered
     partition (LWW-by-arrival-order breaks if a key's writes could be
     split across partitions and consumed out of order).
  3. Messages consumed back from the earliest offset preserve
     production order within each partition.

Run: `python3 -m experiments.kafka_smoke_test` (the broker must already
be running -- see docs/kafka-setup.md for how to start it).
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import uuid
from collections import defaultdict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import for_code

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "kv-lab-kafka-smoke-test"
NUM_PARTITIONS = 4
REPLICATION_FACTOR = 1  # single-node broker -- see docker-compose.kafka.yml

# A handful of distinct keys, several messages each, is enough to exercise
# both checks this script cares about (same key -> same partition, order
# preserved within a partition) without needing real load-test volume.
KEYS = ["alice", "bob", "carol", "dave", "eve", "frank"]
MESSAGES_PER_KEY = 5

TOPIC_POLL_TIMEOUT_S = 15.0
TOPIC_POLL_INTERVAL_S = 0.5
CONSUME_TIMEOUT_S = 15.0


def _raise_on_topic_errors(entries: list[dict[str, object]]) -> None:
    """Raise if any (topic, error_code) entry from a Create/DeleteTopics
    response carries a real error. error_code 0 means no error --
    aiokafka's admin client returns raw protocol responses and leaves
    checking them to the caller (see aiokafka.admin.client), it doesn't
    raise for per-topic errors on its own.
    """
    for entry in entries:
        code = entry["error_code"]
        assert isinstance(code, int)
        if code != 0:
            error_cls = for_code(code)
            raise RuntimeError(f"{entry['topic']}: {error_cls.__name__} (code {code})")


async def recreate_topic(admin: AIOKafkaAdminClient) -> None:
    """Delete TOPIC if a prior run left it behind, then create it fresh
    with NUM_PARTITIONS -- makes this script safely rerunnable.
    """
    existing = await admin.list_topics()
    if TOPIC in existing:
        print(f"Deleting pre-existing topic {TOPIC!r} from a prior run...")
        response = await admin.delete_topics([TOPIC])
        _raise_on_topic_errors(response.to_object()["topic_error_codes"])

        # Deletion is asynchronous on the broker side -- poll until the
        # topic actually stops appearing in metadata before recreating,
        # rather than racing a create against a delete still in flight.
        deadline = time.monotonic() + TOPIC_POLL_TIMEOUT_S
        while TOPIC in await admin.list_topics():
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for {TOPIC!r} to finish deleting")
            await asyncio.sleep(TOPIC_POLL_INTERVAL_S)

    print(f"Creating topic {TOPIC!r} with {NUM_PARTITIONS} partitions...")
    response = await admin.create_topics([NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR)])
    _raise_on_topic_errors(response.to_object()["topic_errors"])

    # Creation is also asynchronous -- poll until the topic is visible
    # with all NUM_PARTITIONS partitions before producing to it.
    deadline = time.monotonic() + TOPIC_POLL_TIMEOUT_S
    while True:
        described = await admin.describe_topics([TOPIC])
        if described and len(described[0]["partitions"]) == NUM_PARTITIONS:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for {TOPIC!r} to become ready")
        await asyncio.sleep(TOPIC_POLL_INTERVAL_S)


async def produce_messages(producer: AIOKafkaProducer) -> list[tuple[str, int, int, int]]:
    """Produce MESSAGES_PER_KEY messages for each key in KEYS, interleaved
    round-robin across keys (not grouped) so routing consistency is
    checked across non-adjacent sends, not just a run of back-to-back
    same-key messages that might happen to batch together.

    Returns a list of (key, seq, partition, offset) for every message
    actually sent, straight from the broker's own RecordMetadata --
    the ground truth this script's checks compare against, not
    predictions.
    """
    sent = []
    for seq in range(MESSAGES_PER_KEY):
        for key in KEYS:
            value = json.dumps({"key": key, "seq": seq}).encode("utf-8")
            metadata = await producer.send_and_wait(TOPIC, value=value, key=key.encode("utf-8"))
            sent.append((key, seq, metadata.partition, metadata.offset))
    return sent


async def consume_messages(expected_count: int) -> list[tuple[str, int, int, int]]:
    """Consume every message back from the earliest offset.

    Returns a list of (key, seq, partition, offset) in the order
    actually consumed (not necessarily production order across keys --
    only within a partition is order guaranteed).
    """
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Fresh, unique group per run so re-running this script never
        # resumes from a prior run's committed offsets -- consistent
        # with recreate_topic's clean-slate guarantee.
        group_id=f"kafka-smoke-test-{uuid.uuid4()}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        received: list[tuple[str, int, int, int]] = []
        deadline = time.monotonic() + CONSUME_TIMEOUT_S
        while len(received) < expected_count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Timed out after receiving {len(received)}/{expected_count} messages")
            batch = await consumer.getmany(timeout_ms=int(remaining * 1000))
            for messages in batch.values():
                for message in messages:
                    payload = json.loads(message.value.decode("utf-8"))
                    received.append((payload["key"], payload["seq"], message.partition, message.offset))
        return received
    finally:
        await consumer.stop()


def check_partition_routing(sent: list[tuple[str, int, int, int]]) -> tuple[bool, str]:
    """Same key must always route to the same partition, across every
    produced message -- not round-robin, not random.
    """
    partitions_by_key: dict[str, set[int]] = defaultdict(set)
    for key, _seq, partition, _offset in sent:
        partitions_by_key[key].add(partition)

    lines = [
        f"  {key!r} -> partition {next(iter(partitions))}" for key, partitions in sorted(partitions_by_key.items())
    ]
    inconsistent = {key: partitions for key, partitions in partitions_by_key.items() if len(partitions) != 1}
    if inconsistent:
        lines.append(f"  INCONSISTENT: {inconsistent}")
        return False, "\n".join(lines)
    return True, "\n".join(lines)


def check_order_preserved(received: list[tuple[str, int, int, int]]) -> tuple[bool, str]:
    """Within each partition, offsets must be strictly increasing, and
    each key's messages (all confined to one partition per the routing
    check above) must be consumed in the same seq order they were sent.
    """
    by_partition: dict[int, list[tuple[int, str, int]]] = defaultdict(list)
    for key, seq, partition, offset in received:
        by_partition[partition].append((offset, key, seq))

    problems = []
    seq_by_key: dict[str, list[int]] = defaultdict(list)
    for partition, entries in sorted(by_partition.items()):
        entries.sort(key=lambda e: e[0])  # consumption order within this partition
        offsets = [offset for offset, _key, _seq in entries]
        if offsets != sorted(set(offsets)) or len(offsets) != len(set(offsets)):
            problems.append(f"  partition {partition}: offsets not strictly increasing: {offsets}")
        for _offset, key, seq in entries:
            seq_by_key[key].append(seq)

    lines = [
        f"  partition {p}: {len(entries)} messages, offsets {[e[0] for e in entries]}"
        for p, entries in sorted(by_partition.items())
    ]
    for key, seqs in sorted(seq_by_key.items()):
        expected = list(range(MESSAGES_PER_KEY))
        if seqs != expected:
            problems.append(f"  {key!r}: seq order {seqs} != expected {expected}")

    if problems:
        return False, "\n".join(lines + ["  PROBLEMS:", *problems])
    return True, "\n".join(lines)


async def main() -> int:
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        await recreate_topic(admin)
    finally:
        await admin.close()

    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        print(f"Producing {MESSAGES_PER_KEY} messages each for {len(KEYS)} keys...")
        sent = await produce_messages(producer)
    finally:
        await producer.stop()

    print(f"Consuming {len(sent)} messages back from earliest offset...")
    received = await consume_messages(expected_count=len(sent))

    routing_ok, routing_report = check_partition_routing(sent)
    order_ok, order_report = check_order_preserved(received)

    print("\n=== Partition routing (same key -> same partition) ===")
    print(routing_report)
    print("\n=== Per-partition order preserved ===")
    print(order_report)

    overall_ok = routing_ok and order_ok
    print("\n=== Summary ===")
    print(f"  Topic recreation:     PASS ({NUM_PARTITIONS} partitions)")
    print(f"  Produced/consumed:    PASS ({len(sent)} sent, {len(received)} received)")
    print(f"  Partition routing:    {'PASS' if routing_ok else 'FAIL'}")
    print(f"  Order preserved:      {'PASS' if order_ok else 'FAIL'}")
    print(f"\n{'PASS' if overall_ok else 'FAIL'}: Kafka smoke test {'succeeded' if overall_ok else 'failed'}")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
