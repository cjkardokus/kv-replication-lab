"""Kafka admin helpers for the message-queue replication strategy.

ensure_topic_exists() guarantees the configured topic exists with the
configured partition count before the producer or a follower starts
using it. It does not delete or recreate an existing topic -- it exists
purely so producer.py and follower.py have no startup-ordering
dependency on each other (either may start first) and don't fall back
on Kafka's own auto-topic-creation, which would size the topic using
the *broker's* default partition count rather than config.num_partitions
-- silently breaking the hash-by-key partitioning this strategy depends
on.

reset_topic() is the real lifecycle primitive: delete the topic (if it
exists), wait for the deletion to actually complete, then recreate it
via ensure_topic_exists(). This is what a later branch's sweep
(experiments/run_comparison.py) uses between configs to give this
strategy the same clean-slate-per-config guarantee leader_follower's and
leaderless's fresh, per-config OS processes already give those two
strategies -- see reset_topic's own docstring for why deleting the topic
itself (not just resetting consumer-group offsets) is what that
actually requires.
"""

from __future__ import annotations

import asyncio
import time

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError, for_code

from message_queue.config import MQConfig

# Single-node broker (see docker-compose.kafka.yml) -- there is nothing
# else to replicate this topic to.
_TOPIC_REPLICATION_FACTOR = 1

# How long reset_topic() waits for a delete to actually finish (Kafka
# topic deletion is asynchronous -- see reset_topic's own docstring)
# before giving up and raising, rather than silently racing a recreate
# against a delete still in flight.
_TOPIC_DELETE_POLL_TIMEOUT_S = 15.0
_TOPIC_DELETE_POLL_INTERVAL_S = 0.5


def _raise_on_topic_errors(entries: list[dict[str, object]], tolerate: type[Exception]) -> None:
    """Raise if any (topic, error_code) entry from a Create/DeleteTopics
    response carries a real error, except `tolerate` -- the specific,
    expected race each caller below can lose and still be correct (see
    each call site's own comment for which race that is).
    """
    for entry in entries:
        code = entry["error_code"]
        assert isinstance(code, int)
        error_cls = for_code(code)
        if code != 0 and error_cls is not tolerate:
            raise RuntimeError(f"{entry['topic']!r}: {error_cls.__name__} (code {code})")


async def ensure_topic_exists(config: MQConfig) -> None:
    """Create config.topic with config.num_partitions if it doesn't
    already exist.

    Safe to call concurrently from multiple processes -- e.g. a
    producer and several followers all starting at once, in any order.
    A race where two callers both see the topic missing and both
    attempt to create it is resolved by tolerating TopicAlreadyExistsError
    from whichever call loses the race, not by coordinating who gets to
    create it first.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=config.bootstrap_servers)
    await admin.start()
    try:
        if config.topic in await admin.list_topics():
            return
        response = await admin.create_topics(
            [NewTopic(config.topic, config.num_partitions, _TOPIC_REPLICATION_FACTOR)]
        )
        _raise_on_topic_errors(response.to_object()["topic_errors"], tolerate=TopicAlreadyExistsError)
    finally:
        await admin.close()


async def reset_topic(config: MQConfig) -> None:
    """Delete config.topic (if it exists), wait for the deletion to
    actually complete, then recreate it fresh via ensure_topic_exists().

    This is the primitive a sweep (experiments/run_comparison.py, a
    later branch) uses between configs to give this strategy the same
    clean-slate guarantee leader_follower's and leaderless's fresh,
    per-config OS processes already give those two strategies: every
    config starts against a topic with no messages and no history left
    over from a previous config, the same way those strategies start
    every config against fresh, empty KVStore instances.

    Resetting consumer-group offsets alone would not be enough -- the
    messages themselves would still be on the topic, so a fresh
    follower resetting to "earliest" would replay every prior config's
    writes, not start from nothing. Deleting the topic itself is what
    actually clears that history; recreating it (rather than leaving it
    deleted) is what lets the next config's producer/followers use it
    immediately without racing Kafka's own auto-topic-creation the same
    way ensure_topic_exists already avoids that race at first startup.

    Kafka topic deletion is asynchronous: the broker acknowledges the
    delete request immediately, but the topic can still briefly appear
    in metadata afterward. This polls list_topics() until it's actually
    gone (up to _TOPIC_DELETE_POLL_TIMEOUT_S) before recreating --
    racing a create against a delete still in flight would otherwise
    risk the recreate silently landing on top of the not-yet-finished
    deletion, or failing outright. experiments/kafka_smoke_test.py's
    own recreate_topic() polls the same way, for the same reason, but
    stays a standalone throwaway script (see that module's docstring);
    this is the real, importable equivalent for actual sweep use.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=config.bootstrap_servers)
    await admin.start()
    try:
        if config.topic in await admin.list_topics():
            response = await admin.delete_topics([config.topic])
            _raise_on_topic_errors(
                response.to_object()["topic_error_codes"], tolerate=UnknownTopicOrPartitionError
            )

            deadline = time.monotonic() + _TOPIC_DELETE_POLL_TIMEOUT_S
            while config.topic in await admin.list_topics():
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out waiting for topic {config.topic!r} to finish deleting")
                await asyncio.sleep(_TOPIC_DELETE_POLL_INTERVAL_S)
    finally:
        await admin.close()

    await ensure_topic_exists(config)
