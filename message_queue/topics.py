"""Kafka admin helper for the message-queue replication strategy.

Topic lifecycle here is deliberately minimal: ensure_topic_exists() only
guarantees the configured topic exists with the configured partition
count before the producer or a follower starts using it. It does not
delete, recreate, or otherwise manage topics across separate runs --
that's a later branch's job (see message_queue/producer.py's module
docstring for this branch's scope). This exists purely so producer.py
and follower.py have no startup-ordering dependency on each other
(either may start first) and don't fall back on Kafka's own
auto-topic-creation, which would size the topic using the *broker's*
default partition count rather than config.num_partitions -- silently
breaking the hash-by-key partitioning this strategy depends on.
"""

from __future__ import annotations

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError, for_code

from message_queue.config import MQConfig

# Single-node broker (see docker-compose.kafka.yml) -- there is nothing
# else to replicate this topic to.
_TOPIC_REPLICATION_FACTOR = 1


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
        for entry in response.to_object()["topic_errors"]:
            code = entry["error_code"]
            assert isinstance(code, int)
            error_cls = for_code(code)
            if code != 0 and error_cls is not TopicAlreadyExistsError:
                raise RuntimeError(
                    f"failed to create topic {entry['topic']!r}: {error_cls.__name__} (code {code})"
                )
    finally:
        await admin.close()
