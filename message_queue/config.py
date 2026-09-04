"""Cluster config for the message-queue replication strategy: how to
reach Kafka, and which topic/partition layout to use.

Unlike leader_follower's and leaderless's ClusterConfig, this carries no
peer/follower addresses at all -- producer.py and follower.py never talk
to each other directly, only to Kafka (see producer.py's module
docstring for why that's the point of this strategy, not an oversight).
Every follower is started independently (its own --node-id/--port, same
as the other two strategies) and needs nothing beyond this file to find
the same topic every other node uses.

README.md's Roadmap deferred a shared ClusterConfig base across all
three strategies specifically until this strategy's config shape was
known. This confirms that was the right call: this shape
(bootstrap_servers/topic/num_partitions/consumer_group_prefix) shares no
fields at all with leader_follower's (followers/ack_required/
timeout_seconds) or leaderless's (nodes/default_w/default_r/
timeout_seconds) beyond the from_dict/from_yaml pattern itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True, slots=True)
class MQConfig:
    """See config/mq_cluster.yaml for the on-disk schema and field meanings."""

    bootstrap_servers: str
    topic: str
    num_partitions: int
    consumer_group_prefix: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> MQConfig:
        bootstrap_servers = str(raw.get("bootstrap_servers", "")).strip()
        topic = str(raw.get("topic", "")).strip()
        consumer_group_prefix = str(raw.get("consumer_group_prefix", "")).strip()
        num_partitions = int(raw.get("num_partitions", 0))

        if not bootstrap_servers:
            raise ValueError("bootstrap_servers must be a non-empty string")
        if not topic:
            raise ValueError("topic must be a non-empty string")
        if not consumer_group_prefix:
            raise ValueError("consumer_group_prefix must be a non-empty string")
        if num_partitions < 1:
            raise ValueError(f"num_partitions must be >= 1, got {num_partitions}")

        return cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            num_partitions=num_partitions,
            consumer_group_prefix=consumer_group_prefix,
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> MQConfig:
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        return cls.from_dict(raw)
