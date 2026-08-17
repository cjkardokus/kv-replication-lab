"""Thread-safe in-memory key-value store with last-write-wins semantics.

This is the shared storage layer used by both replication strategies
(leader-follower and leaderless). It knows nothing about replication,
networking, or nodes beyond the `node_id` string it's handed for each
write — that's purely metadata for LWW tiebreaking.
"""

from __future__ import annotations

import logging
import threading

from common.models import VersionedValue

logger = logging.getLogger(__name__)


class KVStore:
    """An in-memory KV store guarded by a single lock.

    All reads and writes go through this lock, so operations are atomic
    with respect to each other but not particularly high-throughput —
    fine for a lab, not for production.
    """

    def __init__(self) -> None:
        self._data: dict[str, VersionedValue] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> VersionedValue | None:
        """Return the VersionedValue stored at `key`, or None if absent."""
        with self._lock:
            return self._data.get(key)

    def put(self, key: str, value: object, timestamp: float, node_id: str) -> bool:
        """Write `value` at `key` if it's newer than what's stored, per LWW.

        Compares the incoming write against the current entry (if any)
        using VersionedValue.is_newer_than: higher timestamp wins, and
        equal timestamps are broken by higher node_id.

        If the incoming write is not newer, it's a no-op — the write is
        rejected as stale and logged, not raised as an error, since a
        losing write under LWW is an expected outcome, not a failure.

        Returns:
            True if the write was applied, False if it was rejected as stale.
        """
        incoming = VersionedValue(value=value, timestamp=timestamp, node_id=node_id)
        with self._lock:
            current = self._data.get(key)
            if current is not None and not incoming.is_newer_than(current):
                logger.info(
                    "put(%r) rejected as stale: incoming(ts=%s, node=%s) "
                    "vs current(ts=%s, node=%s)",
                    key, incoming.timestamp, incoming.node_id,
                    current.timestamp, current.node_id,
                )
                return False
            self._data[key] = incoming
            return True

    def delete(self, key: str) -> bool:
        """Remove `key` if present.

        Returns:
            True if the key existed and was removed, False if it was
            already absent.
        """
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False
