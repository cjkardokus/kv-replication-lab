"""Data model for versioned values stored in the KV store.

A single value is never stored bare — every entry is wrapped with the
metadata needed to resolve conflicts between concurrent writes (from
different followers, or different nodes in the leaderless quorum) using
last-write-wins (LWW).

Design choice: dataclass over pydantic.
    VersionedValue is an internal wrapper, not a boundary type — nothing
    here parses untrusted input off the wire or needs type coercion;
    `time.time()` and a `str` node_id are already the right types by
    construction. Pydantic's validation/serialization machinery would be
    paying for a feature this class doesn't need. If a later boundary
    (e.g. the HTTP server) needs to (de)serialize this from JSON, that's
    the point to reach for pydantic (or add explicit to/from-dict
    methods) — not here. `frozen=True` because a version, once written,
    is never mutated in place; a "new write" is a new VersionedValue, not
    an edit to an existing one.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class VersionedValue:
    """A stored value plus the metadata needed for LWW conflict resolution.

    Attributes:
        value: The stored value itself. Left untyped (Any-ish via no
            annotation constraint) since the KV store is value-agnostic.
        timestamp: Wall-clock write time, e.g. ``time.time()``. Later
            timestamp wins on conflict.
        node_id: ID of the node that produced this write. Used only as a
            tiebreaker when two writes have the exact same timestamp.
    """

    value: object
    timestamp: float
    node_id: str

    def is_newer_than(self, other: VersionedValue) -> bool:
        """Return True if this version should win over `other` under LWW.

        Comparison rule: higher timestamp wins; if timestamps are exactly
        equal, higher node_id (string comparison) wins as a tiebreaker.

        The node_id tiebreaker is arbitrary, not meaningful — it doesn't
        signal that one node is more authoritative than another. Its only
        job is determinism: every node comparing the same two versions
        must land on the same winner, so string-comparing node_id is just
        a convenient, consistent way to break an exact-timestamp tie the
        same way everywhere.
        """
        if self.timestamp != other.timestamp:
            return self.timestamp > other.timestamp
        # Arbitrary but deterministic: higher node_id wins on exact tie.
        return self.node_id > other.node_id
