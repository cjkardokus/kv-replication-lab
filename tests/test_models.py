"""Tests for common.models.VersionedValue and its LWW comparison rule."""

from common.models import VersionedValue


def test_higher_timestamp_wins():
    older = VersionedValue(value="a", timestamp=1.0, node_id="node-1")
    newer = VersionedValue(value="b", timestamp=2.0, node_id="node-1")

    assert newer.is_newer_than(older)
    assert not older.is_newer_than(newer)


def test_equal_timestamp_breaks_tie_on_higher_node_id():
    low_node = VersionedValue(value="a", timestamp=1.0, node_id="node-1")
    high_node = VersionedValue(value="b", timestamp=1.0, node_id="node-2")

    assert high_node.is_newer_than(low_node)
    assert not low_node.is_newer_than(high_node)


def test_equal_timestamp_and_node_id_is_not_newer():
    # Identical version vs. itself (or an equivalent copy): neither side
    # should be considered strictly newer than the other.
    a = VersionedValue(value="a", timestamp=1.0, node_id="node-1")
    b = VersionedValue(value="a", timestamp=1.0, node_id="node-1")

    assert not a.is_newer_than(b)
    assert not b.is_newer_than(a)


def test_node_id_tiebreak_is_pure_string_comparison():
    # "node-10" < "node-2" lexicographically, even though 10 > 2
    # numerically -- the tiebreaker is documented as an arbitrary but
    # deterministic string comparison, not a numeric one.
    node_10 = VersionedValue(value="a", timestamp=1.0, node_id="node-10")
    node_2 = VersionedValue(value="b", timestamp=1.0, node_id="node-2")

    assert node_2.is_newer_than(node_10)
    assert not node_10.is_newer_than(node_2)


def test_lower_timestamp_loses_even_with_higher_node_id():
    # timestamp always takes precedence over node_id; node_id only
    # matters once timestamps are exactly equal.
    older_high_node = VersionedValue(value="a", timestamp=1.0, node_id="node-9")
    newer_low_node = VersionedValue(value="b", timestamp=2.0, node_id="node-1")

    assert newer_low_node.is_newer_than(older_high_node)
    assert not older_high_node.is_newer_than(newer_low_node)


def test_versioned_value_is_frozen():
    v = VersionedValue(value="a", timestamp=1.0, node_id="node-1")
    try:
        v.value = "b"
        assert False, "expected VersionedValue to be immutable"
    except AttributeError:
        pass
