"""Tests for experiments/run_comparison.py.

Currently just _slug() -- the rest of this module is an orchestration
script (spawns real node processes, runs real load tests) exercised by
actually running it, not by a unit test suite.
"""

from __future__ import annotations

from experiments.run_comparison import _slug


def test_slug_distinguishes_near_collision_labels():
    """_slug() maps every non-alphanumeric character to a single "_",
    so two labels that differ only in *which* single non-alnum
    character sits in the same spot can collide (see the last assertion
    below -- a real collision, not a hypothetical). The sweep's own
    labels are safe from that specific collision because
    LEADERLESS_WR_SWEEP always formats them as "W=X, R=Y" -- comma
    *and* space together, two characters -- never either alone. This
    pins that down directly: a future label format change that drops
    the space (or the comma) must not silently start colliding with
    another config's slug and overwriting its JSONL logs (see
    docs/AUDIT_FINDINGS.md's §10 -- this was a real, previously
    untested gap).
    """
    assert _slug("W=1, R=1") != _slug("W=1,R=1")
    assert _slug("W=1, R=1") != _slug("W=1 R=1")
    assert _slug("W=2, R=3") != _slug("W=2,R=3")
    assert _slug("ack_required=0") != _slug("ack_required =0")

    # The residual limitation, documented rather than silently avoided:
    # a comma and a space are each exactly one non-alnum character, so
    # a label using one collides with the same label using the other.
    # Not exercised by any real label in this codebase today (see
    # above), but real all the same.
    assert _slug("W=1,R=1") == _slug("W=1 R=1")
