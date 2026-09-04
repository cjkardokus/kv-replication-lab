"""Shared field-level validation helpers for this project's cluster
configs.

leader_follower/leader.py's ClusterConfig, leaderless/node.py's
ClusterConfig, and message_queue/config.py's MQConfig each hand-roll the
same handful of small validation idioms independently in their own
from_dict: "positive float, else ValueError" (timeout_seconds, in both
leader_follower's and leaderless's configs -- byte-identical message
already), "int within [lo, hi], else ValueError" (ack_required in
leader_follower; default_w/default_r in leaderless -- same shape,
different bounds/field names), and "non-empty string, else ValueError"
(bootstrap_servers/topic/consumer_group_prefix in message_queue's
MQConfig).

This is deliberately narrower than a shared ClusterConfig base class --
see README.md's Roadmap, which already discusses and defers that
specifically because the three configs' actual *fields* don't share
enough shape to justify one (confirmed once message_queue/'s own config
shape was known: no peer/address list at all, unlike the other two).
These are free functions, not a shape/inheritance decision, so extracting
them doesn't reopen that question -- they just remove the duplicated
message-formatting/comparison logic underneath it.

Deliberately NOT here: message_queue/config.py's num_partitions check
(`if num_partitions < 1: raise ValueError(...)`). It looks similar to
require_in_range at a glance (an integer bound check) but is actually a
different shape -- open lower bound only, no upper bound, and its own
"must be >= N, got X" message wording -- so folding it into
require_in_range would mean force-fitting a helper built for a genuinely
different check (a two-sided [lo, hi] range against a message that always
names what the upper bound counts) rather than reusing it. Left as its
own inline check.
"""

from __future__ import annotations


def require_positive(value: float, name: str) -> None:
    """Raise ValueError unless `value` is strictly positive.

    Message shape: "<name> must be positive, got <value>" -- matches
    what leader_follower's and leaderless's ClusterConfig.from_dict
    already raise for timeout_seconds, word for word.
    """
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")


def require_in_range(value: int, name: str, lo: int, hi: int, hi_label: str) -> None:
    """Raise ValueError unless lo <= value <= hi.

    `hi_label` names what `hi` counts (e.g. "followers", "nodes") --
    matches the shape leader_follower's ack_required and leaderless's
    default_w/default_r validation already raise: "<name> (<value>) must
    be between <lo> and the number of <hi_label> (<hi>)". `hi` is always
    a count derived by the caller (e.g. len(followers)), which is what
    makes this phrasing -- rather than a bare "must be between <lo> and
    <hi>" -- the right generic shape for both existing call sites.
    """
    if not lo <= value <= hi:
        raise ValueError(f"{name} ({value}) must be between {lo} and the number of {hi_label} ({hi})")


def require_nonempty_str(value: str, name: str) -> str:
    """Strip `value` and raise ValueError if the result is empty;
    otherwise return the stripped value.

    Matches message_queue/config.py's MQConfig.from_dict's existing
    shape for its three required string fields: each is read from raw
    config data (already coerced to `str` by the caller, since a YAML
    value isn't guaranteed to already be one) and must end up both a
    real string and genuinely non-empty once whitespace-only content is
    discounted. The stripped result -- not the caller's original,
    possibly-untrimmed value -- is what the caller should store, exactly
    as today.
    """
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{name} must be a non-empty string")
    return stripped
