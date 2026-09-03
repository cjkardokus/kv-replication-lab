"""Shared httpx.AsyncClient factory for node-to-node replication calls.

Both leader_follower/leader.py's Replicator and leaderless/node.py's
QuorumCoordinator need the same shape of outbound client: one that fans
a write (leaderless: a read too) out to several peers concurrently, so
both need a connection pool sized well above httpx's conservative
defaults to do that without spuriously hitting httpx.PoolTimeout. The
pool-sizing constants and the client construction itself live here once,
instead of being duplicated -- with identical values, but separately
maintained -- in both modules (see docs/AUDIT_FINDINGS.md's §5).

Deliberately NOT here: leader_follower/leader.py's Replicator._send
concurrency semaphore (_MAX_CONCURRENT_REPLICATE_CALLS). That bounds how
many replicate() calls a leader admits concurrently, independent of
connection-pool size -- a different kind of limit than anything below,
and leaderless has no equivalent of it yet. See leader_follower/leader.py's
own comment (above _MAX_CONCURRENT_REPLICATE_CALLS) for why it stays
local to that module rather than moving here alongside the client
factory both strategies do share.
"""

from __future__ import annotations

import httpx

# Each node's outbound client fans requests out to several peers
# concurrently -- every write, for both strategies; every read too, for
# leaderless -- so a burst of concurrent client traffic can demand far
# more simultaneous outbound connections than httpx's conservative
# defaults (100 total / 20 keepalive) provide. Once that pool is
# exhausted, a queued call blocks waiting for a slot and times out
# (httpx.PoolTimeout), which the caller can only see as "peer didn't
# respond" -- pushing it toward a failed ack/quorum count it didn't
# actually deserve; the write or read may have gone through fine, the
# caller just never got to see the response before the peer's own
# connection slot freed up. These are sized well above what this lab's
# default load (a handful of nodes, ~50 concurrent load-test workers)
# ordinarily needs, as headroom against bursts rather than a promise
# that no volume of traffic can exhaust them.
#
# This is a single, shared setting: not scoped per caller, per cluster
# config, per node, or (now that both strategies build their client from
# here) per replication strategy -- see leader_follower/leader.py's own
# comment block for what that specifically meant for the ack_required
# sweep in docs/results.md.
_CLIENT_MAX_CONNECTIONS = 500
_CLIENT_MAX_KEEPALIVE_CONNECTIONS = 100

# Fraction of the caller's timeout given to acquiring a connection from
# the pool, specifically, rather than the connect/read/write phases of a
# peer call. A bare float for httpx's `timeout=` applies to all four
# phases alike, so pool-acquire contention would otherwise share the
# exact same budget as a slow-but-reachable peer -- giving pool-acquire
# its own, shorter timeout means a call that can't get a connection
# quickly fails fast instead, freeing up the rest of the caller's own
# ack/quorum-collection deadline rather than tying it all up waiting on
# a slot that was never going to free up in time.
_POOL_TIMEOUT_FRACTION = 0.25


def build_replication_client(timeout_seconds: float) -> httpx.AsyncClient:
    """Build the httpx.AsyncClient a node uses for its node-to-node
    replication calls (leader-follower: replicate only; leaderless:
    replicate and internal reads), sized per this module's constants
    above.

    `timeout_seconds` is the caller's own per-request timeout for peer
    calls (each cluster config's `timeout_seconds`) -- the pool-acquire
    phase specifically gets `timeout_seconds * _POOL_TIMEOUT_FRACTION`
    of that; every other phase (connect/read/write) gets the full
    amount (see _POOL_TIMEOUT_FRACTION above).

    Callers are responsible for closing the returned client, typically
    via `app.router.add_event_handler("shutdown", client.aclose)`.
    """
    client_timeout = httpx.Timeout(
        timeout_seconds,
        pool=timeout_seconds * _POOL_TIMEOUT_FRACTION,
    )
    return httpx.AsyncClient(
        timeout=client_timeout,
        limits=httpx.Limits(
            max_connections=_CLIENT_MAX_CONNECTIONS,
            max_keepalive_connections=_CLIENT_MAX_KEEPALIVE_CONNECTIONS,
        ),
    )
