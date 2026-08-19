"""Concurrent load test measuring replication staleness under load.

Drives a mix of reads and writes against a running leader-follower
cluster and measures how often a follower read returns a value that's
older than what the load test itself already knows to have been
written -- i.e. observable replication lag, not simulated.

Approach
--------
- A fixed pool of 100 keys (key_0..key_99) is reused across the whole
  run instead of generating fresh keys per request, so the same keys
  get hit repeatedly and create real read/write contention.
- Every write's value comes from a single global counter shared across
  all writer workers, so every value written during the run is unique
  and traceable to exactly one write.
- ~50 concurrent asyncio workers fire a 70/30 read/write mix, 10,000
  requests total, against httpx.AsyncClient.
- Writes go to the leader (PUT /kv/{key}). The leader's PutResponse
  only reports `applied` -- it doesn't echo back the timestamp it
  stamped on the write (see common/server.py::PutResponse) -- so on a
  successful write this script immediately reads the key back from the
  leader to learn the authoritative (value, timestamp) now on record,
  and folds that into a shared "source of truth" dict (key -> newest
  known (value, timestamp), by max timestamp) so concurrent writers
  completing out of order can't clobber a newer entry with an older
  one.
- Reads pick a random key and a random follower, snapshot the source
  of truth for that key *before* issuing the read, then compare the
  follower's response against that snapshot. A follower that's missing
  the key, or has an older timestamp than the snapshot, is stale; a
  follower with an equal or newer timestamp is not (it may simply
  reflect a write this script hasn't caught up to itself).

This script only drives HTTP traffic -- it assumes the leader
(localhost:8000) and the followers listed in
config/leader_follower_cluster.yaml are already running as separate
processes.

Run:
    python3 -m experiments.staleness_load_test
"""

from __future__ import annotations

import asyncio
import itertools
import json
import random
import statistics
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from leader_follower.leader import ClusterConfig, Follower

LEADER_URL = "http://localhost:8000"
CONFIG_PATH = "config/leader_follower_cluster.yaml"

NUM_KEYS = 100
NUM_WORKERS = 50
TOTAL_REQUESTS = 10_000
READ_FRACTION = 0.7  # ~70% reads, ~30% writes
REQUEST_TIMEOUT = 5.0

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_LOG_PATH = OUTPUT_DIR / "staleness_log.jsonl"

KEYS = [f"key_{i}" for i in range(NUM_KEYS)]


# --- Shared state -----------------------------------------------------------


class GlobalCounter:
    """Monotonically increasing integer source for write values, shared
    across all writer workers and guarded by an asyncio.Lock so two
    concurrent writers never hand out the same value -- every value
    written during the run is globally unique and traceable to exactly
    one write.
    """

    def __init__(self) -> None:
        self._counter = itertools.count()
        self._lock = asyncio.Lock()

    async def next_value(self) -> int:
        async with self._lock:
            return next(self._counter)


@dataclass(frozen=True, slots=True)
class SourceEntry:
    value: int
    timestamp: float


class SourceOfTruth:
    """This process's own record of the latest confirmed write per key,
    built from successful writes it has observed. Guarded by an
    asyncio.Lock; an update only takes effect if its timestamp is newer
    than what's already recorded (max by timestamp), so writes that
    complete out of order can't clobber a newer entry with an older one.
    """

    def __init__(self) -> None:
        self._entries: dict[str, SourceEntry] = {}
        self._lock = asyncio.Lock()

    async def record(self, key: str, value: int, timestamp: float) -> None:
        async with self._lock:
            current = self._entries.get(key)
            if current is None or timestamp > current.timestamp:
                self._entries[key] = SourceEntry(value=value, timestamp=timestamp)

    async def snapshot(self, key: str) -> SourceEntry | None:
        async with self._lock:
            return self._entries.get(key)


@dataclass
class Stats:
    """Per-worker counters, merged after all workers finish. Each worker
    accumulates into its own instance, so no locking is needed here --
    only SourceOfTruth and GlobalCounter are genuinely shared mid-run.
    """

    total_requests: int = 0
    total_reads: int = 0
    total_writes: int = 0
    comparable_reads: int = 0  # reads with a source-of-truth entry to check against
    stale_reads: int = 0
    stale_events: list[dict[str, Any]] = field(default_factory=list)

    def merge(self, other: "Stats") -> None:
        self.total_requests += other.total_requests
        self.total_reads += other.total_reads
        self.total_writes += other.total_writes
        self.comparable_reads += other.comparable_reads
        self.stale_reads += other.stale_reads
        self.stale_events.extend(other.stale_events)


# --- Worker logic -------------------------------------------------------


async def discover_node_ids(
    client: httpx.AsyncClient, followers: list[Follower]
) -> dict[Follower, str]:
    """Look up each follower's node_id via /health once, up front, so
    stale-read log entries can name the actual node rather than just a
    host:port pair.
    """
    node_ids: dict[Follower, str] = {}
    for follower in followers:
        resp = await client.get(
            f"http://{follower.host}:{follower.port}/health", timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        node_ids[follower] = resp.json()["node_id"]
    return node_ids


def _record_stale(
    stats: Stats,
    key: str,
    node_id: str,
    expected: SourceEntry,
    actual_timestamp: float | None,
) -> None:
    if actual_timestamp is None:
        # Missing entirely -- there's no actual timestamp to diff
        # against, so report the gap as how long it's been since the
        # write we expected to see landed.
        gap_ms = (time.time() - expected.timestamp) * 1000
        actual_repr: Any = "missing"
    else:
        gap_ms = (expected.timestamp - actual_timestamp) * 1000
        actual_repr = actual_timestamp

    stats.stale_reads += 1
    stats.stale_events.append(
        {
            "key": key,
            "follower_node_id": node_id,
            "expected_timestamp": expected.timestamp,
            "actual_timestamp": actual_repr,
            "staleness_gap_ms": gap_ms,
        }
    )


async def _do_write(
    client: httpx.AsyncClient, counter: GlobalCounter, source_of_truth: SourceOfTruth
) -> None:
    key = random.choice(KEYS)
    value = await counter.next_value()

    try:
        resp = await client.put(
            f"{LEADER_URL}/kv/{key}", json={"value": value}, timeout=REQUEST_TIMEOUT
        )
    except httpx.HTTPError:
        return  # transient failure -- nothing confirmed, nothing to record

    if resp.status_code != 200 or not resp.json().get("applied", False):
        return

    # Read the entry straight back from the leader to learn the
    # timestamp it actually stamped (see module docstring). If a later
    # concurrent write to the same key lands first, this may observe
    # that instead of our own write -- source_of_truth.record() takes
    # the max by timestamp regardless, so it's still valid ground truth.
    try:
        entry_resp = await client.get(f"{LEADER_URL}/kv/{key}", timeout=REQUEST_TIMEOUT)
    except httpx.HTTPError:
        return
    if entry_resp.status_code != 200:
        return

    body = entry_resp.json()
    await source_of_truth.record(key, body["value"], body["timestamp"])


async def _do_read(
    client: httpx.AsyncClient,
    source_of_truth: SourceOfTruth,
    followers: list[Follower],
    follower_node_ids: dict[Follower, str],
    stats: Stats,
) -> None:
    key = random.choice(KEYS)
    follower = random.choice(followers)

    expected_before = await source_of_truth.snapshot(key)
    if expected_before is None:
        # No write has completed for this key yet -- nothing to compare
        # against, so skip rather than counting it stale or not-stale.
        return

    stats.comparable_reads += 1
    node_id = follower_node_ids.get(follower, f"{follower.host}:{follower.port}")

    try:
        resp = await client.get(
            f"http://{follower.host}:{follower.port}/kv/{key}", timeout=REQUEST_TIMEOUT
        )
    except httpx.HTTPError:
        return  # can't tell whether it's stale or just unreachable right now

    if resp.status_code == 404:
        _record_stale(stats, key, node_id, expected_before, actual_timestamp=None)
        return

    body = resp.json()
    actual_timestamp = body["timestamp"]
    if actual_timestamp < expected_before.timestamp:
        _record_stale(stats, key, node_id, expected_before, actual_timestamp)
    # else: at or newer than expected -- not stale, even if it reflects
    # a write this script hasn't caught up to itself.


async def run_worker(
    num_requests: int,
    client: httpx.AsyncClient,
    counter: GlobalCounter,
    source_of_truth: SourceOfTruth,
    followers: list[Follower],
    follower_node_ids: dict[Follower, str],
) -> Stats:
    stats = Stats()
    for _ in range(num_requests):
        stats.total_requests += 1
        if random.random() < READ_FRACTION:
            stats.total_reads += 1
            await _do_read(client, source_of_truth, followers, follower_node_ids, stats)
        else:
            stats.total_writes += 1
            await _do_write(client, counter, source_of_truth)
    return stats


# --- Reporting ------------------------------------------------------------


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100) * (len(ordered) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _write_stale_log(events: list[dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_LOG_PATH, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


def _print_summary(stats: Stats, elapsed: float) -> None:
    print()
    print("=== staleness load test summary ===")
    print(f"elapsed:              {elapsed:.2f}s")
    print(f"total requests sent:  {stats.total_requests}")
    print(f"total reads:          {stats.total_reads}")
    print(f"total writes:         {stats.total_writes}")
    print(
        f"comparable reads:     {stats.comparable_reads}  "
        "(reads with a source-of-truth entry to check against)"
    )
    print(f"stale reads observed: {stats.stale_reads}")

    # Staleness rate is reported against comparable reads, not all
    # reads -- reads that had no source-of-truth entry yet (nothing
    # written for that key so far) were skipped, not counted as
    # non-stale, so including them would understate the rate.
    rate = 100 * stats.stale_reads / stats.comparable_reads if stats.comparable_reads else 0.0
    print(f"staleness rate:       {rate:.2f}%  (stale / comparable reads)")

    if stats.stale_events:
        gaps = [e["staleness_gap_ms"] for e in stats.stale_events]
        print()
        print("staleness gap (ms), stale reads only:")
        print(f"  min:  {min(gaps):.2f}")
        print(f"  max:  {max(gaps):.2f}")
        print(f"  mean: {statistics.mean(gaps):.2f}")
        print(f"  p95:  {_percentile(gaps, 95):.2f}")
    else:
        print()
        print("no stale reads observed.")

    print()
    print(f"raw stale-read log written to {OUTPUT_LOG_PATH}")


# --- Entrypoint -------------------------------------------------------------


async def main() -> None:
    config = ClusterConfig.from_yaml(CONFIG_PATH)
    followers = config.followers
    if not followers:
        raise SystemExit(f"no followers configured in {CONFIG_PATH}")

    limits = httpx.Limits(
        max_connections=NUM_WORKERS * 2, max_keepalive_connections=NUM_WORKERS * 2
    )
    async with httpx.AsyncClient(limits=limits) as client:
        print(f"discovering node ids for {len(followers)} followers...")
        follower_node_ids = await discover_node_ids(client, followers)

        counter = GlobalCounter()
        source_of_truth = SourceOfTruth()

        # Split TOTAL_REQUESTS as evenly as possible across NUM_WORKERS.
        base, remainder = divmod(TOTAL_REQUESTS, NUM_WORKERS)
        worker_loads = [base + (1 if i < remainder else 0) for i in range(NUM_WORKERS)]

        print(
            f"starting {NUM_WORKERS} workers, {TOTAL_REQUESTS} total requests "
            f"({READ_FRACTION:.0%} reads / {1 - READ_FRACTION:.0%} writes) "
            f"against leader {LEADER_URL} and {len(followers)} followers..."
        )
        start = time.time()
        results = await asyncio.gather(
            *(
                run_worker(load, client, counter, source_of_truth, followers, follower_node_ids)
                for load in worker_loads
            )
        )
        elapsed = time.time() - start

    stats = Stats()
    for result in results:
        stats.merge(result)

    _write_stale_log(stats.stale_events)
    _print_summary(stats, elapsed)


if __name__ == "__main__":
    asyncio.run(main())
