"""Shared machinery for experiments/staleness_load_test.py and
experiments/leaderless_staleness_load_test.py: seeded per-request
decision planning, per-worker stat tracking, the shared write-value/
source-of-truth state passed into workers, failure classification/
recording, and the printed/JSONL reporting.

Most of this was extracted from those two scripts, which had it
duplicated near-verbatim -- see docs/AUDIT_FINDINGS.md's §4. That part
was a mechanical extraction (pure code motion): every such piece is
unchanged from at least one of the two scripts' own copies, except two
spots that genuinely differed between them and are now parameters
instead -- `_record_stale`'s `node_id_field` (each strategy calls the
node a stale read landed on by a different name: "follower_node_id" vs.
"coordinator_node_id") and `_print_summary`'s `title` (the printed
banner text).

The request-planning machinery (RequestPlan, generate_request_plan,
split_plan, DEFAULT_SEED) is not an extraction -- it's new, shared
logic implementing docs/AUDIT_FINDINGS.md's §8: making a run's
per-request decisions (not real timing) reproducible under a fixed
seed. See generate_request_plan's own docstring for why this has to be
precomputed up front rather than seeded lazily.

Genuinely strategy-specific, and deliberately *not* here: coordinator
selection (leader-follower's fixed leader URL vs. leaderless's
NodeRotation/random read-coordinator choice), node-id discovery
(`discover_node_ids` et al. -- typed against each strategy's own
`Follower`/`Node` address class), and each script's own
`_do_write`/`_do_read` HTTP calls.

Not a standalone entry point -- imported by the two load-test scripts
above, never run directly (hence the leading underscore, this project's
usual convention for internal-only modules, e.g. run_comparison.py's
`_spawn`/`_wait_for_all_healthy`).
"""

from __future__ import annotations

import asyncio
import itertools
import json
import random
import statistics
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

# Fixed default so every standard sweep run is reproducible (see
# generate_request_plan below) unless a caller deliberately overrides
# it -- e.g. to confirm a finding isn't an artifact of this particular
# seed. Shared by both load-test scripts' --seed default.
DEFAULT_SEED = 42


# --- Request planning (seeded determinism) -----------------------------


@dataclass(frozen=True, slots=True)
class RequestPlan:
    """One request's precomputed random decisions -- see
    generate_request_plan for why these are generated synchronously, up
    front, before the timed portion of a run starts, rather than drawn
    live by workers as the run executes.
    """

    is_read: bool
    key: str
    # Index into the caller's own list of read targets (followers, for
    # leader-follower; nodes, for leaderless's read coordinator) --
    # meaningful only when is_read is True. Writes need no random
    # target here: leader-follower always writes to the one leader,
    # and leaderless picks its write coordinator by round robin
    # (NodeRotation) rather than randomly, so neither needs a
    # precomputed decision for "which node".
    read_target_index: int | None


def _derive_seeds(seed: int, count: int) -> list[int]:
    """Derive `count` independent seeds from one top-level `seed`, via
    a throwaway random.Random(seed) rather than e.g. seed, seed + 1,
    seed + 2, ... -- so nearby top-level seeds (1 vs. 2) don't produce
    suspiciously-related derived seeds for each concern.
    """
    deriver = random.Random(seed)
    return [deriver.getrandbits(64) for _ in range(count)]


def generate_request_plan(
    total_requests: int,
    keys: list[str],
    num_read_targets: int,
    *,
    read_fraction: float,
    seed: int,
) -> list[RequestPlan]:
    """Precompute the full sequence of per-request random decisions --
    read vs. write, which key, and (for reads) which target index --
    for an entire run, before the timed portion starts.

    This is what actually makes `seed` reproducible, not just a timing
    nicety: with NUM_WORKERS concurrent async workers each otherwise
    calling into a *shared* random instance live during the timed loop,
    which worker's request draws which random value next depends on
    event-loop scheduling -- not deterministic even with a fixed seed,
    since asyncio gives no ordering guarantee across concurrently
    awaiting workers. Pre-generating the whole sequence up front (a
    plain synchronous loop, no I/O, no concurrency) removes the
    scheduling dependency entirely: split_plan() below hands each
    worker a fixed, contiguous slice of this list to consume strictly
    in order, so which decision lands in which worker's Nth request is
    fixed by `seed` alone, never by timing.

    Uses three independent random.Random instances -- one each for the
    read/write mix, key selection, and read-target selection -- derived
    from the single `seed` (see _derive_seeds), so each concern's
    sequence is isolated from the others: an unrelated random call
    added elsewhere in this module later can't shift an existing
    concern's sequence for a seed that's already documented as
    producing a given run's numbers.

    Real timing/scheduling -- which request actually happens *when*
    relative to another, how long each takes -- is not, and cannot be,
    covered by any of this: two runs with the same seed make the exact
    same sequence of decisions, but under real concurrent network I/O
    can still observe different staleness, since staleness depends on
    whether a read's real arrival happens to race a write's real
    in-flight replication. See docs/results.md's methodology note (and
    README.md's "Results" section) for measured run-to-run variance
    under a fixed seed.
    """
    mix_seed, key_seed, target_seed = _derive_seeds(seed, 3)
    mix_rng = random.Random(mix_seed)
    key_rng = random.Random(key_seed)
    target_rng = random.Random(target_seed)

    plan: list[RequestPlan] = []
    for _ in range(total_requests):
        is_read = mix_rng.random() < read_fraction
        key = key_rng.choice(keys)
        read_target_index = target_rng.randrange(num_read_targets) if is_read else None
        plan.append(RequestPlan(is_read=is_read, key=key, read_target_index=read_target_index))
    return plan


def split_plan(plan: list[RequestPlan], num_workers: int) -> list[list[RequestPlan]]:
    """Split `plan` into `num_workers` contiguous chunks, as evenly as
    possible (the first `len(plan) % num_workers` chunks get one extra
    element) -- the same split TOTAL_REQUESTS used to get divided
    across workers directly (a bare request count), just applied to the
    precomputed plan instead.
    """
    base, remainder = divmod(len(plan), num_workers)
    chunks: list[list[RequestPlan]] = []
    start = 0
    for i in range(num_workers):
        size = base + (1 if i < remainder else 0)
        chunks.append(plan[start:start + size])
        start += size
    return chunks


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
    only the shared, lock-protected state passed into workers (e.g.
    SourceOfTruth, GlobalCounter, and leaderless's additional
    NodeRotation) is genuinely shared mid-run.
    """

    total_requests: int = 0
    total_reads: int = 0
    total_writes: int = 0
    comparable_reads: int = 0  # reads with a source-of-truth entry to check against
    stale_reads: int = 0
    stale_events: list[dict[str, Any]] = field(default_factory=list)

    # A "failure" is a read or write that never got a valid response at
    # all -- a non-2xx status, a timeout, or a connection error -- as
    # opposed to a stale read, which did get a response, just an
    # outdated one. Counted and logged separately: a failed write never
    # updates source_of_truth (nothing durably happened to record), and
    # a failed read is never counted as a stale-read comparison (there's
    # no value to compare).
    failed_reads: int = 0
    failed_writes: int = 0
    failure_counts: Counter[tuple[str, str]] = field(default_factory=Counter)
    failure_events: list[dict[str, Any]] = field(default_factory=list)

    @property
    def total_failures(self) -> int:
        return self.failed_reads + self.failed_writes

    @property
    def staleness_rate(self) -> float:
        """Stale reads as a percentage of *comparable* reads, not all
        reads -- see _print_summary for why comparable reads is the
        right denominator.
        """
        if not self.comparable_reads:
            return 0.0
        return 100 * self.stale_reads / self.comparable_reads

    def merge(self, other: "Stats") -> None:
        self.total_requests += other.total_requests
        self.total_reads += other.total_reads
        self.total_writes += other.total_writes
        self.comparable_reads += other.comparable_reads
        self.stale_reads += other.stale_reads
        self.stale_events.extend(other.stale_events)
        self.failed_reads += other.failed_reads
        self.failed_writes += other.failed_writes
        self.failure_counts.update(other.failure_counts)
        self.failure_events.extend(other.failure_events)


# --- Failure/staleness recording ---------------------------------------------


def _classify_exception(exc: httpx.HTTPError) -> str:
    """Map an httpx exception to one of the failure-type buckets used
    in failure_counts/failure_events. httpx.TimeoutException covers
    connect/read/write/pool timeouts alike; httpx.ConnectError covers
    "node unreachable/refused". Anything else (e.g. a mid-response
    ReadError or a RemoteProtocolError) still gets caught and counted
    -- as "other_error" -- rather than silently escaping
    classification.
    """
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.ConnectError):
        return "connection_error"
    return "other_error"


def _record_failure(
    stats: Stats,
    operation: str,
    failure_type: str,
    key: str,
    node_id: str,
    *,
    status_code: int | None = None,
    detail: str | None = None,
) -> None:
    if operation == "read":
        stats.failed_reads += 1
    else:
        stats.failed_writes += 1
    stats.failure_counts[(operation, failure_type)] += 1
    stats.failure_events.append(
        {
            "operation": operation,
            "failure_type": failure_type,
            "key": key,
            "node_id": node_id,
            "status_code": status_code,
            "detail": detail,
            "timestamp": time.time(),
        }
    )


def _record_stale(
    stats: Stats,
    key: str,
    node_id: str,
    expected: SourceEntry,
    actual_timestamp: float | None,
    *,
    node_id_field: str,
) -> None:
    """`node_id_field` is the one genuine difference between the two
    callers: staleness_load_test.py records "follower_node_id" (a read
    always lands on a specific follower), leaderless_staleness_load_test.py
    records "coordinator_node_id" (a read's coordinator does its own
    internal quorum fan-out, so `node_id` here names the coordinator, not
    a single follower). Everything else about a stale event is identical.
    """
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
            node_id_field: node_id,
            "expected_timestamp": expected.timestamp,
            "actual_timestamp": actual_repr,
            "staleness_gap_ms": gap_ms,
        }
    )


# --- Reporting ----------------------------------------------------------


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


def _write_jsonl(path: Path, events: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


def _print_failure_section(stats: Stats) -> None:
    """A deliberately loud, fixed-position block -- printed in the same
    place every run, bordered with '!' the moment any failure exists --
    so a nonzero failure count can't be missed by skimming past a wall
    of other numbers. Printed even when there are zero failures, so
    "no failures" is an explicit, positively-confirmed reading rather
    than an absence someone has to notice on their own.
    """
    total_failures = stats.total_failures
    border = "!" * 60 if total_failures else "=" * 60

    print()
    print(border)
    print(f"  FAILED REQUESTS: {total_failures}  (reads: {stats.failed_reads}, writes: {stats.failed_writes})")
    if total_failures:
        print(border)
        for (operation, failure_type), count in sorted(stats.failure_counts.items()):
            print(f"    {operation:<6} {failure_type:<17} {count}")
        status_codes = Counter(
            e["status_code"] for e in stats.failure_events if e["failure_type"] == "non_2xx"
        )
        if status_codes:
            print(f"    non-2xx status codes seen: {dict(sorted(status_codes.items()))}")
    print(border)


def _print_summary(
    stats: Stats,
    elapsed: float,
    output_log_path: Path,
    failure_log_path: Path,
    *,
    title: str,
) -> None:
    """`title` is the one genuine difference between the two callers --
    the printed banner text ("staleness load test summary" vs.
    "leaderless staleness load test summary"). Everything else about the
    summary is identical between strategies.
    """
    print()
    print(f"=== {title} ===")
    print(f"elapsed:              {elapsed:.2f}s")
    print(f"total requests sent:  {stats.total_requests}")
    print(f"total reads:          {stats.total_reads}")
    print(f"total writes:         {stats.total_writes}")

    _print_failure_section(stats)

    print()
    print(
        f"comparable reads:     {stats.comparable_reads}  "
        "(reads with a source-of-truth entry to check against; failed reads excluded)"
    )
    print(f"stale reads observed: {stats.stale_reads}")

    # Staleness rate is reported against comparable reads, not all
    # reads -- reads that had no source-of-truth entry yet (nothing
    # written for that key so far), or that failed outright, were
    # skipped rather than counted as non-stale, so including them would
    # understate the rate.
    print(f"staleness rate:       {stats.staleness_rate:.2f}%  (stale / comparable reads)")

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
    print(f"raw stale-read log written to {output_log_path}")
    print(f"raw failure log written to {failure_log_path}")


# --- Result -------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LoadTestResult:
    """Structured result of one run_load_test() call, for callers (e.g.
    experiments/run_comparison.py) that need the numbers to record and
    rank rather than this script's printed summary.
    """

    stats: Stats
    elapsed: float
