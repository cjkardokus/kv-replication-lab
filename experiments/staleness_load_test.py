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
  echoes back the timestamp it stamped on the write (see
  common/server.py::PutResponse), so a successful write folds its own
  (value, timestamp) directly into a shared "source of truth" dict (key
  -> newest known (value, timestamp), by max timestamp) -- no separate
  readback GET needed. That matters for correctness, not just
  efficiency: a readback GET reads whatever's *currently* on the leader
  for that key, which a different, unrelated concurrent write to the
  same key can have already overwritten, misattributing that other
  write's value to this one and corrupting ground truth with a value
  whose own replication status is unknown. Using each write's own
  response avoids that source of false staleness entirely.
  source_of_truth still takes the max by timestamp on every update, so
  concurrent writers completing out of order can't clobber a newer
  entry with an older one.
- Reads pick a random key and a random follower, snapshot the source
  of truth for that key *before* issuing the read, then compare the
  follower's response against that snapshot. A follower that's missing
  the key, or has an older timestamp than the snapshot, is stale; a
  follower with an equal or newer timestamp is not (it may simply
  reflect a write this script hasn't caught up to itself).
- "Random" above is seeded and reproducible by default (--seed,
  default experiments._load_test_common.DEFAULT_SEED): the full
  sequence of read/write-mix, key, and follower decisions for the
  whole run is precomputed up front, before any request is issued, so
  a fixed seed produces the exact same sequence of decisions every
  time regardless of how the concurrent workers below happen to
  interleave -- see generate_request_plan(). Real timing/scheduling
  stays unseeded; only *which decision* each request makes is
  reproducible, not *when* it runs, so staleness numbers can still
  vary run to run under the same seed (see docs/results.md's
  methodology note for measured variance).
- Every request is also classified as succeeded or failed, independent
  of staleness: a non-2xx response (status code recorded), a timeout,
  or a connection error all count as a failure rather than a stale
  read/write. A failed write never updates the source-of-truth dict
  (nothing durably happened to record); a failed read is never counted
  as a stale-read comparison (it never got a value back to compare).
  Failure counts are broken out by type and by read/write in the
  summary, and logged to a separate JSONL file from the stale-read log
  (see OUTPUT_LOG_PATH / FAILURE_LOG_PATH) -- staleness and failure are
  different questions (did a read see old data vs. did a request work
  at all), so keeping them in separate files means a consumer of
  either one doesn't have to filter out the other's fields.

Most of the machinery above (Stats, GlobalCounter, SourceOfTruth,
failure classification/recording, staleness recording, and the printed/
JSONL reporting) lives in experiments/_load_test_common.py, shared with
experiments/leaderless_staleness_load_test.py -- see that module's
docstring. What's here is what's actually specific to the leader-follower
strategy: a fixed leader URL (no coordinator selection needed), and the
_do_write/_do_read HTTP calls themselves.

This script only drives HTTP traffic -- it assumes the leader
(localhost:8000) and the followers listed in
config/leader_follower_cluster.yaml are already running as separate
processes.

Run:
    python3 -m experiments.staleness_load_test
"""

from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path

import httpx

from experiments._load_test_common import (
    DEFAULT_SEED,
    GlobalCounter,
    LoadTestResult,
    RequestPlan,
    SourceOfTruth,
    Stats,
    _classify_exception,
    _print_summary,
    _record_failure,
    _record_stale,
    _write_jsonl,
    generate_request_plan,
    split_plan,
)
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
FAILURE_LOG_PATH = OUTPUT_DIR / "staleness_failures_log.jsonl"

KEYS = [f"key_{i}" for i in range(NUM_KEYS)]


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


async def discover_leader_node_id(client: httpx.AsyncClient) -> str:
    """Look up the leader's node_id via /health once, up front, so
    write-failure log entries can name it rather than a bare "leader".
    """
    resp = await client.get(f"{LEADER_URL}/health", timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return str(resp.json()["node_id"])


async def _do_write(
    client: httpx.AsyncClient,
    counter: GlobalCounter,
    source_of_truth: SourceOfTruth,
    leader_node_id: str,
    stats: Stats,
    key: str,
) -> None:
    value = await counter.next_value()

    try:
        resp = await client.put(
            f"{LEADER_URL}/kv/{key}", json={"value": value}, timeout=REQUEST_TIMEOUT
        )
    except httpx.HTTPError as exc:
        _record_failure(
            stats, "write", _classify_exception(exc), key, leader_node_id, detail=str(exc)
        )
        return

    if resp.status_code != 200:
        _record_failure(
            stats, "write", "non_2xx", key, leader_node_id, status_code=resp.status_code
        )
        return

    body = resp.json()
    if not body.get("applied", False):
        # HTTP succeeded -- this just lost an LWW race to a newer write
        # already on record. Not a failure, nothing to durably record
        # from this write specifically.
        return

    # PutResponse echoes back the timestamp this write was stamped with
    # (see common/server.py::PutResponse) -- no separate readback GET
    # needed to learn it. That matters beyond just saving a round trip:
    # a readback GET reads whatever's *currently* on the leader, which a
    # different, unrelated concurrent write to the same key can have
    # already overwritten -- misattributing that other write's value to
    # this one. Using this write's own response avoids that entirely.
    await source_of_truth.record(key, value, body["timestamp"])


async def _do_read(
    client: httpx.AsyncClient,
    source_of_truth: SourceOfTruth,
    follower: Follower,
    follower_node_ids: dict[Follower, str],
    stats: Stats,
    key: str,
) -> None:
    expected_before = await source_of_truth.snapshot(key)
    if expected_before is None:
        # No write has completed for this key yet -- nothing to compare
        # against, so skip rather than counting it stale, not-stale, or
        # a failure. No request is even issued.
        return

    node_id = follower_node_ids.get(follower, f"{follower.host}:{follower.port}")

    try:
        resp = await client.get(
            f"http://{follower.host}:{follower.port}/kv/{key}", timeout=REQUEST_TIMEOUT
        )
    except httpx.HTTPError as exc:
        _record_failure(stats, "read", _classify_exception(exc), key, node_id, detail=str(exc))
        return

    if resp.status_code not in (200, 404):
        _record_failure(stats, "read", "non_2xx", key, node_id, status_code=resp.status_code)
        return

    # Only now -- once we know the request actually got a valid
    # response -- does this read count toward the staleness comparison.
    # A failed request never got a value back, so there's nothing to
    # compare and it must not be counted here.
    stats.comparable_reads += 1

    if resp.status_code == 404:
        _record_stale(
            stats, key, node_id, expected_before, actual_timestamp=None,
            node_id_field="follower_node_id",
        )
        return

    body = resp.json()
    actual_timestamp = body["timestamp"]
    if actual_timestamp < expected_before.timestamp:
        _record_stale(
            stats, key, node_id, expected_before, actual_timestamp,
            node_id_field="follower_node_id",
        )
    # else: at or newer than expected -- not stale, even if it reflects
    # a write this script hasn't caught up to itself.


async def run_worker(
    plan_chunk: list[RequestPlan],
    client: httpx.AsyncClient,
    counter: GlobalCounter,
    source_of_truth: SourceOfTruth,
    followers: list[Follower],
    follower_node_ids: dict[Follower, str],
    leader_node_id: str,
) -> Stats:
    stats = Stats()
    for item in plan_chunk:
        stats.total_requests += 1
        if item.is_read:
            stats.total_reads += 1
            # read_target_index is only ever None for a write (see
            # RequestPlan) -- this branch is a read, so it's always set;
            # the assert is a narrowing for the type checker as much as
            # a runtime check.
            assert item.read_target_index is not None
            follower = followers[item.read_target_index]
            await _do_read(client, source_of_truth, follower, follower_node_ids, stats, item.key)
        else:
            stats.total_writes += 1
            await _do_write(client, counter, source_of_truth, leader_node_id, stats, item.key)
    return stats


# --- Entrypoint -------------------------------------------------------------


async def run_load_test(
    *,
    seed: int = DEFAULT_SEED,
    verbose: bool = True,
    write_logs: bool = True,
    output_log_path: Path = OUTPUT_LOG_PATH,
    failure_log_path: Path = FAILURE_LOG_PATH,
) -> LoadTestResult:
    """Run the full staleness load test against an already-running
    leader-follower cluster (see module docstring) and return the
    results.

    `seed` determines this run's precomputed per-request decisions (key
    selection, read/write mix, which follower each read targets) -- see
    experiments._load_test_common.generate_request_plan. Defaults to
    DEFAULT_SEED, so every standard run is reproducible unless a caller
    deliberately overrides it. Real timing/scheduling stays unseeded --
    only *which decision* a request makes is made reproducible, not
    *when* it runs, so this alone doesn't make staleness numbers
    identical run to run under the same seed.

    `verbose` controls whether progress/summary text is printed --
    a caller driving several runs back-to-back (run_comparison.py)
    wants its own concise progress line instead of this script's full
    per-run summary. `write_logs` controls whether the stale-read/
    failure JSONL logs are written at all; `output_log_path`/
    `failure_log_path` control *where*, defaulting to
    OUTPUT_LOG_PATH/FAILURE_LOG_PATH -- a caller running many configs in
    sequence should override these per config, or each run's logs would
    silently overwrite the last one's.
    """
    config = ClusterConfig.from_yaml(CONFIG_PATH)
    followers = config.followers
    if not followers:
        raise SystemExit(f"no followers configured in {CONFIG_PATH}")

    # Precomputed before the timed portion starts (see
    # generate_request_plan's docstring for why that's required for
    # `seed` to actually be reproducible, not just a nicety) -- a plain
    # synchronous loop, no I/O, no concurrency, so it costs real time
    # but none of it counts toward `elapsed` below.
    plan = generate_request_plan(
        TOTAL_REQUESTS, KEYS, len(followers), read_fraction=READ_FRACTION, seed=seed
    )
    plan_chunks = split_plan(plan, NUM_WORKERS)

    limits = httpx.Limits(
        max_connections=NUM_WORKERS * 2, max_keepalive_connections=NUM_WORKERS * 2
    )
    async with httpx.AsyncClient(limits=limits) as client:
        if verbose:
            print(f"discovering node ids for {len(followers)} followers...")
        follower_node_ids = await discover_node_ids(client, followers)
        leader_node_id = await discover_leader_node_id(client)

        counter = GlobalCounter()
        source_of_truth = SourceOfTruth()

        if verbose:
            print(
                f"starting {NUM_WORKERS} workers, {TOTAL_REQUESTS} total requests "
                f"({READ_FRACTION:.0%} reads / {1 - READ_FRACTION:.0%} writes) "
                f"against leader {LEADER_URL} and {len(followers)} followers "
                f"(seed={seed})..."
            )
        start = time.time()
        results = await asyncio.gather(
            *(
                run_worker(
                    chunk,
                    client,
                    counter,
                    source_of_truth,
                    followers,
                    follower_node_ids,
                    leader_node_id,
                )
                for chunk in plan_chunks
            )
        )
        elapsed = time.time() - start

    stats = Stats()
    for result in results:
        stats.merge(result)

    if write_logs:
        _write_jsonl(output_log_path, stats.stale_events)
        _write_jsonl(failure_log_path, stats.failure_events)
    if verbose:
        _print_summary(
            stats, elapsed, output_log_path, failure_log_path,
            title="staleness load test summary",
        )

    return LoadTestResult(stats=stats, elapsed=elapsed)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the leader-follower staleness load test."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=(
            "Seed for this run's precomputed per-request decisions (key "
            "selection, read/write mix, which follower each read "
            "targets) -- real timing/scheduling stays unseeded "
            f"regardless (default: {DEFAULT_SEED})."
        ),
    )
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    await run_load_test(seed=args.seed)


if __name__ == "__main__":
    asyncio.run(main())
