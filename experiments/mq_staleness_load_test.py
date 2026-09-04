"""Concurrent load test measuring replication staleness under load, for
the message-queue (Kafka) replication strategy.

Message-queue counterpart to experiments/staleness_load_test.py and
experiments/leaderless_staleness_load_test.py -- same overall design
(seeded request plan, a shared source-of-truth dict built from each
write's own echoed timestamp, staleness measured as "did a read see an
older timestamp than the last write this script itself confirmed"), but
adapted for this strategy's write path: writes always go to the one
producer (message_queue/producer.py), which acks as soon as Kafka
confirms the write is durably logged -- there is no ack-wait knob to
sweep here the way ack_required or W/R are for the other two
strategies. Reads pick a random follower (out of the 4 running
message_queue/follower.py processes) and read its local KVStore
directly, same as the other two scripts read a follower/coordinator's
local view.

Because there's no ack-wait knob, this strategy's whole staleness story
is about load *intensity* itself: every follower independently consumes
the full topic in the background (see message_queue/follower.py), so
staleness here is driven by how far a follower's consumer has fallen
behind under load, not by how many replicas a write waited for. This
script's own --workers (module default: NUM_WORKERS) is what
experiments/run_comparison.py's run_mq_configs sweeps across instead --
see that function for the actual sweep values and why.

Consumer lag sampling
----------------------
When a read IS detected stale, this script additionally queries that
follower's GET /internal/lag once, at that moment, and records the
result (`lag_at_stale_read`, the follower's total lag across all
partitions -- see message_queue/follower.py's LagResponse) alongside
the stale-read event in the JSONL log. This is the ONLY time
/internal/lag is ever queried during a run: an unconditional per-read
lag query would add a round trip to *every* read (not just the small
fraction that are stale), changing this script's own timing/throughput
characteristics relative to the other two strategies' load tests, which
query nothing extra per read -- comparing elapsed times or throughput
across strategies would no longer be apples to apples. Sampling lag only
on stale reads keeps this script's per-read cost identical to theirs.
The trade-off: a stale read's own lag sample is measured moments after
the read itself completed (a second, separate HTTP call), not atomically
with it -- close enough for correlating "was this read stale, and
roughly how far behind was the follower at the time" at this lab's
scale, not a guarantee that lag hasn't shifted in between.

Approach (same as the other two scripts except where noted above)
--------------------------------------------------------------------
- A fixed pool of 100 keys (key_0..key_99) is reused across the whole
  run, so the same keys get hit repeatedly and create real read/write
  contention.
- Every write's value comes from a single global counter shared across
  all writer workers, so every value written during the run is unique
  and traceable to exactly one write.
- Writes PUT to the producer (message_queue/producer.py). Its
  PutResponse echoes back the timestamp it stamped (see
  common/server.py::PutResponse, reused as-is by the producer), so a
  successful write folds its own (value, timestamp) directly into a
  shared "source of truth" dict -- no separate readback GET needed, for
  the same reason as the other two scripts (a readback GET could race a
  different concurrent write to the same key). `applied` is always
  True for a successful publish here -- the producer holds no local
  storage to reject a write as stale against (see
  message_queue/producer.py's own docstring) -- the check below is kept
  only for shape-parity with the other two scripts' write path; it
  should never actually trigger for this strategy.
- Reads pick a random key and a random follower (out of the 4), snapshot
  the source of truth for that key *before* issuing the read, then
  compare the follower's response against that snapshot -- identical
  logic to staleness_load_test.py's follower reads.
- "Random" above is seeded and reproducible by default (--seed, default
  experiments._load_test_common.DEFAULT_SEED) -- see
  generate_request_plan() and the other two scripts' own docstrings for
  why this makes *which decision* each request makes reproducible, not
  *when* it runs; staleness numbers can still vary run to run under the
  same seed.
- Every request is classified as succeeded or failed, independent of
  staleness, exactly like the other two scripts -- see
  experiments._load_test_common's shared failure recording.

Most of the machinery (Stats, GlobalCounter, SourceOfTruth, failure
classification/recording, staleness recording, and the printed/JSONL
reporting) lives in experiments/_load_test_common.py, shared with the
other two scripts. What's here is what's specific to this strategy: a
fixed producer URL and a fixed follower port list (message_queue/'s
MQConfig deliberately carries no node addresses at all -- see
message_queue/config.py -- so, unlike the other two scripts, there's no
cluster YAML to read these from; PRODUCER_HOST/PORT and
FOLLOWER_HOST/PORTS below are this strategy's equivalent, and
experiments/run_comparison.py's run_mq_configs imports them directly
from here rather than redefining its own copy, specifically because
there's no YAML acting as an independent source of truth the way there
is for the other two strategies -- two independently-hardcoded port
lists with nothing to keep them in sync would be a real drift risk),
the lag-on-stale-read sampling described above, and the _do_write/
_do_read HTTP calls themselves.

This script only drives HTTP traffic -- it assumes the producer and 4
followers listed below are already running as separate processes (and
the Kafka broker itself, per docker-compose.kafka.yml, is up).

Run:
    python3 -m experiments.mq_staleness_load_test
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

PRODUCER_HOST = "127.0.0.1"
PRODUCER_PORT = 8200
PRODUCER_URL = f"http://{PRODUCER_HOST}:{PRODUCER_PORT}"

FOLLOWER_HOST = "127.0.0.1"
FOLLOWER_PORTS = [8201, 8202, 8203, 8204]

NUM_KEYS = 100
NUM_WORKERS = 50
TOTAL_REQUESTS = 10_000
READ_FRACTION = 0.7  # ~70% reads, ~30% writes
REQUEST_TIMEOUT = 5.0

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_LOG_PATH = OUTPUT_DIR / "mq_staleness_log.jsonl"
FAILURE_LOG_PATH = OUTPUT_DIR / "mq_staleness_failures_log.jsonl"

KEYS = [f"key_{i}" for i in range(NUM_KEYS)]


# --- Worker logic -------------------------------------------------------


async def discover_follower_node_ids(client: httpx.AsyncClient, ports: list[int]) -> dict[int, str]:
    """Look up each follower's node_id via /health once, up front, so
    stale-read log entries can name the actual node rather than just a
    port number. Keyed by port (not a dedicated address class, unlike
    the other two scripts' Follower/Node) since every node in this
    script runs on FOLLOWER_HOST and a port already uniquely identifies
    one.
    """
    node_ids: dict[int, str] = {}
    for port in ports:
        resp = await client.get(f"http://{FOLLOWER_HOST}:{port}/health", timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        node_ids[port] = resp.json()["node_id"]
    return node_ids


async def discover_producer_node_id(client: httpx.AsyncClient) -> str:
    resp = await client.get(f"{PRODUCER_URL}/health", timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return str(resp.json()["node_id"])


async def _query_lag(client: httpx.AsyncClient, port: int) -> int | None:
    """Best-effort GET /internal/lag on one follower -- see this
    module's own docstring for why this is the only time this endpoint
    is ever queried during a run. Returns None if the query itself
    fails: this is a diagnostic sample attached to a stale-read event,
    not something whose own failure should count against the run's
    failure stats.
    """
    try:
        resp = await client.get(f"http://{FOLLOWER_HOST}:{port}/internal/lag", timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except httpx.HTTPError:
        return None
    return int(resp.json()["total_lag"])


async def _do_write(
    client: httpx.AsyncClient,
    counter: GlobalCounter,
    source_of_truth: SourceOfTruth,
    producer_node_id: str,
    stats: Stats,
    key: str,
) -> None:
    value = await counter.next_value()

    try:
        resp = await client.put(f"{PRODUCER_URL}/kv/{key}", json={"value": value}, timeout=REQUEST_TIMEOUT)
    except httpx.HTTPError as exc:
        _record_failure(stats, "write", _classify_exception(exc), key, producer_node_id, detail=str(exc))
        return

    if resp.status_code != 200:
        _record_failure(stats, "write", "non_2xx", key, producer_node_id, status_code=resp.status_code)
        return

    body = resp.json()
    if not body.get("applied", False):
        # See this module's docstring -- always True for a successful
        # publish here; kept for shape-parity with the other two
        # scripts, should never actually trigger.
        return

    # PutResponse echoes back the timestamp this write was stamped with
    # -- no separate readback GET needed (same reasoning as the other
    # two scripts: a readback GET could race a different concurrent
    # write to the same key).
    await source_of_truth.record(key, value, body["timestamp"])


async def _do_read(
    client: httpx.AsyncClient,
    source_of_truth: SourceOfTruth,
    port: int,
    follower_node_ids: dict[int, str],
    stats: Stats,
    key: str,
) -> None:
    expected_before = await source_of_truth.snapshot(key)
    if expected_before is None:
        # No write has completed for this key yet -- nothing to compare
        # against, so skip rather than counting it stale, not-stale, or
        # a failure. No request is even issued.
        return

    node_id = follower_node_ids.get(port, f"{FOLLOWER_HOST}:{port}")

    try:
        resp = await client.get(f"http://{FOLLOWER_HOST}:{port}/kv/{key}", timeout=REQUEST_TIMEOUT)
    except httpx.HTTPError as exc:
        _record_failure(stats, "read", _classify_exception(exc), key, node_id, detail=str(exc))
        return

    if resp.status_code not in (200, 404):
        _record_failure(stats, "read", "non_2xx", key, node_id, status_code=resp.status_code)
        return

    # Only now -- once we know the request actually got a valid
    # response -- does this read count toward the staleness comparison.
    stats.comparable_reads += 1

    if resp.status_code == 404:
        _record_stale(
            stats, key, node_id, expected_before, actual_timestamp=None,
            node_id_field="follower_node_id",
        )
        # See this module's docstring: lag is sampled *only* here, on a
        # detected-stale read, never unconditionally.
        stats.stale_events[-1]["lag_at_stale_read"] = await _query_lag(client, port)
        return

    body = resp.json()
    actual_timestamp = body["timestamp"]
    if actual_timestamp < expected_before.timestamp:
        _record_stale(
            stats, key, node_id, expected_before, actual_timestamp,
            node_id_field="follower_node_id",
        )
        stats.stale_events[-1]["lag_at_stale_read"] = await _query_lag(client, port)
    # else: at or newer than expected -- not stale, even if it reflects
    # a write this script hasn't caught up to itself.


async def run_worker(
    plan_chunk: list[RequestPlan],
    client: httpx.AsyncClient,
    counter: GlobalCounter,
    source_of_truth: SourceOfTruth,
    follower_ports: list[int],
    follower_node_ids: dict[int, str],
    producer_node_id: str,
) -> Stats:
    stats = Stats()
    for item in plan_chunk:
        stats.total_requests += 1
        if item.is_read:
            stats.total_reads += 1
            # read_target_index is only ever None for a write (see
            # RequestPlan) -- this branch is a read, so it's always set.
            assert item.read_target_index is not None
            port = follower_ports[item.read_target_index]
            await _do_read(client, source_of_truth, port, follower_node_ids, stats, item.key)
        else:
            stats.total_writes += 1
            await _do_write(client, counter, source_of_truth, producer_node_id, stats, item.key)
    return stats


# --- Entrypoint -------------------------------------------------------------


async def run_load_test(
    *,
    num_workers: int = NUM_WORKERS,
    total_requests: int = TOTAL_REQUESTS,
    seed: int = DEFAULT_SEED,
    verbose: bool = True,
    write_logs: bool = True,
    output_log_path: Path = OUTPUT_LOG_PATH,
    failure_log_path: Path = FAILURE_LOG_PATH,
) -> LoadTestResult:
    """Run the full staleness load test against an already-running
    message-queue producer + 4 followers (see module docstring) and
    return the results.

    `num_workers` overrides NUM_WORKERS -- unlike the other two scripts,
    this strategy has no ack-wait knob to sweep (writes always ack
    immediately once Kafka confirms durability), so worker count itself
    is the variable experiments/run_comparison.py's run_mq_configs
    sweeps across. `total_requests` overrides TOTAL_REQUESTS similarly,
    though run_mq_configs keeps it fixed across its sweep (see that
    function for why). `seed` determines this run's precomputed
    per-request decisions the same way as the other two scripts (see
    experiments._load_test_common.generate_request_plan). `verbose`/
    `write_logs`/`output_log_path`/`failure_log_path` all mean exactly
    what they do in the other two scripts' run_load_test.
    """
    plan = generate_request_plan(
        total_requests, KEYS, len(FOLLOWER_PORTS), read_fraction=READ_FRACTION, seed=seed
    )
    plan_chunks = split_plan(plan, num_workers)

    limits = httpx.Limits(max_connections=num_workers * 2, max_keepalive_connections=num_workers * 2)
    async with httpx.AsyncClient(limits=limits) as client:
        if verbose:
            print(f"discovering node ids for {len(FOLLOWER_PORTS)} followers...")
        follower_node_ids = await discover_follower_node_ids(client, FOLLOWER_PORTS)
        producer_node_id = await discover_producer_node_id(client)

        counter = GlobalCounter()
        source_of_truth = SourceOfTruth()

        if verbose:
            print(
                f"starting {num_workers} workers, {total_requests} total requests "
                f"({READ_FRACTION:.0%} reads / {1 - READ_FRACTION:.0%} writes) "
                f"against producer {PRODUCER_URL} and {len(FOLLOWER_PORTS)} followers "
                f"(seed={seed})..."
            )
        start = time.time()
        results = await asyncio.gather(
            *(
                run_worker(
                    chunk, client, counter, source_of_truth,
                    FOLLOWER_PORTS, follower_node_ids, producer_node_id,
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
            title="message-queue staleness load test summary",
        )

    return LoadTestResult(stats=stats, elapsed=elapsed)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the message-queue staleness load test.")
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
    parser.add_argument(
        "--workers",
        type=int,
        default=NUM_WORKERS,
        help=(
            "Concurrent worker count -- this strategy's swept variable "
            f"in run_comparison.py's sweep (default: {NUM_WORKERS})."
        ),
    )
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    await run_load_test(seed=args.seed, num_workers=args.workers)


if __name__ == "__main__":
    asyncio.run(main())
