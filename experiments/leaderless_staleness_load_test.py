"""Concurrent load test measuring replication staleness under load.

Leaderless counterpart to experiments/staleness_load_test.py -- same
overall design, adapted for the quorum-based cluster: there's no
dedicated leader/follower split, so instead of writes always going to
one fixed leader and reads fanning out across followers, every request
(read or write) picks its coordinator node by round robin across all 5
nodes and talks to it via the node's own public /kv/{key}, which
already does quorum coordination internally (see leaderless/node.py).

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
- Write coordinator selection is round robin: a shared, lock-protected
  index cycles through the 5 nodes from config/leaderless_cluster.yaml,
  so each outgoing write targets the next node in turn as coordinator.
  Read coordinator selection is a fresh, independent `random.choice`
  over all 5 nodes per read, matching how staleness_load_test.py picks
  a follower to read from -- round robin for reads would couple which
  node answers a given read to the same fixed cycle every write also
  goes through, a test-harness artifact with no equivalent on the
  leader-follower side, worth removing so any remaining difference
  between the two benchmarks' results reflects the systems, not the
  harnesses measuring them.
- Writes PUT to the chosen coordinator, using the cluster's default_w
  (no `w` override -- this run tests at default_w=3). The coordinator's
  PutResponse echoes back the timestamp it stamped (see
  common/server.py::PutResponse), so a successful write folds its own
  (value, timestamp) directly into a shared "source of truth" dict (key
  -> newest known (value, timestamp), by max timestamp) -- no separate
  readback GET needed. That matters for correctness, not just
  efficiency: a readback GET reads whatever's *currently* on that
  coordinator for that key, which a different, unrelated concurrent
  write to the same key can have already overwritten. Using each
  write's own response avoids that source of false staleness entirely.
  source_of_truth still takes the max by timestamp on every update, so
  concurrent writers completing out of order can't clobber a newer
  entry with an older one.
- Reads pick a random key and an independently random coordinator (see
  above), snapshot the source of truth for that key *before* issuing
  the read, then GET from that coordinator (using the cluster's
  default_r, no override -- default_r=3) and compare its response
  against that snapshot. A coordinator whose quorum read is missing the
  key, or returns an older timestamp than the snapshot, is stale; equal
  or newer is not (it may simply reflect a write this script hasn't
  caught up to itself).
- "Random" above is seeded and reproducible by default (--seed,
  default experiments._load_test_common.DEFAULT_SEED): the full
  sequence of read/write-mix, key, and read-coordinator decisions for
  the whole run is precomputed up front, before any request is issued,
  so a fixed seed produces the exact same sequence of decisions every
  time regardless of how the concurrent workers below happen to
  interleave -- see generate_request_plan(). Write-coordinator
  selection (NodeRotation, round robin) is unaffected -- it was already
  fully deterministic, not random. Real timing/scheduling stays
  unseeded; only *which decision* each request makes is reproducible,
  not *when* it runs, so staleness numbers can still vary run to run
  under the same seed (see docs/results.md's methodology note for
  measured variance).
- Every request is also classified as succeeded or failed, independent
  of staleness: a non-2xx response (status code recorded, including a
  503 when a coordinator can't reach quorum in time), a timeout, or a
  connection error all count as a failure rather than a stale
  read/write. A failed write never updates the source-of-truth dict
  (nothing durably happened to record); a failed read is never counted
  as a stale-read comparison (it never got a value back to compare).
  Failure counts are broken out by type and by read/write in the
  summary, and logged to a separate JSONL file from the stale-read log
  (see OUTPUT_LOG_PATH / FAILURE_LOG_PATH) -- staleness and failure are
  different questions (did a read see old data vs. did a request work
  at all), so keeping them in separate files means a consumer of
  either one doesn't have to filter out the other's fields.

With default_w=3 and default_r=3 against a 5-node cluster, W+R > N, so
every read quorum is guaranteed to overlap every write quorum by at
least one node -- staleness observed here (if any) reflects timing
within that guarantee, not a violation of it.

Most of the machinery above (Stats, GlobalCounter, SourceOfTruth,
failure classification/recording, staleness recording, and the printed/
JSONL reporting) lives in experiments/_load_test_common.py, shared with
experiments/staleness_load_test.py -- see that module's docstring.
What's here is what's actually specific to the leaderless strategy:
NodeRotation (round-robin write coordinator selection), and the
_do_write/_do_read HTTP calls themselves.

This script only drives HTTP traffic -- it assumes all 5 nodes listed
in config/leaderless_cluster.yaml are already running as separate
processes.

Run:
    python3 -m experiments.leaderless_staleness_load_test
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
from leaderless.node import ClusterConfig, Node

CONFIG_PATH = "config/leaderless_cluster.yaml"

NUM_KEYS = 100
NUM_WORKERS = 50
TOTAL_REQUESTS = 10_000
READ_FRACTION = 0.7  # ~70% reads, ~30% writes
REQUEST_TIMEOUT = 5.0

# Known limitation at these settings, for *every* W value, not just
# W=1: QuorumCoordinator.replicate_write fans a write out to *every*
# node in the background unconditionally, regardless of W -- only the
# ack-*wait* is bounded by W, not who gets contacted (see
# leaderless/node.py's module docstring) -- so any write is durable
# everywhere within a few milliseconds on localhost, independent of
# nominal W. At NUM_KEYS=100 and this run's throughput (~300-400 req/s,
# i.e. ~3-4 req/s per key), the average gap between two requests
# touching the *same* key is ~250-330ms -- 50-100x longer than that
# replication window -- so a read essentially never lands on a key
# before its last write has already propagated everywhere, whether or
# not that config's W/R combination is actually guaranteed to prevent
# it. Confirmed via experiments/run_comparison.py: every config,
# including W=2,R=3 (genuinely not guaranteed: 2+3=5, not >5) showed
# 0.00% staleness in a real run, even with QuorumCoordinator now
# contacting exactly the peers each config's literal W/R implies (no
# margin padding coverage -- see leaderless/node.py's removed
# _FANOUT_MARGIN in git history). This is a local-testing artifact, not
# a guarantee of the protocol -- the same category of blind spot the
# top-level README already documents for clock skew ("invisible in
# local testing... becomes real once nodes run on separate EC2
# instances"). Reproducing it locally would need either injecting
# artificial per-replica latency (test-only fakery in production
# replication code) or shrinking NUM_KEYS enough to manufacture heavy
# hot-key contention, neither of which is free -- left undone for now;
# expected to become observable once this project reaches the planned
# multi-machine phase. See docs/results.md for the full writeup.

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_LOG_PATH = OUTPUT_DIR / "leaderless_staleness_log.jsonl"
FAILURE_LOG_PATH = OUTPUT_DIR / "leaderless_staleness_failures_log.jsonl"

KEYS = [f"key_{i}" for i in range(NUM_KEYS)]


# --- Strategy-specific coordinator selection ---------------------------


class NodeRotation:
    """Round-robin coordinator selection across all cluster nodes,
    shared across all workers and guarded by an asyncio.Lock so
    concurrent requests still cycle through the node list in strict
    order rather than racing on a shared index.
    """

    def __init__(self, nodes: list[Node]) -> None:
        self._nodes = nodes
        self._index = 0
        self._lock = asyncio.Lock()

    async def next_node(self) -> Node:
        async with self._lock:
            node = self._nodes[self._index % len(self._nodes)]
            self._index += 1
            return node


# --- Worker logic -------------------------------------------------------


async def discover_node_ids(
    client: httpx.AsyncClient, nodes: list[Node]
) -> dict[Node, str]:
    """Look up each node's node_id via /health once, up front, so
    stale-read log entries can name the actual coordinator node rather
    than just a host:port pair.
    """
    node_ids: dict[Node, str] = {}
    for node in nodes:
        resp = await client.get(
            f"http://{node.host}:{node.port}/health", timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        node_ids[node] = resp.json()["node_id"]
    return node_ids


async def _do_write(
    client: httpx.AsyncClient,
    counter: GlobalCounter,
    source_of_truth: SourceOfTruth,
    rotation: NodeRotation,
    node_ids: dict[Node, str],
    stats: Stats,
    key: str,
    w: int | None = None,
) -> None:
    value = await counter.next_value()
    coordinator = await rotation.next_node()
    base_url = f"http://{coordinator.host}:{coordinator.port}"
    node_id = node_ids.get(coordinator, f"{coordinator.host}:{coordinator.port}")

    # `w` overrides the coordinator's default_w for this write only, via
    # the query param leaderless/node.py's PUT already accepts -- None
    # leaves the cluster's configured default in place.
    write_params = {"w": w} if w is not None else None

    try:
        resp = await client.put(
            f"{base_url}/kv/{key}",
            json={"value": value},
            params=write_params,
            timeout=REQUEST_TIMEOUT,
        )
    except httpx.HTTPError as exc:
        _record_failure(stats, "write", _classify_exception(exc), key, node_id, detail=str(exc))
        return

    if resp.status_code != 200:
        # Includes a 503 when the coordinator couldn't reach its write
        # quorum in time -- a real failure, distinct from an LWW no-op
        # below (which is still an HTTP 200).
        _record_failure(stats, "write", "non_2xx", key, node_id, status_code=resp.status_code)
        return

    body = resp.json()
    if not body.get("applied", False):
        # HTTP succeeded -- this just lost an LWW race to a newer write
        # already on record. Not a failure, nothing to durably record
        # from this write specifically.
        return

    # PutResponse echoes back the timestamp this write was stamped with
    # (see common/server.py::PutResponse) -- no separate readback GET
    # needed. That matters beyond saving a round trip: a readback GET
    # reads whatever's *currently* on the coordinator for that key,
    # which a different, unrelated concurrent write to the same key can
    # have already overwritten -- misattributing that other write's
    # value to this one. Using this write's own response avoids that
    # entirely.
    await source_of_truth.record(key, value, body["timestamp"])


async def _do_read(
    client: httpx.AsyncClient,
    source_of_truth: SourceOfTruth,
    coordinator: Node,
    node_ids: dict[Node, str],
    stats: Stats,
    key: str,
    r: int | None = None,
) -> None:
    expected_before = await source_of_truth.snapshot(key)
    if expected_before is None:
        # No write has completed for this key yet -- nothing to compare
        # against, so skip rather than counting it stale, not-stale, or
        # a failure. No request is even issued.
        return

    node_id = node_ids.get(coordinator, f"{coordinator.host}:{coordinator.port}")

    # `r` overrides the coordinator's default_r for this read only --
    # None leaves the cluster's configured default in place.
    read_params = {"r": r} if r is not None else None

    try:
        resp = await client.get(
            f"http://{coordinator.host}:{coordinator.port}/kv/{key}",
            params=read_params,
            timeout=REQUEST_TIMEOUT,
        )
    except httpx.HTTPError as exc:
        _record_failure(stats, "read", _classify_exception(exc), key, node_id, detail=str(exc))
        return

    if resp.status_code not in (200, 404):
        # Includes a 503 when the coordinator couldn't reach its read
        # quorum in time -- the body won't have `timestamp`, and it's
        # not a definitive "missing" like a 404 either, so there's
        # nothing safe to compare. A real failure, not a stale read.
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
            node_id_field="coordinator_node_id",
        )
        return

    body = resp.json()
    actual_timestamp = body["timestamp"]
    if actual_timestamp < expected_before.timestamp:
        _record_stale(
            stats, key, node_id, expected_before, actual_timestamp,
            node_id_field="coordinator_node_id",
        )
    # else: at or newer than expected -- not stale, even if it reflects
    # a write this script hasn't caught up to itself.


async def run_worker(
    plan_chunk: list[RequestPlan],
    client: httpx.AsyncClient,
    counter: GlobalCounter,
    source_of_truth: SourceOfTruth,
    rotation: NodeRotation,
    nodes: list[Node],
    node_ids: dict[Node, str],
    w: int | None = None,
    r: int | None = None,
) -> Stats:
    stats = Stats()
    for item in plan_chunk:
        stats.total_requests += 1
        if item.is_read:
            stats.total_reads += 1
            coordinator = nodes[item.read_target_index]
            await _do_read(client, source_of_truth, coordinator, node_ids, stats, item.key, r)
        else:
            stats.total_writes += 1
            await _do_write(client, counter, source_of_truth, rotation, node_ids, stats, item.key, w)
    return stats


# --- Entrypoint -------------------------------------------------------------


async def run_load_test(
    *,
    w: int | None = None,
    r: int | None = None,
    seed: int = DEFAULT_SEED,
    verbose: bool = True,
    write_logs: bool = True,
    output_log_path: Path = OUTPUT_LOG_PATH,
    failure_log_path: Path = FAILURE_LOG_PATH,
) -> LoadTestResult:
    """Run the full staleness load test against an already-running
    leaderless cluster (see module docstring) and return the results.

    `w`/`r` override the cluster's configured default_w/default_r for
    every request this run issues, via the same query params
    leaderless/node.py's PUT/GET already accept -- left as None to use
    whatever the cluster is configured with. `seed` determines this
    run's precomputed per-request decisions (key selection, read/write
    mix, which node each read's coordinator is) -- see
    experiments._load_test_common.generate_request_plan. Defaults to
    DEFAULT_SEED, so every standard run is reproducible unless a caller
    deliberately overrides it. Real timing/scheduling stays unseeded --
    only *which decision* a request makes is made reproducible, not
    *when* it runs, so this alone doesn't make staleness numbers
    identical run to run under the same seed. `verbose` controls whether
    progress/summary text is printed -- a caller driving several runs
    back-to-back (run_comparison.py) wants its own concise progress line
    instead of this script's full per-run summary. `write_logs` controls
    whether the stale-read/failure JSONL logs are written at all;
    `output_log_path`/`failure_log_path` control *where*, defaulting to
    OUTPUT_LOG_PATH/FAILURE_LOG_PATH -- a caller running many configs in
    sequence should override these per config, or each run's logs would
    silently overwrite the last one's.
    """
    config = ClusterConfig.from_yaml(CONFIG_PATH)
    nodes = config.nodes
    if not nodes:
        raise SystemExit(f"no nodes configured in {CONFIG_PATH}")

    # Precomputed before the timed portion starts (see
    # generate_request_plan's docstring for why that's required for
    # `seed` to actually be reproducible, not just a nicety) -- a plain
    # synchronous loop, no I/O, no concurrency, so it costs real time
    # but none of it counts toward `elapsed` below.
    plan = generate_request_plan(
        TOTAL_REQUESTS, KEYS, len(nodes), read_fraction=READ_FRACTION, seed=seed
    )
    plan_chunks = split_plan(plan, NUM_WORKERS)

    limits = httpx.Limits(
        max_connections=NUM_WORKERS * 2, max_keepalive_connections=NUM_WORKERS * 2
    )
    async with httpx.AsyncClient(limits=limits) as client:
        if verbose:
            print(f"discovering node ids for {len(nodes)} nodes...")
        node_ids = await discover_node_ids(client, nodes)

        counter = GlobalCounter()
        source_of_truth = SourceOfTruth()
        rotation = NodeRotation(nodes)

        if verbose:
            print(
                f"starting {NUM_WORKERS} workers, {TOTAL_REQUESTS} total requests "
                f"({READ_FRACTION:.0%} reads / {1 - READ_FRACTION:.0%} writes), "
                f"round-robin coordinator across {len(nodes)} nodes "
                f"(w={w if w is not None else config.default_w}, "
                f"r={r if r is not None else config.default_r}, seed={seed})..."
            )
        start = time.time()
        results = await asyncio.gather(
            *(
                run_worker(chunk, client, counter, source_of_truth, rotation, nodes, node_ids, w, r)
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
            title="leaderless staleness load test summary",
        )

    return LoadTestResult(stats=stats, elapsed=elapsed)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the leaderless staleness load test."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=(
            "Seed for this run's precomputed per-request decisions (key "
            "selection, read/write mix, which node each read's "
            "coordinator is) -- real timing/scheduling stays unseeded "
            f"regardless (default: {DEFAULT_SEED})."
        ),
    )
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    await run_load_test(seed=args.seed)


if __name__ == "__main__":
    asyncio.run(main())
