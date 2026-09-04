"""Fully automated cross-strategy replication comparison.

Runs experiments/staleness_load_test.py's,
experiments/leaderless_staleness_load_test.py's, and
experiments/mq_staleness_load_test.py's core load-test logic against
every meaningful configuration of all three replication strategies, back
to back, and produces a combined report -- the first two strategies
ranked together by observed staleness rate, the message-queue strategy
reported separately (see "Configurations tested" below for why).

This script owns the full lifecycle of every node process involved --
spawning them with subprocess.Popen, polling /health (and, for the
message-queue strategy, its followers' actual Kafka partition
assignment -- see _wait_for_mq_followers_assigned) until they're ready,
and terminating them (SIGTERM, falling back to SIGKILL) before moving on
-- so no manual pane setup (starting followers/leader/nodes/producer by
hand in separate terminals) is needed to run it.

Configurations tested
----------------------
Leader-follower: ack_required swept 0..4 against the existing 4-follower
cluster (config/leader_follower_cluster.yaml), restarting the leader
(and, for a clean slate, the followers) fresh for each value -- see
ACK_REQUIRED_SWEEP.

Leaderless: five W/R combinations against the existing 5-node cluster
(config/leaderless_cluster.yaml), sent as per-request overrides -- the
5-node cluster is restarted fresh for each config, exactly like the
leader-follower sweep above, so every config in both sweeps starts from
an empty KVStore and neither strategy's later configs can inherit an
earlier config's state (see docs/AUDIT_FINDINGS.md's §7: this used to
restart leader-follower per config but run all five leaderless configs
against one continuously-live cluster, an asymmetry in measurement rigor
between the two sweeps, not a deliberate design choice). See
LEADERLESS_WR_SWEEP for why each of the five was chosen (two
guaranteed-consistent extremes, the industry-standard majority quorum,
and one deliberately on the W+R=N boundary to make the rule's edge
visible rather than just guaranteed).

Message-queue: unlike the two above, this strategy has no client-facing
wait-and-ack knob to sweep -- a write always acks as soon as Kafka
confirms durability, regardless of load (see
message_queue/producer.py). Its staleness story is about load intensity
instead (how far a follower's consumer falls behind under pressure), so
MQ_WORKER_SWEEP sweeps concurrent worker count -- see run_mq_configs and
that constant's own comment for the values landed on (measured
empirically, not guessed) and message_queue/topics.py's reset_topic()
for how this sweep gives each config a genuinely clean Kafka topic, not
just fresh processes. Reported in its own docs/results.md section and
its own terminal table (see run_mq_configs and _write_mq_markdown_report)
rather than folded into the ranked leader-follower/leaderless table --
worker count isn't a comparable axis to ack_required or W/R, so ranking
it alongside them would imply a comparison that isn't real.

A configuration whose node processes don't become healthy in time is
logged clearly and skipped -- the rest of the sweep continues rather
than the whole run aborting on one bad config.

Output
------
Besides the combined terminal report, this script writes its tables into
docs/results.md's own marked sections (see experiments/_results_doc.py)
-- that file is auto-generated and mechanical only, never hand-edited;
see README.md's "Results" section for the analysis of what these
numbers mean. Every node process's stdout/stderr is also captured under
experiments/output/run_comparison_process_logs/, and each config's own
stale-read/failure JSONL logs (the same ones the three load-test scripts
produce standalone) are written under
experiments/output/run_comparison_load_test_logs/, one pair per config
rather than one shared pair every config would otherwise overwrite.
Both are what caught the first real anomaly this script surfaced (see
git history: ack_required=0 silently dropping ~87% of replicated writes
to httpx.PoolTimeout) -- keep them around when a number looks off.

Run:
    python3 -m experiments.run_comparison

Expect roughly 10 leader-follower/leaderless configurations x ~40s of
load-test traffic each, plus 3 message-queue configurations x ~20-25s
each, plus process startup/teardown overhead -- around 7-8 minutes end
to end.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from experiments import leaderless_staleness_load_test, mq_staleness_load_test, staleness_load_test
from experiments._load_test_common import DEFAULT_SEED
from experiments._results_doc import RESULTS_HEADER, RESULTS_HEADER_MARKER, replace_section
from experiments.mq_staleness_load_test import FOLLOWER_HOST as MQ_HOST
from experiments.mq_staleness_load_test import FOLLOWER_PORTS as MQ_FOLLOWER_PORTS
from experiments.mq_staleness_load_test import PRODUCER_PORT as MQ_PRODUCER_PORT
from message_queue.config import MQConfig
from message_queue.topics import reset_topic

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_PATH = REPO_ROOT / "docs" / "results.md"
PROCESS_LOG_DIR = Path(__file__).parent / "output" / "run_comparison_process_logs"

# Per-config stale-read/failure JSONL logs (see run_load_test's
# output_log_path/failure_log_path), one pair per config rather than
# the single shared path each load-test script defaults to -- otherwise
# each config's logs would silently overwrite the last one's, which is
# exactly what made the first ack_required=0 anomaly harder to diagnose
# than it needed to be (see git history for that investigation).
LOAD_TEST_LOG_DIR = Path(__file__).parent / "output" / "run_comparison_load_test_logs"

LEADER_FOLLOWER_HOST = "127.0.0.1"
LEADER_FOLLOWER_LEADER_PORT = 8000
LEADER_FOLLOWER_FOLLOWER_PORTS = [8001, 8002, 8003, 8004]

LEADERLESS_HOST = "127.0.0.1"
LEADERLESS_NODE_PORTS = [8101, 8102, 8103, 8104, 8105]

# ack_required sweep against the existing 4-follower cluster: 0 (fire
# and forget) through 4 (fully synchronous, every follower must ack).
ACK_REQUIRED_SWEEP = [0, 1, 2, 3, 4]

# (label, W, R) -- against the existing 5-node cluster (N=5).
LEADERLESS_WR_SWEEP = [
    # Weakest, fastest -- DynamoDB-style eventual-consistency default.
    ("W=1, R=1", 1, 1),
    # Write-optimized: cheap writes, full-cluster reads. Guaranteed
    # (1+5>5).
    ("W=1, R=5", 1, 5),
    # Read-optimized: full-cluster writes, cheap reads. Guaranteed
    # (5+1>5).
    ("W=5, R=1", 5, 1),
    # Majority quorum -- the real industry-standard default (e.g.
    # Cassandra QUORUM). Guaranteed (3+3>5).
    ("W=3, R=3", 3, 3),
    # Deliberately on the boundary: 2+3=5, NOT greater than N -- not
    # guaranteed by the rule, unlike the four configs above. Whether
    # that shows up as measurable staleness in *this* benchmark is a
    # separate question from whether it's guaranteed -- see
    # experiments/leaderless_boundary_case_demo.py and README.md's
    # "Results" section.
    ("W=2, R=3", 2, 3),
]

MQ_CONFIG_PATH = "config/mq_cluster.yaml"

# Load intensity (concurrent workers), not an ack-wait knob -- this
# strategy has none (a write always acks as soon as Kafka confirms
# durability, regardless of load; see message_queue/producer.py). Its
# staleness story is instead "how far behind does a follower's consumer
# fall under load", which experiments/mq_staleness_load_test.py's own
# --workers controls directly. Chosen empirically, not guessed: with
# run_mq_configs' own per-config isolation in place (fresh topic, fresh
# follower node-ids -- see that function's docstring) and
# _wait_for_mq_followers_assigned actually gating the load test's start
# on real partition assignment (see that function's docstring for the
# startup-race bug this fixed, found while tuning this exact sweep),
# 1/2/5 workers measured a clean, reproducible, monotonic staleness
# rise across two independent full sweeps (~1.3-1.6% / ~7.4-7.9% /
# ~35-38%) -- see docs/results.md's own mq-sweep section for the actual
# numbers a given run landed on and README.md's "Results" section for
# what they mean. Higher counts (10/20/50) were also tried while
# exploring this, but under an earlier, since-fixed version of this
# harness (no per-config topic reset, no partition-assignment wait) --
# those runs showed non-monotonic, contaminated numbers and are not
# cited as a finding; not re-verified against the corrected harness
# since 1/2/5 already demonstrates the intended near-zero-to-rising
# spread clearly.
MQ_WORKER_SWEEP = [1, 2, 5]

HEALTH_TIMEOUT_SECONDS = 15.0
HEALTH_POLL_INTERVAL_SECONDS = 0.2
SHUTDOWN_GRACE_SECONDS = 5.0


# --- Node process lifecycle -------------------------------------------------


@dataclass
class SpawnedNode:
    """One node process this script started, along with what's needed
    to health-check and later tear it down.
    """

    label: str
    host: str
    port: int
    proc: subprocess.Popen[bytes]
    log_path: Path


class NodeStartupError(RuntimeError):
    """Raised when a spawned node doesn't become healthy in time, or
    exits before it does.
    """


def _spawn(module: str, args: list[str], label: str, host: str, port: int) -> SpawnedNode:
    """Start `python3 -m {module} {args}` as a subprocess, with its
    stdout/stderr captured to a log file (named after `label`) so a
    startup failure can point at exactly what the process said, rather
    than just "didn't come up in time".
    """
    PROCESS_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = PROCESS_LOG_DIR / f"{label}.log"
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            [sys.executable, "-m", module, *args],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
    return SpawnedNode(label=label, host=host, port=port, proc=proc, log_path=log_path)


async def _wait_for_all_healthy(
    client: httpx.AsyncClient,
    nodes: list[SpawnedNode],
    timeout: float = HEALTH_TIMEOUT_SECONDS,
) -> None:
    """Poll every node's /health until each answers 200, or raise
    NodeStartupError the moment any one of them either times out or its
    process has already exited -- whichever happens first, so a node
    that crashed on startup fails fast rather than waiting out the full
    timeout.
    """
    pending = list(nodes)
    deadline = time.monotonic() + timeout
    while pending:
        still_pending: list[SpawnedNode] = []
        for node in pending:
            if node.proc.poll() is not None:
                raise NodeStartupError(
                    f"{node.label} ({node.host}:{node.port}) exited early "
                    f"(code {node.proc.returncode}); see {node.log_path}"
                )
            try:
                resp = await client.get(f"http://{node.host}:{node.port}/health", timeout=1.0)
                if resp.status_code == 200:
                    continue  # healthy -- drop from pending
            except httpx.HTTPError:
                pass
            still_pending.append(node)
        pending = still_pending
        if pending and time.monotonic() > deadline:
            names = ", ".join(f"{n.label} ({n.host}:{n.port})" for n in pending)
            raise NodeStartupError(
                f"timed out after {timeout}s waiting for /health from: {names}"
            )
        if pending:
            await asyncio.sleep(HEALTH_POLL_INTERVAL_SECONDS)


async def _wait_for_mq_followers_assigned(
    client: httpx.AsyncClient,
    followers: list[SpawnedNode],
    timeout: float = HEALTH_TIMEOUT_SECONDS,
) -> None:
    """Poll every MQ follower's GET /internal/lag until it reports at
    least one partition, on top of (and after) _wait_for_all_healthy's
    plain /health check.

    Necessary, not redundant: a follower's /health (inherited generic
    common/server.py behavior, see message_queue/follower.py) reports
    200 as soon as uvicorn is serving requests, which happens *before*
    its background Kafka consumer has actually joined its consumer
    group and been assigned any partitions -- group join/rebalance for a
    brand-new group takes real time, even with
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 (see
    docker-compose.kafka.yml). Starting the load test the moment
    /health goes green measures mostly that startup race, not real
    consumer lag under load -- confirmed empirically while tuning
    MQ_WORKER_SWEEP: every config showed ~100% staleness before this
    wait was added, regardless of worker count, because most or all
    reads landed before any follower had consumed anything at all.
    GET /internal/lag already exists (see message_queue/follower.py) and
    returns its `partitions` list empty until real Kafka partition
    assignment has happened, so it doubles as exactly the readiness
    signal needed here without adding anything new to that endpoint.
    """
    pending = list(followers)
    deadline = time.monotonic() + timeout
    while pending:
        still_pending: list[SpawnedNode] = []
        for node in pending:
            if node.proc.poll() is not None:
                raise NodeStartupError(
                    f"{node.label} ({node.host}:{node.port}) exited early "
                    f"(code {node.proc.returncode}); see {node.log_path}"
                )
            try:
                resp = await client.get(f"http://{node.host}:{node.port}/internal/lag", timeout=1.0)
                if resp.status_code == 200 and resp.json()["partitions"]:
                    continue  # partitions assigned -- drop from pending
            except httpx.HTTPError:
                pass
            still_pending.append(node)
        pending = still_pending
        if pending and time.monotonic() > deadline:
            names = ", ".join(f"{n.label} ({n.host}:{n.port})" for n in pending)
            raise NodeStartupError(
                f"timed out after {timeout}s waiting for a Kafka partition "
                f"assignment from: {names}"
            )
        if pending:
            await asyncio.sleep(HEALTH_POLL_INTERVAL_SECONDS)


def _stop_node(node: SpawnedNode, grace_seconds: float = SHUTDOWN_GRACE_SECONDS) -> None:
    proc = node.proc
    if proc.poll() is not None:
        return  # already exited
    proc.terminate()
    try:
        proc.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=grace_seconds)


def _stop_all(nodes: list[SpawnedNode]) -> None:
    for node in nodes:
        _stop_node(node)


def _slug(label: str) -> str:
    """Turn a config label like "ack_required=0" or "W=2, R=3" into a
    filesystem-safe slug, so each config's stale-read/failure JSONL logs
    get distinct, predictable filenames (see LOAD_TEST_LOG_DIR) instead
    of overwriting each other's.
    """
    return "".join(c if c.isalnum() else "_" for c in label).strip("_")


# --- Results -----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ConfigResult:
    strategy: str
    label: str
    elapsed: float
    staleness_rate: float
    total_failures: int
    total_requests: int


@dataclass(frozen=True, slots=True)
class ConfigError:
    strategy: str
    label: str
    reason: str


Outcome = ConfigResult | ConfigError


# --- Leader-follower sweep -----------------------------------------------


async def run_leader_follower_configs(client: httpx.AsyncClient) -> list[Outcome]:
    outcomes: list[Outcome] = []

    for ack_required in ACK_REQUIRED_SWEEP:
        label = f"ack_required={ack_required}"
        print(f"\n--- leader-follower [{label}]: starting cluster ---")
        nodes: list[SpawnedNode] = []
        try:
            for i, port in enumerate(LEADER_FOLLOWER_FOLLOWER_PORTS, start=1):
                nodes.append(
                    _spawn(
                        "leader_follower.follower",
                        ["--node-id", f"follower{i}", "--port", str(port)],
                        f"lf_ack{ack_required}_follower{i}",
                        LEADER_FOLLOWER_HOST,
                        port,
                    )
                )
            await _wait_for_all_healthy(client, nodes)

            leader = _spawn(
                "leader_follower.leader",
                [
                    "--node-id", "leader",
                    "--port", str(LEADER_FOLLOWER_LEADER_PORT),
                    "--ack-required", str(ack_required),
                ],
                f"lf_ack{ack_required}_leader",
                LEADER_FOLLOWER_HOST,
                LEADER_FOLLOWER_LEADER_PORT,
            )
            nodes.append(leader)
            await _wait_for_all_healthy(client, [leader])

            print(f"--- leader-follower [{label}]: cluster healthy, running load test ---")
            slug = _slug(label)
            result = await staleness_load_test.run_load_test(
                verbose=False,
                write_logs=True,
                output_log_path=LOAD_TEST_LOG_DIR / f"leader_follower_{slug}_stale.jsonl",
                failure_log_path=LOAD_TEST_LOG_DIR / f"leader_follower_{slug}_failures.jsonl",
            )
            outcomes.append(
                ConfigResult(
                    strategy="leader-follower",
                    label=label,
                    elapsed=result.elapsed,
                    staleness_rate=result.stats.staleness_rate,
                    total_failures=result.stats.total_failures,
                    total_requests=result.stats.total_requests,
                )
            )
            print(
                f"--- leader-follower [{label}]: done -- "
                f"staleness={result.stats.staleness_rate:.2f}% "
                f"failures={result.stats.total_failures} "
                f"elapsed={result.elapsed:.1f}s ---"
            )
        except Exception as exc:  # noqa: BLE001 -- deliberately broad: any
            # failure in this config's startup or run must not abort the
            # rest of the sweep (see module docstring).
            print(f"!!! leader-follower [{label}] SKIPPED: {exc} !!!")
            outcomes.append(ConfigError(strategy="leader-follower", label=label, reason=str(exc)))
        finally:
            _stop_all(nodes)

    return outcomes


# --- Leaderless sweep ----------------------------------------------------


async def run_leaderless_configs(client: httpx.AsyncClient) -> list[Outcome]:
    outcomes: list[Outcome] = []

    for label, w, r in LEADERLESS_WR_SWEEP:
        print(f"\n--- leaderless [{label}]: starting cluster (5 nodes) ---")
        slug = _slug(label)
        nodes: list[SpawnedNode] = []
        try:
            for i, port in enumerate(LEADERLESS_NODE_PORTS, start=1):
                nodes.append(
                    _spawn(
                        "leaderless.node",
                        ["--node-id", f"node{i}", "--port", str(port)],
                        f"leaderless_{slug}_node{i}",
                        LEADERLESS_HOST,
                        port,
                    )
                )
            await _wait_for_all_healthy(client, nodes)

            print(f"--- leaderless [{label}]: cluster healthy, running load test ---")
            result = await leaderless_staleness_load_test.run_load_test(
                w=w,
                r=r,
                verbose=False,
                write_logs=True,
                output_log_path=LOAD_TEST_LOG_DIR / f"leaderless_{slug}_stale.jsonl",
                failure_log_path=LOAD_TEST_LOG_DIR / f"leaderless_{slug}_failures.jsonl",
            )
            outcomes.append(
                ConfigResult(
                    strategy="leaderless",
                    label=label,
                    elapsed=result.elapsed,
                    staleness_rate=result.stats.staleness_rate,
                    total_failures=result.stats.total_failures,
                    total_requests=result.stats.total_requests,
                )
            )
            print(
                f"--- leaderless [{label}]: done -- "
                f"staleness={result.stats.staleness_rate:.2f}% "
                f"failures={result.stats.total_failures} "
                f"elapsed={result.elapsed:.1f}s ---"
            )
        except Exception as exc:  # noqa: BLE001 -- deliberately broad: any
            # failure in this config's startup or run must not abort the
            # rest of the sweep (see module docstring).
            print(f"!!! leaderless [{label}] SKIPPED: {exc} !!!")
            outcomes.append(ConfigError(strategy="leaderless", label=label, reason=str(exc)))
        finally:
            _stop_all(nodes)

    return outcomes


# --- Message-queue sweep --------------------------------------------------


async def run_mq_configs(client: httpx.AsyncClient) -> list[Outcome]:
    """Sweep MQ_WORKER_SWEEP (load intensity) against a fresh producer +
    4 followers per config, exactly like the two sweeps above give
    every config a fresh cluster.

    Unlike those two, "fresh" here means more than just fresh OS
    processes (a fresh in-memory KVStore): the Kafka topic itself is
    reset between configs (reset_topic -- delete, wait for the deletion
    to actually complete, recreate), or a later config would inherit
    every earlier config's messages, and each follower is given a
    config-specific --node-id (so its derived consumer-group id --
    "{consumer_group_prefix}-{node_id}", see message_queue/follower.py --
    is unique to this config too, not reused across a topic reset).
    Neither alone would be enough: resetting the topic but reusing the
    same follower node-ids would rely on every Kafka client version
    correctly handling a stale committed offset against a
    deleted-and-recreated topic (falling back to auto_offset_reset=
    "earliest"); giving each config fresh node-ids but never resetting
    the topic would still leave every prior config's messages on it for
    a new follower to replay from earliest. Doing both removes any
    doubt.
    """
    outcomes: list[Outcome] = []
    mq_config = MQConfig.from_yaml(MQ_CONFIG_PATH)

    for num_workers in MQ_WORKER_SWEEP:
        label = f"workers={num_workers}"
        slug = _slug(label)
        print(f"\n--- message-queue [{label}]: resetting topic, starting cluster ---")
        nodes: list[SpawnedNode] = []
        try:
            await reset_topic(mq_config)

            followers: list[SpawnedNode] = []
            for i, port in enumerate(MQ_FOLLOWER_PORTS, start=1):
                followers.append(
                    _spawn(
                        "message_queue.follower",
                        ["--node-id", f"follower{i}_{slug}", "--port", str(port)],
                        f"mq_{slug}_follower{i}",
                        MQ_HOST,
                        port,
                    )
                )
            nodes += followers
            await _wait_for_all_healthy(client, followers)
            # See _wait_for_mq_followers_assigned's own docstring: plain
            # /health isn't enough here, unlike the other two strategies.
            await _wait_for_mq_followers_assigned(client, followers)

            producer = _spawn(
                "message_queue.producer",
                ["--node-id", f"producer_{slug}", "--port", str(MQ_PRODUCER_PORT)],
                f"mq_{slug}_producer",
                MQ_HOST,
                MQ_PRODUCER_PORT,
            )
            nodes.append(producer)
            await _wait_for_all_healthy(client, [producer])

            print(f"--- message-queue [{label}]: cluster healthy, running load test ---")
            result = await mq_staleness_load_test.run_load_test(
                num_workers=num_workers,
                verbose=False,
                write_logs=True,
                output_log_path=LOAD_TEST_LOG_DIR / f"mq_{slug}_stale.jsonl",
                failure_log_path=LOAD_TEST_LOG_DIR / f"mq_{slug}_failures.jsonl",
            )
            outcomes.append(
                ConfigResult(
                    strategy="message-queue",
                    label=label,
                    elapsed=result.elapsed,
                    staleness_rate=result.stats.staleness_rate,
                    total_failures=result.stats.total_failures,
                    total_requests=result.stats.total_requests,
                )
            )
            print(
                f"--- message-queue [{label}]: done -- "
                f"staleness={result.stats.staleness_rate:.2f}% "
                f"failures={result.stats.total_failures} "
                f"elapsed={result.elapsed:.1f}s ---"
            )
        except Exception as exc:  # noqa: BLE001 -- deliberately broad: any
            # failure in this config's startup or run must not abort the
            # rest of the sweep (see module docstring).
            print(f"!!! message-queue [{label}] SKIPPED: {exc} !!!")
            outcomes.append(ConfigError(strategy="message-queue", label=label, reason=str(exc)))
        finally:
            _stop_all(nodes)

    return outcomes


# --- Reporting -----------------------------------------------------------


def _print_report(outcomes: list[Outcome]) -> None:
    results = [o for o in outcomes if isinstance(o, ConfigResult)]
    errors = [o for o in outcomes if isinstance(o, ConfigError)]
    ranked = sorted(results, key=lambda r: r.staleness_rate)

    width = 92
    print()
    print("=" * width)
    print("  COMBINED REPLICATION STRATEGY COMPARISON")
    print("  (ranked by staleness rate ascending -- lowest/best first)")
    print("=" * width)
    header = (
        f"{'rank':<6}{'strategy':<16}{'config':<16}"
        f"{'staleness %':<14}{'elapsed (s)':<13}{'failures':<10}{'requests':<10}"
    )
    print(header)
    print("-" * width)
    for i, r in enumerate(ranked, start=1):
        print(
            f"{i:<6}{r.strategy:<16}{r.label:<16}"
            f"{r.staleness_rate:<14.2f}{r.elapsed:<13.1f}{r.total_failures:<10}{r.total_requests:<10}"
        )
    print("=" * width)

    if errors:
        print()
        print("!" * width)
        print(f"  {len(errors)} configuration(s) SKIPPED due to startup/run failures:")
        for e in errors:
            print(f"    {e.strategy:<16}{e.label:<16}{e.reason}")
        print("!" * width)


def _print_mq_report(outcomes: list[Outcome]) -> None:
    """Printed separately from _print_report's combined ranked table,
    same reason as _write_mq_markdown_report: worker count isn't a
    comparable axis to ack_required or W/R, so folding it into one
    ranked list would imply a comparison that isn't real. Listed in
    sweep order (ascending worker count), matching the docs/results.md
    table.
    """
    results = [o for o in outcomes if isinstance(o, ConfigResult)]
    errors = [o for o in outcomes if isinstance(o, ConfigError)]

    width = 92
    print()
    print("=" * width)
    print("  MESSAGE-QUEUE SWEEP (load intensity -- not ranked against the table above)")
    print("=" * width)
    header = f"{'config':<16}{'staleness %':<14}{'elapsed (s)':<13}{'failures':<10}{'requests':<10}"
    print(header)
    print("-" * width)
    for r in results:
        print(
            f"{r.label:<16}"
            f"{r.staleness_rate:<14.2f}{r.elapsed:<13.1f}{r.total_failures:<10}{r.total_requests:<10}"
        )
    print("=" * width)

    if errors:
        print()
        print("!" * width)
        print(f"  {len(errors)} configuration(s) SKIPPED due to startup/run failures:")
        for e in errors:
            print(f"    {e.label:<16}{e.reason}")
        print("!" * width)


# Fixed methodology notes, printed after the table in every generated
# report -- these describe how the measurement itself is constructed
# (harness mechanics, seeding), not an interpretation of what any
# particular run's numbers mean. That distinction is what keeps this
# from being _KNOWN_CHARACTERISTICS_NOTES under a new name (see
# docs/AUDIT_FINDINGS.md's §6 for why that was removed): nothing below
# cites a result-dependent figure that a future code change could make
# stale without this text being touched -- it's true regardless of what
# the table above shows. Interpretation of the actual numbers lives in
# README.md's "Results" section instead, updated by hand when it goes
# out of date.
_METHODOLOGY_NOTES = (
    "### Methodology notes\n"
    "\n"
    "**ack_required=0 vs. W=1,R=1 aren't apples-to-apples.** Both are "
    "each strategy's weakest/no-durability-wait config, but this harness "
    "gives them very different coordinator load: every leader-follower "
    "write goes through the one leader process (100% of write traffic, "
    "one process), while every leaderless write picks its coordinator by "
    "round robin across all 5 nodes (`NodeRotation` in "
    "`experiments/leaderless_staleness_load_test.py`, roughly 20% of "
    "write traffic per process). `ack_required=0`'s replicate fan-out is "
    "therefore concentrated on a single process; `W=1`'s "
    "architecturally-equivalent fan-out is split roughly five ways. This "
    "is a real, structural difference in what this harness measures for "
    "each config, not just \"there may be some asymmetry here\" -- it "
    "plausibly explains part of the gap between the two configs' "
    "results, independent of anything about the two replication "
    "strategies' actual protocols. See docs/AUDIT_FINDINGS.md's §9.\n"
    "\n"
    f"**Runs are seeded (default seed: {DEFAULT_SEED}; override with "
    "--seed).** Every config's full sequence of per-request decisions -- "
    "which key, read vs. write, which follower/read-coordinator -- is "
    "precomputed before that config's timed portion starts, so the same "
    "seed produces the exact same sequence of decisions on every run "
    "(see `experiments/_load_test_common.py`'s `generate_request_plan`). "
    "This does **not** make a config's results identical run to run: "
    "real request timing/scheduling -- whether a read's real arrival "
    "races a write's real in-flight replication -- is not seeded and "
    "cannot be made deterministic. Measured directly (3 runs each, same "
    f"seed={DEFAULT_SEED}, nothing else changed): leader-follower "
    "`ack_required=2` ranged 13.12-13.31% staleness, 17-30 failures "
    "(elapsed 38.3-42.1s); leaderless `W=1,R=1` measured exactly 0.00% "
    "staleness, 0 failures, all three runs (elapsed 23.1-24.5s) -- "
    "consistent with `W=1,R=1` being a genuine structural blind spot at "
    "this scale (see the boundary-case-demo section) rather than "
    "something timing-sensitive enough to show variance here. Treat any "
    "single run's numbers, including the table above, as one sample "
    "from around that range, not an exact, repeatable figure -- tighter "
    "for some configs than others. See docs/AUDIT_FINDINGS.md's §8."
)


def _write_markdown_report(outcomes: list[Outcome], path: Path) -> None:
    """Write this run's main-sweep table into docs/results.md's own
    marked section (see experiments/_results_doc.py), leaving any other
    section already in the file -- e.g.
    leaderless_boundary_case_demo.py's -- untouched.

    Mostly mechanical, by design: a table, a run timestamp, and a
    pointer to the raw JSONL logs, plus _METHODOLOGY_NOTES above (fixed
    facts about how the measurement itself works, not an interpretation
    of what any particular run's numbers mean -- see that constant's own
    comment for the distinction). Interpretation used to live here too,
    hardcoded as a fixed string reproduced in every report regardless of
    what the table above it actually said -- see docs/AUDIT_FINDINGS.md's
    §6 for why that turned out to be a bad idea (git history:
    _KNOWN_CHARACTERISTICS_NOTES, removed) and README.md's "Results"
    section for where that analysis lives now instead.
    """
    results = [o for o in outcomes if isinstance(o, ConfigResult)]
    errors = [o for o in outcomes if isinstance(o, ConfigError)]
    ranked = sorted(results, key=lambda r: r.staleness_rate)

    generated_at = datetime.now(UTC).isoformat(timespec="seconds")
    lines = [
        "## Main sweep",
        "",
        f"Generated by `experiments/run_comparison.py` on {generated_at}.",
        "",
        "Ranked by staleness rate ascending (lowest/best first). See the top-level "
        "`README.md` for what each strategy/parameter means and the hand-maintained "
        "interpretation of these numbers -- why they look the way they do. Raw "
        "per-config stale-read/failure JSONL logs: "
        f"`{LOAD_TEST_LOG_DIR.relative_to(REPO_ROOT)}/`.",
        "",
        "| Rank | Strategy | Config | Staleness rate | Elapsed | Failures | Total requests |",
        "|---|---|---|---|---|---|---|",
    ]
    for i, r in enumerate(ranked, start=1):
        lines.append(
            f"| {i} | {r.strategy} | {r.label} | {r.staleness_rate:.2f}% | "
            f"{r.elapsed:.1f}s | {r.total_failures} | {r.total_requests} |"
        )

    if errors:
        lines += ["", "### Skipped configurations", ""]
        lines += ["| Strategy | Config | Reason |", "|---|---|---|"]
        for e in errors:
            lines.append(f"| {e.strategy} | {e.label} | {e.reason} |")

    lines += ["", _METHODOLOGY_NOTES]

    replace_section(path, RESULTS_HEADER_MARKER, RESULTS_HEADER)
    replace_section(path, "main-sweep", "\n".join(lines))


# Fixed context printed alongside the mq-sweep table, same "how the
# measurement works, not what a specific run's numbers mean" distinction
# as _METHODOLOGY_NOTES above -- see that constant's own comment.
_MQ_METHODOLOGY_NOTES = (
    "### Methodology notes\n"
    "\n"
    "**This sweep's variable is load intensity (worker count), not an "
    "ack-wait knob.** Unlike `ack_required` or `W`/`R`, this strategy has "
    "no client-facing wait-and-ack setting to sweep -- every write acks "
    "as soon as Kafka confirms the message is durably logged, regardless "
    "of load (see `message_queue/producer.py`). Staleness here instead "
    "reflects how far a follower's consumer has fallen behind under "
    "load; `experiments/mq_staleness_load_test.py`'s `--workers` is what "
    "varies between rows, not a per-request override the way leaderless's "
    "`w`/`r` are.\n"
    "\n"
    "**Every row starts from a genuinely fresh topic, not just fresh "
    "processes.** `message_queue/topics.py`'s `reset_topic()` deletes and "
    "recreates the Kafka topic between configs (waiting for the deletion "
    "to actually complete first -- Kafka topic deletion is asynchronous), "
    "and every follower gets a config-specific node-id so its derived "
    "consumer-group id is unique to that row too -- see "
    "`experiments/run_comparison.py`'s `run_mq_configs` for why both "
    "matter, not just one.\n"
    "\n"
    "**Lag is sampled only on a detected-stale read, not on every read** "
    "(see `experiments/mq_staleness_load_test.py`'s own docstring for "
    "why) -- `lag_at_stale_read` in this sweep's raw JSONL logs is a "
    "per-event diagnostic sample, not a continuous measurement, and its "
    "absence from the table above is deliberate: aggregating it into a "
    "single summary number here would imply a precision this sampling "
    "strategy doesn't have.\n"
    "\n"
    f"**Runs are seeded (default seed: {DEFAULT_SEED}; override with "
    "--seed), with the same caveat as the main sweep above:** which "
    "decision each request makes is reproducible, real timing/scheduling "
    "is not, so treat any single run's numbers as one sample, not an "
    "exact repeatable figure."
)


def _write_mq_markdown_report(outcomes: list[Outcome], path: Path) -> None:
    """Write this run's message-queue sweep into docs/results.md's own
    marked section (mq-sweep), separate from the main sweep's -- see
    module docstring's "Wire run_mq_configs..." note and
    experiments/_results_doc.py. Deliberately not merged into
    _write_markdown_report's table: worker count isn't a comparable axis
    to ack_required or W/R (it's a load-intensity dial the other two
    strategies don't have one of), so ranking it alongside them would
    imply a comparison that isn't real. Listed in sweep order (ascending
    worker count) rather than staleness-ascending, since the point of
    this table is the *progression* as load increases, not "which
    config wins."
    """
    results = [o for o in outcomes if isinstance(o, ConfigResult)]
    errors = [o for o in outcomes if isinstance(o, ConfigError)]

    generated_at = datetime.now(UTC).isoformat(timespec="seconds")
    lines = [
        "## Message-queue sweep",
        "",
        f"Generated by `experiments/run_comparison.py` on {generated_at}.",
        "",
        "Listed in ascending load order (fewest concurrent workers first), not ranked "
        "by staleness -- see the top-level `README.md` for what this strategy's "
        "trade-off means and the hand-maintained interpretation of these numbers. Raw "
        "per-config stale-read/failure JSONL logs (including a `lag_at_stale_read` "
        "field on every stale-read event): "
        f"`{LOAD_TEST_LOG_DIR.relative_to(REPO_ROOT)}/`.",
        "",
        "| Config | Staleness rate | Elapsed | Failures | Total requests |",
        "|---|---|---|---|---|",
    ]
    for r in results:
        lines.append(
            f"| {r.label} | {r.staleness_rate:.2f}% | "
            f"{r.elapsed:.1f}s | {r.total_failures} | {r.total_requests} |"
        )

    if errors:
        lines += ["", "### Skipped configurations", ""]
        lines += ["| Config | Reason |", "|---|---|"]
        for e in errors:
            lines.append(f"| {e.label} | {e.reason} |")

    lines += ["", _MQ_METHODOLOGY_NOTES]

    replace_section(path, RESULTS_HEADER_MARKER, RESULTS_HEADER)
    replace_section(path, "mq-sweep", "\n".join(lines))


# --- Entrypoint -------------------------------------------------------------


async def main() -> None:
    total_configs = len(ACK_REQUIRED_SWEEP) + len(LEADERLESS_WR_SWEEP) + len(MQ_WORKER_SWEEP)
    print(
        f"running {total_configs} configurations "
        f"({len(ACK_REQUIRED_SWEEP)} leader-follower, {len(LEADERLESS_WR_SWEEP)} leaderless, "
        f"{len(MQ_WORKER_SWEEP)} message-queue)..."
    )
    overall_start = time.time()

    async with httpx.AsyncClient() as client:
        main_outcomes: list[Outcome] = []
        main_outcomes += await run_leader_follower_configs(client)
        main_outcomes += await run_leaderless_configs(client)
        mq_outcomes = await run_mq_configs(client)

    overall_elapsed = time.time() - overall_start
    print(f"\nall configurations finished in {overall_elapsed / 60:.1f} min.")

    _print_report(main_outcomes)
    _print_mq_report(mq_outcomes)
    _write_markdown_report(main_outcomes, RESULTS_PATH)
    _write_mq_markdown_report(mq_outcomes, RESULTS_PATH)
    print(f"\nreport written to {RESULTS_PATH}")
    print(f"per-config stale-read/failure JSONL logs written under {LOAD_TEST_LOG_DIR}")


if __name__ == "__main__":
    asyncio.run(main())
