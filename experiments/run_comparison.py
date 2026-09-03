"""Fully automated cross-strategy replication comparison.

Runs experiments/staleness_load_test.py's and
experiments/leaderless_staleness_load_test.py's core load-test logic
against every meaningful configuration of both replication strategies,
back to back, and produces one combined report ranking all of them by
observed staleness rate.

This script owns the full lifecycle of every node process involved --
spawning them with subprocess.Popen, polling /health until they're
ready, and terminating them (SIGTERM, falling back to SIGKILL) before
moving on -- so no manual pane setup (starting followers/leader/nodes
by hand in separate terminals) is needed to run it.

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

A configuration whose node processes don't become healthy in time is
logged clearly and skipped -- the rest of the sweep continues rather
than the whole run aborting on one bad config.

Output
------
Besides the combined terminal report, this script writes its table into
docs/results.md's own marked section (see experiments/_results_doc.py)
-- that file is auto-generated and mechanical only, never hand-edited;
see README.md's "Results" section for the analysis of what these
numbers mean. Every node process's stdout/stderr is also captured under
experiments/output/run_comparison_process_logs/, and each config's own
stale-read/failure JSONL logs (the same ones staleness_load_test.py and
leaderless_staleness_load_test.py produce standalone) are written under
experiments/output/run_comparison_load_test_logs/, one pair per config
rather than one shared pair every config would otherwise overwrite.
Both are what caught the first real anomaly this script surfaced (see
git history: ack_required=0 silently dropping ~87% of replicated writes
to httpx.PoolTimeout) -- keep them around when a number looks off.

Run:
    python3 -m experiments.run_comparison

Expect roughly 10 configurations x ~40s of load-test traffic each, plus
process startup/teardown overhead -- around 7 minutes end to end.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx

from experiments import leaderless_staleness_load_test, staleness_load_test
from experiments._load_test_common import DEFAULT_SEED
from experiments._results_doc import RESULTS_HEADER, RESULTS_HEADER_MARKER, replace_section

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
    proc: subprocess.Popen
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

    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
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


# --- Entrypoint -------------------------------------------------------------


async def main() -> None:
    total_configs = len(ACK_REQUIRED_SWEEP) + len(LEADERLESS_WR_SWEEP)
    print(
        f"running {total_configs} configurations "
        f"({len(ACK_REQUIRED_SWEEP)} leader-follower, {len(LEADERLESS_WR_SWEEP)} leaderless)..."
    )
    overall_start = time.time()

    async with httpx.AsyncClient() as client:
        outcomes: list[Outcome] = []
        outcomes += await run_leader_follower_configs(client)
        outcomes += await run_leaderless_configs(client)

    overall_elapsed = time.time() - overall_start
    print(f"\nall configurations finished in {overall_elapsed / 60:.1f} min.")

    _print_report(outcomes)
    _write_markdown_report(outcomes, RESULTS_PATH)
    print(f"\nreport written to {RESULTS_PATH}")
    print(f"per-config stale-read/failure JSONL logs written under {LOAD_TEST_LOG_DIR}")


if __name__ == "__main__":
    asyncio.run(main())
