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
(config/leaderless_cluster.yaml), all sent as per-request overrides
against one cluster started once -- W/R don't require a restart between
configs, unlike ack_required -- see LEADERLESS_WR_SWEEP for why each of
the five was chosen (two guaranteed-consistent extremes, the
industry-standard majority quorum, and one deliberately on the W+R=N
boundary to make the rule's edge visible rather than just guaranteed).

A configuration whose node processes don't become healthy in time is
logged clearly and skipped -- the rest of the sweep continues rather
than the whole run aborting on one bad config.

Output
------
Besides the combined terminal report and docs/results.md, every node
process's stdout/stderr is captured under
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
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx

from experiments import leaderless_staleness_load_test, staleness_load_test

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
    # _KNOWN_CHARACTERISTICS_NOTES below.
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
    nodes: list[SpawnedNode] = []

    print("\n--- leaderless: starting cluster (5 nodes) ---")
    try:
        for i, port in enumerate(LEADERLESS_NODE_PORTS, start=1):
            nodes.append(
                _spawn(
                    "leaderless.node",
                    ["--node-id", f"node{i}", "--port", str(port)],
                    f"leaderless_node{i}",
                    LEADERLESS_HOST,
                    port,
                )
            )
        await _wait_for_all_healthy(client, nodes)
        print("--- leaderless: cluster healthy ---")
    except Exception as exc:  # noqa: BLE001 -- see run_leader_follower_configs
        print(f"!!! leaderless cluster SKIPPED (all {len(LEADERLESS_WR_SWEEP)} configs): {exc} !!!")
        _stop_all(nodes)
        return [
            ConfigError(strategy="leaderless", label=label, reason=str(exc))
            for label, _, _ in LEADERLESS_WR_SWEEP
        ]

    for label, w, r in LEADERLESS_WR_SWEEP:
        print(f"\n--- leaderless [{label}]: running load test ---")
        try:
            slug = _slug(label)
            result = await leaderless_staleness_load_test.run_load_test(
                w=w,
                r=r,
                verbose=False,
                write_logs=True,
                output_log_path=LOAD_TEST_LOG_DIR / f"leaderless_{slug}_stale.jsonl",
                failure_log_path=LOAD_TEST_LOG_DIR / f"leaderless_{slug}_failures.jsonl",
            )
        except Exception as exc:  # noqa: BLE001 -- one bad config must not
            # cost the rest of the sweep against this same live cluster.
            print(f"!!! leaderless [{label}] SKIPPED: {exc} !!!")
            outcomes.append(ConfigError(strategy="leaderless", label=label, reason=str(exc)))
            continue

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


# Fixed, hand-written explanation of two known, investigated
# characteristics of this sweep's numbers -- included in every generated
# report (not just this run's) since they're properties of the current
# implementation and this benchmark's settings, not one-off flukes.
# Update this alongside the code comments it references
# (leader_follower/leader.py's _CLIENT_MAX_CONNECTIONS block,
# leaderless/node.py's _FANOUT_MARGIN block, and the top of
# experiments/leaderless_staleness_load_test.py) if either changes.
_KNOWN_CHARACTERISTICS_NOTES = """\
## Why these results look the way they do

### `ack_required=0` has high staleness, high failures, and much higher elapsed time than every other leader-follower config

This is expected, not a bug -- `ack_required=0` is fire-and-forget: the \
write path never awaits any follower before returning, so there is zero \
backpressure on how fast writes can be issued. Under this benchmark's \
sustained, unthrottled load, that has two real, compounding \
consequences:

- **Every value is real, but the ranking reflects a genuine trade-off, \
  not a measurement error.** `ack_required=1..3` show a clean monotonic \
  staleness staircase with zero internal exceptions logged on the \
  leader -- awaiting even one real ack naturally bounds how many \
  replicate calls can be in flight at once (bounded by how many client \
  writes are concurrently in flight). `ack_required=4` (fully \
  synchronous -- every follower must ack before the client sees success) \
  now shows exactly 0.00% -- see below.
- **`ack_required=0` has no such bound.** A large connection pool \
  (`leader_follower/leader.py`'s `_CLIENT_MAX_CONNECTIONS`) avoids \
  silently dropping replicated writes to `httpx.PoolTimeout`, but \
  doesn't fix the underlying lack of backpressure: under sustained load \
  the followers (plain uvicorn, no concurrency limiting of their own) \
  get more concurrent replicate traffic than they can drain, which \
  keeps each replicate call alive longer, which lets yet more \
  unthrottled writes pile up before the old ones finish -- a feedback \
  loop that eventually starves the *leader's own* event loop badly \
  enough to time out on its own trivial, in-memory-only client-facing \
  PUT endpoint. See `leader_follower/leader.py`'s `_CLIENT_MAX_CONNECTIONS` \
  comment for the full investigation and exact failure counts.
- **The connection pool is one global setting, not scoped per \
  `ack_required` -- and that's *why* the staircase above is only \
  visible now.** `Replicator` fans every write out to all 4 followers \
  concurrently regardless of `ack_required`, and never cancels the \
  followers it stops waiting on -- `ack_required` only changes how many \
  of those 4 in-flight calls the write path blocks on before returning. \
  So the *old*, smaller pool (httpx's unset-`Limits` default of 100 \
  connections/20 keepalive, before this fix) was silently dropping \
  excess fan-out to `PoolTimeout` at every `ack_required` value, not \
  just 0 -- just severely enough to matter only at 0's much higher \
  concurrency. Isolation-tested directly: with everything else held \
  fixed (same followers, same load), swapping only the pool setting at \
  `ack_required=2` measured 0.00% staleness / 0 failures under the old \
  pool vs. 8.87% under the current one; at `ack_required=3`, 0.06% (old) \
  vs. 4.60% (new). Enlarging the pool to stop ack_required=0's silent \
  drops therefore also stopped 1..4's silent drops, which is what makes \
  the staircase measurable at all now -- it is real replication lag, not \
  an artifact of this fix, but it did not exist as an *observable* number \
  before this fix for the same underlying reason ack_required=0's did \
  not.

Fixing `ack_required=0`'s own missing backpressure for real would mean \
bounding *concurrent in-flight replicate fan-out* independently of \
connection-pool size (e.g. a semaphore in `Replicator`) -- deliberately \
left undone: this is a lab for observing replication trade-offs, and \
`ack_required=0`'s feedback loop and the 1..3 staircase are both real \
characteristics of the current implementation under real load, not \
measurement bugs to engineer away.

### `ack_required=4` shows exactly 0.00% staleness -- provably, not just observed

This is a mathematical floor, not a favorable roll: once \
`ack_required=4` (= the follower count) is satisfied, *every* follower \
has already responded to that write's replicate call -- including a \
follower that rejected it because it already held something \
LWW-newer, since an "ack" only means "responded", not "applied" (see \
`Replicator._send`). Either way, every follower's stored timestamp for \
that key is `>=` the just-acked write's timestamp at that instant, and \
LWW never regresses a stored timestamp -- so it stays `>=` forever \
after. A later read of any follower for that key can therefore never \
see anything older than the newest write that ever reached full \
`ack_required=4` quorum. This isn't specific to this lab's \
implementation -- it's the general property that makes `ack_required = \
len(followers)` "fully synchronous" a meaningful phrase at all.

Earlier runs of this sweep measured a small but nonzero rate here \
(roughly 1.2-1.3%) despite that guarantee, which was the tell that \
something *other* than real replication lag was being measured -- \
tracked down to `staleness_load_test.py`'s write path re-reading the \
leader's *current* value after a write succeeded (since `PutResponse` \
didn't echo back the timestamp it stamped), rather than trusting that \
write's own result. A different, unrelated concurrent write to the \
same key could land in that gap and get misattributed as this write's \
own confirmed value, before its *own* replication had necessarily \
finished. Fixed by having `PutResponse` return the timestamp it \
stamped directly (see `common/server.py::PutResponse`), so both load \
tests record each write's ground truth from its own response, never a \
separate re-read. `ack_required=4` immediately dropped from ~1.2% to \
exactly 0.00%, with no other change to any replication code -- as \
clean a confirmation as this kind of artifact gets.

### Leaderless staleness stays near-zero for every config, including `W=2,R=3`, which is genuinely not guaranteed

`W=1,R=5`, `W=5,R=1`, `W=3,R=3` are legitimately guaranteed by the \
classic W+R>N overlap rule (or one side already equals N) -- 0.00% \
there isn't a finding. `W=1,R=1` and `W=2,R=3` are **not** guaranteed \
(`1+1=2` and `2+3=5`, neither `>5`), and as of this fix, genuinely not: \
`QuorumCoordinator` contacts exactly the peers each config's literal W/R \
implies (see `replicate_write`/`read` in `leaderless/node.py`, and \
`tests/test_leaderless.py`'s `test_write_floods_every_peer_regardless_of_w` \
/ `test_read_contacts_exactly_r_minus_one_peers`, which assert the exact \
contact counts directly) -- no resilience margin quietly padding \
coverage past the nominal values the way the old `_FANOUT_MARGIN` did.

Both still measure at or near 0.00% run to run (`W=1,R=1` has shown a \
small nonzero rate on occasion; `W=2,R=3` has not, so far) -- run-to-run \
noise around a real floor, not two separate coincidences or a residual \
bug in the fix: **every write floods every peer unconditionally, \
regardless of W** (durability is fully decoupled from the ack-wait -- \
see the module docstring), and that flood completes in low single-digit \
milliseconds on localhost -- far faster than the ~250-330ms average gap \
between requests touching the same key at this benchmark's throughput \
(10,000 requests / 100 keys). By the time a subsequent read arrives, the \
write has almost always finished landing everywhere already, so neither \
config's real non-guarantee gets much chance to manifest. This was \
already known and documented for `W=1,R=1` \
specifically (see `experiments/leaderless_staleness_load_test.py`'s \
module comment) -- making the fan-out exact rather than margin-padded \
didn't shrink this effect, it *generalized* it: previously only `W=1`'s \
unconditional full flood was fast enough to hide behind loopback speed; \
now every W value floods unconditionally by design, so every \
non-guaranteed config inherits the same blind spot, not just `W=1,R=1`.

Reproducing genuine leaderless staleness locally now needs the same \
intervention already scoped for `W=1,R=1`: shrinking the key pool \
(heavier same-key contention), injecting artificial per-peer replication \
delay, or real network latency (the planned EC2 stage) -- left undone \
here since it's a benchmark-realism change, not a coordinator-logic fix."""

# The source above wraps long paragraphs across multiple lines with `\`
# continuations, indented for readability under bullets -- collapse the
# resulting runs of 2+ spaces (but not the single spaces `\`
# continuation already inserts, nor newlines/paragraph breaks) back down
# to one, so none of that source-only wrapping leaks into the rendered
# markdown.
_KNOWN_CHARACTERISTICS_NOTES = re.sub(r" {2,}", " ", _KNOWN_CHARACTERISTICS_NOTES)


def _write_markdown_report(outcomes: list[Outcome], path: Path) -> None:
    results = [o for o in outcomes if isinstance(o, ConfigResult)]
    errors = [o for o in outcomes if isinstance(o, ConfigError)]
    ranked = sorted(results, key=lambda r: r.staleness_rate)

    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "# Replication strategy comparison results",
        "",
        f"Generated by `experiments/run_comparison.py` on {generated_at}.",
        "",
        "Ranked by staleness rate ascending (lowest/best first). See the top-level "
        "`README.md` for what each strategy and parameter means, and "
        "`experiments/staleness_load_test.py` / "
        "`experiments/leaderless_staleness_load_test.py` for how staleness and "
        "failures are measured.",
        "",
        "| Rank | Strategy | Config | Staleness rate | Elapsed | Failures | Total requests |",
        "|---|---|---|---|---|---|---|",
    ]
    for i, r in enumerate(ranked, start=1):
        lines.append(
            f"| {i} | {r.strategy} | {r.label} | {r.staleness_rate:.2f}% | "
            f"{r.elapsed:.1f}s | {r.total_failures} | {r.total_requests} |"
        )

    lines += ["", _KNOWN_CHARACTERISTICS_NOTES]

    if errors:
        lines += ["", "## Skipped configurations", ""]
        lines += ["| Strategy | Config | Reason |", "|---|---|---|"]
        for e in errors:
            lines.append(f"| {e.strategy} | {e.label} | {e.reason} |")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


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
