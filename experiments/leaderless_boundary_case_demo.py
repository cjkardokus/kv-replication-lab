"""Demonstrates the leaderless W+R boundary case (W=2,R=3 on this lab's
5-node cluster) that the old, removed `_FANOUT_MARGIN` used to quietly
mask -- see docs/AUDIT_FINDINGS.md's §2 and the top-level README.md's
"Results" section for the condensed writeup; this docstring is the full
version. docs/results.md's own "Demonstrating the W+R boundary case"
section (written by this script) is mechanical only -- this run's raw
numbers, not the reasoning -- see experiments/_results_doc.py for why.

Why this script exists, separately from experiments/run_comparison.py
----------------------------------------------------------------------
`W=2,R=3` is *not* covered by the classic W+R>N overlap guarantee
(2+3=5, not >5) -- unlike every other config in the main sweep. But under
normal local conditions it still measures ~0.00% staleness there (see
docs/results.md), because `QuorumCoordinator.replicate_write` floods
every peer unconditionally regardless of W, and that flood finishes in
low single-digit milliseconds on loopback -- far faster than a
subsequent read can realistically arrive and catch it still in flight.
The boundary case is real (nothing *guarantees* a read won't miss a
recent write here), but this lab's normal local benchmark can't produce
it -- a genuine measurement blind spot, not a bug in the coordinator
logic itself (see leaderless/node.py's tests, which assert W/R fan-out is
exactly literal, no resilience margin).

This script makes that blind spot observable, without changing the
coordinator logic under test at all: it starts the same 5-node cluster
experiments/run_comparison.py's leaderless sweep uses, but gives a
*subset* of the nodes (--delayed-node-count, default 2 of 5) the new
(opt-in, off-by-default) --fault-inject-delay-ms flag, so those specific
peers take a deliberate, artificial moment before acknowledging a
replicated write while the rest respond normally.

What was tried
--------------
The first attempt delayed *every* node by the same fixed amount. That
does not work, at any delay size -- 0.00% staleness at 0ms, 200ms, and
even a full 1000ms/peer. The reason is structural, not a matter of
picking a bigger number: W=2 only waits for one peer ack (`needed=1`
beyond the coordinator's own local write). With every peer under an
identical fixed delay, all four of a write's replicate calls finish in
near lockstep -- so the first ack to satisfy `needed=1` (which is what
lets the coordinator return success to the client, which is in turn what
lets the load test's ground truth for the read comparison below get
populated at all) arrives at essentially the same instant every *other*
peer also finishes. Delaying the whole cluster uniformly just moves the
entire flood later together; it never creates a gap between "the
client was told this write succeeded" and "this write has actually
reached everywhere."

Delaying only a subset of nodes breaks that symmetry: a write's
coordinator can satisfy `needed=1` from one of the *undelayed* peers
almost immediately, returning success to the client while the delayed
peers are still asleep -- a real, observable window in which a read
landing on one of those still-catching-up nodes sees stale data. This is
also a more realistic fault model than uniform delay would have been --
"some replicas are slower than others" is an ordinary, common real-world
condition, not a contrived one.

But the subset has to be large enough, specifically relative to R, not
just nonzero: with only 2 of 5 nodes delayed, staleness was *still*
0.00% (tried at 300ms/peer). The read path resolves R responses by
highest timestamp, not by requiring every sampled node to be current --
so a read only sees stale data if *every one* of its R sampled nodes
happens to still be behind. With R=3 and only 2 nodes ever behind, that's
structurally impossible: at least 1 of any 3 sampled nodes is always one
of the 3 always-current nodes, and its fresher timestamp always wins.
Raising delayed-node-count to 3 (one more than can be structurally
covered by R=3) was what actually produced nonzero staleness (0.41% at
300ms/peer) -- and pushing further to delay-ms=800/delayed-node-count=4
landed on a robust, clearly nonzero, single-digit-percent rate (2.78% and
3.19% across two otherwise-identical runs) without materially slowing
the benchmark down (most writes still get their one required peer ack
from a fast, undelayed node) or introducing new request failures. As a
sanity check, the genuinely-guaranteed `W=3,R=3` config (3+3>5) was run
against this exact same 4-delayed-nodes/800ms setup and stayed at
0.00% -- confirming this setup demonstrates the boundary case
specifically, not just "the cluster is flaky now."

This is an artificially handicapped run, not a measurement of the real
implementation under real load -- same honesty standard as the rest of
docs/results.md. It exists to demonstrate that the boundary case is real
and reachable, not to characterize this project's actual local
performance (see the main sweep in docs/results.md for that). It is
intentionally kept out of experiments/run_comparison.py and out of the
main results table for exactly that reason.

Run:
    python3 -m experiments.leaderless_boundary_case_demo [--delay-ms N] [--delayed-node-count N]
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
from pathlib import Path

import httpx

from experiments import leaderless_staleness_load_test
from experiments._load_test_common import LoadTestResult
from experiments._results_doc import RESULTS_HEADER, RESULTS_HEADER_MARKER, replace_section
from experiments.run_comparison import (
    LEADERLESS_HOST,
    LEADERLESS_NODE_PORTS,
    REPO_ROOT,
    RESULTS_PATH,
    NodeStartupError,
    SpawnedNode,
    _spawn,
    _stop_all,
    _wait_for_all_healthy,
)

# Chosen empirically -- see docs/results.md's "Demonstrating the W+R
# boundary case" section for what was tried and why these values were
# kept as the defaults. Both overridable for anyone who wants to
# reproduce or extend that exploration themselves.
DEFAULT_DELAY_MS = 800.0
DEFAULT_DELAYED_NODE_COUNT = 4

OUTPUT_DIR = Path(__file__).parent / "output" / "leaderless_boundary_case_demo"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Demonstrate leaderless W=2,R=3's boundary-case staleness "
            "locally, by injecting an artificial replicate delay on a "
            "subset of the cluster's nodes (see this module's docstring "
            "for why a subset, not the whole cluster)."
        )
    )
    parser.add_argument(
        "--delay-ms",
        type=float,
        default=DEFAULT_DELAY_MS,
        help=(
            "Artificial replicate delay in milliseconds, applied to each "
            f"of --delayed-node-count nodes (default: {DEFAULT_DELAY_MS:g})."
        ),
    )
    parser.add_argument(
        "--delayed-node-count",
        type=int,
        default=DEFAULT_DELAYED_NODE_COUNT,
        help=(
            "How many of the 5 cluster nodes get the artificial delay; "
            "the rest are left completely unmodified (default: "
            f"{DEFAULT_DELAYED_NODE_COUNT})."
        ),
    )
    args = parser.parse_args(argv)
    if args.delay_ms < 0:
        parser.error("--delay-ms must be >= 0")
    if not 0 <= args.delayed_node_count <= len(LEADERLESS_NODE_PORTS):
        parser.error(
            f"--delayed-node-count must be between 0 and {len(LEADERLESS_NODE_PORTS)}"
        )
    return args


def _write_results_section(
    args: argparse.Namespace, result: LoadTestResult
) -> None:
    """Write this run's result into docs/results.md's own marked
    section (see experiments/_results_doc.py), leaving the main sweep's
    section -- and anything else in the file -- untouched.

    Mechanical only, by design, same as run_comparison.py's own writer:
    this run's numbers and a timestamp, no hand-authored analysis of
    what they mean. See the top-level README.md's "Results" section for
    that, and this module's own docstring above for the exploration
    that led to --delay-ms/--delayed-node-count's default values.
    """
    generated_at = datetime.now(UTC).isoformat(timespec="seconds")
    stats = result.stats
    lines = [
        "## Demonstrating the W+R boundary case",
        "",
        f"Generated by `experiments/leaderless_boundary_case_demo.py` on {generated_at}, "
        f"against `W=2,R=3` with `--delay-ms {args.delay_ms:g} "
        f"--delayed-node-count {args.delayed_node_count}` ({args.delayed_node_count} of "
        f"{len(LEADERLESS_NODE_PORTS)} nodes fault-injected with a {args.delay_ms:g}ms "
        "replicate delay each). See the top-level `README.md`'s \"Results\" section for "
        "what this demonstrates and why, and this script's own module docstring for the "
        "exploration that led to these default parameters.",
        "",
        "| Staleness rate | Comparable reads | Stale reads | Failures | Elapsed | Total requests |",
        "|---|---|---|---|---|---|",
        f"| {stats.staleness_rate:.2f}% | {stats.comparable_reads} | {stats.stale_reads} | "
        f"{stats.total_failures} | {result.elapsed:.1f}s | {stats.total_requests} |",
        "",
        f"Raw stale-read/failure JSONL logs: `{OUTPUT_DIR.relative_to(REPO_ROOT)}/`.",
    ]
    replace_section(RESULTS_PATH, RESULTS_HEADER_MARKER, RESULTS_HEADER)
    replace_section(RESULTS_PATH, "boundary-case-demo", "\n".join(lines))


async def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    nodes: list[SpawnedNode] = []

    async with httpx.AsyncClient() as client:
        try:
            print(
                f"--- boundary-case demo: starting 5-node leaderless cluster "
                f"({args.delayed_node_count} of {len(LEADERLESS_NODE_PORTS)} nodes "
                f"fault-injected with a {args.delay_ms:g}ms replicate delay) ---"
            )
            for i, port in enumerate(LEADERLESS_NODE_PORTS, start=1):
                node_args = ["--node-id", f"node{i}", "--port", str(port)]
                if i <= args.delayed_node_count:
                    node_args += ["--fault-inject-delay-ms", str(args.delay_ms)]
                nodes.append(
                    _spawn(
                        "leaderless.node",
                        node_args,
                        f"boundary_demo_node{i}",
                        LEADERLESS_HOST,
                        port,
                    )
                )
            await _wait_for_all_healthy(client, nodes)
            print("--- boundary-case demo: cluster healthy, running W=2,R=3 load test ---")

            result = await leaderless_staleness_load_test.run_load_test(
                w=2,
                r=3,
                verbose=True,
                write_logs=True,
                output_log_path=OUTPUT_DIR / "W_2__R_3_delayed_stale.jsonl",
                failure_log_path=OUTPUT_DIR / "W_2__R_3_delayed_failures.jsonl",
            )

            print(
                f"\n=== boundary-case demo result (W=2,R=3, "
                f"{args.delayed_node_count}/{len(LEADERLESS_NODE_PORTS)} nodes delayed "
                f"{args.delay_ms:g}ms) ==="
            )
            print(
                f"staleness={result.stats.staleness_rate:.2f}% "
                f"failures={result.stats.total_failures} "
                f"elapsed={result.elapsed:.1f}s"
            )
            _write_results_section(args, result)
            print("result written to docs/results.md's boundary-case-demo section")
        except NodeStartupError as exc:
            print(f"!!! boundary-case demo SKIPPED: {exc} !!!")
        finally:
            _stop_all(nodes)


if __name__ == "__main__":
    asyncio.run(main())
