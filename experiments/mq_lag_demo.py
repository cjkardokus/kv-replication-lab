"""Demonstrates message-queue consumer lag on demand, by injecting an
artificial per-message consume delay on a subset of followers.

docs/results.md's own "Demonstrating message-queue consumer lag" section
(written by this script) is mechanical only -- this run's raw numbers,
not the reasoning -- see experiments/_results_doc.py for why.

Why this script exists, separately from experiments/run_comparison.py
----------------------------------------------------------------------
run_mq_configs' own sweep (see docs/results.md's mq-sweep section)
already shows real, non-manufactured staleness rising with load --
unlike experiments/leaderless_boundary_case_demo.py, this strategy
doesn't need fault injection to make its staleness *visible* at all.
What fault injection adds here instead is control: message_queue/
follower.py's --fault-inject-consume-delay-ms lets a subset of followers
be made deliberately, artificially slow, so this script can show two
things the main sweep's aggregate numbers can't by themselves:

  1. Consumer lag is a genuinely *per-follower* phenomenon, not a
     property of the cluster as a whole -- a slow follower falls behind
     while its siblings, consuming the identical topic, stay caught up.
     The main sweep's staleness rate is necessarily an average across
     all 4 followers; this script reports lag broken out per follower
     instead, delayed vs. normal, so that difference is visible
     directly rather than inferred.
  2. Lag (and the staleness it causes) is reproducible on demand at a
     *fixed, otherwise-modest* load level -- --workers below defaults
     to a level the main sweep shows as only mildly stale with no
     delay -- rather than only being observable by cranking load high
     enough to overwhelm every follower at once.

Run:
    python3 -m experiments.mq_lag_demo [--delay-ms N] [--delayed-follower-count N] [--workers N]
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
from pathlib import Path

import httpx

from experiments import mq_staleness_load_test
from experiments._load_test_common import LoadTestResult
from experiments._results_doc import RESULTS_HEADER, RESULTS_HEADER_MARKER, replace_section
from experiments.mq_staleness_load_test import FOLLOWER_HOST as MQ_HOST
from experiments.mq_staleness_load_test import FOLLOWER_PORTS as MQ_FOLLOWER_PORTS
from experiments.mq_staleness_load_test import PRODUCER_PORT as MQ_PRODUCER_PORT
from experiments.run_comparison import (
    REPO_ROOT,
    RESULTS_PATH,
    NodeStartupError,
    SpawnedNode,
    _spawn,
    _stop_all,
    _wait_for_all_healthy,
    _wait_for_mq_followers_assigned,
)
from message_queue.config import MQConfig
from message_queue.topics import reset_topic

MQ_CONFIG_PATH = "config/mq_cluster.yaml"

# Chosen to make the effect obvious without either drowning it in noise
# (too few workers/too short a delay) or making the whole run painfully
# slow (too many delayed followers/too long a delay) -- see this
# module's docstring point 2 for why --workers deliberately isn't the
# highest level in run_mq_configs' own sweep. Overridable for anyone who
# wants to explore other points.
DEFAULT_DELAY_MS = 500.0
DEFAULT_DELAYED_FOLLOWER_COUNT = 2
DEFAULT_WORKERS = 5

OUTPUT_DIR = Path(__file__).parent / "output" / "mq_lag_demo"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Demonstrate message-queue consumer lag on demand, by "
            "injecting an artificial per-message consume delay on a "
            "subset of the 4 followers."
        )
    )
    parser.add_argument(
        "--delay-ms",
        type=float,
        default=DEFAULT_DELAY_MS,
        help=(
            "Artificial per-message consume delay in milliseconds, "
            f"applied to each of --delayed-follower-count followers "
            f"(default: {DEFAULT_DELAY_MS:g})."
        ),
    )
    parser.add_argument(
        "--delayed-follower-count",
        type=int,
        default=DEFAULT_DELAYED_FOLLOWER_COUNT,
        help=(
            "How many of the 4 followers get the artificial delay; the "
            "rest are left completely unmodified (default: "
            f"{DEFAULT_DELAYED_FOLLOWER_COUNT})."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Concurrent load-test worker count (default: {DEFAULT_WORKERS}).",
    )
    args = parser.parse_args(argv)
    if args.delay_ms < 0:
        parser.error("--delay-ms must be >= 0")
    if not 0 <= args.delayed_follower_count <= len(MQ_FOLLOWER_PORTS):
        parser.error(f"--delayed-follower-count must be between 0 and {len(MQ_FOLLOWER_PORTS)}")
    return args


async def _query_final_lag(client: httpx.AsyncClient, port: int) -> int:
    resp = await client.get(f"http://{MQ_HOST}:{port}/internal/lag", timeout=5.0)
    resp.raise_for_status()
    return int(resp.json()["total_lag"])


def _write_results_section(
    args: argparse.Namespace,
    result: LoadTestResult,
    per_follower_lag: dict[int, int],
    delayed_ports: set[int],
) -> None:
    """Write this run's result into docs/results.md's own marked
    section (see experiments/_results_doc.py), leaving the main sweep's
    and mq-sweep's sections -- and anything else in the file -- untouched.

    Mechanical only, by design, same as run_comparison.py's own writers:
    this run's numbers and a timestamp, no hand-authored analysis of
    what they mean. See the top-level README.md's "Results" section for
    that, and this module's own docstring above for why this
    configuration was chosen.
    """
    generated_at = datetime.now(UTC).isoformat(timespec="seconds")
    stats = result.stats
    lines = [
        "## Demonstrating message-queue consumer lag",
        "",
        f"Generated by `experiments/mq_lag_demo.py` on {generated_at}, at "
        f"`--workers {args.workers}` with `--delay-ms {args.delay_ms:g} "
        f"--delayed-follower-count {args.delayed_follower_count}` "
        f"({args.delayed_follower_count} of {len(MQ_FOLLOWER_PORTS)} followers "
        f"fault-injected with a {args.delay_ms:g}ms consume delay each). See the "
        "top-level `README.md`'s \"Results\" section for what this demonstrates "
        "and why, and this script's own module docstring for why this "
        "configuration was chosen.",
        "",
        "| Staleness rate | Comparable reads | Stale reads | Failures | Elapsed | Total requests |",
        "|---|---|---|---|---|---|",
        f"| {stats.staleness_rate:.2f}% | {stats.comparable_reads} | {stats.stale_reads} | "
        f"{stats.total_failures} | {result.elapsed:.1f}s | {stats.total_requests} |",
        "",
        "Final consumer lag per follower, queried immediately after the load test "
        "finished (delayed followers are still working through their backlog at "
        "this point; undelayed ones have typically already drained theirs):",
        "",
        "| Follower | Delayed? | Final lag |",
        "|---|---|---|",
    ]
    for port, lag in per_follower_lag.items():
        lines.append(f"| `{MQ_HOST}:{port}` | {'yes' if port in delayed_ports else 'no'} | {lag} |")
    lines += [
        "",
        f"Raw stale-read/failure JSONL logs: `{OUTPUT_DIR.relative_to(REPO_ROOT)}/`.",
    ]
    replace_section(RESULTS_PATH, RESULTS_HEADER_MARKER, RESULTS_HEADER)
    replace_section(RESULTS_PATH, "mq-lag-demo", "\n".join(lines))


async def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    mq_config = MQConfig.from_yaml(MQ_CONFIG_PATH)
    nodes: list[SpawnedNode] = []

    async with httpx.AsyncClient() as client:
        try:
            print(
                f"--- mq lag demo: resetting topic, starting cluster "
                f"({args.delayed_follower_count} of {len(MQ_FOLLOWER_PORTS)} followers "
                f"fault-injected with a {args.delay_ms:g}ms consume delay) ---"
            )
            await reset_topic(mq_config)

            delayed_ports = set(MQ_FOLLOWER_PORTS[: args.delayed_follower_count])
            followers: list[SpawnedNode] = []
            for i, port in enumerate(MQ_FOLLOWER_PORTS, start=1):
                follower_args = ["--node-id", f"follower{i}_lagdemo", "--port", str(port)]
                if port in delayed_ports:
                    follower_args += ["--fault-inject-consume-delay-ms", str(args.delay_ms)]
                followers.append(
                    _spawn("message_queue.follower", follower_args, f"lagdemo_follower{i}", MQ_HOST, port)
                )
            nodes += followers
            await _wait_for_all_healthy(client, followers)
            # See _wait_for_mq_followers_assigned's own docstring:
            # plain /health isn't enough to know a follower's consumer
            # has actually joined and been assigned partitions.
            await _wait_for_mq_followers_assigned(client, followers)

            producer = _spawn(
                "message_queue.producer",
                ["--node-id", "producer_lagdemo", "--port", str(MQ_PRODUCER_PORT)],
                "lagdemo_producer",
                MQ_HOST,
                MQ_PRODUCER_PORT,
            )
            nodes.append(producer)
            await _wait_for_all_healthy(client, [producer])

            print(f"--- mq lag demo: cluster healthy, running load test (--workers {args.workers}) ---")
            result = await mq_staleness_load_test.run_load_test(
                num_workers=args.workers,
                verbose=True,
                write_logs=True,
                output_log_path=OUTPUT_DIR / "stale.jsonl",
                failure_log_path=OUTPUT_DIR / "failures.jsonl",
            )

            per_follower_lag = {port: await _query_final_lag(client, port) for port in MQ_FOLLOWER_PORTS}

            print(
                f"\n=== mq lag demo result ({args.delayed_follower_count}/{len(MQ_FOLLOWER_PORTS)} "
                f"followers delayed {args.delay_ms:g}ms) ==="
            )
            print(
                f"staleness={result.stats.staleness_rate:.2f}% "
                f"failures={result.stats.total_failures} "
                f"elapsed={result.elapsed:.1f}s"
            )
            for port, lag in per_follower_lag.items():
                marker = "DELAYED" if port in delayed_ports else "normal "
                print(f"  follower {MQ_HOST}:{port} [{marker}]: final lag={lag}")

            _write_results_section(args, result, per_follower_lag, delayed_ports)
            print("result written to docs/results.md's mq-lag-demo section")
        except NodeStartupError as exc:
            print(f"!!! mq lag demo SKIPPED: {exc} !!!")
        finally:
            _stop_all(nodes)


if __name__ == "__main__":
    asyncio.run(main())
