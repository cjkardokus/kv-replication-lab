# kv-replication-lab

A distributed key-value store used to explore how replication strategy and
configuration choices — leader-follower vs. leaderless, quorum settings,
acknowledgment thresholds — affect performance, consistency, and fault
tolerance.

This project implements three replication strategies side by side against
the same underlying key-value data model, with tunable parameters so the
trade-offs between them (concepts drawn from *Designing Data-Intensive
Applications*) are directly observable through experiments rather than
theoretical.

## Status

Early development. Local, single-machine implementation in progress. An AWS
deployment phase (via Terraform, on EC2) is planned as a follow-up once the
local version is working and validated — see [Roadmap](#roadmap).

A third replication strategy (message-queue/Kafka-based) is now implemented
and benchmarked alongside the two below — see the Architecture and Results
sections. It runs against a local, single-node Kafka broker (KRaft mode, no
ZooKeeper); see [`docs/kafka-setup.md`](docs/kafka-setup.md) for how to start
it and run its own smoke test.

## Architecture

Three replication strategies, sharing a common storage/server layer:

### Leader-follower

- One leader node accepts all writes and pushes them to N follower nodes.
- **`ack_required`** (0 to N) controls how many followers must acknowledge a
  write before the leader responds to the client — a tunable trade-off
  between write latency and durability, rather than a binary sync/async
  choice.
- Followers serve reads directly (not leader-only), which makes replication
  lag/staleness observable — a follower can briefly serve a stale value
  after a low-`ack_required` write.
- Failed acknowledgments (timeout) fail the write loudly rather than
  returning a partial-success response.

### Leaderless (quorum-based)

- N symmetric nodes, no leader. Each request specifies **R** (read quorum)
  and **W** (write quorum), configurable per-request within the bounds of N.
- Conflict resolution uses **last-write-wins** (wall-clock timestamp, with
  node ID as a tiebreaker) for now.
- No read-repair yet — planned for a future iteration.

### Message-queue (Kafka-based)

- One producer node accepts all writes and publishes each one to a Kafka
  topic, partitioned by a hash of the key (so every write to a given key
  lands in the same partition and stays strictly ordered within it). The
  producer **acks the client as soon as the write is durably in the Kafka
  log** — before any follower has consumed it, let alone applied it.
- This is the core distinction from the other two strategies: leader-follower
  and leaderless both have the coordinating node wait on some number of
  replica acknowledgments (`ack_required`, or `W`) before acking the client.
  This strategy has no such wait at all — replication to followers happens
  entirely asynchronously, decoupled from the client-facing write path.
  Staleness here is therefore bounded by **consumer lag/throughput, not by
  network/ack timing** — a different kind of unboundedness than either other
  strategy can express: theirs is bounded by picking how many replicas to
  wait for (with a mathematical floor at `ack_required=N`/`W=N`); this one
  has nothing to wait for in the first place, so its bound is "how fast can
  a follower keep up," not "how many replicas said yes."
- Each follower runs its **own independent Kafka consumer group** and
  consumes the entire topic (every partition), applying writes to its own
  local copy as they arrive — full-topic replication, the same
  every-node-holds-a-complete-copy guarantee leader-follower's followers and
  leaderless's peers both give. This is the architecturally correct design
  for what this project models, not a simplification: a **shared**,
  work-splitting consumer group (Kafka's standard pattern for scaling
  consumption, where each partition — and so each portion of the keyspace —
  is handled by only one group member) would mean each follower holds only
  part of the data, i.e. **sharding**, not replication. That's a genuinely
  different architecture answering a different question (how to scale
  reads/storage across nodes) from the one this project compares (how many
  copies of the same data exist, and how consistent are they) — see
  [Roadmap](#roadmap).
- Followers serve reads directly from their local copy, exactly like
  leader-follower's followers — no coordination, no waiting on the consumer
  to "catch up." Each follower's real-time consumer lag (broker log-end
  offset minus that follower's own last committed offset) is directly
  queryable, not just inferred from staleness — see Results.

### Shared

- Versioning uses wall-clock timestamps. **Known limitation:** clock skew
  across independent machines can cause LWW to pick the wrong "winner."
  This is invisible in local testing (all nodes share one system clock) but
  becomes real once nodes run on separate EC2 instances — see
  [Roadmap](#roadmap).
- Cluster topology is static config (YAML), not dynamic discovery.

## Results

`docs/results.md` is auto-generated (by `experiments/run_comparison.py`,
`experiments/leaderless_boundary_case_demo.py`, and
`experiments/mq_lag_demo.py` — see that file's own header) and contains only
tables, raw numbers, and a run timestamp: no hand-authored
analysis, on purpose. An earlier version of this project hardcoded that kind
of analysis directly into the generator script, as a fixed string
reproduced in every report regardless of what the table above it actually
said — it went stale the first time the underlying numbers changed, and
nothing forced it to be updated. This section is that analysis instead:
hand-maintained here, referencing the current table and the raw JSONL
logs under `experiments/output/` rather than duplicating
numbers that can drift out of sync with them. Specific percentages below
were current as of the run cited in `docs/results.md` at the time this was
written — rerun the sweep (`python3 -m experiments.run_comparison`) for
today's numbers; this benchmark is unthrottled and unseeded, so exact
figures vary somewhat run to run even when nothing about the code changed.

### `ack_required` trades off latency, failures, and staleness — not a straight line

At `ack_required>=1`, the leader-follower write path awaits at least one
follower ack before returning, which naturally bounds how many replicate
calls can be in flight at once. `ack_required=1..3` show a clean monotonic
staleness staircase (roughly 21% down to 4% in the run cited) with zero
internal exceptions logged on the leader; their small nonzero failure
counts are ordinary `503`s from a quorum genuinely not reached within
`timeout_seconds` under real, timing-sensitive load, not exceptions.
`ack_required=4` (fully synchronous) sits at exactly 0.00% — a mathematical
floor, not just an observation (see below).

`ack_required=0` (fire-and-forget) has no such bound, and its numbers used
to tell a different, misleading story: an undersized httpx connection pool
meant a burst of unthrottled writes could starve the *leader's own* event
loop badly enough to produce thousands of client-visible failures and a
~3x elapsed-time blowup on a full sweep — looking like the leader was
crashing, not like a staleness trade-off. Enlarging the connection pool
(now `common/replication_client.py`, shared by both replication
strategies) fixed the immediate symptom, but also revealed that the same
undersized pool had been silently masking real replication lag at *every*
`ack_required` value, not just 0 — isolation-testing the pool change alone
at `ack_required=2` (same load, same followers) measured 0.00% staleness
under the old pool vs. high single digits under the enlarged one. The pool
was never a per-`ack_required` setting, so enlarging it for 0's sake
unmasked the 1..4 staircase at the same time.

Enlarging the pool alone didn't fix `ack_required=0`'s actual missing
backpressure, though — it only relocated where it broke. `Replicator` now
bounds concurrent in-flight replicate calls directly with a semaphore
(`_MAX_CONCURRENT_REPLICATE_CALLS` in `leader_follower/leader.py`),
independent of connection-pool size. That's a materially different story:
`ack_required=0` no longer starves the leader's event loop (in the run
cited: 1 failure, 26.2s elapsed — one of the *faster* configs, not the
slowest), but staleness rose sharply instead (92.58%) — replicate calls
now queue for a semaphore slot rather than flooding straight into an
overloaded follower, and that queueing is exactly what a subsequent read
can catch mid-flight. `ack_required=0` is now a cleaner demonstration of
fire-and-forget's real trade-off (bounded latency, no failure storm, paid
for in staler reads under load) than a leader-crash-shaped failure storm
standing in for it.

leaderless has no equivalent semaphore yet: `QuorumCoordinator` floods
every peer on every write exactly the way `Replicator` does, so it has the
same theoretical unbounded-fan-out shape — but that's never been measured
or confirmed to actually cause leaderless the same failure mode, and
adding the fix without that measurement would be guessing, not fixing
(see `leader_follower/leader.py`'s own comment on
`_MAX_CONCURRENT_REPLICATE_CALLS` for the fuller reasoning).

### `ack_required=4` is a mathematical floor, not a favorable roll

Once every follower has acked a write, every follower's stored timestamp
for that key is `>=` that write's timestamp, and LWW never regresses — so
a later read of any follower can never see anything older than the newest
write that ever reached full quorum. This isn't specific to this lab's
implementation; it's the general property that makes
"`ack_required = len(followers)`" a meaningful phrase at all. (Earlier
sweeps measured a small nonzero rate here despite the guarantee — that
turned out to be a measurement artifact in the load test itself, not the
protocol: a readback GET could race a different concurrent write to the
same key. Fixed by having `PutResponse` echo back the timestamp it
stamped directly, so the load test never needs a separate readback — see
`common/server.py`'s `PutResponse`.)

### Leaderless staleness stays near-zero locally — a benchmark-speed blind spot, not proof of a guarantee

`W=1,R=5`, `W=5,R=1`, and `W=3,R=3` are legitimately guaranteed by the
classic W+R>N overlap rule. `W=1,R=1` and `W=2,R=3` are **not**
(`1+1=2`, `2+3=5`, neither `>5`) — `QuorumCoordinator` contacts exactly
the peers each config's literal W/R implies, no resilience margin quietly
padding coverage (see `tests/test_leaderless.py`'s exact-fan-out tests).
Both still measure at or near 0.00% locally, though, because every write
floods every peer unconditionally regardless of W, and that flood
finishes in low single-digit milliseconds on loopback — far faster than
this benchmark's real request-arrival rate. That's a genuine
local-testing blind spot (the same category as the clock-skew limitation
above), not evidence the boundary case doesn't exist — see below for how
it's made observable anyway.

### Demonstrating the W+R boundary case

`leaderless/node.py` has an opt-in, off-by-default fault-injection flag
(`--fault-inject-delay-ms` / `FAULT_INJECT_REPLICATE_DELAY_MS`) purely to
make the blind spot above reproducible locally: an artificial sleep in a
node's `/internal/replicate` handler, before the write is applied, before
that node acknowledges it — disabled, that handler is
`common/server.py`'s stock one, byte-for-byte, and every config in the
main sweep ran with it completely off.
`experiments/leaderless_boundary_case_demo.py` runs the same 5-node
cluster with this flag enabled on a *subset* of the nodes and re-runs the
`W=2,R=3` load test against it — an artificially handicapped run, not a
measurement of the real implementation under real load, kept deliberately
out of the main sweep for exactly that reason.

The subset matters, not just the delay size: delaying *every* node
uniformly never produces nonzero staleness, at any delay tested up to a
full second — `W=2` only waits for one peer ack, so with every peer
delayed identically, that one ack (which is what lets a client-visible
write succeed at all) arrives right as every other peer also finishes,
never opening a gap. Delaying only some nodes breaks that symmetry, but
the delayed count has to exceed what R can structurally cover — with only
2 of 5 nodes ever delayed, `R=3`'s highest-timestamp resolution always has
an undelayed, current node among its 3 samples, so it can never observe
staleness either. Delaying 3+ nodes (the script's default: 4 of 5, at
800ms/peer) is what actually produces a robust, repeatable,
low-single-digit-percent staleness rate (3.63% in the run cited) — while
the genuinely-guaranteed `W=3,R=3`, run against the identical fault
conditions, stayed at 0.00%, confirming the effect is specific to
`W=2,R=3`'s real lack of a guarantee, not general flakiness. See that
script's own module docstring for the full exploration (what delay/subset
sizes were tried and why they were or weren't enough), and the current
`docs/results.md` for this run's actual numbers.

### Message-queue staleness tracks load directly — there's no ack-wait knob to trade off

Unlike `ack_required` or `W`/`R`, the message-queue strategy has no
client-facing wait-and-ack setting at all (see Architecture above) — so its
sweep (`experiments/mq_staleness_load_test.py`'s `--workers`, swept by
`experiments/run_comparison.py`'s `run_mq_configs`) varies load intensity
instead of an ack threshold, and its own section of `docs/results.md`
(mq-sweep) shows a clean, monotonic staleness rise with load, reproduced
across independent runs: roughly 1.5% at the lowest worker count, up through
high single digits, into the 30s-40% range at the highest — see that
section for the current run's exact figures. A follower's Kafka consumer
genuinely falls further behind the producer's write rate as concurrent load
increases; that's the whole trade-off this strategy exists to demonstrate,
made directly visible rather than argued for.

Getting a clean signal took a real fix, not just running the sweep: the
first attempt measured close to 100% staleness at *every* worker count — an
artifact of the test harness, not the replication strategy. The node-startup
check `run_comparison.py` already uses for the other two strategies only
confirms a follower's HTTP server is serving (`/health`); for a
message-queue follower, that happens *before* its background Kafka consumer
has actually joined its consumer group and been assigned partitions, which
takes real (if usually brief) time, and the load test's reads were starting
well before it did. `_wait_for_mq_followers_assigned` (polling the
follower's own `/internal/lag` endpoint until it reports a real partition
assignment) closed that race — unmasking the genuine, load-driven staircase
above.

Also worth noting for anyone reading `docs/results.md`'s raw per-config
JSONL logs: `lag_at_stale_read` is sampled only on a *detected-stale* read,
never unconditionally on every read. An unconditional per-read lag query
would add a round trip to every single read (not just the small stale
fraction), changing this script's own timing/throughput characteristics
relative to the other two strategies' load tests, which query nothing extra
per read — comparing elapsed times across strategies would stop being
apples to apples. Sampling only on staleness keeps this script's per-read
cost identical to theirs, at the cost of that field being a point-in-time
diagnostic sample rather than a continuous measurement.

### Demonstrating message-queue consumer lag directly, not just inferring it from staleness

The sweep above shows staleness rising with load, but "consumer lag" is
still an inference from that number unless it's measured directly.
`message_queue/follower.py`'s opt-in, off-by-default fault-injection flag
(`--fault-inject-consume-delay-ms`) — the same pattern as leaderless's
`--fault-inject-delay-ms` above — sleeps for a fixed duration before
applying/committing each consumed message, purely to manufacture a real,
controllable backlog on demand. `experiments/mq_lag_demo.py` runs the same
4-follower cluster with this flag enabled on a *subset* of the followers
(default: 2 of 4, 500ms/message) at an otherwise-modest load level, then
reports each follower's actual lag (via its own `/internal/lag`) immediately
after the run.

The result ties staleness directly to measurable lag rather than leaving
the connection implicit: in the run cited in `docs/results.md`'s
mq-lag-demo section, the two delayed followers ended the run with a lag of
2976 messages each — still visibly working through their backlog — while
the two undelayed followers, consuming the identical topic with no
artificial slowdown, had already drained theirs (9 and 0). Overall staleness
for that run (68.72%) landed well above this same worker count's
undelayed baseline in the mq-sweep table, consistent with the delayed
followers' backlog being exactly what a fraction of reads were landing on.

## Roadmap

The project is being built in deliberate stages, starting narrow and adding
complexity once each stage is validated:

1. **Local, single-machine version** (current stage) — all three replication
   strategies working correctly, with experiments demonstrating the
   ack/quorum/load trade-offs described above.
2. **AWS deployment** (planned fork) — nodes deployed to separate EC2
   instances via Terraform, turning the clock-skew limitation above into a
   concrete, demonstrable experiment rather than a theoretical caveat.
3. **Future iterations** (not yet scheduled): fault injection (killing nodes
   mid-write to observe failure behavior), read-repair for the leaderless
   path, read-your-own-writes consistency, and a Redis caching layer to
   explore cache staleness alongside replication staleness. Beyond those:

   - A **sharded/partitioned message-queue variant**, using standard Kafka
     consumer-group work-splitting (a *shared* consumer group across
     followers, so each one holds only part of the keyspace) instead of
     `message_queue/follower.py`'s current design (one independent group
     per follower, full-topic replication on every one). This would be a
     genuinely different architecture from the full-replication strategies
     compared here, not an extension of this one — see Architecture above.
   - A **multi-broker Kafka cluster**, for real inter-broker
     replication/leader election (currently a single-node broker, per
     `docker-compose.kafka.yml`) — deferred alongside the AWS deployment
     stage above, for the same reason leader-follower/leaderless's own
     topology has stayed local/single-machine through this hardening pass:
     RAM constraints on the local dev machine this project has been built
     on so far.
   - A shared `ClusterConfig` base for leader_follower, leaderless, *and*
     message_queue (currently duplicated scaffolding —
     `from_dict`/`from_yaml`/`_validate_*`/`with_*` — around
     differently-shaped config fields). Still deliberately deferred, now
     with all three shapes known rather than two:
     `message_queue/config.py`'s `MQConfig` turned out to share nothing
     with the other two beyond the `from_dict`/`from_yaml` pattern itself
     — no peer/follower address list at all, since Kafka itself is the
     coordination mechanism, not this project's own HTTP fan-out — which
     confirms the original deferral was the right call rather than just
     settling it. A shared base remains possible, just clearly narrower
     than it would have looked from only two examples: the real
     duplication across all three is the *pattern*
     (`from_dict`/`from_yaml`/validation), not shared *fields*.

## Why this project

Built to make the trade-offs described in *Designing Data-Intensive
Applications*'s replication chapter concrete and measurable, rather than
just implementing "a" distributed key-value store. The goal is a project
where a specific design choice (e.g., `ack_required=0` vs. `ack_required=N`)
produces a visible, explainable difference in behavior — not just several
working implementations.

## License

[MIT](LICENSE)
