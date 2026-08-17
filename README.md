# kv-replication-lab

A distributed key-value store used to explore how replication strategy and
configuration choices — leader-follower vs. leaderless, quorum settings,
acknowledgment thresholds — affect performance, consistency, and fault
tolerance.

This project implements two replication strategies side by side against the
same underlying key-value data model, with tunable parameters so the
trade-offs between them (concepts drawn from *Designing Data-Intensive
Applications*) are directly observable through experiments rather than
theoretical.

## Status

Early development. Local, single-machine implementation in progress. An AWS
deployment phase (via Terraform, on EC2) is planned as a follow-up once the
local version is working and validated — see [Roadmap](#roadmap).

## Architecture

Two replication strategies, sharing a common storage/server layer:

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

### Shared

- Versioning uses wall-clock timestamps. **Known limitation:** clock skew
  across independent machines can cause LWW to pick the wrong "winner."
  This is invisible in local testing (all nodes share one system clock) but
  becomes real once nodes run on separate EC2 instances — see
  [Roadmap](#roadmap).
- Cluster topology is static config (YAML), not dynamic discovery.

## Roadmap

The project is being built in deliberate stages, starting narrow and adding
complexity once each stage is validated:

1. **Local, single-machine version** (current stage) — both replication
   strategies working correctly, with experiments demonstrating the
   ack/quorum trade-offs described above.
2. **AWS deployment** (planned fork) — nodes deployed to separate EC2
   instances via Terraform, turning the clock-skew limitation above into a
   concrete, demonstrable experiment rather than a theoretical caveat.
3. **Future iterations** (not yet scheduled): fault injection (killing nodes
   mid-write to observe failure behavior), read-repair for the leaderless
   path, read-your-own-writes consistency, and a Redis caching layer to
   explore cache staleness alongside replication staleness.

## Why this project

Built to make the trade-offs described in *Designing Data-Intensive
Applications*'s replication chapter concrete and measurable, rather than
just implementing "a" distributed key-value store. The goal is a project
where a specific design choice (e.g., `ack_required=0` vs. `ack_required=N`)
produces a visible, explainable difference in behavior — not just two
working implementations.
