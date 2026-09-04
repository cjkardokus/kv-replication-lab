# Kafka setup (message-queue replication infrastructure)

Infrastructure for the upcoming message-queue replication strategy (a
`message_queue/` package, mirroring `leader_follower/` and `leaderless/` --
not built yet). This document only covers the broker and the standalone
smoke test that proves it works; there's no replication logic to run yet.

## What this is, and isn't

- **Is:** a single-node Kafka broker in KRaft mode (no ZooKeeper), run via
  Docker, plus `experiments/kafka_smoke_test.py`, a throwaway script that
  proves produce/consume and key-based partition routing work correctly.
- **Isn't:** managed by `experiments/run_comparison.py` or any test
  harness. Unlike the leader-follower/leaderless node processes (spawned
  fresh per config, torn down after each run), this broker is meant to be
  started once and left running, the way a real Kafka cluster would be --
  start/stop it yourself, whenever you need it.

## Starting and stopping the broker

```bash
# Start (detached, stays running in the background)
docker compose -f docker-compose.kafka.yml up -d

# Check it's healthy
docker compose -f docker-compose.kafka.yml ps

# Stop (data persists in a named Docker volume)
docker compose -f docker-compose.kafka.yml down

# Stop AND discard all topics/messages (fresh cluster next `up`)
docker compose -f docker-compose.kafka.yml down -v
```

The broker listens on `localhost:9092` for any host-run Python process
(the smoke test now, `message_queue/` nodes later) to connect to directly
-- no container-network address translation to worry about, since nothing
else runs inside Docker in this project.

## Running the smoke test

With the broker up:

```bash
python3 -m experiments.kafka_smoke_test
```

It deletes-and-recreates a 4-partition topic (so it's safe to rerun any
number of times), produces several messages across several keys, and
verifies two things empirically rather than by assumption:

1. **Partition routing is deterministic by key.** The same key always
   lands on the same partition (a hash of the key, not round-robin or
   random) -- this is required for correctness once real replication
   logic exists: LWW-by-arrival-order only works if every write to a
   given KV key is strictly ordered within one partition.
2. **Per-partition order is preserved** end to end, from produce through
   consume-from-earliest.

It prints a partition-routing table and a pass/fail summary, and exits
nonzero if either check fails.

**Cold-start note:** on a broker that was *just* started (or just had a
topic created on it for the first time), you may see a burst of retried
`NotLeaderForPartitionError` / `GroupCoordinatorNotAvailableError`
messages while internal metadata (partition leadership, the
`__consumer_offsets` topic) settles. aiokafka retries through this
automatically and the script still passes -- it's noise, not a failure.
Give the broker a few extra seconds after `up` if you'd rather not see it
at all.

## Why KRaft, why this image

Kafka's ZooKeeper mode is deprecated and removed outright starting in
Kafka 4.x -- KRaft (Kafka's own Raft-based metadata quorum, no separate
ZooKeeper process) is the current standard way to run Kafka, including
locally. `docker-compose.kafka.yml` uses `apache/kafka`, the official
upstream image (as opposed to a third-party distribution), pinned to
`4.1.0`, which only supports KRaft -- there's no "which mode" footgun to
get wrong.

A single container plays both the controller and broker roles
(`KAFKA_PROCESS_ROLES: broker,controller`), which is the standard
single-node local setup; a real multi-node deployment would split those
roles across machines.
