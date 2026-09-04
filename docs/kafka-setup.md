# Kafka setup (message-queue replication infrastructure)

Infrastructure for the message-queue replication strategy (the
`message_queue/` package, mirroring `leader_follower/` and `leaderless/` --
built and integrated, see README.md's Architecture and Results sections
for the strategy itself). This document only covers the broker and the
standalone smoke test that proves it works in isolation, independent of
any replication logic built on top of it.

## What this is, and isn't

- **Is:** a single-node Kafka broker in KRaft mode (no ZooKeeper), run via
  Docker, plus `experiments/kafka_smoke_test.py`, a throwaway script that
  proves produce/consume and key-based partition routing work correctly.
- **The broker process itself isn't managed by any harness.** Unlike the
  leader-follower/leaderless node processes (spawned fresh per config,
  torn down after each run), this broker is meant to be started once and
  left running, the way a real Kafka cluster would be -- start/stop it
  yourself, whenever you need it.
- **What *is* managed by a harness is the topic/consumer-group lifecycle
  on top of that persistent broker.** `experiments/run_comparison.py`'s
  `run_mq_configs` spawns the producer/follower processes per sweep
  config and calls `message_queue/topics.py`'s `reset_topic()` between
  them (deleting and recreating the topic so each config starts from a
  clean slate), and `tests/test_message_queue.py`'s
  `kafka_integration`-marked tests are a real test harness that depends
  on a broker being reachable -- see that file's own docstring for how it
  self-skips (not fails) when one isn't. Neither of these starts or stops
  the broker process itself, only what runs against it once it's up.
- **CI runs its own, separate broker.** `.github/workflows/tests.yml`'s
  `test-mq` job spins up its own Kafka service container (same image/
  config shape as `docker-compose.kafka.yml`, kept in sync by hand) for
  the duration of that job only -- it has nothing to do with, and doesn't
  touch, whatever broker `docker-compose.kafka.yml` may have started on
  your own machine. If you're debugging a CI-only failure in the
  `test-mq` job, you're looking at that ephemeral service container, not
  your local one.

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

The broker listens on `localhost:9092` for any host-run Python process --
the smoke test, `message_queue/`'s producer and followers, and
`tests/test_message_queue.py`'s integration tests alike -- to connect to
directly. No container-network address translation to worry about, since
nothing else runs inside Docker in this project.

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
