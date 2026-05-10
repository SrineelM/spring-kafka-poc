# Cooperative Sticky Rebalancing — Production Guide

## Overview

This document explains how Cooperative Sticky Rebalancing is implemented in this project, why it matters for production Kafka consumers, and how to operate it.

---

## 1. Eager vs. Cooperative Rebalance

### Eager Rebalance (Legacy — Default before Kafka 2.4)

The original rebalance protocol is a **"stop the world"** event:

```
Scale 3 consumers → 4 consumers on a 9-partition topic:

STEP 1: ALL consumers revoke ALL partitions simultaneously
  Consumer-A revokes: {0, 1, 2}   ← stops processing
  Consumer-B revokes: {3, 4, 5}   ← stops processing
  Consumer-C revokes: {6, 7, 8}   ← stops processing

STEP 2: Group coordinator reassigns from scratch
  Consumer-A gets: {0, 1}
  Consumer-B gets: {3, 4}
  Consumer-C gets: {6, 7}
  Consumer-D gets: {2, 5, 8}     ← new consumer

DOWNTIME: All 9 partitions paused during reassignment
```

**Problems:**
- Full partition revocation causes consumer lag spikes.
- In a topic with millions of records/second, even 1–2 seconds of downtime causes significant lag.
- Every rolling restart in Kubernetes triggers a full group rebalance.

### Cooperative Sticky Rebalance (KIP-429 — Default in Kafka 3.1+)

Only the partitions that **must move** are revoked. All other partitions continue processing without interruption.

```
Scale 3 consumers → 4 consumers on a 9-partition topic:

REBALANCE ROUND 1 — Incremental Revocation:
  Consumer-A revokes only: {2}    ← continues {0, 1}
  Consumer-B revokes only: {5}    ← continues {3, 4}
  Consumer-C revokes nothing      ← continues {6, 7, 8}

REBALANCE ROUND 2 — Assignment to new consumer:
  Consumer-D assigned: {2, 5}

DOWNTIME: Only partitions {2, 5} paused briefly — 7/9 partitions kept processing
```

**Benefits:**
- **Minimal partition movement**: only the partitions that must change hands are revoked.
- **Zero downtime for stationary partitions**: 7 out of 9 partitions never paused.
- **Sticky assignment**: existing consumer–partition pairings are preserved across rebalances, protecting warm caches and local state stores.

---

## 2. Sticky Assignment

The `CooperativeStickyAssignor` combines two concepts:

| Concept | Meaning |
|---|---|
| **Cooperative** | Rebalance is incremental — only partitions that MUST move are revoked |
| **Sticky** | The assignor tries to keep existing consumer→partition bindings stable |

**Why stickiness matters:**

- **RocksDB state stores (Kafka Streams):** Rebuilding state from a changelog after a reassignment causes a "cold start" that can take minutes.
- **Application-level caches:** A consumer that has been warming a lookup cache for partition 3 for hours would lose that state on reassignment.
- **Predictable load distribution:** Operators can reason about which pod owns which partitions.

---

## 3. Static Membership (`group.instance.id`)

Static membership is the complement to cooperative rebalancing for **rolling restarts**.

### Without static membership (default dynamic membership):

```
Pod-1 restarts:
  1. Pod-1 sends LeaveGroup
  2. Broker triggers rebalance for all consumers immediately
  3. Pod-1 rejoins after ~30s startup
  4. Second rebalance triggered to assign partitions back to Pod-1
```

### With static membership (`group.instance.id=pod-1`):

```
Pod-1 restarts:
  1. Pod-1 disconnects (does NOT send LeaveGroup)
  2. Broker waits up to session.timeout.ms (45s) for pod-1 to reconnect
  3. Pod-1 reconnects within 45s (Spring Boot typically restarts in <15s)
  4. Broker assigns its prior partitions {0,1,2} directly — NO rebalance
```

**Configuration:**

```yaml
# In application.yml (override per pod via environment variable)
# The CooperativeStickyConsumerConfig reads POD_NAME from the environment:
environment:
  POD_NAME: pod-1   # Set uniquely per Kubernetes pod
```

**Kubernetes `StatefulSet` pattern:**
```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name  # e.g., app-0, app-1, app-2
```

---

## 4. Key Files

| File | Purpose |
|---|---|
| `config/CooperativeStickyConsumerConfig.java` | Consumer factory + container factory wired with `CooperativeStickyAssignor` |
| `consumer/CooperativeStickyRebalanceListener.java` | Logs partition assignments/revocations for operational visibility |
| `consumer/CooperativeStickyTransactionConsumer.java` | Sample `@KafkaListener` with idempotent processing pattern |
| `docker-compose-kraft.yml` | 3-instance demo with KRaft (no ZooKeeper) and 9-partition topic |

---

## 5. Production Tuning Reference

| Property | Recommended Value | Rationale |
|---|---|---|
| `partition.assignment.strategy` | `CooperativeStickyAssignor` | Incremental rebalance, sticky assignments |
| `session.timeout.ms` | `45000` | 3 × heartbeat interval; tolerates GC pauses without false eviction |
| `heartbeat.interval.ms` | `15000` | 1/3 of session timeout per Kafka docs |
| `max.poll.interval.ms` | `300000` (5 min) | Must exceed your slowest batch processing time |
| `max.poll.records` | `100` | Limits per-poll batch size for predictable processing time |
| `enable.auto.commit` | `false` | Manual offset commit after successful processing only |
| `group.instance.id` | `${POD_NAME}` | Static membership; eliminates rolling-restart rebalances |
| `concurrency` | = partition count | 1 thread per partition = maximum throughput, no idle threads |
| `group.initial.rebalance.delay.ms` | `3000` (broker-side) | Gives all instances time to join before first rebalance |

---

## 6. Idempotent Processing

Cooperative rebalance does **not** eliminate redelivery. You must always handle duplicates:

```
Scenario: Consumer-A is processing partition 3, offset 5000
  → Consumer-A crashes before calling ack.acknowledge()
  → Partition 3 is reassigned to Consumer-B
  → Consumer-B starts from last committed offset (e.g., 4990)
  → Records 4990–5000 are REDELIVERED to Consumer-B
```

**Guards implemented in `CooperativeStickyTransactionConsumer`:**

1. **Manual ack mode** (`MANUAL_IMMEDIATE`): offsets committed only after `ack.acknowledge()` succeeds.
2. **Idempotency check pattern**: DB lookup or unique constraint rejects duplicate inserts silently.
3. **Exception re-throw**: on failure, the record is not acked — the error handler retries or routes to DLT.

---

## 7. Running the Demo

### Prerequisites
```bash
# Generate a stable KRaft cluster ID (do this once, store the value)
docker run --rm confluentinc/cp-kafka:7.6.0 kafka-storage random-uuid
# → e.g., MkU3OEVBNTcwNTJENDM2Qk
# Set this in docker-compose-kraft.yml as CLUSTER_ID
```

### Start the stack
```bash
docker compose -f docker-compose-kraft.yml up -d
```

### Watch cooperative rebalance logs
```bash
# Watch all consumer instances simultaneously
docker compose -f docker-compose-kraft.yml logs -f app-1 app-2 app-3

# Look for these log patterns:
# "REBALANCE — onPartitionsRevoked: 2 partition(s) being revoked"
# "REBALANCE — onPartitionsAssigned: 3 partition(s) assigned"
```

### Observe scale-up (3 → 4 consumers)
```bash
# In a second terminal: produce test messages continuously
docker exec kafka-kraft kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic raw-transactions-topic

# In a third terminal: scale up (triggers cooperative rebalance)
docker compose -f docker-compose-kraft.yml up -d --scale app=4

# Expected behaviour:
# - Only 2-3 partitions are revoked (not all 9)
# - onPartitionsRevoked fires with a SMALL list
# - Processing continues on the remaining 6-7 partitions
```

### Observe scale-down (4 → 3 consumers)
```bash
docker compose -f docker-compose-kraft.yml stop app-3

# Expected behaviour:
# - app-3's partitions are redistributed to app-1 and app-2
# - app-1 and app-2 show onPartitionsAssigned with the transferred partitions
# - app-3 shows onPartitionsRevoked just before stopping
```

### Observe static membership (rolling restart with no rebalance)
```bash
# Restart app-1 — it has POD_NAME=pod-1 (group.instance.id=pod-1)
docker compose -f docker-compose-kraft.yml restart app-1

# Expected behaviour:
# - NO rebalance triggered on app-2 or app-3
# - app-1 reconnects within session.timeout.ms and reclaims {0,1,2}
# - Log: "Static membership enabled: group.instance.id=pod-1"
```

---

## 8. Eager vs. Cooperative — Side-by-Side Impact

| Scenario | Eager (RangeAssignor) | Cooperative Sticky |
|---|---|---|
| Scale 3→4 consumers (9 partitions) | All 9 partitions paused | Only 2–3 partitions paused |
| Rolling restart (1 of 3 pods) | Full group rebalance | No rebalance (static membership) |
| Pod crash (no graceful shutdown) | Full group rebalance after session timeout | Partitions redistributed after session timeout; others unaffected |
| Partition lag during rebalance | High (all 9 partitions) | Low (only moving partitions) |
| Cache/state warm-up cost | High (all partitions reassigned) | Low (stationary partitions unaffected) |
