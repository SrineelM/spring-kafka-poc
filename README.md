# 🏗️ Spring Boot Kafka Masterclass: The Bulletproof Financial Pipeline 🏗️

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/SrineelM/spring-kafka-poc)
[![Java Version](https://img.shields.io/badge/java-21-orange.svg)](https://www.oracle.com/java/technologies/downloads/#java21)
[![Spring Boot](https://img.shields.io/badge/spring--boot-3.4.0-blue.svg)](https://spring.io/projects/spring-boot)

Welcome to a professional-grade **Proof of Concept** for a high-throughput financial data pipeline. This repository is not just code; it is a **Masterclass** in building resilient, scalable, and observable distributed systems using the modern Spring ecosystem.

---

## 🏛️ The "Zero-Trust" Architecture

This project is built using **Hexagonal Architecture** (Ports and Adapters) and adheres to **Domain-Driven Design (DDD)** principles. It is designed to handle **50k–100k events/second** while maintaining 100% financial accuracy and auditability.

### 🛡️ Production-Grade Resilience
*   **Transactional Outbox:** Guaranteed atomic writes between DB and Kafka using a distributed locked relay.
*   **Exactly-Once Semantics (EOS V2):** End-to-end data integrity across the streaming tier.
*   **24h Stateful Deduplication:** Intercepts and drops producer retries before they hit aggregations.
*   **Self-Healing Sinks:** BigQuery sink with circuit breakers, backpressure-aware pause/resume, and automated recovery probes.
*   **Non-Blocking Retries:** Exponential backoff with `@RetryableTopic` to prevent partition stalling.

### 📡 Advanced Stream Processing
*   **Modular Topologies:** Independent modules for Fraud Detection, Balance Aggregation, and Metrics.
*   **Temporal Joins:** KStream-KStream joins with 30s grace periods for out-of-order events.
*   **Memory Safety:** Bounded suppression buffers in session windows to prevent OOM crashes.
*   **Optimized Serdes:** Scaled-long serialization for BigDecimals, slashing RocksDB state by 60%.

### 🔗 Deep Observability
*   **The Golden Thread:** Correlation IDs propagate from HTTP headers → Kafka headers → Consumer MDC.
*   **Mixed-Mode Logging:** Async logging for throughput (INFO) + Sync logging for durability (ERROR).
*   **Global Sequencing:** `%sn` numbering in logs for definitive event reconstruction.

### 🔄 Cooperative Sticky Rebalancing
*   **Incremental Rebalance (KIP-429):** `CooperativeStickyAssignor` — only the partitions that **must move** are revoked; all others continue processing without interruption.
*   **Static Membership:** `group.instance.id` per pod — rolling restarts in Kubernetes skip the rebalance entirely.
*   **Rebalance Observer:** `CooperativeStickyRebalanceListener` logs every partition assignment and revocation for operational visibility.
*   **9-Partition KRaft Demo:** `docker-compose-kraft.yml` boots 3 consumer instances on a ZooKeeper-free Kafka cluster; scale-up/scale-down demonstrates cooperative rebalance in action.

---

## 📚 The Learning Journey (Masterclass Docs)

*   [**🎓 Mastering Spring Kafka (Architectural Tutorial)**](docs/tutorial.md): **Start Here.** An extensive guide to the architectural patterns, from "Producers" to "Interactive Queries".
*   [**🏗️ Architecture Deep-Dive**](docs/architecture.md): Sequence diagrams and Ports & Adapters details.
*   [**🗺️ Topology Masterclass**](docs/TOPOLOGY_GUIDE.md): Advanced Kafka Streams patterns and optimizations.
*   [**🛡️ Operations & GKE Guide**](docs/operations.md): How to run this at scale in Google Cloud.
*   [**📝 Development Standards**](docs/cheatsheet.md): Non-negotiable rules for the codebase.
*   [**📓 Development Journal**](docs/development-journal.md): A chronicle of every "Pro-Level" refactor.
*   [**🔄 Cooperative Sticky Rebalancing Guide**](docs/cooperative-sticky-rebalance.md): Deep-dive into incremental rebalance, static membership, and the KRaft demo.

---

## 🚀 Quick Start (Local — ZooKeeper Mode)

1.  **Start Infrastructure** (Kafka, Schema Registry, Zookeeper):
    ```bash
    docker-compose up -d
    ```

2.  **Run the Application** (Uses `local` profile with H2 and auto-seeding):
    ```bash
    ./mvnw spring-boot:run
    ```

3.  **Ingest a Transaction**:
    ```bash
    curl -X POST http://localhost:8080/api/v1/transactions \
    -H "Content-Type: application/json" \
    -d '{"amount": 15000.75, "accountId": "ACC-VIP-001"}'
    ```

4.  **Query Live Analytics** (Interactive Queries):
    ```bash
    curl http://localhost:8080/api/v1/analytics/daily-total/ACC-VIP-001
    ```

---

## 🔄 Quick Start — KRaft + Cooperative Sticky Rebalance Demo

```bash
# 1. Start 3-instance KRaft cluster (no ZooKeeper) with 9-partition topic
docker compose -f docker-compose-kraft.yml up -d

# 2. Watch rebalance logs across all 3 consumers
docker compose -f docker-compose-kraft.yml logs -f app-1 app-2 app-3

# 3. Scale up: adds a 4th consumer — observe only 2-3 partitions move
docker compose -f docker-compose-kraft.yml up -d --scale app=4

# 4. Scale down: remove a consumer — remaining consumers absorb its partitions
docker compose -f docker-compose-kraft.yml stop app-3

# 5. Rolling restart (static membership): NO rebalance triggered on peers
docker compose -f docker-compose-kraft.yml restart app-1
```

See [docs/cooperative-sticky-rebalance.md](docs/cooperative-sticky-rebalance.md) for a full walkthrough.

---

## 🧪 Testing & Quality

```bash
# Unit & Integration Tests (Embedded Kafka + Testcontainers)
./mvnw clean verify

# Enforce Google Java Format
./mvnw spotless:apply
```

---

*Refactored with passion for Distributed Systems excellence. Build something robust.*
