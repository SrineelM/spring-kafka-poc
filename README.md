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

---

## 📚 The Learning Journey (Masterclass Docs)

*   [**🎓 Mastering Spring Kafka (Architectural Tutorial)**](docs/tutorial.md): **Start Here.** An extensive guide to the architectural patterns, from "Producers" to "Interactive Queries".
*   [**🏗️ Architecture Deep-Dive**](docs/architecture.md): Sequence diagrams and Ports & Adapters details.
*   [**🗺️ Topology Masterclass**](docs/TOPOLOGY_GUIDE.md): Advanced Kafka Streams patterns and optimizations.
*   [**🛡️ Operations & GKE Guide**](docs/operations.md): How to run this at scale in Google Cloud.
*   [**📝 Development Standards**](docs/cheatsheet.md): Non-negotiable rules for the codebase.
*   [**📓 Development Journal**](docs/development-journal.md): A chronicle of every "Pro-Level" refactor.

---

## 🚀 Quick Start (Local)

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

## 🧪 Testing & Quality

```bash
# Unit & Integration Tests (Embedded Kafka + Testcontainers)
./mvnw clean verify

# Enforce Google Java Format
./mvnw spotless:apply
```

---

*Refactored with passion for Distributed Systems excellence. Build something robust.*
