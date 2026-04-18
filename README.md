# 💂‍♂️ Spring Boot Kafka POC: Financial Data Pipeline 💂‍♂️

A production-ready **Proof of Concept** demonstrating a resilient, high-throughput financial data pipeline using **Spring Boot 3.4**, **Apache Kafka**, **Kafka Streams**, and **Multi-Cloud Persistence** (Google Spanner / AlloyDB / H2).

---

## 🏛️ Refactored Architecture (Production-Grade)

This project has been refactored to adhere to strict **Hexagonal Architecture** and **Domain-Driven Design (DDD)** principles. It is designed to be "Bulletproof" in high-compliance environments.

*   **🔒 Transactional Outbox:** Guarantees atomic writes between the database and Kafka. No message loss, guaranteed consistency.
*   **📡 Intelligence:** Real-time stream processing with **Kafka Streams**, including **GlobalKTable** enrichment and **Tumbling Window** aggregation.
*   **🔗 Observability:** Full-lifecycle tracing using **Correlation IDs** that propagate from HTTP Request Headers through to Kafka Headers and deep into Consumer threads.
*   **🛡️ Resilience:** **Exactly-Once Semantics (EOS)**, **Idempotent Producers**, **Circuit Breakers** on sinks, and **Exponential Backoff** retries with **DLT** support.
*   **🕋 Persistence Agnostic:** Swaps between **Cloud Spanner**, **AlloyDB**, and **H2** (local fallback) using a **Dynamic Persistence Router**.
*   **⚡ High-Performance Logging:** Fully asynchronous logging via **Log4j2 & LMAX Disruptor** with guaranteed arrival ordering and **Global Sequence Numbering** for out-of-order log reconstruction.

---

## 📚 Masterclass Documentation

*   [**🎓 Mastering Spring Kafka (Tutorial)**](docs/tutorial.md): **Start Here.** An extensive guide to the architectural patterns, from "Producers" to "Interactive Queries". (In Queens English).
*   [**🏗️ Architecture Deep-Dive**](docs/architecture.md): Detailed Mermaid diagrams for data flows and system layers.
*   [**🛠️ Local Setup Guide**](docs/setup-local.md): How to bootstrap the infrastructure and run the PoC on your developer machine.
*   [**📓 Development Journal**](docs/development-journal.md): A chronicle of the refactoring decisions and the "The Why" behind the current design.

---

## 🚀 Quick Start (Local)

1.  **Start Infrastructure** (Kafka, Schema Registry, Zookeeper):
    ```bash
    docker-compose up -d
    ```

2.  **Run the Application** (Uses the `local` profile with H2 and auto-seeding):
    ```bash
    ./mvnw spring-boot:run
    ```

3.  **Ingest a Transaction**:
    ```bash
    curl -X POST http://localhost:8080/api/v1/transactions \
    -H "Content-Type: application/json" \
    -d '{"amount": 12500.50, "accountId": "ACC-CLIENT-001"}'
    ```

4.  **Query Real-Time Analytics** (Interactive Queries):
    ```bash
    curl http://localhost:8080/api/v1/analytics/daily-total/ACC-CLIENT-001
    ```

---

## 🏗️ The Technology Stack

*   **Runtime:** Java 21 (LTS) & Spring Boot 3.4.0
*   **Messaging:** Apache Kafka (Confluent Platform)
*   **Streaming:** Kafka Streams (KStream, KTable, Windowing)
*   **Persistence:** Spring Data JPA, Hibernate, Google Cloud Spanner / AlloyDB
*   **Serialization:** Apache Avro (Strict Schema Enforcement)
*   **Resilience:** Resilience4j & Spring Kafka `@RetryableTopic`
*   **Observability:** Micrometer Observation API & SLF4J MDC Tracing

---

## 🛠️ Environment Variables & Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SPRING_PROFILES_ACTIVE` | No | `local` | Active Spring profile |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | Yes (prod) | `localhost:9092` | Kafka broker locations |
| `SCHEMA_REGISTRY_URL` | Yes (prod) | `http://localhost:8081` | Confluent Schema Registry |
| `JAVA_OPTS` | No | — | Custom JVM arguments |

## 🧪 Testing Strategy

```bash
# Unit tests
./mvnw test

# Integration tests (Embedded Kafka)
./mvnw verify

# Code formatting (Spotless/Google Java Format)
./mvnw spotless:apply
```

## 🗺️ Data Flow Map

1. **[HTTP POST]** → `IngestionController`
2. **[PRODUCE]** → `raw-transactions-topic` (TransactionId Key)
3. **[LISTEN]** → `TransactionEventSingleProcessor`
4. **[DB WRITE]** → Transaction Entity + Outbox Record (Atomic)
5. **[POLL]** → `OutboxPublisherService` (Bounded, Locked)
6. **[PRODUCE]** → `processed-transactions-topic`
7. **[STREAM]** → `AnalyticsTopology` (Join + Aggregate)
8. **[Materialize]** → `daily-account-aggregates-store`
9. **[SINK]** → `BigQuerySinkService` (Resilient Circuit Breaker)

---

*Refactored with passion for Distributed Systems excellence.*
