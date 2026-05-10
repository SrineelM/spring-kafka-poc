# 📝 Pipeline Development Standards & Best Practices 📝

This document defines the non-negotiable rules and architectural patterns that ensure the reliability, scalability, and maintainability of the `spring-kafka-poc` pipeline.

---

## 🛑 1. Core Principles

1.  **Fail Fast, Fail Loud.**
    *   Validate all data at the `IngestionController`. Bad data is like a contagion in a distributed system. If a request is missing an amount or has an invalid account ID, reject it immediately with a `400 Bad Request` before it ever touches Kafka.

2.  **Financial Precision is Absolute.**
    *   Use `BigDecimal` for every monetary value. Floating-point types (`double`, `float`) introduce rounding errors that are unacceptable in financial systems.

3.  **The Database is the Source of Truth.**
    *   Always write to the database *before* attempting to produce a message. Use the **Transactional Outbox** pattern to guarantee atomicity between local state and downstream events.

4.  **Resilience is Built-In.**
    *   Implement **Circuit Breakers** and **Retryable Topics** for every external dependency. The system must be designed to handle partial failures gracefully.

5.  **Strict Context Isolation.**
    *   Always clear `ThreadLocal` contexts (like Correlation IDs). Cross-contamination of request contexts in a thread-pooled environment leads to misleading logs and untraceable incidents.

---

## 📡 2. Kafka Messaging Standards

### 🔹 The Outbox Relay
Do not invoke `kafkaTemplate.send()` directly within business service methods.
1.  Persist the business entity.
2.  Persist the `Outbox` record in the **same database transaction**.
3.  The `OutboxPublisherService` (protected by a **Distributed Lock**) will asynchronously relay these records to Kafka.

### 🔹 Stream Processing (KStreams)
*   **Stateful Deduplication:** Every aggregation must be preceded by the `DeduplicationProcessor` to guard against "at-least-once" delivery duplicates.
*   **Bounded Suppression:** Use the Suppress API with memory limits to prevent OutOfMemory crashes during high-volume sessions.
*   **Optimized Serialization:** Use scaled-long SerDes for BigDecimals to minimize the RocksDB state store footprint.

---

## 🛡️ 3. Observability & Tracing

*   **Correlation ID:** Every log line across every microservice must include the `correlationId`.
*   **Backpressure Monitoring:** Monitor the `bigquery.sink.paused` metric. A value of `1` indicates downstream failure and active pipeline protection.
*   **Forensics:** Ensure `sourcePartition` and `sourceOffset` are persisted in the database for every transaction to allow for exact Kafka re-fetching.

---

## 📈 4. Critical Health Metrics

Watch these metrics in your monitoring dashboards:
*   `kafka_consumer_lag`: The primary indicator of pipeline throughput health.
*   `transaction.dlq.count`: Any value > 0 indicates an unrecoverable "Poison Pill."
*   `resilience4j_circuitbreaker_state`: Monitor for `OPEN` states indicating failing dependencies.

---

*“Robustness is the result of disciplined architecture and consistent application of patterns.”*
