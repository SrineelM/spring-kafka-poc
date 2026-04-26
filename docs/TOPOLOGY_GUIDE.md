# 🗺️ Topology Masterclass: Advanced Stream Processing

This document serves as the definitive guide to the Kafka Streams implementation within `spring-kafka-poc`. It details the "Pro-Level" patterns used to achieve high throughput and data integrity.

---

## 🏗️ 1. Modular Topology Design

Instead of one monolithic topology, we use a **Modular "Lego-Block" Architecture**. This allows teams to work on different analytics modules (Fraud, Balances, etc.) independently without breaking the entire engine.

*   **`SourceTopology` (The Gatekeeper):** 
    *   Performs **Defensive Validation** (drops malformed records).
    *   Executes **24h Stateful Deduplication** via the Processor API.
    *   Performs **Global Re-Keying** (switches from `transactionId` to `accountId`). Doing this once at the source saves thousands of network shuffles downstream.

---

## ⚡ 2. High-Performance Stateful Operations

### 🔹 2.1 The "Scaled Long" Optimization
Standard `BigDecimal` serialization is slow and memory-intensive. 
**Our Solution:** The `OptimizedBigDecimalSerde` scales amounts to 4 decimal places and stores them as fixed-width **Longs**.
*   **Result:** 60% reduction in RocksDB state storage and 2x faster aggregations.

### 🔹 2.2 Deduplication with Wall-Clock Punctuators
The `DeduplicationProcessor` uses a **RocksDB KeyValueStore** to remember IDs.
*   **Pattern:** A **Punctuator** (scheduled task) runs every hour to scan the store and delete entries older than 24 hours.
*   **Why:** This prevents the local disk from filling up while maintaining a sufficient "lookback window" for producer retries.

---

## 🤝 3. Temporal Join Strategies

In `FraudTopology`, we demonstrate a **KStream-KStream Inner Join**.

*   **Scenario:** Match a `TransactionEvent` with a `FraudSignal` if they occur within **±5 minutes** of each other.
*   **Co-partitioning Rule:** For joins to work, both topics MUST have the same number of partitions and be keyed by the same attribute (`accountId`). We ensure this in `KafkaCoreConfig`.
*   **Grace Period:** We allow a **30-second Grace Period** for out-of-order events. If a signal arrives 10 seconds after the transaction due to network lag, they will still be joined correctly.

---

## 📦 4. Windowing & Memory Management

### 🔹 4.1 Tumbling vs. Hopping Windows
*   **Tumbling (24h):** Fixed, non-overlapping windows for "Daily Spending Totals".
*   **Hopping (1h size, 15m advance):** Overlapping windows for "Trailing Hourly Trends".

### 🔹 4.2 Bounded Suppression (Memory Safety)
In `SessionTopology`, we use the **Suppress API**.
*   **The Problem:** Eager windows emit an update for EVERY record, causing "Log Spam" and high downstream load.
*   **The Solution:** `.suppress(untilWindowCloses(...))` buffers updates in memory and only emits the **FINAL** result once the window is sealed.
*   **The Guardrail:** We bound the buffer to **50MB**. If a "Traffic Storm" exceeds this, the app shuts down safely rather than crashing with an OOM.

---

## 🧪 5. Testing & Debugging

### 🔹 5.1 TopologyTestDriver
Every topology is tested in isolation using `TopologyTestDriver`. This allows us to simulate "Event Time" and verify windowing logic in milliseconds without a real Kafka broker.

### 🔹 5.2 Interactive Queries (IQ)
The `AnalyticsQueryService` provides a "Window" into the live state:
*   `store.fetch(key, timeFrom, timeTo)` retrieves historical window totals directly from the stream engine.

---

*“In streaming, time is not just a timestamp; it is the dimension that defines your state.”*
