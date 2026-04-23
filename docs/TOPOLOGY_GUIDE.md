# Kafka Streams Architectural & Production Guide

This guide outlines critical production considerations for the Spring Kafka Streams pipeline, as implemented in this repository.

## 0. Modular Architecture
The topology is split into specialized modules for maintainability and performance:
- **SourceTopology**: Handles ingestion, defensive validation, and re-keying. Returns a `SourceContext` containing both keyed `KStream` and `KGroupedStream`.
- **Specialized Topologies**: `BalanceTopology`, `MetricsTopology`, `SessionTopology`, `FraudTopology`, and `RoutingTopology` consume these shared streams, eliminating redundant re-partitioning.

## 1. Partition Alignment (Co-partitioning)
**CRITICAL:** Any join operation (e.g., in `FraudTopology`) requires that both input streams are co-partitioned.
- **Rule:** `PROCESSED_TRANSACTIONS` partition count **MUST EQUAL** `FRAUD_SIGNALS` partition count.
- **Rule:** Both topics must be keyed by the same attribute (e.g., `accountId`).
- **Implementation:** If keys don't match, use `.selectKey(...)` followed by an internal repartition topic.

## 2. Schema Evolution & Compatibility
Since we use Apache Avro for serialization, the following policies apply:
- **Compatibility Mode:** We recommend `FULL` or `BACKWARD` compatibility in the Confluent Schema Registry.
- **Versioning:** Always use the same major version of the schema across all producers and consumers.
- **Handling Deletions:** Never delete a field from the schema; instead, mark it as `deprecated` and keep it optional.

## 3. Production Resilience (Exception Handling)
The pipeline is configured with the following handlers in `KafkaStreamsConfig`:
- **DeserializationExceptionHandler:** Uses `StreamsDeserializationErrorHandler`. This prevents a single "poison pill" (corrupt record) from stopping the entire processing thread by logging and skipping the bad record.
- **ProductionExceptionHandler:** Handles errors during record publishing to output topics.
- **UncaughtExceptionHandler:** Monitors the overall health of the Stream threads and triggers alerts/restarts if a thread dies.

## 4. Performance & Memory Management
- **Suppression Buffers:** Windowed aggregations (especially sessions) use bounded buffers (`BufferConfig.maxBytes`) to prevent OutOfMemory (OOM) errors during traffic spikes.
- **Serde Efficiency:** We use a custom `OptimizedBigDecimalSerde` that scales values to longs, significantly reducing CPU cycles and memory footprint compared to String-based serialization.
- **Deduplication TTL:** The `DeduplicationProcessor` uses a **24-hour TTL** with an hourly punctuator cleanup. This prevents infinite RocksDB state growth while ensuring exactly-once semantics for high-volume event streams.
- **Logging:** All hot-path logging is wrapped in `if (log.isDebugEnabled())` to eliminate unnecessary string concatenation and object creation under high throughput.

## 5. Monitoring & Observability
Prometheus/Actuator should monitor these key Kafka Streams metrics:
- `kafka_streams_thread_commit_latency_avg`: Measures how long it takes to commit offsets and state.
- `kafka_streams_client_consumer_lag_max`: The ultimate measure of whether your pipeline is keeping up with real-time data.
- `kafka_streams_state_rocksdb_window_size`: Monitors the growth of local state stores.
- `kafka_streams_processor_node_suppression_buffer_size`: Monitors memory usage in session windows.
- `kafka_streams_client_rebalance_total`: High counts indicate cluster instability.

## 6. Interactive Queries (IQ)
This topology supports "Pull" logic via Interactive Queries:
- **Direct Store Access**: The `AnalyticsQueryService` queries the `account-balance-store` directly without going through an external DB.
- **RPC Routing**: In production, use the `KafkaStreams.queryMetadataForKey()` API to route requests to the specific node that owns the partition for a given `accountId`. This ensures sub-millisecond query performance across a distributed cluster.

## 7. Resilience & Error Handling (DLQ)

A production pipeline must handle "Poison Pills" (corrupt or invalid records) without crashing.

#### 🔹 Standard Consumers (Spring Kafka)
*   **Strategy:** `DeadLetterPublishingRecoverer`.
*   **Behavior:** If a record fails after all retries, it is moved to `raw-transactions-topic.DLT`.
*   **Audit:** The `@DltHandler` in `TransactionEventSingleProcessor` records this in the `AuditLog` table for forensics.

#### 🔹 Streaming Tier (Kafka Streams)
*   **Strategy:** `StreamsDeserializationErrorHandler`.
*   **Behavior:** If Avro deserialization fails, the record is logged and the stream **CONTINUES** (skips the record).
*   **Why:** In a 24/7 streaming engine, one corrupt record should not stop the analytics for thousands of other accounts.
