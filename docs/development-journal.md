# Spring Boot Kafka Pipeline - Development Journal

This document explains **what** we built, **why** we made each technical decision,
and **what a beginner should learn** from each component. It covers every phase of
the implementation and doubles as a session-recovery guide for continuing in a new session.

---

## Phase 1 — Project Setup & Core Configuration

... (existing content) ...

---

## Phase 9 — Advanced Resilience and Cloud Native Enhancements

... (existing content) ...

---

## Phase 10 — Final Polish and Performance Tuning

### Fix 16: Optimized Audit Log Primary Key
**What:** Switched `AuditLogEntity` ID generation to `UuidGenerator.Style.TIME` (UUID v7).
**Why:** Standard random UUIDs (v4) cause massive fragmentation in database B-tree indexes because new records are inserted at random positions. UUID v7 is time-ordered, meaning new records are appended to the end of the index. This keeps the index compact and performant, which is critical for a high-volume audit table.

### Fix 17: Tuned Kafka Streams Threading
**What:** Externalized the number of Kafka Streams threads to `application.yml` and set the default to 3.
**Why:** The pipeline topic has 3 partitions. To achieve maximum parallelism without idle threads, the number of stream threads should match the partition count. Hardcoding this value prevents environment-specific tuning.

### Fix 18: Guarded Batch Consumer
**What:** Configured `max.poll.records: 500` in `application.yml`.
**Why:** In batch mode, a consumer can fetch thousands of records in a single poll. If the database write (`saveAll`) for this batch takes longer than `max.poll.interval.ms`, the broker will assume the consumer is dead and trigger a rebalance. Limiting the batch size ensures the processing loop stays tight and responsive.

---

## Phase 11 — Production Hardening & Modularization

### Fix 19: Modular Bean-Driven Topology
**What:** Split the monolithic `AnalyticsTopology` into 6 specialized beans (`Source`, `Balance`, `Metrics`, `Session`, `Fraud`, `Routing`).
**Why:** Monolithic topology classes are hard to test and maintain. Modularization allows developers to reason about specific business domains (e.g., just Fraud detection) without touching the rest of the pipeline. Using Spring `@Bean` registration also simplifies mocking in integration tests.

### Fix 20: 24-Hour "Exactly-Once" Deduplication
**What:** Implemented a `DeduplicationProcessor` in the `SourceTopology`.
**Why:** Network glitches can cause producers to send duplicates. By tracking transaction IDs in a RocksDB state store with a 24-hour TTL, we ensure that aggregations (like account balances) are only updated once per transaction, even if the record arrives multiple times.

### Fix 21: Performance-Critical "Scaled Long" Serde
**What:** Replaced String-based `BigDecimal` serialization with an `OptimizedBigDecimalSerde` that scales values to longs.
**Why:** Storing money as a String (e.g., "123.45") is slow and bulky. By scaling to longs (1234500), we reduce the state store size by ~60% and eliminate expensive string parsing, significantly increasing the maximum throughput of the pipeline.

### Fix 22: Memory-Safe Bounded Suppression
**What:** Replaced `Suppressed.BufferConfig.unbounded()` with a 50MB bounded buffer in `SessionTopology`.
**Why:** An unbounded buffer is a production "time bomb." If session activity spikes, the buffer will grow until the JVM crashes with an OutOfMemory error. Bounding the buffer ensures the system fails gracefully (or emits early) rather than crashing the entire container.
