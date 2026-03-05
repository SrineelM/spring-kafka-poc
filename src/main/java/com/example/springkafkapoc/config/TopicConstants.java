package com.example.springkafkapoc.config;

/**
 * Central registry of all Kafka topic names used across the pipeline.
 *
 * <p>
 * WHY: Hardcoding topic strings in multiple files is a maintenance trap.
 * A single typo causes a silent split-brain where producers and consumers
 * write/read from different topics. Using constants prevents this entirely
 * and makes topic names refactorable in one place.
 *
 * <p>
 * BEGINNER TIP: Think of this as an "address book" for all message channels in
 * the system.
 */
public final class TopicConstants {

    private TopicConstants() {
        // Utility class — do not instantiate
    }

    // -------------------------------------------------------------------------
    // Ingestion Pipeline Topics
    // -------------------------------------------------------------------------

    /** Raw events arriving from the external REST edge node. */
    public static final String RAW_TRANSACTIONS = "raw-transactions-topic";

    /**
     * Events that have been saved to DB and enriched. Downstream consumers
     * and Kafka Streams topologies read from here.
     */
    public static final String PROCESSED_TRANSACTIONS = "processed-transactions-topic";

    /**
     * Reference / lookup data topic. Backed by a {@code GlobalKTable} so that
     * <em>every</em> Streams instance has a full copy of the data locally,
     * enabling enrichment joins without network shuffles.
     */
    public static final String ACCOUNT_REFERENCE = "account-reference-topic";

    // -------------------------------------------------------------------------
    // Analytics Output Topics
    // -------------------------------------------------------------------------

    /**
     * Aggregated 24-hour tumbling-window account spending totals emitted by
     * {@link com.example.springkafkapoc.streams.AnalyticsTopology}.
     */
    public static final String DAILY_ACCOUNT_METRICS = "daily-account-metrics-topic";

    // -------------------------------------------------------------------------
    // Dead Letter Queues (DLQ) — by convention they use the source topic + ".DLT"
    // -------------------------------------------------------------------------

    /**
     * Poisoned records from the raw ingestion topic after retries are exhausted.
     */
    public static final String RAW_TRANSACTIONS_DLT = RAW_TRANSACTIONS + ".DLT";
}
