package com.example.springkafkapoc.streams;

/**
 * <b>State Store Registry — The Address Book</b>
 *
 * <p><b>TUTORIAL — What is a Kafka Streams State Store?</b><br>
 * Unlike stateless operations (filter, map), aggregations (sum, count, join) need to remember data
 * across records. Kafka Streams stores this data locally in an embedded <b>RocksDB</b> database — a
 * high-performance key-value store from Facebook. RocksDB is persistent: it survives stream-thread
 * restarts and is backed up to Kafka changelog topics automatically.
 *
 * <p>Each store is identified by a unique string name. These constants ensure every part of the
 * codebase (topology builders, interactive query services, tests) refers to the same store by the
 * same name — preventing hard-to-debug "store not found" errors at runtime.
 *
 * <p><b>Two uses for these names:</b>
 *
 * <ol>
 *   <li><b>Topology Building:</b> When you call {@code Materialized.as(STORE_NAME)}, you tell Kafka
 *       Streams where to save the aggregated state.
 *   <li><b>Interactive Queries (IQ):</b> When you call {@code
 *       streams.store(StoreQueryParameters.fromNameAndType(STORE_NAME, ...))} from a REST API,
 *       you're reading live aggregated data directly from the local store — no database needed.
 * </ol>
 *
 * <p><b>PRO TIP — Interactive Queries in a cluster:</b><br>
 * State stores are <em>local</em> to the instance that owns the partition for a given key. In a
 * multi-pod deployment, you need RPC routing: ask Kafka "which instance owns key X?" then forward
 * the HTTP request to that pod. {@code KafkaStreams.queryMetadataForKey()} provides this routing
 * information.
 */
public final class StoreConstants {

  // Prevent instantiation — utility class with only string constants
  private StoreConstants() {}

  // ─── Window Stores (used by MetricsTopology) ──────────────────────────────────────────────────

  /** 24-hour tumbling window aggregates — used for daily account spending totals. */
  public static final String DAILY_ACCOUNT_AGGREGATES_STORE = "daily-account-aggregates-store";

  /** 1-hour hopping window (advancing every 15 min) — used for trailing spend trends. */
  public static final String HOURLY_HOPPING_STORE = "hourly-hopping-store";

  // ─── Key-Value Stores (used by BalanceTopology and SourceTopology) ────────────────────────────

  /** Running account balance — a KTable of accountId → total. Updated on every transaction. */
  public static final String ACCOUNT_BALANCE_STORE = "account-balance-store";

  /**
   * Deduplication store — maps transactionId → first-seen timestamp.
   *
   * <p>Used by {@link DeduplicationProcessor} to identify and drop duplicate records caused by
   * producer retries or at-least-once delivery guarantees. Expired entries are pruned hourly by the
   * processor's built-in Punctuator.
   */
  public static final String TRANSACTION_DEDUPLICATION_STORE = "transaction-deduplication-store";

  // ─── Session Stores (used by SessionTopology) ─────────────────────────────────────────────────

  /**
   * Session activity store — maps accountId → session total.
   *
   * <p>Session windows have dynamic size: they close after 30 minutes of inactivity. The store
   * holds the running total for the current open session.
   */
  public static final String SESSION_ACTIVITY_STORE = "session-activity-store";

  // ─── Global Table (used by SourceTopology) ────────────────────────────────────────────────────

  /**
   * Account reference data — a GlobalKTable populated from the account-reference topic.
   *
   * <p>GlobalKTable is replicated to ALL stream-processing instances (unlike a normal KTable which
   * is partitioned). This makes it perfect for small, frequently-joined reference data (account
   * metadata, exchange rates, etc.) without triggering a re-partition.
   */
  public static final String ACCOUNT_REFERENCE_STORE = "account-reference-store";
}
