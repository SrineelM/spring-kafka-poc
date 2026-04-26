package com.example.springkafkapoc.config;

/**
 * <b>Central Registry of Kafka Topics — The Address Book</b>
 *
 * <p>In a production system, hardcoding strings is the enemy of reliability. A single typo in a
 * topic name leads to a "Silent Failure" where data vanishes into a void.
 *
 * <p><b>TUTORIAL — Why a Constant Registry?</b>
 *
 * <ul>
 *   <li><b>Single Source of Truth:</b> If a topic name changes, you update it here once.
 *   <li><b>Discovery:</b> A new developer can open this file and immediately see the entire data
 *       landscape of the application.
 *   <li><b>Refactoring Safety:</b> Renaming a constant here updates all references across
 *       Producers, Consumers, and Streams.
 * </ul>
 */
public final class TopicConstants {

  private TopicConstants() {
    // Utility class — do not instantiate
  }

  // =========================================================================
  // 1. INGESTION TIER (The Entry Points)
  // =========================================================================

  /**
   * Raw events arriving from the REST API.
   *
   * <p>Config: Partitions=3, Retention=24h, Cleanup=Delete
   */
  public static final String RAW_TRANSACTIONS = "raw-transactions-topic";

  /**
   * Events successfully saved to the primary DB.
   *
   * <p>This topic is the source for all analytical streams.
   */
  public static final String PROCESSED_TRANSACTIONS = "processed-transactions-topic";

  /**
   * Reference data for account metadata.
   *
   * <p>Config: Cleanup=Compact. Used as a GlobalKTable for enrichment joins.
   */
  public static final String ACCOUNT_REFERENCE = "account-reference-topic";

  // =========================================================================
  // 2. ANALYTICS TIER (Streaming Outputs)
  // =========================================================================

  /** 24h Tumbling Window results (Daily Totals) */
  public static final String DAILY_ACCOUNT_METRICS = "daily-account-metrics-topic";

  /** 1h Hopping Window results (Trailing Hourly Trends) */
  public static final String HOURLY_ACCOUNT_METRICS = "hourly-account-metrics-topic";

  /** Sessionized activity bursts */
  public static final String SESSION_ACTIVITY = "session-activity-topic";

  /** Cumulative account balance KTable changelog */
  public static final String ACCOUNT_BALANCES = "account-balances-topic";

  // =========================================================================
  // 3. ROUTING & FRAUD TIER
  // =========================================================================

  /** High-value transactions routed for premium processing (> $10k) */
  public static final String HIGH_VALUE_TRANSACTIONS = "high-value-transactions";

  /** Normal transactions routed for standard processing */
  public static final String NORMAL_TRANSACTIONS = "normal-transactions";

  /** Fraud alert signals emitted by the join topology */
  public static final String FRAUD_ALERTS = "fraud-alerts-topic";

  /** Input signals from external risk engines for joins */
  public static final String FRAUD_SIGNALS = "fraud-signals-topic";

  // =========================================================================
  // 4. RESILIENCE TIER (DLQ)
  // =========================================================================

  /**
   * Dead Letter Topic (DLT) for "Poison Pill" records. Records here have failed all retries and
   * require manual forensic investigation.
   */
  public static final String RAW_TRANSACTIONS_DLT = RAW_TRANSACTIONS + ".DLT";
}
