package com.example.springkafkapoc.streams;

/**
 * <b>State Store Registry (The Registry)</b>
 *
 * <p><b>TUTORIAL:</b> In Kafka Streams, a "State Store" is a local database (usually RocksDB)
 * embedded within your application.
 *
 * <p>These constants are critical because they act as the "Address" for:
 *
 * <ul>
 *   <li><b>Topology Building:</b> Telling the engine where to save data.
 *   <li><b>Interactive Queries (IQ):</b> Allowing your REST APIs to "query" the streaming engine
 *       for real-time balances directly from memory.
 * </ul>
 */
public final class StoreConstants {

  private StoreConstants() {}

  public static final String DAILY_ACCOUNT_AGGREGATES_STORE = "daily-account-aggregates-store";
  public static final String ACCOUNT_BALANCE_STORE = "account-balance-store";
  public static final String HOURLY_HOPPING_STORE = "hourly-hopping-store";
  public static final String SESSION_ACTIVITY_STORE = "session-activity-store";
  public static final String TRANSACTION_DEDUPLICATION_STORE = "transaction-deduplication-store";
  public static final String ACCOUNT_REFERENCE_STORE = "account-reference-store";
}
