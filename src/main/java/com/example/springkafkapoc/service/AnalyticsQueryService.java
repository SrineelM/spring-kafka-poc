package com.example.springkafkapoc.service;

import com.example.springkafkapoc.streams.StoreConstants;
import java.math.BigDecimal;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

/**
 * <b>Analytics Query Service — Interactive Queries Against Live Stream State</b>
 *
 * <p><b>TUTORIAL — What are Interactive Queries?</b><br>
 * Traditionally, stream processing works in "Push" mode: compute an aggregation, write the result
 * to a database or topic, and let an API query the database. Interactive Queries (IQ) flip this:
 * the API queries the <em>running stream engine's local state stores directly</em> — no external
 * database required for real-time lookups.
 *
 * <p><b>Why does this matter?</b><br>
 * For real-time dashboards (e.g., "What is this account's balance RIGHT NOW?"), a database write +
 * database read introduces latency and complexity. With IQ, the answer is already in RocksDB —
 * local to the stream thread — and can be read in microseconds.
 *
 * <p><b>TUTORIAL — Multi-Node Cluster Consideration (IMPORTANT):</b><br>
 * State stores are <em>local</em> to the Kafka Streams instance that currently owns the partition
 * for a given key. In a multi-pod Kubernetes deployment, you CANNOT simply query any pod for any
 * key — you must route the query to the pod that owns the partition.
 *
 * <p>The correct cluster-aware approach:
 *
 * <ol>
 *   <li>Call {@code KafkaStreams.queryMetadataForKey(storeName, key, keySerializer)} to find which
 *       host+port owns the partition for that key.
 *   <li>If it's this instance → query locally.
 *   <li>If it's another instance → forward the HTTP request to that pod via RPC (e.g., REST or
 *       gRPC). That pod then performs the local query and returns the result.
 * </ol>
 *
 * <p>This PoC assumes a single-instance deployment where all partitions are local.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AnalyticsQueryService {

  // StreamsBuilderFactoryBean is the Spring-managed lifecycle wrapper around KafkaStreams.
  // It gives us access to the underlying KafkaStreams instance for Interactive Queries.
  private final StreamsBuilderFactoryBean factoryBean;

  /**
   * Retrieves the 24-hour running total for a given account from the window state store.
   *
   * <p><b>TUTORIAL — Reading a Window Store:</b><br>
   * Window stores are indexed by (key, time range). We fetch all windows for the accountId over the
   * last 24 hours and return the latest (most up-to-date) total. For a 24h tumbling window, there
   * should typically be only ONE active window at a time — but we iterate for safety in case the
   * window boundaries cause edge cases.
   *
   * @param accountId the account to query
   * @return the 24h total amount, or {@link BigDecimal#ZERO} if the store is unavailable or no data
   *     exists for this account
   */
  public BigDecimal getDailyAccountTotal(String accountId) {
    log.debug("Interactive Query: 24h total for accountId={}", accountId);

    // Step 1: Obtain the underlying KafkaStreams instance from Spring's factory bean
    KafkaStreams streams = factoryBean.getKafkaStreams();

    // Safety guard: return zero if the engine isn't running (startup, restart, or rebalancing)
    // Attempting to query a non-RUNNING instance throws an InvalidStateStoreException
    if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
      log.warn("KafkaStreams is not RUNNING — returning ZERO for accountId={}", accountId);
      return BigDecimal.ZERO;
    }

    // Step 2: Access the named window store that MetricsTopology materialized
    // StoreQueryParameters tells the engine: "I want a read-only view of this window store"
    ReadOnlyWindowStore<String, BigDecimal> store =
        streams.store(
            StoreQueryParameters.fromNameAndType(
                StoreConstants.DAILY_ACCOUNT_AGGREGATES_STORE,
                QueryableStoreTypes
                    .windowStore())); // Must match the store type used in MetricsTopology

    // Step 3: Fetch all windows for this accountId over the entire epoch (all time)
    // We use epoch 0 to Instant.now() to ensure we capture any active window regardless
    // of when it started. For tumbling windows, this returns at most 1 window per accountId.
    try (WindowStoreIterator<BigDecimal> iterator =
        store.fetch(accountId, Instant.ofEpochMilli(0), Instant.now())) {

      BigDecimal latestTotal = BigDecimal.ZERO;

      // Iterate all windows — for a tumbling 24h window there's typically only one
      // but iterating is future-proof in case the window type changes
      while (iterator.hasNext()) {
        // Each next() call returns a KeyValue<Long windowStartMs, BigDecimal total>
        latestTotal = iterator.next().value;
        // We overwrite latestTotal on each iteration — the last value is the most recent window
      }

      return latestTotal;

    } catch (Exception e) {
      // Store may be temporarily unavailable during a rebalance — return zero gracefully
      log.error("Error querying window store for accountId={}: {}", accountId, e.getMessage());
      return BigDecimal.ZERO;
    }
  }
}
