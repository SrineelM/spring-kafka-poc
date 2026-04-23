package com.example.springkafkapoc.service;

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
 * <b>Interactive Query Service</b>
 *
 * <p><b>TUTORIAL:</b> This service demonstrates <b>Interactive Queries (IQ)</b> in Kafka Streams.
 * Traditionally, you aggregate data in a stream and write it to an external database (like Spanner)
 * for an API to query. That's "Push" logic.
 *
 * <p>IQ changes the game by allowing the API to query the <b>Active Processing State Store</b>
 * directly. No external database is required for real-time dashboards or lookups!
 *
 * <p><b>Key Architecture Tip:</b> State stores are <b>local</b> to the processing instance that
 * owns that key's partition. In a multi-node production cluster, your API front-end would need to
 * perform <b>RPC Routing</b>: 1. Ask Kafka: "Which node currently owns the data for Account X?" 2.
 * Forward the HTTP request to that specific node. 3. That node queries its local store and returns
 * the result.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AnalyticsQueryService {

  private final StreamsBuilderFactoryBean factoryBean;

  /**
   * Retrieves the 24-hour total for an account from the Window Store.
   *
   * @param accountId account ID to query
   * @return 24h total or 0
   */
  public BigDecimal getDailyAccountTotal(String accountId) {
    log.debug("Querying state store for accountId={}", accountId);

    // 1. Get the underlying KafkaStreams instance from Spring's FactoryBean
    KafkaStreams streams = factoryBean.getKafkaStreams();
    if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
      log.warn("Kafka Streams is not RUNNING - returning ZERO.");
      return BigDecimal.ZERO;
    }

    // 2. Access the WindowStore — this is where the 24h aggregates are kept
    ReadOnlyWindowStore<String, BigDecimal> store =
        streams.store(
            StoreQueryParameters.fromNameAndType(
                com.example.springkafkapoc.streams.StoreConstants.DAILY_ACCOUNT_AGGREGATES_STORE,
                QueryableStoreTypes.windowStore()));

    // 3. Fetch the windows for the given accountId within the last 24h
    // We use epoch 0 to 'now' to ensure we catch any active windows.
    try (WindowStoreIterator<BigDecimal> iterator =
        store.fetch(accountId, Instant.ofEpochMilli(0), Instant.now())) {

      BigDecimal latestTotal = BigDecimal.ZERO;
      // A window store can have multiple windows (e.g. if we used hopping windows).
      // For tumbling windows, there should only be one active window,
      // but we iterate to be safe and find the most recent.
      while (iterator.hasNext()) {
        latestTotal = iterator.next().value;
      }
      return latestTotal;
    } catch (Exception e) {
      log.error("Error querying state store for {}: {}", accountId, e.getMessage());
      return BigDecimal.ZERO;
    }
  }
}
