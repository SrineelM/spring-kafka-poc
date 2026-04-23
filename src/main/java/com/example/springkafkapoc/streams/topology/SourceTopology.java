package com.example.springkafkapoc.streams.topology;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.streams.DeduplicationProcessor;
import com.example.springkafkapoc.streams.SerdeConfig;
import com.example.springkafkapoc.streams.StoreConstants;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * <b>Source & Enrichment Topology (The Entry Point)</b>
 *
 * <p><b>TUTORIAL:</b> In a modular Kafka Streams application, the Source Topology is the most
 * critical part. It handles the initial "raw" ingestion and prepares the data for all other
 * specialized modules.
 *
 * <p>Patterns implemented here:
 *
 * <ul>
 *   <li><b>Defensive Validation:</b> Dropping "poison pills" before they reach stateful logic.
 *   <li><b>Deduplication:</b> Ensuring "Exactly-Once" even if the producer retries.
 *   <li><b>Centralized Re-keying:</b> Keying by AccountId once so downstream sub-topologies don't
 *       have to re-partition individually, saving massive amounts of network and CPU.
 * </ul>
 */
@Slf4j
@Component
public class SourceTopology {

  /** Minimum amount to process; smaller amounts are considered noise/auth-checks. */
  private static final BigDecimal MINIMUM_VALID_AMOUNT = new BigDecimal("0.01");

  /** How long to remember a transaction ID for deduplication. */
  private static final Duration DEDUPLICATION_TTL = Duration.ofHours(24);

  private final SerdeConfig serdeConfig;
  private final MeterRegistry meterRegistry;

  private final Counter malformedCounter;
  private final Counter smallAmountCounter;

  @Autowired
  public SourceTopology(SerdeConfig serdeConfig, MeterRegistry meterRegistry) {
    this.serdeConfig = serdeConfig;
    this.meterRegistry = meterRegistry;

    this.malformedCounter =
        Counter.builder("streams.source.malformed.count")
            .description("Number of malformed records dropped at the source")
            .register(meterRegistry);
    this.smallAmountCounter =
        Counter.builder("streams.source.small_amount.count")
            .description("Number of transactions dropped because the amount was too small")
            .register(meterRegistry);
  }

  /**
   * <b>Source Context Pattern</b>
   *
   * <p>TUTORIAL: Instead of returning a single stream, we return a "Context". This allows
   * downstream topologies to choose between the flat Keyed Stream (for joins/routing) or the
   * Grouped Stream (for windowed aggregations), all derived from the same source node.
   */
  @Data
  @Builder
  public static class SourceContext {
    private final KStream<String, TransactionEvent> keyedStream;
    private final KGroupedStream<String, TransactionEvent> groupedStream;
  }

  public SourceContext buildSource(StreamsBuilder builder) {
    var txnSerde = serdeConfig.transactionEventSerde();

    // Register deduplication store (used by the DeduplicationProcessor)
    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(StoreConstants.TRANSACTION_DEDUPLICATION_STORE),
            Serdes.String(),
            Serdes.Long()));

    // GlobalKTable for enrichment
    // TUTORIAL: GlobalKTable is broadcast to ALL instances, making it perfect for small
    // reference data (like Account Metadata) that you want to join without re-partitioning.
    builder.globalTable(
        TopicConstants.ACCOUNT_REFERENCE,
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.as(StoreConstants.ACCOUNT_REFERENCE_STORE));

    KStream<String, TransactionEvent> stream =
        builder.stream(
            TopicConstants.PROCESSED_TRANSACTIONS, Consumed.with(Serdes.String(), txnSerde));

    KStream<String, TransactionEvent> keyedStream =
        stream
            // 1. Defensive Validation
            .filter(
                (k, v) -> {
                  boolean valid =
                      v != null
                          && v.getTransactionId() != null
                          && v.getAccountId() != null
                          && v.getAmount() != null;
                  if (!valid) {
                    malformedCounter.increment();
                    if (log.isWarnEnabled()) {
                      log.warn("Dropping malformed record: key={}", k);
                    }
                  }
                  return valid;
                })
            // 2. Deduplication
            .process(
                () ->
                    new DeduplicationProcessor<>(
                        StoreConstants.TRANSACTION_DEDUPLICATION_STORE,
                        event -> event.getTransactionId().toString(),
                        DEDUPLICATION_TTL,
                        meterRegistry),
                StoreConstants.TRANSACTION_DEDUPLICATION_STORE)
            // 3. Minimum amount filter
            .filter(
                (id, event) -> {
                  boolean valid = event.getAmount().compareTo(MINIMUM_VALID_AMOUNT) >= 0;
                  if (!valid) {
                    smallAmountCounter.increment();
                    if (log.isDebugEnabled()) {
                      log.debug(
                          "Dropping small transaction: id={}, amount={}", id, event.getAmount());
                    }
                  }
                  return valid;
                })
            // 4. Centralized Re-keying
            .selectKey((txnId, event) -> event.getAccountId().toString());

    KGroupedStream<String, TransactionEvent> groupedStream =
        keyedStream.groupByKey(Grouped.with(Serdes.String(), txnSerde));

    return SourceContext.builder().keyedStream(keyedStream).groupedStream(groupedStream).build();
  }
}
