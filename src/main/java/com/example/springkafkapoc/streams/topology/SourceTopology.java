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
 * <b>Source & Enrichment Topology — The Entry Gate</b>
 *
 * <p><b>TUTORIAL — Role in the pipeline:</b><br>
 * This is the first and most critical topology. ALL data flowing into the Kafka Streams engine
 * passes through here. Its job is to: validate, deduplicate, enrich, re-key, and group records — so
 * that downstream topologies receive clean, consistently-keyed data with no duplicates.
 *
 * <p><b>Processing steps (in order):</b>
 *
 * <ol>
 *   <li><b>Read from topic:</b> Consume from {@code processed-transactions} using Avro SerDe.
 *   <li><b>Register state store:</b> Add the RocksDB dedup store to the topology graph.
 *   <li><b>Register GlobalKTable:</b> Populate account reference data for future enrichment joins.
 *   <li><b>Defensive validation:</b> Drop records with null required fields — prevents NPE cascades
 *       deep in stateful processors.
 *   <li><b>Deduplication:</b> Use the {@link DeduplicationProcessor} to drop records with IDs seen
 *       within the last 24h — guards against producer retries and at-least-once redelivery.
 *   <li><b>Minimum amount filter:</b> Drop sub-cent transactions (auth checks / noise).
 *   <li><b>Centralized re-keying:</b> Change the Kafka message key from transactionId to accountId.
 *       This is the most expensive step (triggers a re-partition network shuffle) — doing it once
 *       here saves every downstream topology from having to do it individually.
 *   <li><b>Group:</b> Produce a {@link KGroupedStream} keyed by accountId for aggregations.
 * </ol>
 *
 * <p><b>SourceContext Pattern:</b><br>
 * Rather than returning a single stream, this topology returns a {@link SourceContext} container
 * that holds both the keyed stream and the grouped stream. Downstream topologies consume the
 * appropriate form without re-reading from Kafka.
 */
@Slf4j
@Component
public class SourceTopology {

  /** Sub-cent transactions are typically auth-checks or rounding artefacts — treat as noise. */
  private static final BigDecimal MINIMUM_VALID_AMOUNT = new BigDecimal("0.01");

  /**
   * How long to retain a transaction ID in the deduplication store. 24h covers all realistic
   * producer retry windows and at-least-once redelivery gaps.
   */
  private static final Duration DEDUPLICATION_TTL = Duration.ofHours(24);

  private final SerdeConfig serdeConfig;
  private final MeterRegistry meterRegistry;

  // ─── Metrics ──────────────────────────────────────────────────────────────────────────────────
  private final Counter malformedCounter; // Records dropped due to null required fields
  private final Counter smallAmountCounter; // Records dropped because amount < $0.01

  @Autowired
  public SourceTopology(SerdeConfig serdeConfig, MeterRegistry meterRegistry) {
    this.serdeConfig = serdeConfig;
    this.meterRegistry = meterRegistry;

    this.malformedCounter =
        Counter.builder("streams.source.malformed.count")
            .description("Malformed records dropped at the source before entering the pipeline")
            .register(meterRegistry);
    this.smallAmountCounter =
        Counter.builder("streams.source.small_amount.count")
            .description("Transactions dropped because amount was below the minimum threshold")
            .register(meterRegistry);
  }

  /**
   * Output of {@link SourceTopology#buildSource}. Carries two views of the same stream.
   *
   * <p><b>TUTORIAL:</b> Returning this context object avoids forcing downstream topologies to
   * re-read from Kafka. All downstream modules share the same source node in the topology DAG —
   * zero duplicated consumption, zero extra broker load.
   *
   * <ul>
   *   <li>{@code keyedStream} — a flat KStream keyed by accountId; used for joins and routing.
   *   <li>{@code groupedStream} — a KGroupedStream keyed by accountId; used for aggregations
   *       (balance, metrics, sessions).
   * </ul>
   */
  @Data
  @Builder
  public static class SourceContext {
    private final KStream<String, TransactionEvent> keyedStream;
    private final KGroupedStream<String, TransactionEvent> groupedStream;
  }

  /**
   * Builds the source topology and returns both stream views.
   *
   * @param builder the shared {@link StreamsBuilder} provided by {@code @EnableKafkaStreams}
   * @return a {@link SourceContext} containing the keyed and grouped streams
   */
  public SourceContext buildSource(StreamsBuilder builder) {
    var txnSerde = serdeConfig.transactionEventSerde();

    // ─── Step 1: Register Deduplication State Store ───────────────────────────────────────────
    // The store must be declared BEFORE the processor that uses it is added to the topology.
    // Kafka Streams validates at build time that every processor's named stores exist.
    // We use a persistent (RocksDB) store so dedup state survives stream-thread restarts.
    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(StoreConstants.TRANSACTION_DEDUPLICATION_STORE),
            Serdes.String(), // Key: transactionId (String)
            Serdes.Long())); // Value: first-seen timestamp (epoch millis as Long)

    // ─── Step 2: Register GlobalKTable for Account Enrichment ─────────────────────────────────
    // TUTORIAL: GlobalKTable vs KTable:
    // - KTable: partitioned — each stream instance holds a subset of keys.
    // - GlobalKTable: fully replicated — ALL instances hold ALL keys.
    // Use GlobalKTable for small reference datasets (like account metadata) that you want
    // to join without triggering a co-partitioning requirement.
    builder.globalTable(
        TopicConstants.ACCOUNT_REFERENCE,
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.as(StoreConstants.ACCOUNT_REFERENCE_STORE));

    // ─── Step 3: Read from the source topic ──────────────────────────────────────────────────
    // We read from PROCESSED_TRANSACTIONS (not RAW) because only committed, audited
    // transactions are ready for stream analytics.
    KStream<String, TransactionEvent> stream =
        builder.stream(
            TopicConstants.PROCESSED_TRANSACTIONS, Consumed.with(Serdes.String(), txnSerde));

    KStream<String, TransactionEvent> keyedStream =
        stream
            // ─── Step 4: Defensive Validation ───────────────────────────────────────────────
            // Drop any record missing required business fields before it reaches stateful logic.
            // A null amount in a balance aggregation would throw an NPE deep in RocksDB operations
            // — much harder to debug than catching it here at the gate.
            .filter(
                (k, v) -> {
                  boolean valid =
                      v != null
                          && v.getTransactionId() != null
                          && v.getAccountId() != null
                          && v.getAmount() != null;
                  if (!valid) {
                    malformedCounter.increment();
                    log.warn("Dropping malformed record (null required field): key={}", k);
                  }
                  return valid;
                })

            // ─── Step 5: Deduplication via Low-Level Processor API ───────────────────────────
            // TUTORIAL: .process() is the bridge from the high-level DSL to the Processor API.
            // The lambda creates a new DeduplicationProcessor instance for each stream task
            // (one per partition). Each task has its own isolated state store partition.
            // The second argument names the state store this processor needs access to.
            .process(
                () ->
                    new DeduplicationProcessor<>(
                        StoreConstants.TRANSACTION_DEDUPLICATION_STORE,
                        event -> event.getTransactionId().toString(), // ID extraction lambda
                        DEDUPLICATION_TTL,
                        meterRegistry),
                StoreConstants.TRANSACTION_DEDUPLICATION_STORE)

            // ─── Step 6: Minimum Amount Filter ───────────────────────────────────────────────
            // Auth-check transactions and rounding artefacts (< $0.01) distort balance totals.
            // Drop them here to keep aggregations meaningful.
            .filter(
                (id, event) -> {
                  boolean valid = event.getAmount().compareTo(MINIMUM_VALID_AMOUNT) >= 0;
                  if (!valid) {
                    smallAmountCounter.increment();
                    log.debug(
                        "Dropping sub-threshold amount: id={}, amount={}", id, event.getAmount());
                  }
                  return valid;
                })

            // ─── Step 7: Centralized Re-keying by AccountId ──────────────────────────────────
            // TUTORIAL — Why re-key here?
            // Kafka partitions records by their key using a hash. The original key is
            // transactionId.
            // Aggregations (balance, metrics, sessions) need records grouped by accountId.
            // selectKey() changes the key and triggers a repartition (an internal shuffle over
            // the network to ensure all records for the same accountId go to the same partition).
            // By doing this ONCE here, we save every downstream topology from doing its own
            // expensive repartition — a significant network bandwidth and CPU saving.
            .selectKey((txnId, event) -> event.getAccountId().toString());

    // ─── Step 8: Group for Aggregations ──────────────────────────────────────────────────────
    // groupByKey() is cheaper than groupBy() because the re-keying already happened in selectKey().
    // It does NOT trigger an additional repartition — it simply marks the stream as grouped.
    KGroupedStream<String, TransactionEvent> groupedStream =
        keyedStream.groupByKey(Grouped.with(Serdes.String(), txnSerde));

    return SourceContext.builder()
        .keyedStream(
            keyedStream) // For downstream joins (FraudTopology) and routing (RoutingTopology)
        .groupedStream(groupedStream) // For downstream aggregations (Balance, Metrics, Session)
        .build();
  }
}
