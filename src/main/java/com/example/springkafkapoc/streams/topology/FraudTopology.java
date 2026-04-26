package com.example.springkafkapoc.streams.topology;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.domain.model.FraudAlert;
import com.example.springkafkapoc.streams.SerdeConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

/**
 * <b>Fraud Detection Topology — Temporal KStream-KStream Join</b>
 *
 * <p><b>TUTORIAL — Stream-Stream Joins:</b><br>
 * A KStream-KStream join correlates two independent event streams within a shared time window.
 * Unlike KTable joins (which join a stream against a point-in-time snapshot), a stream-stream join
 * considers ALL events within a time window on BOTH sides.
 *
 * <p><b>What this topology does:</b><br>
 * When a transaction occurs for an account that also has a fraud signal within ±5 minutes, a {@link
 * FraudAlert} is emitted to the fraud-alerts topic. This is "temporal correlation" — two
 * independent events are linked by proximity in time, not by a shared database lookup.
 *
 * <p><b>Real-world analogy:</b><br>
 * Think of a bank's fraud team sending a "watch this account" signal. If that same account makes a
 * transaction within 5 minutes of the signal, the system automatically flags it. The detection
 * happens in milliseconds — far faster than any human alert-checking workflow.
 *
 * <p><b>Co-partitioning requirement:</b><br>
 * For a KStream-KStream join to work correctly, BOTH input topics MUST be:
 *
 * <ol>
 *   <li>Partitioned by the same key (here: accountId — set in SourceTopology re-keying).
 *   <li>Have the same number of partitions.
 * </ol>
 *
 * If either condition is violated, records with the same key may land on different partitions in
 * each topic, and the join will miss them. Partition counts are defined in {@link
 * com.example.springkafkapoc.config.KafkaCoreConfig}.
 *
 * <p><b>PRO TIP — JoinWindows and Grace:</b><br>
 * The {@code ofTimeDifferenceAndGrace(5min, 30sec)} means:
 *
 * <ul>
 *   <li>Join window: ±5 minutes (a transaction within 5 min of a signal = match).
 *   <li>Grace period: 30 seconds for late-arriving records to still be joined.
 * </ul>
 *
 * Without grace, a 1-second network delay could cause a transaction to miss its fraud signal.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudTopology {

  private final SerdeConfig serdeConfig;

  /**
   * Builds the fraud detection join topology.
   *
   * @param builder the shared {@link StreamsBuilder}
   * @param keyedTransactionStream the transaction stream keyed by accountId (from SourceTopology)
   */
  public void build(
      StreamsBuilder builder, KStream<String, TransactionEvent> keyedTransactionStream) {
    var txnSerde = serdeConfig.transactionEventSerde();

    // Use Jackson-based JSON SerDe for FraudAlert — it's a domain POJO, not an Avro record.
    // In production, consider converting FraudAlert to Avro for schema enforcement.
    var alertSerde = new JsonSerde<>(FraudAlert.class);

    // ─── Read the Fraud Signals Stream ────────────────────────────────────────────────────────
    // Fraud signals are simple strings: the key = accountId, value = reason/description.
    // They come from an external fraud detection system (e.g., ML model, rules engine).
    // IMPORTANT: this topic must have the SAME partition count as processed-transactions
    // for the co-partitioned join to work. See KafkaCoreConfig.fraudSignalsTopic().
    KStream<String, String> fraudSignalStream =
        builder.stream(
            TopicConstants.FRAUD_SIGNALS, Consumed.with(Serdes.String(), Serdes.String()));

    // ─── Define the Join Window ───────────────────────────────────────────────────────────────
    // Match a transaction with a fraud signal if they arrive within ±5 minutes of each other.
    // Grace period (30s): allows slightly late records to still be included in the join.
    JoinWindows fraudWindow =
        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30));

    // ─── Inner Join: Transaction ⋈ FraudSignal ───────────────────────────────────────────────
    // An INNER join emits a result ONLY when BOTH sides have a matching record within the window.
    // If a transaction has no corresponding fraud signal → no alert emitted (normal transaction).
    // If a fraud signal has no corresponding transaction → no alert emitted (signal expires).
    keyedTransactionStream
        .join(
            fraudSignalStream,
            // Joiner: called for every (transaction, fraudSignal) pair within the time window
            (transaction, fraudReason) -> {
              log.warn(
                  "FRAUD CORRELATION: txn={}, account={}, signal={}",
                  transaction.getTransactionId(),
                  transaction.getAccountId(),
                  fraudReason);

              // Build a structured FraudAlert domain object for downstream consumers
              return FraudAlert.builder()
                  .alertId(UUID.randomUUID().toString()) // Unique alert ID for tracking
                  .transactionId(transaction.getTransactionId().toString())
                  .accountId(transaction.getAccountId().toString())
                  .amount(transaction.getAmount())
                  .signal(fraudReason) // The fraud signal description from the signal stream
                  .timestamp(Instant.now()) // Wall-clock time of the alert (not event time)
                  .build();
            },
            fraudWindow,
            // StreamJoined: specifies SerDes for BOTH join sides and internal state stores
            // Kafka Streams materializes both sides of a stream-stream join into state stores
            // to buffer records while waiting for the matching record to arrive within the window
            StreamJoined.with(Serdes.String(), txnSerde, Serdes.String()))
        // Publish the FraudAlert to the fraud-alerts topic for downstream notification systems
        .to(TopicConstants.FRAUD_ALERTS, Produced.with(Serdes.String(), alertSerde));
  }
}
