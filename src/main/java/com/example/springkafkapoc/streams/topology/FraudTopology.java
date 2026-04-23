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
 * <b>Fraud Detection Topology (Stream-Stream Join)</b>
 *
 * <p><b>TUTORIAL:</b> This module demonstrates <b>Temporal Correlation</b>. We join the primary
 * Transaction stream with a "Fraud Signal" stream.
 *
 * <p>A fraud alert is only raised if a transaction occurs within a <b>5-minute window</b> of a
 * signal being received for that account. This is the foundation of real-time pattern detection.
 *
 * <p>Key Patterns:
 *
 * <ul>
 *   <li><b>Stream-Stream Join:</b> Correlating two high-velocity event streams.
 *   <li><b>Structured Alerts:</b> Emitting a typed {@link FraudAlert} for better downstream
 *       processing.
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudTopology {

  private final SerdeConfig serdeConfig;

  public void build(
      StreamsBuilder builder, KStream<String, TransactionEvent> keyedTransactionStream) {
    var txnSerde = serdeConfig.transactionEventSerde();
    var alertSerde = new JsonSerde<>(FraudAlert.class);

    KStream<String, String> fraudSignalStream =
        builder.stream(
            TopicConstants.FRAUD_SIGNALS, Consumed.with(Serdes.String(), Serdes.String()));

    JoinWindows fraudWindow =
        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30));

    keyedTransactionStream
        .join(
            fraudSignalStream,
            (transaction, fraudReason) -> {
              if (log.isWarnEnabled()) {
                log.warn(
                    "Fraud correlation detected: txn={}, account={}, reason={}",
                    transaction.getTransactionId(),
                    transaction.getAccountId(),
                    fraudReason);
              }
              return FraudAlert.builder()
                  .alertId(UUID.randomUUID().toString())
                  .transactionId(transaction.getTransactionId().toString())
                  .accountId(transaction.getAccountId().toString())
                  .amount(transaction.getAmount())
                  .signal(fraudReason)
                  .timestamp(Instant.now())
                  .build();
            },
            fraudWindow,
            StreamJoined.with(Serdes.String(), txnSerde, Serdes.String()))
        .to(TopicConstants.FRAUD_ALERTS, Produced.with(Serdes.String(), alertSerde));
  }
}
