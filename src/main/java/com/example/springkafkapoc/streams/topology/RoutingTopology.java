package com.example.springkafkapoc.streams.topology;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.streams.SerdeConfig;
import java.math.BigDecimal;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

/**
 * <b>Routing & Branching Topology (Content-Based Routing)</b>
 *
 * <p><b>TUTORIAL:</b> This module demonstrates how to route data to different topics based on its
 * content.
 *
 * <p>We split the main stream into "High Value" and "Standard" transaction topics. This is useful
 * when downstream systems only care about specific subsets of data (e.g., a High-Value Audit
 * System).
 *
 * <p>Key Concept: <b>Named Branching</b>. Using `.split(Named.as(...))` makes the topology graph
 * much easier to read and debug.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RoutingTopology {

  private static final BigDecimal HIGH_VALUE_THRESHOLD = new BigDecimal("10000");
  private final SerdeConfig serdeConfig;

  public void build(KStream<String, TransactionEvent> keyedTransactionStream) {
    var txnSerde = serdeConfig.transactionEventSerde();

    var branches =
        keyedTransactionStream
            .split(Named.as("txn-"))
            .branch(
                (key, value) -> value.getAmount().compareTo(HIGH_VALUE_THRESHOLD) > 0,
                Branched.as("high-value"))
            .defaultBranch(Branched.as("standard"));

    // Null-safe access to branches as recommended
    KStream<String, TransactionEvent> highValue =
        Objects.requireNonNull(branches.get("txn-high-value"));
    KStream<String, TransactionEvent> standard =
        Objects.requireNonNull(branches.get("txn-standard"));

    highValue.to(TopicConstants.HIGH_VALUE_TRANSACTIONS, Produced.with(Serdes.String(), txnSerde));
    standard.to(TopicConstants.NORMAL_TRANSACTIONS, Produced.with(Serdes.String(), txnSerde));

    // Unified audit sink
    highValue
        .merge(standard)
        .to(TopicConstants.ALL_TRANSACTIONS_AUDIT, Produced.with(Serdes.String(), txnSerde));

    if (log.isDebugEnabled()) {
      log.debug("RoutingTopology initialized with threshold: {}", HIGH_VALUE_THRESHOLD);
    }
  }
}
