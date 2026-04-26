package com.example.springkafkapoc.streams.topology;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.streams.SerdeConfig;
import com.example.springkafkapoc.streams.StoreConstants;
import java.math.BigDecimal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

/**
 * <b>Balance Aggregation Topology (KTable & State Stores)</b>
 *
 * <p><b>TUTORIAL:</b> This module maintains the real-time "Source of Truth" for account balances.
 *
 * <p>Instead of querying a database for every transaction, we maintain a <b>Local State Store</b>
 * (backed by RocksDB). This allows for sub-millisecond lookups and updates, achieving incredible
 * throughput.
 *
 * <p>Key Concepts:
 *
 * <ul>
 *   <li><b>KTable:</b> Represents a changelog stream where every record is an update to a primary
 *       key (AccountId).
 *   <li><b>State Store:</b> The local persistent database where the balance is saved.
 *   <li><b>Optimized Serdes:</b> Using scaled longs for financial totals to minimize state size.
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BalanceTopology {

  private final SerdeConfig serdeConfig;

  public void build(KGroupedStream<String, TransactionEvent> groupedStream) {
    var decimalSerde = serdeConfig.optimizedBigDecimalSerde();

    groupedStream
        .aggregate(
            () -> BigDecimal.ZERO,
            (accountId, event, currentBalance) -> {
              // Standard addition for running totals
              BigDecimal updated = currentBalance.add(event.getAmount());
              if (log.isDebugEnabled()) {
                log.debug("Balance update: account={}, newBalance={}", accountId, updated);
              }
              return updated;
            },
            Materialized.<String, BigDecimal, KeyValueStore<Bytes, byte[]>>as(
                    StoreConstants.ACCOUNT_BALANCE_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(decimalSerde))
        // Convert the KTable back to a Stream so we can publish the updates to a topic
        .toStream()
        .to(TopicConstants.ACCOUNT_BALANCES, Produced.with(Serdes.String(), decimalSerde));
  }
}
