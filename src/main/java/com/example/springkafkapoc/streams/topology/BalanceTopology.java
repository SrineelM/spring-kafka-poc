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
 * <b>Balance Aggregation Topology — Real-Time Running Totals via KTable</b>
 *
 * <p><b>TUTORIAL — KTable: Changelog Streams</b><br>
 * A {@code KTable} in Kafka Streams is a mutable, partitioned key-value table backed by a local
 * RocksDB state store. Every time a new record arrives for a key, the KTable's aggregator updates
 * the running value for that key. The resulting state is continuously "materialized" to the store.
 *
 * <p><b>What this topology does:</b><br>
 * Consumes from the {@link KGroupedStream} (already keyed by accountId from {@link SourceTopology})
 * and maintains a running balance for each account. Every transaction increments the balance by its
 * amount. The updated balance is then published to the {@code account-balances} topic.
 *
 * <p><b>Why KTable and not a database?</b>
 *
 * <ul>
 *   <li>Lookups are sub-millisecond (local RocksDB vs. network round-trip to DB).
 *   <li>State is automatically fault-tolerant via Kafka changelog topics.
 *   <li>No schema migrations or index maintenance required.
 *   <li>Horizontal scalability: each instance handles a partition subset independently.
 * </ul>
 *
 * <p><b>PRO TIP — Optimized Serde:</b><br>
 * We use the {@code optimizedBigDecimalSerde} (scaled longs) rather than a string representation.
 * This cuts RocksDB memory use by ~60% and eliminates BigDecimal/String parsing from the hot path.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BalanceTopology {

  private final SerdeConfig serdeConfig; // Provides the optimized BigDecimal SerDe

  /**
   * Appends the balance aggregation steps to the provided grouped stream.
   *
   * @param groupedStream a {@link KGroupedStream} already keyed by accountId, provided by {@link
   *     SourceTopology}
   */
  public void build(KGroupedStream<String, TransactionEvent> groupedStream) {
    // Use the compact scaled-long SerDe for RocksDB storage — saves ~60% state store space
    var decimalSerde = serdeConfig.optimizedBigDecimalSerde();

    groupedStream
        // aggregate(): the KTable aggregator
        // - Initializer: () -> BigDecimal.ZERO — starting balance for a brand-new account
        // - Adder: (accountId, event, currentBalance) -> currentBalance.add(event.getAmount())
        //          Each arriving transaction adds its amount to the current running total.
        // - Materialized: persists the result to a named RocksDB store for Interactive Queries
        .aggregate(
            () -> BigDecimal.ZERO, // Initial value for new keys (first transaction for an account)
            (accountId, event, currentBalance) -> {
              BigDecimal updated = currentBalance.add(event.getAmount());
              log.debug("Balance update: account={}, newTotal={}", accountId, updated);
              return updated;
            },
            // Materialize to a named store so REST APIs can query it via Interactive Queries
            Materialized.<String, BigDecimal, KeyValueStore<Bytes, byte[]>>as(
                    StoreConstants.ACCOUNT_BALANCE_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(decimalSerde))

        // TUTORIAL — KTable → KStream conversion:
        // A KTable only publishes updates when a key's value changes. toStream() converts these
        // changelog events back into a plain KStream so we can write them to an output topic.
        // Each entry in the output topic represents the LATEST balance for that account at
        // the time of the update — not a historical record.
        .toStream()
        .to(TopicConstants.ACCOUNT_BALANCES, Produced.with(Serdes.String(), decimalSerde));
  }
}
