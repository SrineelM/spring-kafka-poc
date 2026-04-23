package com.example.springkafkapoc.streams.topology;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.streams.SerdeConfig;
import com.example.springkafkapoc.streams.StoreConstants;
import java.math.BigDecimal;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.stereotype.Component;

/**
 * <b>Metrics & Windowing Topology (Tumbling & Hopping)</b>
 *
 * <p><b>TUTORIAL:</b> This module demonstrates the two most common windowing strategies in
 * streaming analytics.
 *
 * <p>Key Window Types:
 *
 * <ul>
 *   <li><b>Tumbling Windows (24h):</b> Non-overlapping windows. Perfect for "Daily Totals". Once a
 *       day ends, a new window starts with a clean slate.
 *   <li><b>Hopping Windows (1h size, 15m advance):</b> Overlapping windows. Perfect for "Trailing
 *       Trends". This shows the spend in the last hour, but updates every 15 minutes.
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MetricsTopology {

  private final SerdeConfig serdeConfig;

  public void build(KGroupedStream<String, TransactionEvent> groupedStream) {
    var decimalSerde = serdeConfig.optimizedBigDecimalSerde();

    // 24-hour Tumbling Window
    groupedStream
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofHours(24), Duration.ofMinutes(5)))
        .aggregate(
            () -> BigDecimal.ZERO,
            (accountId, event, total) -> total.add(event.getAmount()),
            Materialized.<String, BigDecimal, WindowStore<Bytes, byte[]>>as(
                    StoreConstants.DAILY_ACCOUNT_AGGREGATES_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(decimalSerde))
        .toStream()
        .map((wk, total) -> new KeyValue<>(wk.key() + "@" + wk.window().start(), total))
        .to(TopicConstants.DAILY_ACCOUNT_METRICS, Produced.with(Serdes.String(), decimalSerde));

    // 1-hour Hopping Window
    groupedStream
        .windowedBy(
            TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(2))
                .advanceBy(Duration.ofMinutes(15)))
        .aggregate(
            () -> BigDecimal.ZERO,
            (accountId, event, total) -> total.add(event.getAmount()),
            Materialized.<String, BigDecimal, WindowStore<Bytes, byte[]>>as(
                    StoreConstants.HOURLY_HOPPING_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(decimalSerde))
        .toStream()
        .map((wk, total) -> new KeyValue<>(wk.key() + "@hop:" + wk.window().start(), total))
        .to(TopicConstants.HOURLY_ACCOUNT_METRICS, Produced.with(Serdes.String(), decimalSerde));
  }
}
