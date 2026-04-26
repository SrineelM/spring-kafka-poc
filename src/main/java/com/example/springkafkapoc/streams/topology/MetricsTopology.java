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
 * <b>Metrics & Windowing Topology — Tumbling and Hopping Windows</b>
 *
 * <p><b>TUTORIAL — Three Window Types in Kafka Streams:</b>
 *
 * <ol>
 *   <li><b>Tumbling Windows (used here for 24h daily totals):</b><br>
 *       Fixed-size, non-overlapping. Each event belongs to exactly ONE window. Think of it as
 *       a daily summary that resets at midnight. Midnight Tuesday's events have no overlap with
 *       Wednesday's window.
 *       <pre>
 *       |--- Day 1 ---|--- Day 2 ---|--- Day 3 ---|
 *       </pre>
 *
 *   <li><b>Hopping Windows (used here for 1h trailing view, advancing every 15 min):</b><br>
 *       Fixed-size, overlapping. Each event belongs to MULTIPLE windows. Think of it as a
 *       "rolling hour" dashboard that refreshes every 15 minutes.
 *       <pre>
 *       |--- 1h ---|
 *            |--- 1h ---|
 *                 |--- 1h ---|
 *       </pre>
 *
 *   <li><b>Session Windows (see {@link SessionTopology}):</b><br>
 *       Dynamic-size. Close after an inactivity gap. No fixed duration.
 * </ol>
 *
 * <p><b>Grace Period:</b><br>
 * {@code ofSizeAndGrace(size, grace)} allows late-arriving events to still be included in a
 * window that has logically closed. The grace period = how late can a record be (by event time)
 * and still be counted. After the grace expires, the window is sealed and its final value emitted.
 * Without a grace period, network delays would cause under-counting in time-sensitive aggregations.
 *
 * <p><b>Interactive Queries:</b><br>
 * Both stores ({@code DAILY_ACCOUNT_AGGREGATES_STORE} and {@code HOURLY_HOPPING_STORE}) are
 * materialized and queryable via {@link com.example.springkafkapoc.service.AnalyticsQueryService}
 * without going through an external database.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MetricsTopology {

  private final SerdeConfig serdeConfig;

  /**
   * Appends two windowed aggregations to the grouped stream.
   *
   * @param groupedStream keyed by accountId, provided by {@link SourceTopology}
   */
  public void build(KGroupedStream<String, TransactionEvent> groupedStream) {
    var decimalSerde = serdeConfig.optimizedBigDecimalSerde();

    // ─── 24h Tumbling Window — Daily Account Spending Total ──────────────────────────────────
    // Each 24h window is non-overlapping. A transaction on Tuesday is counted only in
    // Tuesday's window — it does NOT appear in Monday's or Wednesday's totals.
    //
    // Grace period (5 min): allows records up to 5 minutes late to still join their correct
    // window. Without this, a 5-minute network delay would cause under-counting.
    groupedStream
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofHours(24), Duration.ofMinutes(5)))
        .aggregate(
            () -> BigDecimal.ZERO,                                     // Starting value
            (accountId, event, total) -> total.add(event.getAmount()), // Accumulator
            Materialized.<String, BigDecimal, WindowStore<Bytes, byte[]>>as(
                    StoreConstants.DAILY_ACCOUNT_AGGREGATES_STORE)     // Named for Interactive Queries
                .withKeySerde(Serdes.String())
                .withValueSerde(decimalSerde))
        .toStream()
        // Format the output key as "accountId@windowStartEpochMs" for downstream visualization.
        // Dashboards can parse this key to align records on a time axis without a separate
        // timestamp field in the value.
        .map((wk, total) -> new KeyValue<>(wk.key() + "@" + wk.window().start(), total))
        .to(TopicConstants.DAILY_ACCOUNT_METRICS, Produced.with(Serdes.String(), decimalSerde));

    // ─── 1h Hopping Window — Trailing Spend View (15-min refresh) ────────────────────────────
    // Size = 1 hour: each window covers 60 minutes of data.
    // Advance = 15 minutes: a new window opens every 15 minutes.
    // This means a record at T=0 contributes to windows starting at:
    //   T-45m, T-30m, T-15m, T+0 (4 overlapping windows simultaneously)
    // This gives a "smoothed trailing hour" view rather than discrete 1h snapshots.
    groupedStream
        .windowedBy(
            TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(2))
                .advanceBy(Duration.ofMinutes(15))) // Hopping interval — creates overlapping windows
        .aggregate(
            () -> BigDecimal.ZERO,
            (accountId, event, total) -> total.add(event.getAmount()),
            Materialized.<String, BigDecimal, WindowStore<Bytes, byte[]>>as(
                    StoreConstants.HOURLY_HOPPING_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(decimalSerde))
        .toStream()
        // Prefix key with "hop:" to distinguish from daily metrics in downstream consumers
        .map((wk, total) -> new KeyValue<>(wk.key() + "@hop:" + wk.window().start(), total))
        .to(TopicConstants.HOURLY_ACCOUNT_METRICS, Produced.with(Serdes.String(), decimalSerde));
  }
}
