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
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.stereotype.Component;

/**
 * <b>Session Activity Topology — Dynamic Windows & Suppression</b>
 *
 * <p><b>TUTORIAL — Session Windows vs. Time Windows:</b><br>
 * Time windows (tumbling/hopping) have a fixed duration. Session windows have <em>no</em> fixed
 * size — they grow as long as activity continues. A session closes (and emits its final result)
 * only when there has been no activity for a configured "inactivity gap."
 *
 * <p><b>Real-world analogy:</b><br>
 * Think of a user's shopping session. It starts with the first transaction and ends when the user
 * stops transacting for 30 minutes. If they make transactions at 10:00, 10:10, 10:25, and then
 * nothing until 11:15, the session covers 10:00–10:25 and the total is the sum of all three.
 *
 * <p><b>Why Suppression?</b><br>
 * Session windows are "eager" by default — they emit an intermediate result on EVERY record that
 * extends the session. For a 30-transaction session, you'd get 30 intermediate updates to the
 * downstream topic, 29 of which are not the final answer. {@code Suppressed.untilWindowCloses()}
 * buffers all intermediate updates in memory and emits ONLY the final result once the inactivity
 * gap expires. This dramatically reduces downstream noise.
 *
 * <p><b>PRO TIP — Memory management:</b><br>
 * Suppression buffers in memory. The {@code BufferConfig.maxBytes(50MB)} ensures we don't run
 * out of heap. If the buffer fills up, {@code shutDownWhenFull()} halts the stream thread with
 * an exception — better than silently dropping data. In production, tune this to match your
 * expected peak session volume × average session size.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SessionTopology {

  private final SerdeConfig serdeConfig;

  /**
   * Appends session-windowed aggregation to the grouped stream.
   *
   * @param groupedStream keyed by accountId, from {@link SourceTopology}
   */
  public void build(KGroupedStream<String, TransactionEvent> groupedStream) {
    var decimalSerde = serdeConfig.optimizedBigDecimalSerde();

    groupedStream
        // ─── Session Window Definition ─────────────────────────────────────────────────────────
        // inactivityGap(30min): close the session if no transaction arrives for 30 minutes.
        // grace(2min): allow records up to 2 minutes late to still extend an open session.
        .windowedBy(
            SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(30), Duration.ofMinutes(2)))
        .aggregate(
            () -> BigDecimal.ZERO,  // Initial value for new sessions
            // Adder: called for each new record that extends the session
            (accountId, event, total) -> total.add(event.getAmount()),
            // Merger: called when two previously separate sessions are merged.
            // TUTORIAL: Session merging happens when a late record arrives that bridges two
            // existing sessions. Kafka Streams merges them into one and calls this lambda.
            (accountId, total1, total2) -> total1.add(total2),
            // Materialize to a SessionStore — note the different store type vs KeyValueStore
            Materialized.<String, BigDecimal, SessionStore<Bytes, byte[]>>as(
                    StoreConstants.SESSION_ACTIVITY_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(decimalSerde))

        // ─── Suppression: emit only the FINAL result, not every intermediate update ───────────
        // TUTORIAL: Without suppress(), a session with 100 transactions would emit 100 records
        // downstream — 99 of them useless intermediate updates. suppress() buffers all updates
        // in a memory-bounded buffer and emits once when the inactivity gap expires.
        //
        // BufferConfig.maxBytes(50MB): if the buffer fills (too many concurrent open sessions),
        // shutDownWhenFull() halts with an exception — safe failure rather than silent data loss.
        .suppress(
            Suppressed.untilWindowCloses(
                BufferConfig.maxBytes(50 * 1024 * 1024L).shutDownWhenFull()))

        // Convert the windowed KTable back to a flat KStream for output
        .toStream()
        // Format the output key as "accountId@session:startEpochMs-endEpochMs"
        // This encodes both the account and the full session time range in the key,
        // enabling downstream consumers to reconstruct the timeline without extra lookups.
        .map(
            (wk, total) ->
                new KeyValue<>(
                    wk.key() + "@session:" + wk.window().start() + "-" + wk.window().end(), total))
        .to(TopicConstants.SESSION_ACTIVITY, Produced.with(Serdes.String(), decimalSerde));
  }
}
