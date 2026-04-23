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
 * <b>Session Activity Topology (Dynamic Windows & Suppression)</b>
 *
 * <p><b>TUTORIAL:</b> This module demonstrates advanced user-behavior tracking.
 *
 * <p>Unlike Time Windows (fixed size), <b>Session Windows</b> have no fixed size. They grow as long
 * as activity occurs. If there is an "inactivity gap" of 30 minutes, the session is closed and
 * emitted.
 *
 * <p><b>The Power of Suppression:</b> By using `.suppress()`, we ensure that only the **final**
 * state of the session is emitted to downstream topics, drastically reducing noise.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SessionTopology {

  private final SerdeConfig serdeConfig;

  public void build(KGroupedStream<String, TransactionEvent> groupedStream) {
    var decimalSerde = serdeConfig.optimizedBigDecimalSerde();

    groupedStream
        .windowedBy(
            SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(30), Duration.ofMinutes(2)))
        .aggregate(
            () -> BigDecimal.ZERO,
            (accountId, event, total) -> total.add(event.getAmount()),
            (accountId, total1, total2) -> total1.add(total2),
            Materialized.<String, BigDecimal, SessionStore<Bytes, byte[]>>as(
                    StoreConstants.SESSION_ACTIVITY_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(decimalSerde))
        // Bounded suppression to protect against memory spikes
        .suppress(
            Suppressed.untilWindowCloses(
                BufferConfig.maxBytes(50 * 1024 * 1024L).shutDownWhenFull()))
        .toStream()
        .map(
            (wk, total) ->
                new KeyValue<>(
                    wk.key() + "@session:" + wk.window().start() + "-" + wk.window().end(), total))
        .to(TopicConstants.SESSION_ACTIVITY, Produced.with(Serdes.String(), decimalSerde));
  }
}
