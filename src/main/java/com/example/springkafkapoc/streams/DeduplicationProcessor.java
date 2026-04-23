package com.example.springkafkapoc.streams;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * <b>Deduplication Processor (Low-Level Processor API)</b>
 *
 * <p><b>TUTORIAL:</b> While the High-Level DSL (KStream/KTable) is great for 90% of tasks, some
 * logic requires the <b>Processor API</b>.
 *
 * <p>This processor uses a local state store to track "Seen" transaction IDs. If a duplicate
 * arrives (e.g., due to a producer retry), it is simply dropped.
 *
 * <p><b>Production Tip:</b> We use a **Punctuator** to periodically clean up the store. Without
 * this, the RocksDB state would grow infinitely and eventually crash the node.
 */
@Slf4j
public class DeduplicationProcessor<K, V, ID> extends ContextualProcessor<K, V, K, V> {

  private final String storeName;
  private final IdExtractor<V, ID> idExtractor;
  private final Duration ttl;
  private final MeterRegistry meterRegistry;
  private KeyValueStore<ID, Long> store;

  private final Counter duplicateCounter;
  private final Counter recordCounter;

  public DeduplicationProcessor(
      String storeName, IdExtractor<V, ID> idExtractor, Duration ttl, MeterRegistry meterRegistry) {
    this.storeName = storeName;
    this.idExtractor = idExtractor;
    this.ttl = ttl;
    this.meterRegistry = meterRegistry;

    this.duplicateCounter =
        Counter.builder("streams.deduplication.duplicates.count")
            .description("Number of duplicate records detected and dropped by the stream processor")
            .tag("store", storeName)
            .register(meterRegistry);
    this.recordCounter =
        Counter.builder("streams.deduplication.records.count")
            .description("Total number of records processed by the deduplication engine")
            .tag("store", storeName)
            .register(meterRegistry);
  }

  @Override
  public void init(ProcessorContext<K, V> context) {
    super.init(context);
    this.store = context.getStateStore(storeName);

    // Punctuator to clean up old entries every hour to prevent infinite growth
    context.schedule(
        Duration.ofHours(1),
        org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME,
        timestamp -> {
          log.debug("Running deduplication store cleanup for store: {}", storeName);
          try (var iterator = store.all()) {
            long cutoff = timestamp - ttl.toMillis();
            while (iterator.hasNext()) {
              var entry = iterator.next();
              if (entry.value < cutoff) {
                store.delete(entry.key);
              }
            }
          }
        });
  }

  @Override
  public void process(Record<K, V> record) {
    if (record.value() == null) return;

    ID id = idExtractor.extract(record.value());
    if (id == null) {
      context().forward(record);
      return;
    }

    Long seenAt = store.get(id);
    recordCounter.increment();

    if (seenAt != null) {
      log.debug("Duplicate detected in Streams: id={}. Dropping record.", id);
      duplicateCounter.increment();
      return;
    }

    // Store the ID with the record timestamp
    store.put(id, record.timestamp());
    context().forward(record);
  }

  @FunctionalInterface
  public interface IdExtractor<V, ID> {
    ID extract(V value);
  }
}
