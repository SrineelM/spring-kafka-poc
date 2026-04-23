package com.example.springkafkapoc.streams;

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
  private KeyValueStore<ID, Long> store;

  public DeduplicationProcessor(String storeName, IdExtractor<V, ID> idExtractor, Duration ttl) {
    this.storeName = storeName;
    this.idExtractor = idExtractor;
    this.ttl = ttl;
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
    if (seenAt != null) {
      log.debug("Duplicate detected in Streams: id={}. Dropping record.", id);
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
