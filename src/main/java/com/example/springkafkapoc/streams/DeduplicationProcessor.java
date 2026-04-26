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
 * <b>Deduplication Processor — Stateful Idempotency at the Stream Layer</b>
 *
 * <p><b>TUTORIAL — Why use the Low-Level Processor API here?</b><br>
 * The Kafka Streams DSL (KStream.filter, KStream.map, etc.) is declarative and easy to use, but
 * it doesn't give you access to the raw state store or the ability to schedule periodic tasks.
 * For deduplication — where you need both a state store lookup AND a timed cleanup — the
 * <b>Processor API</b> is the right tool.
 *
 * <p><b>How deduplication works:</b>
 * <ol>
 *   <li>When a record arrives, extract its unique ID (e.g., transactionId) via the
 *       {@link IdExtractor} lambda.
 *   <li>Look up that ID in the RocksDB {@link KeyValueStore}. If found → duplicate → drop.
 *   <li>If not found → first-time record → store the ID with its timestamp → forward downstream.
 *   <li>A scheduled Punctuator runs every hour to delete entries older than the configured TTL
 *       (24h), preventing the state store from growing indefinitely.
 * </ol>
 *
 * <p><b>WHY a TTL?</b><br>
 * Without TTL cleanup, the deduplication store would accumulate every transaction ID ever
 * processed — eventually exhausting disk space or causing RocksDB compaction to become very slow.
 * The 24h TTL means we protect against duplicates within a day's processing window, which covers
 * all realistic producer retry scenarios.
 *
 * <p><b>PRO TIP — Full-Scan Warning:</b><br>
 * The cleanup Punctuator uses {@code store.all()} which iterates the entire RocksDB store. At low
 * volumes (&lt;1M entries) this is fine. At very high volumes, consider a custom state store
 * with built-in TTL support (e.g., RocksDB TTL compaction filter) to avoid periodic full scans.
 *
 * <p><b>Generic type parameters:</b>
 * <ul>
 *   <li>{@code K} — the Kafka record key type (usually {@code String})
 *   <li>{@code V} — the Kafka record value type (e.g., {@code TransactionEvent})
 *   <li>{@code ID} — the type of the deduplicated ID extracted from the value (e.g., {@code String}
 *       for transactionId)
 * </ul>
 */
@Slf4j
public class DeduplicationProcessor<K, V, ID> extends ContextualProcessor<K, V, K, V> {

  private final String storeName;           // Name of the RocksDB state store to look up
  private final IdExtractor<V, ID> idExtractor; // Lambda to extract the dedup ID from the value
  private final Duration ttl;               // How long to remember a seen ID before evicting it
  private final MeterRegistry meterRegistry;

  // The actual RocksDB state store — obtained at init() time
  private KeyValueStore<ID, Long> store;

  // ─── Metrics ──────────────────────────────────────────────────────────────────────────────────

  private final Counter duplicateCounter;  // How many records were dropped as duplicates
  private final Counter recordCounter;     // Total records seen (for computing dedup rate)

  public DeduplicationProcessor(
      String storeName, IdExtractor<V, ID> idExtractor, Duration ttl, MeterRegistry meterRegistry) {
    this.storeName = storeName;
    this.idExtractor = idExtractor;
    this.ttl = ttl;
    this.meterRegistry = meterRegistry;

    // Tag by store name so dashboards can distinguish between multiple dedup processors
    this.duplicateCounter =
        Counter.builder("streams.deduplication.duplicates.count")
            .description("Duplicate records detected and dropped by the stream processor")
            .tag("store", storeName)
            .register(meterRegistry);
    this.recordCounter =
        Counter.builder("streams.deduplication.records.count")
            .description("Total records seen by the deduplication processor")
            .tag("store", storeName)
            .register(meterRegistry);
  }

  /**
   * Called once by Kafka Streams when the processor is initialized.
   *
   * <p><b>TUTORIAL:</b> This is where you obtain references to state stores and register
   * Punctuators. The {@link ProcessorContext} is the "toolbox" for the Processor API — it gives
   * access to the store registry, the ability to schedule tasks, and the ability to forward
   * records downstream.
   */
  @Override
  public void init(ProcessorContext<K, V> context) {
    super.init(context); // Always call super.init — it stores the context for context() calls

    // Retrieve the pre-registered RocksDB store by name
    // The store was registered in SourceTopology.buildSource() via builder.addStateStore(...)
    this.store = context.getStateStore(storeName);

    // ─── Punctuator: Scheduled TTL Cleanup ──────────────────────────────────────────────────────
    // TUTORIAL: A Punctuator is a periodic task that runs inside the stream thread.
    // WALL_CLOCK_TIME: triggers based on real wall-clock time (not event time).
    // This runs every hour to evict expired deduplication entries.
    context.schedule(
        Duration.ofHours(1),
        org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME,
        timestamp -> {
          log.debug("Running TTL cleanup for dedup store: {}", storeName);

          // Calculate the oldest acceptable first-seen timestamp
          long cutoff = timestamp - ttl.toMillis();

          // Iterate the entire store — evict entries older than the TTL cutoff
          // PRO TIP: At high scale (>5M entries), replace this with a RocksDB TTL filter
          // to avoid periodically scanning the entire store on the stream thread
          try (var iterator = store.all()) {
            while (iterator.hasNext()) {
              var entry = iterator.next();
              if (entry.value < cutoff) {
                // This ID was first seen more than TTL ago — safe to evict
                store.delete(entry.key);
              }
            }
          }
          log.debug("Dedup store cleanup complete for: {}", storeName);
        });
  }

  /**
   * Called for every incoming record.
   *
   * <p><b>TUTORIAL — Processing flow:</b>
   * <ol>
   *   <li>Guard: null values pass through without deduplication (e.g., tombstone records).
   *   <li>Extract the business ID from the record value using the injected {@link IdExtractor}.
   *   <li>Look up the ID in the store. If present → duplicate → drop (don't forward).
   *   <li>If absent → first time seen → save with timestamp → forward to the next processor.
   * </ol>
   */
  @Override
  public void process(Record<K, V> record) {
    // Null values (tombstones) pass through unconditionally — don't try to extract an ID
    if (record.value() == null) return;

    ID id = idExtractor.extract(record.value());

    // If no ID can be extracted (schema issue?), let the record pass — better than dropping data
    if (id == null) {
      context().forward(record);
      return;
    }

    recordCounter.increment(); // Count every non-null record for dedup rate calculation

    Long seenAt = store.get(id); // Null means never seen; non-null means we've seen it before

    if (seenAt != null) {
      // Duplicate detected — log at DEBUG to avoid overwhelming logs at high throughput
      log.debug("Duplicate in stream: id={}. Dropping.", id);
      duplicateCounter.increment();
      return; // Do NOT call context().forward() — this drops the record from the topology
    }

    // First time seeing this ID — record the arrival timestamp for future TTL checks
    store.put(id, record.timestamp());

    // Forward the record to the next processor/sink in the topology
    context().forward(record);
  }

  /**
   * Functional interface for extracting the deduplication key from a record value.
   *
   * <p><b>TUTORIAL:</b> Using a lambda here makes the processor fully generic. The caller
   * passes in {@code event -> event.getTransactionId().toString()} at construction time.
   * This means the same processor class can be reused for ANY event type — not just
   * TransactionEvent.
   *
   * @param <V>  the value type
   * @param <ID> the extracted ID type (must be storable in the state store)
   */
  @FunctionalInterface
  public interface IdExtractor<V, ID> {
    ID extract(V value);
  }
}
