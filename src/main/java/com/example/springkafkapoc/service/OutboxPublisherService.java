package com.example.springkafkapoc.service;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.domain.model.Outbox;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * <b>Outbox Publisher — The Relay Engine</b>
 *
 * <p><b>TUTORIAL — What does this service do?</b><br>
 * The {@link OutboxService} saves "pending Kafka messages" to the database as part of the business
 * transaction. This service is the complementary piece: it runs on a schedule, reads those pending
 * records, and reliably ships them to Kafka.
 *
 * <p><b>WHY a separate poller? Why not just send to Kafka directly in the consumer?</b><br>
 * Direct sends create a "dual-write" problem: if the DB commit succeeds but Kafka is momentarily
 * unavailable, you get an inconsistent state — the transaction is recorded in the DB but the
 * downstream systems never hear about it. The Outbox pattern eliminates this: the Kafka send is
 * decoupled from the DB write and retried indefinitely until it succeeds.
 *
 * <p><b>Distributed Locking:</b><br>
 * In a multi-pod deployment, every pod runs this scheduler. Without locking, all pods would
 * read the same unprocessed records simultaneously and publish each message multiple times.
 * {@link LockRegistry} (backed by a JDBC lock table) ensures only <em>one</em> instance is
 * active at any moment — the others skip their cycle gracefully.
 *
 * <p><b>Observability:</b><br>
 * Every key event (published, error, lock failure, backlog size) is instrumented with Micrometer
 * counters and gauges so Prometheus/Grafana dashboards can show the health of this relay in
 * real time.
 */
@Slf4j
@Service
@EnableScheduling  // Enables @Scheduled methods in this class
public class OutboxPublisherService {

  private final OutboxService outboxService;
  private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
  private final LockRegistry lockRegistry;        // JDBC-backed distributed lock
  private final ObjectMapper objectMapper;         // JSON deserializer for stored payloads
  private final MeterRegistry meterRegistry;       // Micrometer metrics registry

  // ─── Metrics ──────────────────────────────────────────────────────────────────────────────────

  private final Timer pollTimer;                   // How long one full poll cycle takes
  private final Counter publishedCounter;          // Total records successfully relayed to Kafka
  private final Counter errorCounter;              // Total relay failures
  private final Counter lockFailureCounter;        // Times another instance held the lock
  private final AtomicLong backlogSize = new AtomicLong(0); // Current pending outbox size

  // The key used to obtain the distributed lock — all instances compete for the same key
  private static final String LOCK_KEY = "outbox-publisher-lock";

  @Autowired
  public OutboxPublisherService(
      OutboxService outboxService,
      KafkaTemplate<String, TransactionEvent> kafkaTemplate,
      LockRegistry lockRegistry,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry) {
    this.outboxService = outboxService;
    this.kafkaTemplate = kafkaTemplate;
    this.lockRegistry = lockRegistry;
    this.objectMapper = objectMapper;
    this.meterRegistry = meterRegistry;

    // Register metrics at construction time so they appear in Prometheus even before any events
    this.pollTimer =
        Timer.builder("outbox.poll.time")
            .description("Time taken for one outbox poll cycle")
            .register(meterRegistry);
    this.publishedCounter =
        Counter.builder("outbox.published.count")
            .description("Total outbox messages successfully published to Kafka")
            .register(meterRegistry);
    this.errorCounter =
        Counter.builder("outbox.publish.errors")
            .description("Number of errors during outbox publishing")
            .register(meterRegistry);
    this.lockFailureCounter =
        Counter.builder("outbox.lock.failures")
            .description("Times the outbox poller skipped because another instance held the lock")
            .register(meterRegistry);

    // Gauge — reflects the current backlog size live on every Prometheus scrape
    meterRegistry.gauge("outbox.backlog.size", backlogSize);
  }

  /**
   * Scheduled polling method — runs every {@code app.outbox.poll-interval-ms} milliseconds
   * (default 5 seconds).
   *
   * <p><b>TUTORIAL — Step-by-step flow:</b>
   *
   * <ol>
   *   <li>Attempt to acquire the distributed JDBC lock. Non-blocking — returns immediately if
   *       another instance holds it.
   *   <li>Query the outbox table for unprocessed records (batched to avoid OOM).
   *   <li>For each record: deserialize the JSON payload back to a {@link TransactionEvent}.
   *   <li>Send to Kafka <em>within a Kafka transaction</em>. Only after a successful {@code .get()}
   *       (synchronous confirmation from the broker) do we mark the record as processed.
   *   <li>Release the lock in the {@code finally} block — always, even on failure.
   * </ol>
   *
   * <p><b>PRO TIP — At-Least-Once guarantee:</b><br>
   * If the JVM crashes after the Kafka send but before {@code markAsProcessed}, the record will
   * be sent to Kafka again on the next poll. Downstream consumers must therefore be idempotent.
   * This is a deliberate trade-off: we prefer duplicate delivery over silent data loss.
   */
  @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:5000}")
  public void publishOutboxMessages() {

    // Step 1: Try to acquire the distributed lock.
    // lockRegistry.obtain() fetches a Lock object keyed by LOCK_KEY.
    Lock lock = lockRegistry.obtain(LOCK_KEY);

    if (!lock.tryLock()) {
      // This is NOT an error — it just means another pod is already doing this work.
      // We count it for observability (to detect if locking is too contentious)
      // and exit gracefully.
      lockFailureCounter.increment();
      log.trace("Outbox lock held by another instance. Skipping this cycle.");
      return;
    }

    try {
      // Wrap the entire poll cycle in a timer so Grafana can show cycle duration trends
      pollTimer.record(
          () -> {
            // Step 2: Fetch pending records in a page (max 100) to avoid loading the entire
            // table into memory. In a high-volume system, reduce this page size and run more
            // frequent cycles instead.
            List<Outbox> messages = outboxService.findUnprocessedMessages();
            backlogSize.set(messages.size()); // Update backlog gauge for Prometheus

            if (messages.isEmpty()) return; // Nothing to do — exit the lambda early

            log.info("Outbox Poller: {} pending records to relay.", messages.size());

            for (Outbox msg : messages) {
              try {
                // Step 3: Deserialize the stored JSON back into the Avro domain object.
                // The payload was serialized when the consumer originally wrote the outbox record.
                TransactionEvent event =
                    objectMapper.readValue(msg.getPayload(), TransactionEvent.class);

                // Step 4: Send within a Kafka transaction for atomicity.
                // executeInTransaction() begins a producer transaction, and commits or
                // aborts it automatically based on whether the lambda throws.
                kafkaTemplate.executeInTransaction(
                    ops -> {
                      try {
                        // Synchronous send — .get() blocks until the broker ACKs.
                        // This ensures we don't mark the record as processed before Kafka
                        // has durably accepted the message.
                        ops.send(msg.getDestinationTopic(), msg.getAggregateId(), event).get();

                        // Step 5: Mark the outbox record as processed ONLY after broker ACK.
                        // This guarantees at-least-once delivery. If the JVM crashes between
                        // send and markAsProcessed, the next poll will re-send — acceptable.
                        outboxService.markAsProcessed(msg.getId());
                        publishedCounter.increment();
                        log.debug("Relayed and closed Outbox ID: {}", msg.getId());
                      } catch (Exception e) {
                        errorCounter.increment();
                        // Throwing here rolls back the Kafka transaction
                        throw new RuntimeException("Failed to relay outbox message", e);
                      }
                      return null; // executeInTransaction requires a return value
                    });
              } catch (Exception e) {
                // Log and continue to the next record — don't let one bad record stall all others
                errorCounter.increment();
                log.error("Error processing Outbox record {}: {}", msg.getId(), e.getMessage());
              }
            }
          });
    } finally {
      // Step 6: Always release the lock — even on exception — so other instances can proceed
      lock.unlock();
    }
  }
}
