package com.example.springkafkapoc.service;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.domain.model.Outbox;
import com.example.springkafkapoc.domain.model.Transaction;
import com.example.springkafkapoc.observability.CorrelationIdContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * <b>Transaction Event Processor (Single-Record Mode)</b>
 *
 * <p><b>TUTORIAL — What this service does:</b><br>
 * This is the primary Kafka consumer. It listens to the raw-transactions topic and processes each
 * message ONE AT A TIME. Each message triggers a full database transaction that saves the domain
 * model, writes an Outbox record, and audits the event — all atomically.
 *
 * <p><b>Transactional Outbox Pattern (step-by-step):</b>
 *
 * <ol>
 *   <li>Kafka delivers a {@link TransactionEvent} message.
 *   <li>Idempotency check: was this transaction ID already processed? If yes → ack and skip.
 *   <li>Open a DB transaction.
 *   <li>Save the domain {@link Transaction} to the database.
 *   <li>Save an {@link Outbox} record (JSON payload) to the outbox table in the SAME transaction.
 *   <li>Commit the DB transaction. Both rows are now committed atomically.
 *   <li>Acknowledge the Kafka offset — tells the broker "done, move on."
 * </ol>
 *
 * <p><b>PRO TIP — The Outbox Pattern solves dual-write:</b><br>
 * Writing directly to Kafka AND the DB in the same method creates a "dual-write" problem. The
 * Outbox row and the Transaction row share a DB commit, so they succeed or fail together. The
 * actual Kafka publish is deferred to the {@link OutboxPublisherService} relay.
 *
 * <p><b>Active when:</b> any profile except {@code batch}. The batch processor is its sibling.
 */
@Slf4j
@Service
@Profile("!batch") // Activates in all profiles EXCEPT 'batch' — mutual exclusion with batch mode
public class TransactionEventSingleProcessor {

  private final TransactionPersistencePort transactionPersistencePort;
  private final OutboxService outboxService;
  private final PlatformTransactionManager transactionManager; // JPA transaction manager
  private final AuditService auditService;
  private final ObjectMapper objectMapper; // Jackson for serializing the outbox payload
  private final MeterRegistry meterRegistry;

  // ─── Metrics ──────────────────────────────────────────────────────────────────────────────────

  private final Timer processingTimer; // Latency per message from receive to ack
  private final Counter dlqCounter; // How many messages ended up in the DLT
  // Tracks how many messages are currently in-flight (received but not yet acked)
  private final AtomicLong backlogSize = new AtomicLong(0);

  public TransactionEventSingleProcessor(
      TransactionPersistencePort transactionPersistencePort,
      OutboxService outboxService,
      PlatformTransactionManager transactionManager,
      AuditService auditService,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry) {
    this.transactionPersistencePort = transactionPersistencePort;
    this.outboxService = outboxService;
    this.transactionManager = transactionManager;
    this.auditService = auditService;
    this.objectMapper = objectMapper;
    this.meterRegistry = meterRegistry;

    this.processingTimer =
        Timer.builder("transaction.processing.time")
            .description("End-to-end time to process one transaction message")
            .register(meterRegistry);
    this.dlqCounter =
        Counter.builder("transaction.dlq.count")
            .description("Total messages sent to the Dead Letter Topic")
            .register(meterRegistry);

    // Gauge: live in-flight count — useful for spotting processing bottlenecks in Grafana
    meterRegistry.gauge("transaction.backlog.size", backlogSize);
  }

  // ─── Retry + DLQ Strategy ─────────────────────────────────────────────────────────────────────

  /**
   * <b>TUTORIAL — @RetryableTopic: Non-Blocking Retry Strategy</b>
   *
   * <p>Traditional retry strategies block the consumer thread (and thus the partition) while
   * waiting between attempts. This causes consumer lag, potential rebalances, and reduced
   * throughput across all partitions.
   *
   * <p>{@code @RetryableTopic} avoids this by using <em>retry topics</em>. When a message fails,
   * Spring Kafka publishes it to a companion topic (e.g., {@code raw-transactions-retry-0}) with a
   * future timestamp header. A separate consumer group reads the retry topic and only processes the
   * message after the delay elapses. The main topic consumer is never blocked.
   *
   * <p><b>Configuration explained:</b>
   *
   * <ul>
   *   <li>{@code attempts="3"}: Try 3 times total (1 original + 2 retries).
   *   <li>{@code delay=1000, multiplier=2.0}: Backoff: 1s, 2s (exponential).
   *   <li>{@code SUFFIX_WITH_INDEX_VALUE}: Retry topics named {@code …-retry-0}, {@code …-retry-1}.
   *   <li>{@code exclude}: These exceptions are NOT retried — they indicate permanent failures
   *       (corrupt schema, NPE, bad data) where retrying would be pointless. They go straight to
   *       the DLT.
   * </ul>
   */
  @RetryableTopic(
      attempts = "3",
      backoff = @Backoff(delay = 1000, multiplier = 2.0),
      topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
      exclude = {
        DataAccessException.class, // DB is down — retrying won't fix schema issues
        DeserializationException.class, // Corrupt bytes — no retry will fix the payload
        SerializationException.class,
        NullPointerException.class, // Programming error — fix the code, not the retry
        IllegalArgumentException.class
      })
  @KafkaListener(
      topics = TopicConstants.RAW_TRANSACTIONS,
      groupId = "transaction-processor-group",
      containerFactory = "kafkaListenerContainerFactory")
  public void processTransaction(
      ConsumerRecord<String, TransactionEvent> record, Acknowledgment ack) {

    // ─── Backpressure / Lag Alert ────────────────────────────────────────────────────────────────
    // Track how many messages are currently being processed concurrently.
    // If this exceeds 1000, it means we're receiving faster than we're committing — alert ops!
    long currentBacklog = backlogSize.incrementAndGet();
    if (currentBacklog > 1000) {
      log.warn(
          "LAG ALERT: in-flight backlog is high ({}). Consider scaling consumers.", currentBacklog);
    }

    try {
      // Wrap the entire processing block in a timer measurement
      processingTimer.record(
          () -> {
            TransactionEvent event = record.value();
            String transactionId = event.getTransactionId().toString();
            String accountId = event.getAccountId().toString();

            // Save the correlation ID before we enter the transaction — the ThreadLocal
            // may be cleared by Spring's transaction management infrastructure
            String correlationId = CorrelationIdContext.getCorrelationId();

            log.info(
                "Processing: transactionId={} partition={} offset={}",
                transactionId,
                record.partition(),
                record.offset());

            try {
              // ─── Idempotency Check ───────────────────────────────────────────────────────────
              // Kafka guarantees AT-LEAST-ONCE delivery. If the network drops the ack after we
              // committed to the DB, Kafka will redeliver the same message. Without this check,
              // the transaction would be inserted twice — a double-charge in a payments system!
              if (transactionPersistencePort.findById(transactionId).isPresent()) {
                log.info(
                    "Idempotency: transactionId={} already processed. Skipping.", transactionId);

                // IMPORTANT: We MUST acknowledge even duplicates.
                // If we don't ack, Kafka will keep redelivering this record forever,
                // causing an infinite retry loop and consuming thread resources.
                ack.acknowledge();
                return;
              }

              // ─── Atomic DB Transaction ───────────────────────────────────────────────────────
              // TransactionTemplate executes the lambda within a single DB transaction.
              // If ANY step inside throws, the entire transaction rolls back — atomically.
              // This means the Transaction row and the Outbox row are either BOTH saved
              // or BOTH rolled back. No partial state is ever possible.
              new TransactionTemplate(transactionManager)
                  .executeWithoutResult(
                      status -> {
                        // Restore the MDC correlation ID inside the transaction block
                        // (Spring may have cleared it when entering the new transaction context)
                        CorrelationIdContext.setCorrelationId(correlationId);

                        // Step 1: Record the processing attempt in the audit log
                        // Uses REQUIRES_NEW propagation — audit survives even if this TX rolls back
                        auditService.recordProcessing(
                            transactionId, accountId, record.partition(), record.offset());

                        // Step 2: Build and persist the Domain model
                        Transaction transaction =
                            Transaction.builder()
                                .transactionId(transactionId)
                                .accountId(accountId)
                                .amount(event.getAmount())
                                // Convert epoch millis to Instant — stored as UTC in the DB
                                .timestamp(Instant.ofEpochMilli(event.getTimestamp()))
                                .status("PROCESSED")
                                .processedBy(
                                    "PROCESSOR-SINGLE") // Identifies which processor wrote this row
                                .sourcePartition(record.partition()) // Kafka forensics metadata
                                .sourceOffset(record.offset())
                                .build();

                        transactionPersistencePort.save(transaction);

                        // Step 3: Save the Outbox record IN THE SAME TRANSACTION
                        // This is the "Outbox" half of the pattern. The OutboxPublisherService
                        // will later read this row and relay it to the processed-transactions
                        // topic.
                        try {
                          Outbox outbox =
                              Outbox.builder()
                                  .aggregateType("Transaction")
                                  .aggregateId(
                                      transactionId) // Used as Kafka message key at relay time
                                  .destinationTopic(TopicConstants.PROCESSED_TRANSACTIONS)
                                  .payload(
                                      objectMapper.writeValueAsString(event)) // JSON for storage
                                  .createdAt(Instant.now())
                                  .processed(false) // Relay service will flip this to true
                                  .build();
                          outboxService.save(outbox);
                        } catch (JsonProcessingException e) {
                          // Serialization failure is fatal — roll back the whole transaction
                          throw new RuntimeException("Outbox serialization failure", e);
                        }

                        // Step 4: Record successful processing in the audit log
                        auditService.recordSuccess(
                            transactionId, accountId, transactionPersistencePort.getStoreName());
                      });

            } catch (DataIntegrityViolationException e) {
              // A unique-constraint violation means the record was inserted by a concurrent
              // consumer thread — this is a race-condition duplicate, not a bug.
              // Treat it as idempotent: the data is already there, so we ack and move on.
              log.warn(
                  "Race-condition duplicate: transactionId={} already exists. Skipping.",
                  transactionId);
            }

            // Step 5: Acknowledge the offset ONLY after the DB transaction has committed.
            // This is the key guarantee: if the JVM crashes between DB commit and this ack,
            // Kafka will redeliver the message and our idempotency check will skip it safely.
            ack.acknowledge();
          });
    } finally {
      // Always decrement the backlog gauge — even if processing threw an exception
      backlogSize.decrementAndGet();
    }
  }

  // ─── Dead Letter Topic Handler ────────────────────────────────────────────────────────────────

  /**
   * <b>TUTORIAL — @DltHandler: The Last Resort</b>
   *
   * <p>After all {@code @RetryableTopic} attempts are exhausted, Spring Kafka routes the message to
   * the Dead Letter Topic (DLT). This method handles those records.
   *
   * <p>This is NOT a retry — the message will NOT be reprocessed automatically. Operations must
   * manually investigate and replay DLT records after fixing the root cause.
   *
   * <p><b>What we do here:</b>
   *
   * <ul>
   *   <li>Log the failure with full context for incident investigations.
   *   <li>Increment the DLQ Micrometer counter for Grafana alerting.
   *   <li>Write an audit record so compliance teams can see which transactions were never
   *       processed.
   *   <li>Acknowledge the DLT message — otherwise the DLT consumer would loop indefinitely!
   * </ul>
   */
  @DltHandler
  public void dlt(
      ConsumerRecord<String, TransactionEvent> record,
      @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage,
      Acknowledgment ack) {

    log.error(
        "DLT: record exhausted all retries. key={}, reason={}", record.key(), exceptionMessage);
    dlqCounter.increment();

    // Attempt to write a DLQ audit entry — wrapped in try/catch because if THIS fails,
    // we must still acknowledge the DLT message to prevent an infinite loop
    try {
      auditService.recordDlq(
          record.key(), // transactionId
          record.value().getAccountId().toString(), // accountId
          record.partition(),
          record.offset(),
          exceptionMessage);
    } catch (Exception e) {
      log.error("Failed to write DLT audit entry: {}", e.getMessage());
    } finally {
      // Critical: always ack DLT records. Failing to do so creates an infinite DLT retry loop.
      ack.acknowledge();
    }
  }
}
