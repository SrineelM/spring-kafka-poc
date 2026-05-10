package com.example.springkafkapoc.service;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.domain.model.Transaction;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * <b>Transaction Event Processor (Batch Mode)</b>
 *
 * <p><b>TUTORIAL — Batch vs Single mode:</b><br>
 * Single mode: 1 message → 1 DB round-trip → 1 commit. Simple but slow under load.<br>
 * Batch mode: N messages → 1 DB round-trip → 1 commit. Far more efficient, but requires per-record
 * idempotency checks because the entire batch is redelivered on retry.
 *
 * <p><b>Speed vs. Risk trade-off:</b><br>
 * Batching is 10×–100× faster, but if ONE record fails, Kafka redelivers ALL records in the batch.
 * Records already successfully inserted will be redelivered too — making idempotency mandatory.
 *
 * <p><b>Activation:</b> Only active when {@code SPRING_PROFILES_ACTIVE=batch}. Mutually exclusive
 * with {@code TransactionEventSingleProcessor}.
 */
@Slf4j
@Service
@Profile("batch")
public class TransactionEventBatchProcessor {

  private final TransactionPersistencePort persistencePort;
  private final PlatformTransactionManager transactionManager;
  private final MeterRegistry meterRegistry;

  private final Timer batchTimer;
  private final Counter processedCounter;
  private final Counter skippedCounter; // Duplicates skipped by idempotency check
  private final Counter dltCounter;

  @Autowired
  public TransactionEventBatchProcessor(
      TransactionPersistencePort persistencePort,
      PlatformTransactionManager transactionManager,
      MeterRegistry meterRegistry) {
    this.persistencePort = persistencePort;
    this.transactionManager = transactionManager;
    this.meterRegistry = meterRegistry;

    this.batchTimer =
        Timer.builder("transaction.batch.processing.time")
            .description("End-to-end time for one Kafka batch")
            .register(meterRegistry);
    this.processedCounter =
        Counter.builder("transaction.batch.processed.count")
            .description("Records successfully processed in batch mode")
            .register(meterRegistry);
    this.skippedCounter =
        Counter.builder("transaction.batch.skipped.count")
            .description("Duplicate records skipped in batch mode")
            .register(meterRegistry);
    this.dltCounter =
        Counter.builder("transaction.batch.dlt.count")
            .description("Batch records routed to DLT")
            .register(meterRegistry);
  }

  /**
   * Processes a batch of Kafka records in a single DB transaction.
   *
   * <p><b>TUTORIAL — What happens here:</b>
   *
   * <ol>
   *   <li>Kafka delivers a list of up to 500 records (controlled by {@code max.poll.records}).
   *   <li>A {@link TransactionTemplate} opens ONE DB transaction for the entire batch.
   *   <li>For each record: check idempotency, then insert. Skips already-processed records.
   *   <li>All inserts commit in one round-trip.
   *   <li>Acknowledge ALL offsets after DB commit.
   * </ol>
   *
   * <p><b>Backpressure signal:</b> If the batch is consistently full (500 records), the consumer is
   * lagging behind the producer — a clear signal to scale out consumer pods.
   */
  @KafkaListener(
      topics = TopicConstants.RAW_TRANSACTIONS,
      groupId = "transaction-batch-group",
      containerFactory = "batchKafkaListenerContainerFactory")
  public void processTransactionBatch(
      List<ConsumerRecord<String, TransactionEvent>> records, Acknowledgment ack) {

    // Backpressure/Lag Alert: full batch = consumer may be falling behind
    if (records.size() >= 500) {
      log.warn("LAG ALERT: maximum batch size ({}) — consumer may be lagging!", records.size());
    } else {
      log.info("Processing batch of {} records", records.size());
    }

    // Wrap entire loop in a timer so Grafana can show batch latency trends
    batchTimer.record(
        () -> {
          // ONE DB transaction for all N records — this is the throughput advantage of batch mode
          new TransactionTemplate(transactionManager)
              .executeWithoutResult(
                  status -> {
                    for (ConsumerRecord<String, TransactionEvent> record : records) {
                      String transactionId = record.value().getTransactionId().toString();

                      // Idempotency Check — CRITICAL in batch mode.
                      // On retry, ALL records in the batch are redelivered — including those
                      // that were already inserted in the previous (partial) attempt.
                      // Without this check, we'd insert them again → double accounting.
                      if (persistencePort.findById(transactionId).isPresent()) {
                        log.info(
                            "Batch Idempotency: {} already processed. Skipping.", transactionId);
                        skippedCounter.increment();
                        continue; // Skip this record; process the rest of the batch
                      }

                      TransactionEvent event = record.value();
                      // Map Avro event to the persistence-agnostic domain model
                      Transaction transaction =
                          Transaction.builder()
                              .transactionId(transactionId)
                              .accountId(event.getAccountId().toString())
                              .amount(event.getAmount())
                              .timestamp(Instant.ofEpochMilli(event.getTimestamp()))
                              .status(
                                  "PROCESSED_BATCH") // Distinguishes batch-mode rows in reporting
                              .sourcePartition(record.partition()) // Kafka forensics metadata
                              .sourceOffset(record.offset())
                              .build();

                      persistencePort.save(transaction); // Joins the active TX — no commit yet
                      processedCounter.increment();
                    }
                    // All records inserted — TransactionTemplate commits here
                  });
        });

    // Acknowledge ALL offsets in the batch after the DB commit succeeds.
    // If the JVM crashes between DB commit and this ack, the batch is redelivered —
    // idempotency checks handle that safely (at-least-once guarantee).
    ack.acknowledge();
  }

  /**
   * Dead Letter Topic handler — called when a record exhausts all batch-split retries.
   *
   * <p><b>TUTORIAL:</b> Spring Kafka's error handler splits persistently-failing batches into
   * individual records and routes each one here after all retries are exhausted. We log it,
   * increment the metric, and acknowledge — otherwise the DLT consumer loops forever.
   */
  @DltHandler
  public void dlt(
      ConsumerRecord<String, TransactionEvent> record,
      @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage,
      Acknowledgment ack) {

    log.error("Batch DLT: key={}, reason={}", record.key(), exceptionMessage);
    dltCounter.increment();

    // Always ack DLT records to prevent an infinite retry loop on the DLT consumer
    ack.acknowledge();
  }
}
