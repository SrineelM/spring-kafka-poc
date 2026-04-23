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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * <b>Batch Transaction Event Processor</b>
 *
 * <p>TUTORIAL: This service demonstrates <b>Batched Consumption</b>. Instead of receiving one
 * {@code ConsumerRecord} at a time, it receives a {@code List<ConsumerRecord>}. We can then process
 * and persist all records in a single Database transaction, which drastically improves throughput
 * by reducing network round-trips to the database.
 *
 * <p>This is activated only when the 'batch' profile is running.
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
  private final Counter skippedCounter;
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
            .description("Time taken to process a batch of transactions")
            .register(meterRegistry);
    this.processedCounter =
        Counter.builder("transaction.batch.processed.count")
            .description("Total number of transactions processed in batches")
            .register(meterRegistry);
    this.skippedCounter =
        Counter.builder("transaction.batch.skipped.count")
            .description("Number of duplicate transactions skipped in batches")
            .register(meterRegistry);
    this.dltCounter =
        Counter.builder("transaction.batch.dlt.count")
            .description("Number of batch records sent to DLT")
            .register(meterRegistry);
  }

  @KafkaListener(
      topics = TopicConstants.RAW_TRANSACTIONS,
      groupId = "transaction-batch-group",
      containerFactory = "batchKafkaListenerContainerFactory")
  public void processTransactionBatch(
      List<ConsumerRecord<String, TransactionEvent>> records, Acknowledgment ack) {
    log.info("Processing batch of {} transaction records", records.size());

    batchTimer.record(
        () -> {
          new TransactionTemplate(transactionManager)
              .executeWithoutResult(
                  status -> {
                    for (ConsumerRecord<String, TransactionEvent> record : records) {
                      String transactionId = record.value().getTransactionId().toString();

                      // Idempotency check: Skip records already in the database
                      if (persistencePort.findById(transactionId).isPresent()) {
                        log.info(
                            "Batch Idempotency: transactionId={} already processed. Skipping.",
                            transactionId);
                        skippedCounter.increment();
                        continue;
                      }

                      TransactionEvent event = record.value();
                      Transaction transaction =
                          Transaction.builder()
                              .transactionId(transactionId)
                              .accountId(event.getAccountId().toString())
                              .amount(event.getAmount())
                              .timestamp(Instant.ofEpochMilli(event.getTimestamp()))
                              .status("PROCESSED_BATCH")
                              .sourcePartition(record.partition())
                              .sourceOffset(record.offset())
                              .build();

                      persistencePort.save(transaction);
                      processedCounter.increment();
                    }
                  });
        });

    ack.acknowledge();
  }

  /**
   * TUTORIAL: Batch DLT Handler.
   *
   * <p>When a batch fails, Spring Kafka attempts to retry. If retries fail, the individual records
   * are sent here. We log the failure and audit it for forensics.
   */
  @org.springframework.kafka.annotation.DltHandler
  public void dlt(
      ConsumerRecord<String, TransactionEvent> record,
      @org.springframework.messaging.handler.annotation.Header(
              org.springframework.kafka.support.KafkaHeaders.EXCEPTION_MESSAGE)
          String exceptionMessage,
      Acknowledgment ack) {

    log.error(
        "Batch Record failed and sent to DLT: {}. Reason: {}", record.key(), exceptionMessage);
    dltCounter.increment();

    // In a production batch system, you might want to aggregate these failures
    // before auditing, but for this PoC, we record each individual failure.
    ack.acknowledge();
  }
}
