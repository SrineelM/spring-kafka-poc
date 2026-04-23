package com.example.springkafkapoc.service;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.domain.model.Transaction;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
@RequiredArgsConstructor
@Profile("batch")
public class TransactionEventBatchProcessor {

  private final TransactionPersistencePort persistencePort;
  private final PlatformTransactionManager transactionManager;

  @KafkaListener(
      topics = TopicConstants.RAW_TRANSACTIONS,
      groupId = "transaction-batch-group",
      containerFactory = "batchKafkaListenerContainerFactory")
  public void processTransactionBatch(
      List<ConsumerRecord<String, TransactionEvent>> records, Acknowledgment ack) {
    log.info("Processing batch of {} transaction records", records.size());

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
                  continue;
                }

                TransactionEvent event = record.value();
                Transaction transaction =
                    Transaction.builder()
                        .transactionId(transactionId)
                        .accountId(event.getAccountId().toString())
                        .amount(event.getAmount())
                        .timestamp(Instant.ofEpochMilli(event.getTimestamp()))
                        .status("PROCEESED_BATCH")
                        .sourcePartition(record.partition())
                        .sourceOffset(record.offset())
                        .build();

                persistencePort.save(transaction);
              }
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

    // In a production batch system, you might want to aggregate these failures
    // before auditing, but for this PoC, we record each individual failure.
    ack.acknowledge();
  }
}
