package com.example.springkafkapoc.service;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.domain.model.Outbox;
import com.example.springkafkapoc.domain.model.Transaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * <b>Transaction Processor (Single Mode)</b>
 *
 * <p><b>TUTORIAL:</b> This is the main consumer. It listens to the incoming Kafka topic and
 * processes records ONE AT A TIME. This provides the highest level of error tracking and
 * transaction isolation, at the cost of some overall throughput compared to batching.
 *
 * <p>It demonstrates the <b>Transactional Outbox Pattern</b>: 1. Read message. 2. Write domain
 * entity to DB. 3. Write 'Outbox' event to DB. 4. Commit DB Transaction. 5. Acknowledge Kafka
 * message.
 */
@Slf4j
@Service
@Profile("!batch")
public class TransactionEventSingleProcessor {

  private final TransactionPersistencePort transactionPersistencePort;
  private final OutboxService outboxService;
  private final PlatformTransactionManager transactionManager;
  private final AuditService auditService;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;

  private final Timer processingTimer;
  private final Counter dlqCounter;
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
            .description("Time taken to process a transaction")
            .register(meterRegistry);
    this.dlqCounter =
        Counter.builder("transaction.dlq.count")
            .description("Number of transactions sent to DLQ")
            .register(meterRegistry);

    meterRegistry.gauge("transaction.backlog.size", backlogSize);
  }

  /**
   * TUTORIAL: {@code @RetryableTopic} provides non-blocking, delay-based retries out of the box. If
   * this method throws an exception, Spring creates companion topics (e.g., topic-retry-0,
   * topic-retry-1) and forwards the message there automatically. It backs off exponentially.
   *
   * <p>If all retries fail, it goes to the DLT (Dead Letter Topic).
   *
   * <p>TUTORIAL: {@code @KafkaListener} marks this method as a Kafka Consumer. By using {@code
   * containerFactory = "kafkaListenerContainerFactory"}, we ensure it uses our custom interceptors
   * and manual acknowledgement settings.
   */
  @RetryableTopic(
      attempts = "3",
      backoff = @Backoff(delay = 1000, multiplier = 2.0),
      topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
      exclude = {DataAccessException.class})
  @KafkaListener(
      topics = TopicConstants.RAW_TRANSACTIONS,
      groupId = "transaction-processor-group",
      containerFactory = "kafkaListenerContainerFactory")
  public void processTransaction(
      ConsumerRecord<String, TransactionEvent> record, Acknowledgment ack) {
    backlogSize.incrementAndGet();

    processingTimer.record(
        () -> {
          TransactionEvent event = record.value();
          String transactionId = event.getTransactionId().toString();
          String accountId = event.getAccountId().toString();

          log.info(
              "Processing transactionId={} partition={} offset={}",
              transactionId,
              record.partition(),
              record.offset());

          new TransactionTemplate(transactionManager)
              .executeWithoutResult(
                  status -> {
                    // 1. Audit processing start
                    auditService.recordProcessing(
                        transactionId, accountId, record.partition(), record.offset());

                    // 2. Persist
                    // TUTORIAL: We save the core business entity.
                    Transaction transaction =
                        Transaction.builder()
                            .transactionId(transactionId)
                            .accountId(accountId)
                            .amount(event.getAmount())
                            .timestamp(Instant.ofEpochMilli(event.getTimestamp()))
                            .status("PROCESSED")
                            .processedBy("PROCESSOR-SINGLE")
                            .sourcePartition(record.partition())
                            .sourceOffset(record.offset())
                            .build();

                    transactionPersistencePort.save(transaction);

                    // 3. Outbox
                    // TUTORIAL: Instead of pushing to a new Kafka topic immediately, we save an
                    // 'Outbox' record
                    // to the SAME database transaction. This prevents dual-write failures (e.g. DB
                    // commit succeeds,
                    // but Kafka networking fails, causing data inconsistency).
                    try {
                      Outbox outbox =
                          Outbox.builder()
                              .aggregateType("Transaction")
                              .aggregateId(transactionId)
                              .destinationTopic(TopicConstants.PROCESSED_TRANSACTIONS)
                              .payload(objectMapper.writeValueAsString(event))
                              .createdAt(Instant.now())
                              .processed(false)
                              .build();
                      outboxService.save(outbox);
                    } catch (JsonProcessingException e) {
                      throw new RuntimeException("Serialization failure", e);
                    }

                    // 4. Audit success
                    auditService.recordSuccess(
                        transactionId, accountId, transactionPersistencePort.getStoreName());
                  });

          // TUTORIAL: We manually acknowledge the Kafka message only AFTER the database
          // transaction fully commits.
          // If the application crashes before this line, Kafka will re-deliver the
          // message to another instance.
          ack.acknowledge();
          backlogSize.decrementAndGet();
        });
  }

  @DltHandler
  public void dlt(
      ConsumerRecord<String, TransactionEvent> record,
      @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {

    log.error("Sent to DLT: {}. Reason: {}", record.value(), exceptionMessage);
    dlqCounter.increment();

    auditService.recordDlq(
        record.key(),
        record.value().getAccountId().toString(),
        record.partition(),
        record.offset(),
        exceptionMessage);
  }
}
