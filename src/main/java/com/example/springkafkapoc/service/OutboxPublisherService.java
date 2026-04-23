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
 * <b>Outbox Publisher (The Poller)</b>
 *
 * <p><b>TUTORIAL:</b> This service is the "Engine" of the <b>Transactional Outbox Pattern</b>.
 * While the business service saves to the database, this service is responsible for "shipping"
 * those records to Kafka.
 *
 * <p><b>Key Architecture Tip:</b> We use <b>Distributed Locking</b> (via {@link LockRegistry}) to
 * ensure that even if you run 10 instances of this application, only <b>ONE</b> instance is polling
 * the outbox at any given time. This prevents duplicate messages and race conditions!
 */
@Slf4j
@Service
@EnableScheduling
public class OutboxPublisherService {

  private final OutboxService outboxService;
  private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
  private final LockRegistry lockRegistry;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;

  private final Timer pollTimer;
  private final Counter publishedCounter;
  private final Counter errorCounter;
  private final Counter lockFailureCounter;
  private final AtomicLong backlogSize = new AtomicLong(0);

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

    this.pollTimer =
        Timer.builder("outbox.poll.time")
            .description("Time taken for one outbox poll cycle")
            .register(meterRegistry);
    this.publishedCounter =
        Counter.builder("outbox.published.count")
            .description("Total number of outbox messages successfully published")
            .register(meterRegistry);
    this.errorCounter =
        Counter.builder("outbox.publish.errors")
            .description("Number of errors during outbox publishing")
            .register(meterRegistry);
    this.lockFailureCounter =
        Counter.builder("outbox.lock.failures")
            .description("Number of times the outbox poller failed to acquire the distributed lock")
            .register(meterRegistry);

    meterRegistry.gauge("outbox.backlog.size", backlogSize);
  }

  @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:5000}")
  public void publishOutboxMessages() {
    Lock lock = lockRegistry.obtain(LOCK_KEY);

    if (!lock.tryLock()) {
      lockFailureCounter.increment();
      return;
    }

    try {
      pollTimer.record(
          () -> {
            List<Outbox> messages = outboxService.findUnprocessedMessages();
            backlogSize.set(messages.size());
            if (messages.isEmpty()) return;

            log.info("Outbox Poller: Found {} pending messages.", messages.size());

            for (Outbox msg : messages) {
              try {
                // Try to deserialize JSON payload back to Avro object
                TransactionEvent event =
                    objectMapper.readValue(msg.getPayload(), TransactionEvent.class);

                // Synchronous send within a Kafka transaction
                kafkaTemplate.executeInTransaction(
                    ops -> {
                      try {
                        ops.send(msg.getDestinationTopic(), msg.getAggregateId(), event).get();
                        // Mark as processed ONLY after the send is confirmed
                        outboxService.markAsProcessed(msg.getId());
                        publishedCounter.increment();
                        log.debug("Published and marked Outbox ID: {}", msg.getId());
                      } catch (Exception e) {
                        errorCounter.increment();
                        throw new RuntimeException("Failed to send outbox message", e);
                      }
                      return null;
                    });
              } catch (Exception e) {
                errorCounter.increment();
                log.error("Error processing Outbox Record {}: {}", msg.getId(), e.getMessage());
              }
            }
          });
    } finally {
      lock.unlock();
    }
  }
}
