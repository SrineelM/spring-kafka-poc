package com.example.springkafkapoc.service;

import com.example.springkafkapoc.domain.model.Outbox;
import com.example.springkafkapoc.persistence.mapper.OutboxMapper;
import com.example.springkafkapoc.persistence.repository.OutboxRepository;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * <b>Transactional Outbox Service</b>
 *
 * <p><b>TUTORIAL:</b> The Outbox pattern is a vital microservices architecture pattern used to
 * achieve atomic operations between a local database and an external message broker (Kafka).
 *
 * <p>Problem: You need to update a database AND send a Kafka message. If the DB commits but Kafka
 * is down, the system is in an inconsistent state.
 *
 * <p>Solution: Save the intended Kafka message into a local DB table (the "Outbox") as part of the
 * SAME transaction that updates the business entity. Later, a separate process (like Debezium or a
 * polling relayer) reads the outbox table and pushes to Kafka.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxService {

  private final OutboxRepository outboxRepository;
  private final OutboxMapper mapper;

  /**
   * Saves a message to the Outbox.
   *
   * <p>WHY: This is usually called from within an existing transaction (e.g., inside the
   * TransactionEventSingleProcessor).
   */
  @Transactional
  public void save(Outbox outbox) {
    if (outbox.getCreatedAt() == null) {
      outbox.setCreatedAt(Instant.now());
    }
    outboxRepository.save(mapper.toEntity(outbox));
    log.debug("Outbox record created for aggregateId={}", outbox.getAggregateId());
  }

  /** Legacy helper for simpler calls where you don't want to build the model manually. */
  @Transactional
  public void saveToOutbox(String aggregateId, Object payload, String topic) {
    save(
        Outbox.builder()
            .aggregateId(aggregateId)
            .aggregateType("Transaction")
            .payload(payload.toString())
            .destinationTopic(topic)
            .build());
  }

  /**
   * Finds unprocessed messages. (Note: Real implementation would have a 'findByProcessedFalse'
   * repository method)
   */
  public List<Outbox> findUnprocessedMessages() {
    // Simple mock implementation as the repository doesn't have the method yet
    return outboxRepository.findAll().stream()
        .filter(e -> !e.isProcessed())
        .map(mapper::toDomain)
        .toList();
  }

  /** Marks a message as processed. */
  @Transactional
  public void markAsProcessed(Long id) {
    outboxRepository
        .findById(id)
        .ifPresent(
            entity -> {
              entity.setProcessed(true);
              outboxRepository.save(entity);
            });
  }
}
