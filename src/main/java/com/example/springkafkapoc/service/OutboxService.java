package com.example.springkafkapoc.service;

import com.example.springkafkapoc.domain.model.Outbox;
import com.example.springkafkapoc.persistence.mapper.OutboxMapper;
import com.example.springkafkapoc.persistence.repository.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
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
  private final ObjectMapper objectMapper;

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

  /**
   * Helper to build and save a message to the Outbox.
   *
   * @param aggregateId the business ID
   * @param payload the object to be serialized to JSON
   * @param topic the destination topic
   */
  @Transactional
  public void saveToOutbox(String aggregateId, Object payload, String topic) {
    try {
      String jsonPayload = objectMapper.writeValueAsString(payload);
      save(
          Outbox.builder()
              .aggregateId(aggregateId)
              .aggregateType("Transaction")
              .payload(jsonPayload)
              .destinationTopic(topic)
              .build());
    } catch (JsonProcessingException e) {
      log.error("Failed to serialize outbox payload for ID: {}", aggregateId, e);
      throw new RuntimeException("Outbox serialization failure", e);
    }
  }

  /** Finds unprocessed messages in batches to prevent OOM errors. */
  @Transactional(readOnly = true)
  public List<Outbox> findUnprocessedMessages() {
    return outboxRepository.findUnprocessedLocked(PageRequest.of(0, 100)).stream()
        .map(mapper::toDomain)
        .toList();
  }

  /** Marks a message as processed atomically. */
  @Transactional
  public void markAsProcessed(Long id) {
    int rows = outboxRepository.markProcessed(id);
    if (rows == 0) {
      log.warn("Outbox record {} was already processed or does not exist.", id);
    }
  }
}
