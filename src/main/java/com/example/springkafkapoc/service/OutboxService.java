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
 * <b>Transactional Outbox Service — The Write Side</b>
 *
 * <p><b>TUTORIAL — The Outbox Pattern in detail:</b><br>
 * The core problem: you need to update your database AND publish a Kafka message. If you do both
 * independently, you risk two failure scenarios:
 *
 * <ol>
 *   <li>DB commits, Kafka send fails → message is lost, downstream never knows.
 *   <li>Kafka publishes, DB commit fails → duplicate message with no corresponding DB record.
 * </ol>
 *
 * <p>The Outbox pattern solves this by making the Kafka "intent" part of the same DB transaction as
 * the business update. Instead of publishing to Kafka directly, you write a row to an {@code
 * outbox} table — inside the same DB transaction. A separate relay process ({@link
 * OutboxPublisherService}) then reads that table and forwards to Kafka.
 *
 * <p>This guarantees that a message is published <em>if and only if</em> the business data was also
 * saved — atomically, with the same ACID guarantees your DB provides.
 *
 * <p><b>PRO TIP:</b> In high-scale systems, CDC (Change Data Capture) tools like Debezium can
 * replace the polling relay by streaming the outbox table's WAL log directly to Kafka — removing
 * polling latency entirely.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxService {

  private final OutboxRepository outboxRepository; // JPA repository for the outbox table
  private final OutboxMapper mapper; // MapStruct domain ↔ entity converter
  private final ObjectMapper objectMapper; // Jackson JSON serializer for payloads

  /**
   * Saves an Outbox record — typically called from within an existing business transaction.
   *
   * <p><b>TUTORIAL:</b> The default {@code @Transactional} propagation is {@code REQUIRED}, which
   * means this method joins the caller's existing transaction. If the caller rolls back, this write
   * rolls back too — that's exactly what we want. The outbox record only survives if the business
   * record also survives.
   *
   * @param outbox the domain model describing the message to relay
   */
  @Transactional
  public void save(Outbox outbox) {
    // Set the creation timestamp if not already set by the caller
    if (outbox.getCreatedAt() == null) {
      outbox.setCreatedAt(Instant.now());
    }
    // Convert domain model to JPA entity and persist
    outboxRepository.save(mapper.toEntity(outbox));
    log.debug("Outbox record queued for aggregateId={}", outbox.getAggregateId());
  }

  /**
   * Convenience overload — serializes the payload object to JSON and saves the Outbox record.
   *
   * <p><b>WHY JSON in the payload column?</b><br>
   * The outbox table stores the intent generically. JSON is human-readable (helps debugging),
   * schema-agnostic, and supported by virtually every database. At relay time, the relay service
   * deserializes back to the specific type ({@code TransactionEvent}).
   *
   * @param aggregateId the business key to use as the Kafka message key
   * @param payload the Java object to serialize as the Kafka message value
   * @param topic the destination Kafka topic name
   */
  @Transactional
  public void saveToOutbox(String aggregateId, Object payload, String topic) {
    try {
      // Serialize the payload to a compact JSON string for storage
      String jsonPayload = objectMapper.writeValueAsString(payload);
      save(
          Outbox.builder()
              .aggregateId(aggregateId)
              .aggregateType("Transaction") // Domain entity type — useful for multi-type outboxes
              .payload(jsonPayload)
              .destinationTopic(topic)
              .build());
    } catch (JsonProcessingException e) {
      log.error("Failed to serialize outbox payload for ID: {}", aggregateId, e);
      // Re-throw as unchecked so the surrounding @Transactional rolls back
      throw new RuntimeException("Outbox serialization failure", e);
    }
  }

  /**
   * Finds pending (unprocessed) Outbox records in a bounded batch.
   *
   * <p><b>TUTORIAL — Why paged? Why not load all?</b><br>
   * If the relay falls behind (e.g., Kafka was down for hours), there could be thousands of pending
   * rows. Loading them all into memory in one query would cause an OOM error. A page of 100 rows
   * balances throughput with memory safety. The relay runs in a tight loop (every 5s), so even with
   * page size 100, it can relay 1200 records/minute.
   *
   * <p>The repository query uses {@code SELECT FOR UPDATE SKIP LOCKED} — this ensures that if two
   * instances of the poller somehow run simultaneously, they get <em>different</em> rows rather
   * than competing for the same ones.
   */
  @Transactional(readOnly = true) // readOnly = true hints to the DB optimizer that no writes occur
  public List<Outbox> findUnprocessedMessages() {
    return outboxRepository.findUnprocessedLocked(PageRequest.of(0, 100)).stream()
        .map(mapper::toDomain)
        .toList();
  }

  /**
   * Atomically marks an Outbox record as processed.
   *
   * <p>Uses a targeted UPDATE query (not a full entity load + save) for minimal DB round-trips. The
   * {@code rows == 0} check catches races where two threads somehow both attempt to mark the same
   * record — the second one logs a warning rather than silently succeeding.
   *
   * @param id the database primary key of the Outbox record to close
   */
  @Transactional
  public void markAsProcessed(Long id) {
    int rows = outboxRepository.markProcessed(id); // Returns the number of rows updated
    if (rows == 0) {
      // This can happen in edge cases (e.g., concurrent relay pods despite locking)
      // It's not catastrophic — the record is already closed — but worth monitoring
      log.warn("Outbox record {} was already processed or does not exist.", id);
    }
  }
}
