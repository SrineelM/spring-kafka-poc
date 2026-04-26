package com.example.springkafkapoc.service;

import com.example.springkafkapoc.domain.model.Transaction;
import java.util.Optional;

/**
 * <b>Persistence Port — Hexagonal (Ports & Adapters) Architecture</b>
 *
 * <p><b>TUTORIAL — What is a "Port"?</b><br>
 * In Hexagonal Architecture, the application's core business logic is at the centre. It
 * communicates with the outside world (databases, queues, HTTP APIs) only through well-defined
 * <em>ports</em> (interfaces). Concrete implementations — called <em>adapters</em> — plug in from
 * outside.
 *
 * <p><b>Why does this matter?</b><br>
 * Our Kafka consumers ({@code TransactionEventSingleProcessor}) use <em>this interface</em> — not
 * H2, not Spanner, not AlloyDB. This means:
 *
 * <ul>
 *   <li>You can swap databases without touching consumer code.
 *   <li>You can write unit tests with a mock/in-memory implementation.
 *   <li>You can run locally (H2) and in production (Spanner) with zero code changes — just a
 *       different Spring profile.
 * </ul>
 *
 * <p><b>PRO TIP:</b> Always return the <em>domain model</em> ({@link Transaction}) from ports, not
 * the JPA entity. This decouples the rest of the system from any persistence-framework specifics.
 */
public interface TransactionPersistencePort {

  /**
   * Persists a transaction record and returns the saved state.
   *
   * <p>The returned object may contain DB-generated fields (e.g., auto-assigned timestamps) that
   * the caller may need for subsequent operations.
   *
   * @param transaction the domain model to save
   * @return the saved domain model, potentially enriched by the persistence layer
   */
  Transaction save(Transaction transaction);

  /**
   * Looks up a transaction by its unique business identifier.
   *
   * <p><b>TUTORIAL — Idempotency Guard:</b><br>
   * This method is the backbone of our Idempotent Consumer pattern. Before processing any message,
   * consumers call this to check whether the transaction was already handled. If it exists, the
   * message is acknowledged and skipped — preventing double processing.
   *
   * @param transactionId unique business ID (a UUIDv7 string)
   * @return an {@link Optional} containing the found transaction, or empty if not found
   */
  Optional<Transaction> findById(String transactionId);

  /**
   * Returns a human-readable name identifying which storage engine is active.
   *
   * <p>Used for audit logs and diagnostic endpoints so operations can confirm which backend is live
   * without inspecting Spring profiles directly.
   *
   * @return e.g. "H2 In-Memory", "Google Cloud Spanner", "Google Cloud AlloyDB"
   */
  String getStoreName();
}
