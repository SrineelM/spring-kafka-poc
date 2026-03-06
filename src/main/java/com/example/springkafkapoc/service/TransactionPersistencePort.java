package com.example.springkafkapoc.service;

import com.example.springkafkapoc.domain.model.Transaction;
import java.util.Optional;

/**
 * <b>Persistence Port (Hexagonal Architecture)</b>
 *
 * <p>Defining an interface as a "Port" allows us to swap different "Adapters" (H2, Spanner,
 * AlloyDB) without changing our business logic. This is a core principle of Clean Architecture.
 *
 * <p>Patterns used:
 *
 * <ul>
 *   <li><b>Dependency Inversion:</b> Business logic depends on this port, not on concrete JPA
 *       implementations.
 *   <li><b>Repository Pattern:</b> Provides an abstraction for data access.
 * </ul>
 *
 * <p>Tutorial Tip: By returning {@link Transaction} (Domain Model) instead of an Entity, we
 * decouple the rest of the system from the specifics of how data is saved (SQL vs NoSQL vs
 * In-Memory).
 */
public interface TransactionPersistencePort {

  /**
   * Persists a transaction record.
   *
   * @param transaction the domain model to save
   * @return the saved domain model (potentially updated with DB-generated IDs or status)
   */
  Transaction save(Transaction transaction);

  /**
   * Retrieves a transaction by its business ID.
   *
   * @param transactionId unique business ID
   * @return Optional containing the transaction if found
   */
  Optional<Transaction> findById(String transactionId);

  /**
   * Diagnostic method to identify which storage engine is currently active.
   *
   * @return human-readable name of the store (e.g. "Cloud Spanner")
   */
  String getStoreName();
}
