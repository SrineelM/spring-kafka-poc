package com.example.springkafkapoc.service;

import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

/**
 * <b>Dynamic Storage Router</b>
 *
 * <p><b>TUTORIAL:</b> This service implements the <b>Polymorphic Persistence / Service Selection
 * Pattern</b>. In a multi-cloud or hybrid environment, you often need to switch storage engines
 * (e.g., H2 for local, Spanner for GCP, PostgreSQL for on-prem) without rewriting your business
 * logic.
 *
 * <p><b>How it works:</b>
 *
 * <ul>
 *   <li>It is marked {@link @Primary} so Spring prioritizes this bean during injection.
 *   <li>It receives a {@code List<TransactionPersistencePort>} containing ALL active
 *       implementations.
 *   <li>The {@code getActiveProvider()} method decides which concrete service to use at runtime
 *       based on environment availability or feature flags.
 * </ul>
 *
 * <p><b>WHY THIS ARCHITECTURE?</b> It achieves <b>Strict Decoupling</b>. Your Kafka processors just
 * say "save this", and they don't care if it goes to an in-memory database or a
 * globally-distributed cloud database. This makes testing and cloud-migration trivial.
 */
@Slf4j
@Service
@Primary
@RequiredArgsConstructor
public class DynamicPersistenceRouter implements TransactionPersistencePort {

  /**
   * Spring automatically injects ALL bean implementations of the interface into this list. We'll
   * have [H2TransactionService, SpannerTransactionService, AlloyDbTransactionService] depending on
   * which profiles are active.
   */
  private final List<TransactionPersistencePort> providers;

  /**
   * Chooses the "safest" or most appropriate storage engine at runtime.
   *
   * <p>Tutorial Tip: In a real system, you might have a health check or a fallback chain here.
   */
  private TransactionPersistencePort getActiveProvider() {
    // Priority: 1. Spanner, 2. AlloyDB, 3. H2 (Fallback)
    return providers.stream()
        .filter(p -> p.getStoreName().contains("Spanner"))
        .findFirst()
        .or(() -> providers.stream().filter(p -> p.getStoreName().contains("AlloyDB")).findFirst())
        .orElse(
            providers.stream()
                .filter(p -> p.getStoreName().contains("H2"))
                .findFirst()
                .orElseThrow(
                    () -> new IllegalStateException("No valid persistence provider found!")));
  }

  @Override
  public com.example.springkafkapoc.domain.model.Transaction save(
      com.example.springkafkapoc.domain.model.Transaction transaction) {
    TransactionPersistencePort active = getActiveProvider();
    log.trace("Routing save to: {}", active.getStoreName());
    return active.save(transaction);
  }

  @Override
  public java.util.Optional<com.example.springkafkapoc.domain.model.Transaction> findById(
      String transactionId) {
    return getActiveProvider().findById(transactionId);
  }

  @Override
  public String getStoreName() {
    return getActiveProvider().getStoreName();
  }
}
