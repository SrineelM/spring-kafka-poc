package com.example.springkafkapoc.service;

import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

/**
 * <b>Dynamic Persistence Router — Polymorphic Storage Selection</b>
 *
 * <p><b>TUTORIAL — The Strategy Pattern applied to persistence:</b><br>
 * Instead of scattering {@code if (spannerEnabled)} checks across every service, we centralize all
 * storage selection logic here. This class implements the same {@link TransactionPersistencePort}
 * interface as H2, Spanner, and AlloyDB — but its implementation delegates to whichever concrete
 * provider is most appropriate at runtime.
 *
 * <p><b>How Spring wires this:</b><br>
 * When Spring sees a field of type {@code List<TransactionPersistencePort>}, it automatically
 * injects ALL beans that implement that interface. This router receives the full list and picks the
 * right one at runtime based on name matching (Spanner preferred over AlloyDB over H2).
 *
 * <p><b>WHY {@code @Primary}?</b><br>
 * Without {@code @Primary}, Spring wouldn't know which {@code TransactionPersistencePort} to inject
 * when a service declares a single dependency on the interface — it would see multiple candidates
 * and throw an exception. {@code @Primary} tells Spring "when in doubt, use me."
 *
 * <p><b>WHY this architecture?</b><br>
 * Your Kafka processors say {@code persistencePort.save(tx)} without knowing or caring whether data
 * goes to an in-memory H2 test database or a globally-distributed Cloud Spanner cluster. Swapping
 * environments (local → staging → prod) requires zero code changes — only a property flag ({@code
 * app.database.spanner-enabled=true}).
 */
@Slf4j
@Service
@Primary // This bean wins in any single-bean injection site of TransactionPersistencePort
@RequiredArgsConstructor
public class DynamicPersistenceRouter implements TransactionPersistencePort {

  /**
   * Spring injects ALL active {@link TransactionPersistencePort} beans into this list.
   *
   * <p>Inactive beans (e.g., {@code SpannerTransactionService} guarded by
   * {@code @ConditionalOnProperty}) are excluded automatically — they simply won't be in the list.
   * So in a local development environment, this list contains only {@code H2TransactionService}.
   */
  private final List<TransactionPersistencePort> providers;

  /**
   * Selects the most capable available storage engine.
   *
   * <p><b>Priority order:</b>
   *
   * <ol>
   *   <li>Cloud Spanner — globally distributed, strongly consistent, preferred for production.
   *   <li>AlloyDB — high-performance PostgreSQL-compatible, preferred for APAC/regional prod.
   *   <li>H2 — in-memory, dev/test fallback, always available.
   * </ol>
   *
   * <p><b>PRO TIP:</b> In a real system you could replace this simple name-matching logic with a
   * health-check driven fallback chain: try Spanner, if it's down fall back to AlloyDB, etc.
   *
   * @throws IllegalStateException if no provider is configured — this should never happen because
   *     H2 is always on the classpath
   */
  private TransactionPersistencePort getActiveProvider() {
    // Prefer Spanner: highest consistency for production workloads
    return providers.stream()
        .filter(p -> p.getStoreName().contains("Spanner"))
        .findFirst()
        // Fall back to AlloyDB (PostgreSQL-compatible, regional GCP option)
        .or(() -> providers.stream().filter(p -> p.getStoreName().contains("AlloyDB")).findFirst())
        // Final fallback: H2 in-memory — always present for local dev/test
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
    // Trace-level log — cheap to produce and useful when debugging provider selection
    log.trace("Routing save → {}", active.getStoreName());
    return active.save(transaction);
  }

  @Override
  public java.util.Optional<com.example.springkafkapoc.domain.model.Transaction> findById(
      String transactionId) {
    // findById is used for idempotency checks — must always go to the same store as save()
    return getActiveProvider().findById(transactionId);
  }

  @Override
  public String getStoreName() {
    // Returns the active provider's name — surfaced in audit logs and health endpoints
    return getActiveProvider().getStoreName();
  }
}
