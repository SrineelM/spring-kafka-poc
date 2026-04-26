package com.example.springkafkapoc.service;

import com.example.springkafkapoc.domain.model.Transaction;
import com.example.springkafkapoc.persistence.entity.TransactionEntity;
import com.example.springkafkapoc.persistence.mapper.TransactionMapper;
import com.example.springkafkapoc.persistence.repository.TransactionRepository;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * <b>Cloud Spanner Persistence Adapter — Global Production Backend</b>
 *
 * <p><b>TUTORIAL — What is Cloud Spanner?</b><br>
 * Google Cloud Spanner is a globally distributed, horizontally scalable, strongly consistent
 * relational database. It's the only database that simultaneously provides ACID transactions,
 * SQL semantics, and horizontal scaling across global regions — making it ideal for financial
 * workloads where data consistency and availability are non-negotiable.
 *
 * <p><b>{@code @ConditionalOnProperty} — Conditional Bean Loading:</b><br>
 * This adapter only registers as a Spring bean when {@code app.database.spanner-enabled=true}.
 * When that property is absent or false (e.g., local development), this class is completely
 * invisible to Spring — no bean, no database connections, no startup cost.
 *
 * <p>This is a production-grade alternative to {@code @Profile("production")}: properties are
 * more flexible than profiles because they can be set per-environment via ConfigMaps, Secrets,
 * or environment variables without changing the deployment descriptor.
 *
 * <p><b>Distributed Consistency:</b><br>
 * Spanner provides external consistency — stronger than serializable isolation. This means that
 * even across geographically distributed nodes, reads always reflect the latest committed write.
 * For a payments system processing transactions across time zones, this is essential.
 */
@Slf4j
@Service("spannerTransactionService")  // Named bean for @Qualifier-based injection
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.database.spanner-enabled", havingValue = "true")
public class SpannerTransactionService implements TransactionPersistencePort {

  // The same JPA repository used by H2 — Spring Data abstracts the underlying dialect
  // The Spanner JDBC driver translates standard SQL to Spanner's internal query engine
  private final TransactionRepository repository;
  private final TransactionMapper mapper; // MapStruct converter — same as H2 adapter

  /**
   * Persists a transaction to Cloud Spanner.
   *
   * <p>The implementation is intentionally identical to the H2 adapter — this is the power of
   * the adapter pattern. The only difference is the database dialect and connection pool,
   * configured in the Spring datasource properties for the Spanner profile.
   */
  @Override
  @Transactional
  public Transaction save(Transaction transaction) {
    log.debug("Saving via Cloud Spanner: transactionId={}", transaction.getTransactionId());
    TransactionEntity entity = mapper.toEntity(transaction);
    TransactionEntity saved = repository.save(entity);
    return mapper.toDomain(saved);
  }

  /**
   * Idempotency lookup against Spanner.
   *
   * <p><b>PRO TIP — Spanner Read Performance:</b><br>
   * Spanner supports "stale reads" (reading data that's slightly behind current time) which are
   * significantly cheaper than strongly-consistent reads. For idempotency checks where eventual
   * consistency is acceptable, consider using a stale read to reduce latency and cost.
   */
  @Override
  public Optional<Transaction> findById(String transactionId) {
    return repository.findById(transactionId).map(mapper::toDomain);
  }

  @Override
  public String getStoreName() {
    return "Google Cloud Spanner"; // Shown in audit logs and DynamicPersistenceRouter logs
  }
}
