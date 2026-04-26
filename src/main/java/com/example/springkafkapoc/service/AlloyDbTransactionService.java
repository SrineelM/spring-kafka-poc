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
 * <b>Cloud AlloyDB Persistence Adapter — High-Performance PostgreSQL Backend</b>
 *
 * <p><b>TUTORIAL — When to choose AlloyDB over Spanner:</b><br>
 * Both are GCP-managed databases, but they serve different trade-offs:
 *
 * <ul>
 *   <li><b>Cloud Spanner:</b> Horizontally scalable, globally distributed, strong external
 *       consistency. Choose it when you need multi-region active-active writes.
 *   <li><b>AlloyDB:</b> PostgreSQL-compatible, 4× faster OLTP than standard CloudSQL PostgreSQL,
 *       supports columnar engine for hybrid OLTP/analytics. Choose it when you're migrating from
 *       PostgreSQL and need high-performance without operational overhead.
 * </ul>
 *
 * <p>This adapter only activates when {@code spring.cloud.gcp.alloydb.enabled=true} is set —
 * typically in a staging or AlloyDB-specific deployment profile.
 *
 * <p><b>PRO TIP — Zero-Code Migration:</b><br>
 * Because both AlloyDB and H2 are SQL-compatible, and this class uses the same Spring Data
 * {@link TransactionRepository}, switching from H2 (dev) to AlloyDB (staging) requires only a
 * property change. No code changes. This is the adapter pattern's greatest strength.
 */
@Slf4j
@Service("alloyDbTransactionService")
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.cloud.gcp.alloydb.enabled", havingValue = "true")
public class AlloyDbTransactionService implements TransactionPersistencePort {

  private final TransactionRepository repository;
  private final TransactionMapper mapper;

  /**
   * Saves a transaction via the AlloyDB JDBC connection.
   *
   * <p>AlloyDB auto-scales compute and uses a distributed storage layer, so this {@code save}
   * call can handle very high concurrency without manual connection pool tuning.
   */
  @Override
  @Transactional
  public Transaction save(Transaction transaction) {
    log.debug("Saving via Cloud AlloyDB: transactionId={}", transaction.getTransactionId());
    TransactionEntity entity = mapper.toEntity(transaction);
    TransactionEntity saved = repository.save(entity);
    return mapper.toDomain(saved);
  }

  /**
   * Idempotency lookup.
   *
   * <p>AlloyDB supports covering indexes, which can make this query very fast if
   * {@code transaction_id} is indexed — which it is, via the {@code @UniqueConstraint} on the
   * {@link TransactionEntity}.
   */
  @Override
  public Optional<Transaction> findById(String transactionId) {
    return repository.findById(transactionId).map(mapper::toDomain);
  }

  @Override
  public String getStoreName() {
    return "Google Cloud AlloyDB";
  }
}
