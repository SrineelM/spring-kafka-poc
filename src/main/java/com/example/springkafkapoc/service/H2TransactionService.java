package com.example.springkafkapoc.service;

import com.example.springkafkapoc.domain.model.Transaction;
import com.example.springkafkapoc.persistence.entity.TransactionEntity;
import com.example.springkafkapoc.persistence.mapper.TransactionMapper;
import com.example.springkafkapoc.persistence.repository.TransactionRepository;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * <b>H2 Persistence Adapter — Local Development & Testing Backend</b>
 *
 * <p><b>TUTORIAL — Role in Hexagonal Architecture:</b><br>
 * This is an <em>Adapter</em> in the ports-and-adapters model. It implements the {@link
 * TransactionPersistencePort} interface using an embedded H2 in-memory database. H2 needs no
 * external server, starts instantly, and wipes itself clean on each restart — making it ideal for
 * local development and CI pipelines.
 *
 * <p><b>WHY no {@code @ConditionalOnProperty} here?</b><br>
 * Unlike Spanner and AlloyDB which are conditional, H2 is always available. The {@link
 * DynamicPersistenceRouter} treats it as the default fallback. In a CI environment with no cloud
 * credentials, H2 ensures the application can still start and be tested.
 *
 * <p><b>PRO TIP:</b> The bean name {@code "h2TransactionService"} is explicit. If you inject by
 * name ({@code @Qualifier("h2TransactionService")}), you can force H2 even when Spanner is
 * conditionally enabled — useful for write-behind cache patterns or integration tests.
 */
@Slf4j
@Service("h2TransactionService") // Explicit bean name for @Qualifier injection
@RequiredArgsConstructor
public class H2TransactionService implements TransactionPersistencePort {

  // Spring Data CrudRepository — provides save/findById/delete out of the box
  private final TransactionRepository repository;

  // MapStruct-generated mapper — converts between domain model and JPA entity
  // Generated code is in target/generated-sources; much faster than reflection-based mappers
  private final TransactionMapper mapper;

  /**
   * Persists the domain transaction to H2 via JPA.
   *
   * <p>The mapper converts the persistence-agnostic {@link Transaction} domain model to a {@link
   * com.example.springkafkapoc.persistence.entity.TransactionEntity} before saving. This prevents
   * JPA annotations from leaking into the domain layer.
   */
  @Override
  @Transactional // Wraps the save in a DB transaction — rolls back on any RuntimeException
  public Transaction save(Transaction transaction) {
    log.debug("Saving via H2: transactionId={}", transaction.getTransactionId());

    // Map domain → entity (strips away business logic, adds persistence metadata)
    TransactionEntity entity = mapper.toEntity(transaction);

    // JPA save: INSERT if new primary key, UPDATE if existing
    TransactionEntity saved = repository.save(entity);

    // Map entity → domain (strips away persistence metadata, returns clean domain object)
    return mapper.toDomain(saved);
  }

  /**
   * Looks up a transaction by its business ID — used for idempotency checks.
   *
   * <p>{@code readOnly} is not set here but could be added for slight performance improvement:
   * {@code @Transactional(readOnly = true)} hints to the JPA provider to skip dirty checking.
   */
  @Override
  public Optional<Transaction> findById(String transactionId) {
    // CrudRepository.findById returns Optional<Entity> — map to domain model if found
    return repository.findById(transactionId).map(mapper::toDomain);
  }

  @Override
  public String getStoreName() {
    return "H2 In-Memory"; // Surfaced in audit logs and the DynamicPersistenceRouter selection
    // logic
  }
}
