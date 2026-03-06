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
 * <b>H2 Persistence Adapter</b>
 *
 * <p><b>TUTORIAL:</b> This is the "Development/Testing" implementation of our persistence layer.
 * Since it uses an in-memory database, it's perfect for local development or CI/CD pipelines where
 * you don't have access to Cloud Spanner.
 */
@Slf4j
@Service("h2TransactionService")
@RequiredArgsConstructor
public class H2TransactionService implements TransactionPersistencePort {

  private final TransactionRepository repository;
  private final TransactionMapper mapper;

  @Override
  @Transactional
  public Transaction save(Transaction transaction) {
    log.debug("Saving via H2: transactionId={}", transaction.getTransactionId());
    TransactionEntity entity = mapper.toEntity(transaction);
    TransactionEntity saved = repository.save(entity);
    return mapper.toDomain(saved);
  }

  @Override
  public Optional<Transaction> findById(String transactionId) {
    return repository.findById(transactionId).map(mapper::toDomain);
  }

  @Override
  public String getStoreName() {
    return "H2 In-Memory";
  }
}
