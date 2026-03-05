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
 * <b>AlloyDB Persistence Adapter</b>
 *
 * <p>
 * <b>TUTORIAL:</b> This implementation is for <b>Google Cloud AlloyDB</b> (a
 * PostgreSQL-compatible
 * database). It demonstrates how easy it is to switch between globally
 * distributed Spanner and
 * high-performance PostgreSQL-based storage using the Adapter pattern.
 */
@Slf4j
@Service("alloyDbTransactionService")
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.cloud.gcp.alloydb.enabled", havingValue = "true")
public class AlloyDbTransactionService implements TransactionPersistencePort {

    private final TransactionRepository repository;
    private final TransactionMapper mapper;

    @Override
    @Transactional
    public Transaction save(Transaction transaction) {
        log.debug("Saving via Cloud AlloyDB: transactionId={}", transaction.getTransactionId());
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
        return "Google Cloud AlloyDB";
    }
}
