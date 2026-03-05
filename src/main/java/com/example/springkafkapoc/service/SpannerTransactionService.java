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
 * <b>Cloud Spanner Persistence Adapter</b>
 *
 * <p>
 * <b>TUTORIAL:</b> This implementation is an <b>Adapter</b> for the
 * {@link TransactionPersistencePort} specifically for Google Cloud Spanner.
 *
 * <p>
 * <b>Conditional Loading:</b> Notice the {@code @ConditionalOnProperty}. This
 * service only exists in the Spring application context if we are in a
 * production-like
 * environment where Spanner is enabled.
 *
 * <p>
 * <b>Distributed Consistency:</b> Spanner provides external consistency and
 * high-availability. By using it as our persistence layer, we ensure that our
 * "Transaction" and "Outbox" updates are committed across global regions with
 * strong ACID guarantees.
 */
@Slf4j
@Service("spannerTransactionService")
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.database.spanner-enabled", havingValue = "true")
public class SpannerTransactionService implements TransactionPersistencePort {

    private final TransactionRepository repository;
    private final TransactionMapper mapper;

    @Override
    @Transactional
    public Transaction save(Transaction transaction) {
        log.debug("Saving via Cloud Spanner: transactionId={}", transaction.getTransactionId());
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
        return "Google Cloud Spanner";
    }
}
