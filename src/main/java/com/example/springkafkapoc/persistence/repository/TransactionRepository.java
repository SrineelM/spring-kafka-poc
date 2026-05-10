package com.example.springkafkapoc.persistence.repository;

import com.example.springkafkapoc.persistence.entity.TransactionEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

/**
 * <b>Spring Data Repository: Transaction</b>
 *
 * <p><b>TUTORIAL:</b> Spring Data JPA provides the implementation of this interface at runtime.
 * What makes this powerful is its <b>Dialect Agnostic</b> nature.
 *
 * <p>The same repository works for:
 *
 * <ul>
 *   <li><b>H2:</b> Using standard SQL locally.
 *   <li><b>Cloud Spanner:</b> Using Spanner-specific partitioning and indexing in the cloud.
 * </ul>
 */
@Repository
public interface TransactionRepository extends CrudRepository<TransactionEntity, String> {}
