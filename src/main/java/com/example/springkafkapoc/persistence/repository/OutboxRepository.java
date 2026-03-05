package com.example.springkafkapoc.persistence.repository;

import com.example.springkafkapoc.persistence.entity.OutboxEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * <b>Transactional Outbox Repository</b>
 *
 * <p>
 * <b>TUTORIAL:</b> This repository is the "Waiting Room" for your Kafka messages.
 * By using a standard {@link JpaRepository}, we can take advantage of ACID transactions
 * to ensure a message is only recorded if the business data is also successfully saved.
 */
@Repository
public interface OutboxRepository extends JpaRepository<OutboxEntity, Long> {}
