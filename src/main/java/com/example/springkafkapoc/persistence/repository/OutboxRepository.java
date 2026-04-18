package com.example.springkafkapoc.persistence.repository;

import com.example.springkafkapoc.persistence.entity.OutboxEntity;
import jakarta.persistence.LockModeType;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * <b>Transactional Outbox Repository</b>
 *
 * <p><b>TUTORIAL:</b> This repository is the "Waiting Room" for your Kafka messages. By using a
 * standard {@link JpaRepository}, we can take advantage of ACID transactions to ensure a message is
 * only recorded if the business data is also successfully saved.
 */
@Repository
public interface OutboxRepository extends JpaRepository<OutboxEntity, Long> {

  /**
   * Finds unprocessed messages with a pessimistic write lock to prevent concurrent processing.
   *
   * @param pageable specifies the batch size and sort order
   * @return a list of unprocessed outbox entities
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("SELECT o FROM OutboxEntity o WHERE o.processed = false ORDER BY o.createdAt ASC")
  List<OutboxEntity> findUnprocessedLocked(Pageable pageable);

  /**
   * Atomically marks a message as processed.
   *
   * @param id the record ID
   * @return number of rows updated
   */
  @Modifying
  @Query("UPDATE OutboxEntity o SET o.processed = true WHERE o.id = :id AND o.processed = false")
  int markProcessed(@Param("id") Long id);
}
