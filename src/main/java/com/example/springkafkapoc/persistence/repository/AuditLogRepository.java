package com.example.springkafkapoc.persistence.repository;

import com.example.springkafkapoc.domain.model.AuditEventType;
import com.example.springkafkapoc.persistence.entity.AuditLogEntity;
import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * <b>Audit Log Repository</b>
 *
 * <p><b>TUTORIAL:</b> This repository demonstrates <b>Derived Query Methods</b>. Notice how methods
 * like {@code findByTransactionIdOrderByEventTimeAsc} don't have any SQL or JPQL? Spring Data JPA
 * parses the method name and generates the query for you!
 */
@Repository
public interface AuditLogRepository extends JpaRepository<AuditLogEntity, String> {

  List<AuditLogEntity> findByTransactionIdOrderByEventTimeAsc(String transactionId);

  long countByTransactionIdAndEventType(String transactionId, AuditEventType eventType);

  @Query(
      "SELECT a FROM AuditLogEntity a WHERE a.eventType = 'SENT_TO_DLQ' AND a.eventTime BETWEEN :from AND :to ORDER BY a.eventTime DESC")
  List<AuditLogEntity> findDlqEventsBetween(@Param("from") Instant from, @Param("to") Instant to);

  List<AuditLogEntity> findByAccountIdOrderByEventTimeDesc(String accountId);
}
