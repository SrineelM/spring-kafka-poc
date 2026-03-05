package com.example.springkafkapoc.service;

import com.example.springkafkapoc.domain.model.AuditEventType;
import com.example.springkafkapoc.domain.model.AuditLog;
import com.example.springkafkapoc.persistence.mapper.AuditLogMapper;
import com.example.springkafkapoc.persistence.repository.AuditLogRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * <b>Audit & Compliance Service</b>
 *
 * <p>
 * <b>TUTORIAL:</b> In enterprise systems, it's not enough that something failed
 * or succeeded;
 * you must know exactly <em>when</em> and <em>why</em>. This service acts as an
 * append-only
 * log of critical domain events.
 *
 * <p>
 * Note the use of
 * {@code @Transactional(propagation = Propagation.REQUIRES_NEW)}.
 * This is crucial! Even if the main business transaction rolls back (e.g., due
 * to a
 * database error), the audit log of that attempt MUST still be persisted. This
 * annotation
 * suspends the current transaction and starts a new, independent one just for
 * the audit log.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuditService {

    private final AuditLogRepository auditRepository;
    private final AuditLogMapper mapper;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordEvent(String transactionId, String accountId, AuditEventType eventType, String detail) {
        recordEvent(transactionId, accountId, eventType, null, null, detail);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordEvent(
            String transactionId,
            String accountId,
            AuditEventType eventType,
            Integer partition,
            Long offset,
            String detail) {
        log.debug("Auditing behavior: id={}, event={}, details={}", transactionId, eventType, detail);

        AuditLog logEntry = AuditLog.builder()
                .transactionId(transactionId)
                .accountId(accountId)
                .eventType(eventType)
                .kafkaPartition(partition)
                .kafkaOffset(offset)
                .detail(detail)
                .eventTime(Instant.now())
                .build();

        auditRepository.save(mapper.toEntity(logEntry));
    }

    public void recordReceived(String transactionId, String accountId, BigDecimal amount, String details) {
        recordEvent(
                transactionId, accountId, AuditEventType.RECEIVED, String.format("Amount: %s. %s", amount, details));
    }

    public void recordProcessing(String transactionId, String accountId, int partition, long offset) {
        recordEvent(
                transactionId,
                accountId,
                AuditEventType.RETRY_ATTEMPTED,
                partition,
                offset,
                "Processing started on consumer thread.");
    }

    public void recordSuccess(String transactionId, String accountId, String store) {
        recordEvent(transactionId, accountId, AuditEventType.PROCESSED, "Successfully stored in " + store);
    }

    public void recordDlq(String transactionId, String accountId, int partition, long offset, String reason) {
        recordEvent(transactionId, accountId, AuditEventType.SENT_TO_DLQ, partition, offset, reason);
    }

    public List<AuditLog> getAuditTrail(String transactionId) {
        return auditRepository.findByTransactionIdOrderByEventTimeAsc(transactionId).stream()
                .map(mapper::toDomain)
                .toList();
    }

    public List<AuditLog> getAccountAuditHistory(String accountId) {
        return auditRepository.findByAccountIdOrderByEventTimeDesc(accountId).stream()
                .map(mapper::toDomain)
                .toList();
    }

    public long countRetries(String transactionId) {
        return auditRepository.countByTransactionIdAndEventType(transactionId, AuditEventType.RETRY_ATTEMPTED);
    }
}
