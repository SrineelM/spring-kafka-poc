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
 * <b>Audit & Compliance Service — The Immutable Event Log</b>
 *
 * <p><b>TUTORIAL — Why audit logging matters in financial systems:</b><br>
 * In regulated industries (banking, payments), you must be able to answer: "Who touched this
 * record, when, and what happened?" A standard application log is insufficient — logs are not
 * queryable and are often rotated away. A dedicated audit table provides a persistent, queryable
 * timeline for compliance audits, customer disputes, and forensic investigations.
 *
 * <p><b>Key Design Decisions:</b>
 *
 * <ul>
 *   <li><b>Append-Only:</b> Audit records are never updated or deleted. Each event (RECEIVED,
 *       PROCESSED, RETRY, DLQ) is a new row. This preserves the full history.
 *   <li><b>{@code REQUIRES_NEW} Propagation:</b> This is the most important design choice here. If
 *       the main business transaction rolls back (e.g., a DB constraint violation), the audit log
 *       of that <em>attempt</em> must still be saved. Using {@code REQUIRES_NEW} suspends the
 *       current transaction and opens a fresh, independent one just for the audit write.
 *   <li><b>MapStruct Mapping:</b> Domain {@link AuditLog} objects are converted to JPA entities via
 *       a generated mapper — keeping the domain model free of persistence annotations.
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuditService {

  private final AuditLogRepository auditRepository; // Spring Data JPA repository for audit rows
  private final AuditLogMapper mapper; // MapStruct domain ↔ entity converter

  /**
   * Core event recorder — all other audit methods delegate here.
   *
   * <p>{@code Propagation.REQUIRES_NEW}: even if the caller is mid-rollback, this method opens a
   * <em>new</em> transaction to ensure the audit entry is always persisted.
   *
   * @param transactionId the business transaction being audited
   * @param accountId the account associated with the transaction
   * @param eventType the lifecycle stage (RECEIVED, PROCESSED, RETRY, DLQ, etc.)
   * @param detail a human-readable description of what happened
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void recordEvent(
      String transactionId, String accountId, AuditEventType eventType, String detail) {
    // Delegate to the full-signature overload with null partition/offset
    recordEvent(transactionId, accountId, eventType, null, null, detail);
  }

  /**
   * Full-signature recorder — includes Kafka partition and offset for forensic tracing.
   *
   * <p>Storing the partition and offset alongside the business event means you can use {@code
   * kafka-console-consumer --partition X --offset Y} to retrieve the exact raw message that caused
   * an issue — incredibly useful during incident investigations.
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void recordEvent(
      String transactionId,
      String accountId,
      AuditEventType eventType,
      Integer partition, // The Kafka partition the record arrived from (may be null for non-Kafka
      // events)
      Long offset, // The Kafka offset — cross-reference with broker logs
      String detail) {
    log.debug("Auditing: id={}, event={}, detail={}", transactionId, eventType, detail);

    // Build the domain model — it stays framework-agnostic (no JPA annotations here)
    AuditLog logEntry =
        AuditLog.builder()
            .transactionId(transactionId)
            .accountId(accountId)
            .eventType(eventType)
            .kafkaPartition(partition)
            .kafkaOffset(offset)
            .detail(detail)
            .eventTime(Instant.now()) // Record wall-clock time of the audit event
            .build();

    // MapStruct converts the domain object to a JPA entity; the mapper is generated at build time
    auditRepository.save(mapper.toEntity(logEntry));
  }

  /**
   * Convenience method for the initial message receipt event.
   *
   * <p>Called when a transaction first arrives via REST — before any Kafka processing begins.
   * Captures the amount so that any subsequent DLQ or failure can be correlated back to the
   * original dollar value.
   */
  public void recordReceived(
      String transactionId, String accountId, BigDecimal amount, String details) {
    recordEvent(
        transactionId,
        accountId,
        AuditEventType.RECEIVED,
        String.format("Amount: %s. %s", amount, details));
  }

  /**
   * Called at the start of Kafka consumer processing.
   *
   * <p>Recording the Kafka partition and offset here means you can prove exactly which message was
   * being processed if the system crashes mid-flight.
   */
  public void recordProcessing(String transactionId, String accountId, int partition, long offset) {
    recordEvent(
        transactionId,
        accountId,
        AuditEventType.RECEIVED, // Using RECEIVED as the "processing started" marker
        partition,
        offset,
        "Processing started on consumer thread.");
  }

  /**
   * Records a successful persistence event — the "happy path" completion marker.
   *
   * @param store the name of the storage engine that accepted the record (e.g. "H2", "Spanner")
   */
  public void recordSuccess(String transactionId, String accountId, String store) {
    recordEvent(
        transactionId, accountId, AuditEventType.PROCESSED, "Successfully stored in " + store);
  }

  /**
   * Records a Dead Letter Queue (DLQ) event — the record could not be processed after all retries.
   *
   * <p>This is critical for compliance: auditors can query all DLQ events to see which transactions
   * were never processed and why.
   */
  public void recordDlq(
      String transactionId, String accountId, int partition, long offset, String reason) {
    recordEvent(transactionId, accountId, AuditEventType.SENT_TO_DLQ, partition, offset, reason);
  }

  /**
   * Returns the full ordered audit trail for a specific transaction.
   *
   * <p>Useful for API endpoints that let operations replay or investigate a specific transaction.
   * Results are ordered by event time ascending to show the lifecycle chronologically.
   */
  public List<AuditLog> getAuditTrail(String transactionId) {
    return auditRepository.findByTransactionIdOrderByEventTimeAsc(transactionId).stream()
        .map(mapper::toDomain) // Convert entities back to domain objects
        .toList();
  }

  /** Returns all audit events for an account, most recent first. Useful for account-level views. */
  public List<AuditLog> getAccountAuditHistory(String accountId) {
    return auditRepository.findByAccountIdOrderByEventTimeDesc(accountId).stream()
        .map(mapper::toDomain)
        .toList();
  }

  /**
   * Counts how many retry attempts were recorded for a transaction.
   *
   * <p>High retry counts indicate persistent infrastructure issues or poison-pill records. Expose
   * this via a metrics endpoint or Grafana alert.
   */
  public long countRetries(String transactionId) {
    return auditRepository.countByTransactionIdAndEventType(
        transactionId, AuditEventType.RETRY_ATTEMPTED);
  }
}
