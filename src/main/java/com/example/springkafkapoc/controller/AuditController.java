package com.example.springkafkapoc.controller;

import com.example.springkafkapoc.domain.model.AuditLog;
import com.example.springkafkapoc.service.AuditService;
import com.example.springkafkapoc.service.DynamicPersistenceRouter;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <b>Audit API</b>
 *
 * <p>
 * This controller provides visibility into the lifecycle of transactions.
 * In a production system, auditing is crucial for debugging distributed race
 * conditions
 * and honoring SLAs.
 *
 * <p>
 * Tutorial Tip: Notice how we return Domain Models (AuditLog) rather than
 * persistence entities (AuditLogEntity). This decouples our API from our
 * database schema.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/audit")
@RequiredArgsConstructor
public class AuditController {

    private final AuditService auditService;
    private final DynamicPersistenceRouter persistenceRouter;

    /**
     * Returns the complete ordered audit trail for a transaction.
     *
     * @param transactionId the ID to query
     * @return list of audit log entries
     */
    @GetMapping("/transaction/{transactionId}")
    public ResponseEntity<List<AuditLog>> getTransactionAuditTrail(@PathVariable String transactionId) {
        log.info("Audit trail requested for transactionId={}", transactionId);
        List<AuditLog> trail = auditService.getAuditTrail(transactionId);
        if (trail.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(trail);
    }

    /**
     * Returns all audit events for an account, newest first.
     */
    @GetMapping("/account/{accountId}")
    public ResponseEntity<List<AuditLog>> getAccountAuditHistory(@PathVariable String accountId) {
        log.info("Account audit history requested for accountId={}", accountId);
        return ResponseEntity.ok(auditService.getAccountAuditHistory(accountId));
    }

    /**
     * Returns how many times a transaction was retried.
     * A high retry count is an early warning signal of a systemic issue.
     */
    @GetMapping("/transaction/{transactionId}/retry-count")
    public ResponseEntity<Map<String, Object>> getRetryCount(@PathVariable String transactionId) {
        long retries = auditService.countRetries(transactionId);
        return ResponseEntity.ok(Map.of(
                "transactionId", transactionId,
                "retryCount", retries,
                "warning", retries >= 2 ? "HIGH_RETRY_COUNT — investigate!" : "OK"));
    }

    /**
     * Shows which database backend is currently active.
     * Returns "Cloud Spanner (with H2 fallback)" in production,
     * or "H2 In-Memory" in local/fallback mode.
     */
    @GetMapping("/storage-info")
    public ResponseEntity<Map<String, String>> storageInfo() {
        return ResponseEntity.ok(Map.of("activeStorage", persistenceRouter.getStoreName(), "status", "UP"));
    }
}
