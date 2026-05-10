package com.example.springkafkapoc.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import com.example.springkafkapoc.persistence.mapper.AuditLogMapper;
import com.example.springkafkapoc.persistence.repository.AuditLogRepository;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuditServiceTest {

  @Mock private AuditLogRepository auditLogRepository;

  @Mock private AuditLogMapper mapper;

  @InjectMocks private AuditService auditService;

  @Test
  void shouldRecordAuditLog() {
    // Given
    String transactionId = "transaction-1";
    String accountId = "acc-1";
    BigDecimal amount = new BigDecimal("100.00");
    String detail = "Test Detail";

    // When
    auditService.recordReceived(transactionId, accountId, amount, detail);

    // Then
    verify(auditLogRepository).save(any());
  }
}
