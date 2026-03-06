package com.example.springkafkapoc.domain.model;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <b>Audit Trail Domain Model</b>
 *
 * <p><b>TUTORIAL:</b> This is our <b>Clean Domain Model</b> for auditing. Unlike the entity, this
 * class is free from {@code @Entity} or {@code @Column} tags. This makes it "Portable" — we can
 * pass it between services, controllers, and even serialize it for the web without exposing our
 * database secrets.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditLog {
  private String id;
  private String transactionId;
  private String accountId;
  private AuditEventType eventType;
  private Instant eventTime;
  private Integer kafkaPartition;
  private Long kafkaOffset;
  private String detail;
}
