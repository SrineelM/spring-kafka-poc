package com.example.springkafkapoc.persistence.entity;

import com.example.springkafkapoc.domain.model.AuditEventType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.UuidGenerator;

/**
 * <b>Audit Trail Entity</b>
 *
 * <p>
 * <b>TUTORIAL:</b> This entity represents a single "Snapshot" of an event in
 * our system. In bank-grade applications, auditing is as critical as the
 * transaction itself for regulatory compliance.
 *
 * <p>
 * <b>Key Architecture Tip:</b> we use a <b>Time-based UUID</b> for the ID
 * to ensure that audit records are naturally sortable by time across a
 * distributed system, while avoiding the lock-contention of a sequential
 * integer.
 */
@Entity
@Table(name = "AuditLog")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditLogEntity {

    @Id
    @GeneratedValue
    @UuidGenerator(style = UuidGenerator.Style.TIME)
    private String id;

    @Column(nullable = false, length = 64)
    private String transactionId;

    @Column(nullable = false, length = 128)
    private String accountId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private AuditEventType eventType;

    @Column(nullable = false)
    private Instant eventTime;

    @Column
    private Integer kafkaPartition;

    @Column
    private Long kafkaOffset;

    @Column(length = 1024)
    private String detail;
}
