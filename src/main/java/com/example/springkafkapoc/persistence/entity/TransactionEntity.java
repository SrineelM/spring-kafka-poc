package com.example.springkafkapoc.persistence.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <b>Transaction JPA Entity — The Persistence Layer Contract</b>
 *
 * <p><b>TUTORIAL — Why separate the domain model from the entity?</b><br>
 * The domain model ({@link com.example.springkafkapoc.domain.model.Transaction}) is a plain POJO
 * known to every layer of the application. If we annotated IT with {@code @Entity}, {@code @Table},
 * and {@code @Column}, then every business service that imports Transaction would now depend on JPA
 * — making unit tests harder, Spring Boot startup mandatory, and database coupling pervasive.
 *
 * <p>The entity class is an <em>adapter</em> concern: it only lives in the persistence layer.
 * Business logic never imports it. A MapStruct mapper bridges the two representations.
 *
 * <p><b>Key JPA/Schema decisions:</b>
 *
 * <ul>
 *   <li><b>{@code @UniqueConstraint(columnNames = {"transaction_id"})}:</b> Enforces uniqueness at
 *       the database level — the last line of defence for idempotency. Even if the application
 *       idempotency check is bypassed by a race condition, the DB will reject the duplicate with a
 *       {@code DataIntegrityViolationException}, which the consumer handles gracefully.
 *   <li><b>{@code precision = 18, scale = 4} on amount:</b> Supports amounts up to
 *       99,999,999,999,999.9999 — more than enough for any transaction value. Scale 4 gives 4
 *       decimal places, matching the Avro schema's decimal logical type configuration.
 *   <li><b>{@code nullable = false} on required columns:</b> Explicitly declares NOT NULL at the
 *       schema level. Hibernate's default is nullable — omitting this allows corrupt records to be
 *       persisted without Java-level validation.
 * </ul>
 *
 * <p><b>PRO TIP — Spanner compatibility:</b><br>
 * Spanner uses String primary keys (not auto-increment integers). Using a {@code String} ID
 * (UUIDv7) here is intentional: it works identically with H2, PostgreSQL, and Cloud Spanner without
 * any schema changes when switching backends.
 */
@Entity
@Table(
    name = "Transactions",
    uniqueConstraints = {
      // Unique constraint on transaction_id prevents duplicate inserts even under concurrent load.
      // The DB constraint is the safety net below the application-level idempotency check.
      @UniqueConstraint(columnNames = {"transaction_id"})
    })
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionEntity {

  /**
   * Primary key — the business transaction ID (UUIDv7).
   *
   * <p>UUIDv7 is used over auto-increment ({@code @GeneratedValue}) because:
   *
   * <ul>
   *   <li>It's globally unique without a database sequence — no distributed lock needed.
   *   <li>It's time-ordered, so B-tree index insertions are mostly sequential (fewer page splits).
   *   <li>It's compatible with Cloud Spanner which does not support auto-increment integers.
   * </ul>
   */
  @Id
  @Column(nullable = false, length = 64)
  private String transactionId;

  /** The originating account — maximum 128 chars to accommodate external account ID formats. */
  @Column(nullable = false, length = 128)
  private String accountId;

  /**
   * Financial amount — always stored as {@link BigDecimal} with explicit precision and scale.
   *
   * <p>NEVER use {@code double} for money: floating-point arithmetic cannot represent 0.1 exactly.
   * At scale, these rounding errors accumulate and create reconciliation discrepancies. Precision
   * 18, Scale 4 → max value: 99,999,999,999,999.9999
   */
  @Column(nullable = false, precision = 18, scale = 4)
  private BigDecimal amount;

  /**
   * UTC timestamp of when the transaction was originated (not when it was processed).
   *
   * <p>Always store in UTC ({@link Instant}) — never local time. Kafka Streams uses event time for
   * windowing; if different producers use different timezones, windows will be incorrect.
   */
  @Column(nullable = false)
  private Instant timestamp;

  /**
   * Lifecycle status of the record in the pipeline.
   *
   * <p>Possible values: {@code "PROCESSED"} (single-record mode), {@code "PROCESSED_BATCH"} (batch
   * mode). Distinguishing them in the DB helps debugging performance regressions when switching
   * between single and batch consumer modes.
   */
  @Column(nullable = false, length = 32)
  private String status;

  /**
   * The identifier of which consumer group / processor class handled this record.
   *
   * <p>Used in forensic investigations to answer: "Was this processed by the single consumer or the
   * batch consumer? Which pod handled it?" Critical for debugging race conditions.
   */
  @Column(length = 64)
  private String processedBy;

  /**
   * The Kafka partition this record arrived from.
   *
   * <p>Paired with {@code sourceOffset}, this uniquely identifies the original Kafka message. Use
   * {@code kafka-console-consumer --partition X --offset Y} to retrieve the raw bytes.
   */
  @Column private Integer sourcePartition;

  /** The Kafka offset — cross-reference with broker logs during incident investigations. */
  @Column private Long sourceOffset;
}
