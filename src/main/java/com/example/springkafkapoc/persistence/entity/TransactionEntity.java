package com.example.springkafkapoc.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Enriched version of the core transaction record.
 *
 * <p>Extends the basic {@link TransactionEntity} with metadata fields that are populated as the
 * record flows through the pipeline. Used to demonstrate idempotent upserts in both H2 and
 * Spanner.
 *
 * <p><b>BEGINNER TIP on @Column annotations:</b> Always specify {@code @Column(nullable =
 * false)} for required fields. Without it, Hibernate generates a nullable column, which allows
 * corrupt data to be saved. Explicit constraints on the DB layer are your last line of defence
 * if Java-land validation is bypassed.
 */
@Entity
@Table(name = "Transactions")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionEntity {

    /**
     * The Kafka record key — also the immutable business transaction ID. String UUIDs are
     * Spanner-compatible primary keys.
     */
    @Id
    @Column(nullable = false, length = 64)
    private String transactionId;

    @Column(nullable = false, length = 128)
    private String accountId;

    /**
     * WHY: BigDecimal for money. Using double for financial calculations is a critical error
     * due to floating-point inaccuracies (e.g., 0.1 + 0.2 != 0.3). BigDecimal provides exact
     * precision. The Avro schema is configured with a `decimal` logical type to match.
     *
     * <p>Precision 18, Scale 4 allows for amounts up to 99,999,999,999,999.9999.
     */
    @Column(nullable = false, precision = 18, scale = 4)
    private BigDecimal amount;

    /** Epoch timestamp of when the transaction originated. */
    @Column(nullable = false)
    private Instant timestamp;

    /**
     * Processing state mirrors the Avro schema's {@code processingState} field. Possible
     * values: INITIAL, PROCESSED.
     */
    @Column(nullable = false, length = 32)
    private String status;

    /**
     * The Kafka consumer group that last processed this record. Helps in DLQ forensics — tells
     * you which consumer thread touched this record.
     */
    @Column(length = 64)
    private String processedBy;

    /**
     * The source Kafka topic partition this record arrived from. Useful for correlating with
     * Kafka's built-in monitoring tools.
     */
    @Column
    private Integer sourcePartition;

    /** Source Kafka offset — correlates exactly with broker logs. */
    @Column
    private Long sourceOffset;
}
