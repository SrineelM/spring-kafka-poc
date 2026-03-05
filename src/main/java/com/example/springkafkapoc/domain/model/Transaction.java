package com.example.springkafkapoc.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <b>Core Transaction Domain Model</b>
 *
 * <p>
 * This is the central model of our system. It represents a processed financial
 * transaction that has been validated and persisted.
 *
 * <p>
 * WHY BIGDECIMAL? Never use {@code double} or {@code float} for currency!
 * IEEE-754 precision issues can cause cent-level errors during arithmetic.
 *
 * <p>
 * Tutorial Tip: This domain model is "POJO-first". It contains no JPA or
 * Spanner
 * tags, making it easy to test and share across different layers of the app.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    private String transactionId;
    private String accountId;
    private BigDecimal amount;
    private Instant timestamp;
    private String status;
    private String processedBy;
    private int sourcePartition;
    private long sourceOffset;
}
