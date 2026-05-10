package com.example.springkafkapoc.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import lombok.Builder;
import lombok.Data;

/**
 * <b>Structured Fraud Alert Model (The Event)</b>
 *
 * <p><b>TUTORIAL:</b> In early prototypes, "Fraud Alert: TXN-123" might be a simple String.
 * However, in a production system, you need a structured model.
 *
 * <p>Why use a POJO/Avro model?
 *
 * <ul>
 *   <li><b>Downstream Routing:</b> A "High-Value" fraud service can easily filter by the {@code
 *       amount} field.
 *   <li><b>Machine Learning:</b> Data scientists can ingest these alerts into a feature store using
 *       the structured fields.
 *   <li><b>Schema Evolution:</b> You can add new fields (like {@code location}) without breaking
 *       older consumers.
 * </ul>
 */
@Data
@Builder
public class FraudAlert {
  private String alertId;
  private String transactionId;
  private String accountId;
  private BigDecimal amount;
  private String signal;
  private Instant timestamp;
}
