package com.example.springkafkapoc.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <b>Core Transaction Domain Model — The Business Heart</b>
 *
 * <p><b>TUTORIAL — Domain Model vs. Entity vs. Avro Schema:</b><br>
 * This project uses three representations of a "transaction" — each serving a distinct purpose:
 *
 * <ul>
 *   <li><b>TransactionEvent (Avro):</b> The wire format. Travels over Kafka. Schema-validated by
 *       the Schema Registry. Used by producers and consumers for transport.
 *   <li><b>Transaction (this class):</b> The domain model. Framework-agnostic POJO. Used by all
 *       business services. Has no JPA, Avro, or Kafka annotations.
 *   <li><b>TransactionEntity (JPA):</b> The persistence model. Used only by the persistence adapter
 *       (H2/Spanner/AlloyDB). Has {@code @Entity}, {@code @Column}, etc.
 * </ul>
 *
 * <p>This separation is the essence of <b>Hexagonal Architecture</b>: your business logic (which
 * uses this class) is completely isolated from the infrastructure it runs on. You can swap Kafka
 * for RabbitMQ or H2 for Spanner without touching a single line of business code.
 *
 * <p><b>WHY {@link BigDecimal} for amount?</b><br>
 * NEVER use {@code double} or {@code float} for financial amounts. IEEE-754 floating-point
 * arithmetic introduces rounding errors — {@code 0.1 + 0.2 == 0.30000000000000004} in Java. For a
 * payment system, even a one-cent discrepancy per transaction is unacceptable at scale. {@link
 * BigDecimal} provides arbitrary-precision decimal arithmetic with explicit rounding modes.
 *
 * <p><b>PRO TIP — Always use {@code HALF_UP} rounding:</b><br>
 * When rounding is needed (e.g., currency conversion), use {@code RoundingMode.HALF_UP} — the
 * mathematical "banker's rounding." {@code HALF_EVEN} (Java's default) is more statistically
 * unbiased but surprises most developers. Document whichever you choose.
 *
 * <p><b>Kafka forensics fields:</b><br>
 * {@code sourcePartition} and {@code sourceOffset} are stored alongside the business data so that
 * any record in the database can be traced back to its exact position in the Kafka topic. This is
 * invaluable during incident investigations: {@code kafka-console-consumer --partition X --offset
 * Y} retrieves the original raw bytes.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

  /** The business-unique transaction identifier — UUIDv7 (time-ordered, globally unique). */
  private String transactionId;

  /** The account this transaction belongs to — used as the Kafka partition key after re-keying. */
  private String accountId;

  /** Transaction monetary amount — always {@link BigDecimal}, never {@code double}. */
  private BigDecimal amount;

  /**
   * Epoch-based UTC timestamp of when the transaction was originated.
   *
   * <p>Using {@link Instant} (not {@code Date} or {@code LocalDateTime}) ensures the value is
   * always in UTC with nanosecond precision. Never store local times in event-driven systems —
   * timezone ambiguity causes incorrect time-window boundaries in Kafka Streams aggregations.
   */
  private Instant timestamp;

  /**
   * Processing lifecycle status.
   *
   * <p>Possible values:
   *
   * <ul>
   *   <li>{@code "INITIAL"} — Published by producer, not yet consumed.
   *   <li>{@code "PROCESSED"} — Consumed and persisted by the single-record processor.
   *   <li>{@code "PROCESSED_BATCH"} — Consumed and persisted by the batch processor.
   * </ul>
   */
  private String status;

  /**
   * Identifies which consumer thread/instance processed this record.
   *
   * <p>Useful for debugging: if you see unexpected duplicates, the processedBy field tells you
   * whether two different consumer instances both wrote the same transaction.
   */
  private String processedBy;

  /** The Kafka partition this record was consumed from. For forensic re-fetch from Kafka. */
  private int sourcePartition;

  /** The Kafka offset this record was consumed at. Pair with sourcePartition for exact lookup. */
  private long sourceOffset;
}
