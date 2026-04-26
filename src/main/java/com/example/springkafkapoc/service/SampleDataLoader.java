package com.example.springkafkapoc.service;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import java.math.BigDecimal;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * <b>Sample Data Loader — Local Development Seeder</b>
 *
 * <p><b>TUTORIAL — CommandLineRunner:</b><br>
 * {@link CommandLineRunner} is a Spring Boot hook that runs automatically after the application
 * context is fully started. It's commonly used for: database seeding, data migrations, and
 * triggering warmup logic. The {@code run(String... args)} method receives command-line arguments
 * but can be ignored for seeding purposes.
 *
 * <p><b>WHY {@code @Profile("local")} — Profile-Gating:</b><br>
 * Profile-gating ensures this seeder is ONLY registered as a Spring bean when the {@code local}
 * profile is active. In production ({@code SPRING_PROFILES_ACTIVE=prod}) or staging, this bean
 * simply doesn't exist — it's never loaded, never executed. No risk of polluting real data.
 *
 * <p><b>PRO TIP:</b> Always gate data seeders, test fixtures, and debug utilities behind a
 * profile. A common production incident pattern is a seeder accidentally running and flooding a
 * production Kafka topic with thousands of synthetic records.
 *
 * <p><b>Why {@code @Transactional}?</b><br>
 * Each {@code seedTransaction()} call involves a Kafka send and an audit log write. Wrapping
 * the entire {@code run()} method in a transaction ensures that if any seed record fails, the
 * whole seed batch rolls back — leaving the system in a clean state rather than partially seeded.
 */
@Slf4j
@Component
@Profile("local")  // ONLY active when SPRING_PROFILES_ACTIVE includes "local"
@RequiredArgsConstructor
public class SampleDataLoader implements CommandLineRunner {

  private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
  private final AuditService auditService; // Record audit entries for seeded transactions too

  /**
   * Runs automatically at startup (when the local profile is active).
   *
   * <p>Seeds a small, representative set of transactions covering common scenarios:
   * <ul>
   *   <li>Two standard retail transactions (below high-value threshold).
   *   <li>One high-value transaction (above $10,000) — will be routed by the custom partitioner.
   * </ul>
   */
  @Override
  @Transactional
  public void run(String... args) {
    log.info("=== [LOCAL PROFILE] Seeding sample transactions ===");

    // Standard retail transactions — will go to NORMAL_TRANSACTIONS after stream routing
    seedTransaction("TXN-SEED-001", "ACC-RETAIL-001", new BigDecimal("250.00"), "Morning Coffee");
    seedTransaction("TXN-SEED-002", "ACC-RETAIL-001", new BigDecimal("89.99"),  "Cloud Service Subscription");

    // High-value transaction — will be routed to HIGH_VALUE_TRANSACTIONS by RoutingTopology
    // and sent to Partition 0 by HighValueTransactionPartitioner on the producer side
    seedTransaction("TXN-SEED-003", "ACC-PREMIUM-001", new BigDecimal("75000.00"), "Real Estate Deposit");

    log.info("=== Seeding complete. Streams processing will begin shortly. ===");
  }

  /**
   * Builds and publishes a single seed transaction event.
   *
   * <p>Uses a {@code whenComplete} callback on the returned {@code CompletableFuture} to log
   * success/failure asynchronously — the seed method does not block waiting for the broker ACK.
   *
   * @param transactionId a stable, human-readable seed ID for easy log correlation
   * @param account       the account to associate the transaction with
   * @param amount        the transaction amount
   * @param detail        a human-readable description used in the audit log
   */
  private void seedTransaction(
      String transactionId, String account, BigDecimal amount, String detail) {

    // Build the Avro TransactionEvent from our seed data
    TransactionEvent event =
        TransactionEvent.newBuilder()
            .setTransactionId(transactionId)
            .setAccountId(account)
            .setAmount(amount)
            .setTimestamp(Instant.now().toEpochMilli())
            .setProcessingState("INITIAL") // Marks this as an un-processed seed record
            .build();

    // Publish to the raw-transactions topic using transactionId as the Kafka key
    // The key determines which partition the record goes to (after the custom partitioner runs)
    kafkaTemplate
        .send(TopicConstants.RAW_TRANSACTIONS, transactionId, event)
        .whenComplete(
            (result, ex) -> {
              if (ex == null) {
                log.info(
                    "Seeded: {} → {} | {} | partition={}",
                    transactionId, account, detail,
                    result.getRecordMetadata().partition());
              } else {
                log.error("Seed failed for {}: {}", transactionId, ex.getMessage());
              }
            });

    // Write an audit entry for every seeded transaction — even seed data should be traceable
    auditService.recordReceived(transactionId, account, amount, "SEED: " + detail);
  }
}
