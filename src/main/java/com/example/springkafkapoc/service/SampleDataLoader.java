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
 * <b>Sample Data Seeder</b>
 *
 * <p><b>TUTORIAL:</b> This component "jumpstarts" the system for local development. Instead of
 * manually calling the API, this class runs automatically (if the "local" profile is active) and
 * feeds initial transactions into Kafka.
 *
 * <p><b>Key Architecture Tip:</b> Notice the {@link Profile} annotation. This ensures the seeder
 * <b>NEVER</b> runs in production. You don't want your real production system getting flooded with
 * morning coffee records!
 */
@Slf4j
@Component
@Profile("local")
@RequiredArgsConstructor
public class SampleDataLoader implements CommandLineRunner {

  private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
  private final AuditService auditService;

  @Override
  @Transactional
  public void run(String... args) {
    log.info("--- SEEDING DATA ---");

    seedTransaction("TXN-SEED-001", "ACC-RETAIL-001", new BigDecimal("250.00"), "Morning Coffee");
    seedTransaction("TXN-SEED-002", "ACC-RETAIL-001", new BigDecimal("89.99"), "Cloud Service");
    seedTransaction("TXN-SEED-003", "ACC-PREMIUM-001", new BigDecimal("75000.00"), "Real Estate");

    log.info("--- SEEDING COMPLETE ---");
  }

  private void seedTransaction(
      String transactionId, String account, BigDecimal amount, String detail) {
    TransactionEvent event =
        TransactionEvent.newBuilder()
            .setTransactionId(transactionId)
            .setAccountId(account)
            .setAmount(amount)
            .setTimestamp(Instant.now().toEpochMilli())
            .setProcessingState("INITIAL")
            .build();

    kafkaTemplate
        .send(TopicConstants.RAW_TRANSACTIONS, transactionId, event)
        .whenComplete(
            (result, ex) -> {
              if (ex == null) {
                log.info("Seeded: {} - {} - {}", transactionId, account, detail);
              } else {
                log.error("Failed to seed: {}", transactionId, ex);
              }
            });

    // Use specialized audit helper
    auditService.recordReceived(transactionId, account, amount, "SEED: " + detail);
  }
}
