package com.example.springkafkapoc.service;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.github.f4b6a3.uuid.UuidCreator;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * <b>Data Ingestion Service — The Front Door Producer</b>
 *
 * <p><b>TUTORIAL — Role in the pipeline:</b><br>
 * This is the first service data touches when it enters the system. It accepts a raw transaction
 * (amount + account ID), wraps it in an <b>Avro-encoded</b> {@link TransactionEvent}, and publishes
 * it to the raw-transactions Kafka topic. Everything downstream (consumers, Streams topologies)
 * reads from that topic.
 *
 * <p><b>WHY Avro?</b><br>
 * Avro enforces a strict schema contract between producer and consumer. The schema is stored in a
 * central <b>Schema Registry</b>. If a producer tries to publish a record that is incompatible
 * with the registered schema (e.g., a required field is removed), the serializer will throw an
 * exception — preventing the breaking change from ever reaching consumers at runtime.
 *
 * <p><b>PRO TIP — Schema-first design:</b><br>
 * Always define your Avro schema ({@code .avsc}) before writing producer code. Generate the Java
 * classes via the Maven Avro plugin ({@code avro-maven-plugin}). This ensures the contract is
 * machine-enforced, not just a convention.
 *
 * <p><b>WHY {@code @Transactional}?</b><br>
 * {@code KafkaTemplate.send()} participates in an active Kafka transaction if one exists. The
 * {@code @Transactional} here ensures the send is wrapped in a producer transaction — if anything
 * goes wrong in the calling chain, the Kafka send is rolled back too.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DataIngestionService {

  // KafkaTemplate is the high-level Spring abstraction for producing Kafka messages.
  // It wraps the low-level KafkaProducer and handles serialization transparently.
  private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

  /**
   * Ingests a raw transaction, builds an Avro event, and publishes it to Kafka.
   *
   * <p>The send is <b>asynchronous</b> — this method returns a {@link CompletableFuture} that
   * completes when the Kafka broker acknowledges the record. The caller (controller) does not
   * block.
   *
   * @param amount    the transaction monetary value — {@link BigDecimal} to avoid floating-point
   *                  precision errors that plague {@code double} and {@code float} for currency
   * @param accountId the originating account identifier
   * @return a future that resolves to the Kafka {@link SendResult} on success
   */
  @Transactional
  public CompletableFuture<SendResult<String, TransactionEvent>> ingestTransaction(
      BigDecimal amount, String accountId) {

    // Generate a time-ordered UUID (UUIDv7) as the transaction ID.
    // WHY UUIDv7? Unlike random UUIDv4, UUIDv7 is monotonically increasing, making it:
    //   1. Sortable by time without a secondary timestamp column
    //   2. Excellent for distributed systems — globally unique AND temporally ordered
    //   3. Friendly to B-tree indexes (sequential inserts vs. random page splits)
    String transactionId = UuidCreator.getTimeOrderedEpoch().toString();
    log.info("Ingesting transaction: id={}, account={}, amount={}", transactionId, accountId, amount);

    // Build the Avro event using the generated builder pattern.
    // All fields here map directly to the TransactionEvent.avsc schema definition.
    TransactionEvent event =
        TransactionEvent.newBuilder()
            .setTransactionId(transactionId)
            .setAccountId(accountId)
            .setAmount(amount)
            // Store epoch millis — compact and timezone-agnostic
            .setTimestamp(Instant.now().toEpochMilli())
            // Initial state flag — downstream processors check this to detect reprocessing
            .setProcessingState("INITIAL")
            .build();

    // Use the transactionId as the Kafka message KEY.
    // WHY? Kafka guarantees ordering within a partition. By using the transactionId as the key,
    // all events related to the same transaction land on the same partition — preserving order.
    // The custom partitioner in KafkaCoreConfig may further route high-value keys to Partition 0.
    return kafkaTemplate
        .send(TopicConstants.RAW_TRANSACTIONS, transactionId, event)
        .whenComplete(
            (result, ex) -> {
              if (ex == null) {
                // Log partition + offset for debugging — cross-reference with Kafka tool output
                log.info(
                    "Ingested: id={}, partition={}, offset={}",
                    transactionId,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
              } else {
                // This is a producer-level failure (e.g., broker unreachable, schema mismatch)
                log.error("Ingestion failed: id={}, error={}", transactionId, ex.getMessage());
              }
            });
  }
}
