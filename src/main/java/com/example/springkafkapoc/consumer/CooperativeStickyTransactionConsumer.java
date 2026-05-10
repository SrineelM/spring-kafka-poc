package com.example.springkafkapoc.consumer;

import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * <b>Cooperative Sticky Consumer — Sample Listener Implementation</b>
 *
 * <p>This listener demonstrates the correct patterns for a consumer operating under the
 * {@link CooperativeStickyAssignor}:
 *
 * <ol>
 *   <li><b>MANUAL_IMMEDIATE ack:</b> Offsets are committed only after all processing in the method
 *       succeeds. If the JVM crashes, the record is redelivered — no silent data loss.
 *   <li><b>Idempotent processing:</b> Because cooperative rebalance can reassign partitions
 *       mid-stream and at-least-once delivery guarantees redelivery after a crash, every processing
 *       path must tolerate receiving the same record twice.
 *   <li><b>Bounded processing time:</b> Each invocation must complete within
 *       {@code max.poll.interval.ms} (300s). Long-running work should be offloaded to a thread
 *       pool with a timeout guard.
 * </ol>
 *
 * <p><b>Production Recommendations — Scale-Up / Scale-Down:</b>
 *
 * <ul>
 *   <li><b>Scale-Up:</b> Add more consumer instances. Cooperative rebalance moves only the
 *       necessary partitions to the new instance. Existing consumers continue processing
 *       uninterrupted.
 *   <li><b>Scale-Down:</b> Remove a consumer. Its partitions are distributed to the remaining
 *       consumers. With cooperative rebalance, only the affected consumers pause briefly.
 *   <li><b>Rolling Restart (Kubernetes):</b> Use {@code group.instance.id} (static membership)
 *       so that the restarted pod claims its prior partitions without a full rebalance.
 * </ul>
 *
 * <p><b>Why {@code containerFactory = "cooperativeStickyContainerFactory"}?</b><br>
 * The factory name binds this listener to the cooperative sticky consumer config. Using a named
 * factory keeps cooperative consumers isolated from the default eager consumers in
 * {@link com.example.springkafkapoc.config.KafkaCoreConfig}, preventing protocol mismatch errors.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CooperativeStickyTransactionConsumer {

  /**
   * Processes raw transaction events with Cooperative Sticky partition assignment.
   *
   * <p><b>Key implementation decisions:</b>
   *
   * <ul>
   *   <li>We inject {@link Acknowledgment} rather than relying on auto-commit.
   *   <li>We log the partition and offset on every record for offset tracking.
   *   <li>We catch and log exceptions before calling {@code ack.acknowledge()} — if processing
   *       throws, we do NOT ack, allowing the error handler or retry topic to take over.
   * </ul>
   *
   * @param record      the raw Kafka record — key is the transactionId, value is the raw payload
   * @param ack         the manual acknowledgment handle — commit offset only after success
   * @param partition   the Kafka partition this record arrived from (injected from headers)
   * @param offset      the Kafka offset of this record (injected from headers)
   */
  @KafkaListener(
      topics = "${app.cooperative-consumer.topic:raw-transactions-topic}",
      groupId = "cooperative-sticky-consumer-group",
      containerFactory = "cooperativeStickyContainerFactory")
  public void consume(
      ConsumerRecord<String, String> record,
      Acknowledgment ack,
      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
      @Header(KafkaHeaders.OFFSET) long offset) {

    log.debug(
        "Received record: key={}, partition={}, offset={}, timestamp={}",
        record.key(),
        partition,
        offset,
        Instant.ofEpochMilli(record.timestamp()));

    try {
      processRecord(record, partition, offset);

      // ── Idempotent Acknowledgment ─────────────────────────────────────────────────────────────
      // Commit the offset only AFTER processing succeeds.
      // If the JVM crashes here before this line, Kafka redelivers the record.
      // Our processRecord() must tolerate receiving the same record twice (idempotent).
      ack.acknowledge();

      log.debug(
          "Acknowledged: partition={}, offset={}, key={}", partition, offset, record.key());

    } catch (Exception ex) {
      // Do NOT ack on failure — let the configured error handler (DefaultErrorHandler +
      // ExponentialBackOff) decide whether to retry or route to the DLT.
      log.error(
          "Processing failed for partition={}, offset={}, key={}. "
              + "Record will be retried per the error handler policy. cause={}",
          partition,
          offset,
          record.key(),
          ex.getMessage(),
          ex);
      throw ex; // Re-throw so Spring Kafka's error handler takes control
    }
  }

  /**
   * Core business logic for a single transaction record.
   *
   * <p>This method is extracted from the listener to:
   *
   * <ul>
   *   <li>Keep the listener method thin (only Kafka concerns: ack, error handling).
   *   <li>Make business logic unit-testable without a Kafka container.
   *   <li>Allow future extraction to a service class without touching the listener.
   * </ul>
   *
   * <p><b>Idempotency contract:</b> This method must be safe to call more than once with the same
   * record. Use a unique key check (e.g., deduplication table or DB unique constraint) as the
   * guard.
   *
   * @param record    the consumer record to process
   * @param partition the source partition (for logging and idempotency key construction)
   * @param offset    the source offset (for logging and forensic correlation)
   */
  private void processRecord(ConsumerRecord<String, String> record, int partition, long offset) {
    String key = record.key();
    String value = record.value();

    log.info(
        "Processing transaction: key={}, partition={}, offset={}, valueLength={}",
        key,
        partition,
        offset,
        value != null ? value.length() : 0);

    // ── PRO TIP — Idempotency Check ───────────────────────────────────────────────────────────────
    // In production, perform a DB lookup or cache check here:
    //   if (transactionRepository.existsById(key)) {
    //     log.info("Idempotency: key={} already processed, skipping", key);
    //     return;
    //   }
    //
    // This guard is essential because:
    // 1. Cooperative rebalance reassigns partitions incrementally — uncommitted offsets
    //    on revoked partitions are redelivered to the new consumer.
    // 2. Kafka guarantees at-least-once delivery, not exactly-once for @KafkaListener.

    // ── Simulate processing ────────────────────────────────────────────────────────────────────────
    log.debug("Business logic executing for key={}", key);
    // [INSERT ACTUAL BUSINESS LOGIC HERE — e.g., save to DB, call downstream service]

    log.info(
        "Successfully processed transaction: key={}, partition={}, offset={}",
        key,
        partition,
        offset);
  }
}
