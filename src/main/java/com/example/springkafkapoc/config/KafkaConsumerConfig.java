package com.example.springkafkapoc.config;

import com.example.springkafkapoc.observability.KafkaCorrelationIdExtractor;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

/**
 * <b>Kafka Consumer Configuration — Factory & Listener Container Setup</b>
 *
 * <p><b>TUTORIAL — Consumer Factory vs. Listener Container Factory:</b><br>
 * These are two separate concepts that are often confused:
 *
 * <ul>
 *   <li><b>{@link ConsumerFactory}:</b> Creates the raw Apache Kafka {@code KafkaConsumer}
 *       instances. Think of it as a JDBC {@code DataSource} — it governs connection-level
 *       settings: deserialization, group ID, isolation level, and offsets.
 *   <li><b>{@link ConcurrentKafkaListenerContainerFactory}:</b> Wraps the consumer factory with
 *       Spring-specific concerns: error handling, acknowledgement mode, concurrency (threads), and
 *       record interceptors. It manages the message-dispatch lifecycle.
 * </ul>
 *
 * <p><b>Key Consumer Settings:</b>
 *
 * <ul>
 *   <li>{@code specific.avro.reader=true}: Generates typed Java classes ({@code TransactionEvent})
 *       rather than generic {@code GenericRecord}. Required for typed field access.
 *   <li>{@code isolation.level=read_committed}: Consumer only sees records from fully committed
 *       producer transactions. Without this, aborted producer transactions are visible.
 *   <li>{@code MAX_POLL_RECORDS=500}: Limits records per {@code poll()} to bound processing time
 *       and prevent {@code max.poll.interval.ms} violations.
 * </ul>
 *
 * <p><b>MANUAL_IMMEDIATE Ack Mode:</b><br>
 * Offsets are committed only when {@code ack.acknowledge()} is explicitly called. This is the
 * critical guard against data loss: if the JVM crashes before {@code ack} is called, Kafka
 * redelivers the record. Only ack AFTER your database transaction has committed.
 *
 * <p><b>Single vs. Batch listeners:</b>
 *
 * <ul>
 *   <li><b>Single ({@code kafkaListenerContainerFactory}):</b> Default. One {@code
 *       ConsumerRecord} per listener invocation. Lower throughput but simpler error isolation.
 *   <li><b>Batch ({@code batchKafkaListenerContainerFactory}):</b> A {@code
 *       List<ConsumerRecord>} per invocation. 10–100× higher throughput for DB sinks, but all
 *       records in the batch are redelivered on failure — idempotency is mandatory.
 * </ul>
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

  private final KafkaProperties kafkaProperties;

  // ─── Consumer Factory ─────────────────────────────────────────────────────────────────────────

  /**
   * Shared consumer factory used by both the single-record and batch listener container factories.
   *
   * <p>Marked as a {@code @Bean} so it can be overridden in tests via {@code @MockBean} or
   * {@code @TestConfiguration} without changing listener definitions.
   */
  @Bean
  public ConsumerFactory<String, Object> consumerFactory() {
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    // Confluent Avro deserializer — fetches schema from Schema Registry on first message
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.KafkaAvroDeserializer");

    // Generate typed Java classes (TransactionEvent) not GenericRecord
    props.put("specific.avro.reader", "true");

    // Limit records per poll — keeps per-poll processing time predictable
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

    // Only see records from committed producer transactions
    // PRO TIP: Without this, aborted transactional producer writes are still visible
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    log.debug(
        "Consumer factory created: maxPollRecords=500, isolationLevel=read_committed,"
            + " deserializer=KafkaAvroDeserializer");

    return new DefaultKafkaConsumerFactory<>(props);
  }

  // ─── Listener Container Factories ────────────────────────────────────────────────────────────

  /**
   * <b>Single-Record Listener Container Factory (Default)</b>
   *
   * <p>Three critical wires applied here:
   *
   * <ol>
   *   <li><b>Error Handler:</b> Applies exponential-backoff retries then routes to DLT on
   *       exhaustion. Listeners don't need to handle retries themselves.
   *   <li><b>Correlation ID Extractor:</b> Runs before every listener method to restore the trace
   *       context from Kafka headers into MDC — enables end-to-end log correlation.
   *   <li><b>MANUAL_IMMEDIATE Ack Mode:</b> Offset committed only after {@code ack.acknowledge()}
   *       is called. Guards against silent data loss on JVM crash.
   * </ol>
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
      ConsumerFactory<String, Object> consumerFactory,
      DefaultErrorHandler errorHandler,
      KafkaCorrelationIdExtractor<String, Object> correlationIdExtractor) {

    ConcurrentKafkaListenerContainerFactory<String, Object> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setCommonErrorHandler(errorHandler);

    // Restore correlationId into MDC before every listener call
    factory.setRecordInterceptor(correlationIdExtractor);

    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

    log.debug(
        "Single-record listener container factory created: ackMode=MANUAL_IMMEDIATE,"
            + " errorHandler={}, interceptor={}",
        errorHandler.getClass().getSimpleName(),
        correlationIdExtractor.getClass().getSimpleName());

    return factory;
  }

  /**
   * <b>Batch Listener Container Factory</b>
   *
   * <p><b>TUTORIAL — Batching trade-offs:</b>
   *
   * <ul>
   *   <li><b>Speed:</b> One DB transaction for N records = N× fewer round-trips. Throughput can be
   *       10–100× higher than single-record mode.
   *   <li><b>Risk:</b> If the batch fails and retries, ALL records in the batch are redelivered —
   *       including those already persisted. Idempotency checks are mandatory.
   *   <li><b>Error Isolation:</b> Spring Kafka's error handler splits a consistently-failing batch
   *       into single records and routes each failed record to the DLT individually.
   * </ul>
   *
   * <p>Note: No {@code correlationIdExtractor} here — batch mode delivers a list of records, so
   * per-record header extraction is done inside the listener itself.
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, Object>
      batchKafkaListenerContainerFactory(
          ConsumerFactory<String, Object> consumerFactory, DefaultErrorHandler errorHandler) {

    ConcurrentKafkaListenerContainerFactory<String, Object> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setCommonErrorHandler(errorHandler);

    // Enable batch mode — listener receives List<ConsumerRecord> instead of single record
    factory.setBatchListener(true);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

    log.debug(
        "Batch listener container factory created: batchMode=true, ackMode=MANUAL_IMMEDIATE");

    return factory;
  }
}
