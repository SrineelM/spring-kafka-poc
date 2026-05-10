package com.example.springkafkapoc.config;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.observability.KafkaCorrelationIdInterceptor;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * <b>Kafka Producer Configuration — Factory & Template Setup</b>
 *
 * <p><b>TUTORIAL — Why centralise producer config?</b><br>
 * Without a shared config, each service that needs to produce to Kafka would have to independently
 * specify acks, idempotency, serializers, and interceptors. One missed setting (e.g., forgetting
 * {@code enable.idempotence}) in one service creates an inconsistent and unsafe pipeline.
 *
 * <p><b>What this class owns:</b>
 *
 * <ul>
 *   <li><b>Primary ProducerFactory:</b> Used by most services via the primary {@link KafkaTemplate}.
 *   <li><b>Custom-Partitioner ProducerFactory:</b> Routes high-value transactions to a dedicated
 *       partition via {@link HighValueTransactionPartitioner}.
 *   <li><b>{@code commonProducerProps()}:</b> Package-private shared property builder — keeps
 *       Avro serialization, idempotency, and interceptor config in a single place.
 * </ul>
 *
 * <p><b>Key Producer Settings (set in {@code commonProducerProps()}):</b>
 *
 * <ul>
 *   <li>{@code ENABLE_IDEMPOTENCE=true}: The broker assigns a PID + sequence number to every
 *       batch. If the producer retries after a network timeout, the broker deduplicates using these
 *       identifiers — the record appears exactly once in the partition.
 *   <li>{@code ACKS=all}: Producer waits for the leader AND all in-sync replicas to confirm before
 *       considering a send successful. No data loss even if the leader crashes post-ack.
 *   <li>{@code INTERCEPTOR_CLASSES}: Injects {@link KafkaCorrelationIdInterceptor} into every
 *       outgoing record header — zero coupling to business code.
 * </ul>
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {

  private final KafkaProperties kafkaProperties;

  // ─── Producer Factories ───────────────────────────────────────────────────────────────────────

  /**
   * <b>Primary Producer Factory</b>
   *
   * <p>{@code @Primary} ensures this factory is injected by default whenever a {@code
   * ProducerFactory<String, TransactionEvent>} is declared as a dependency. Only use {@code
   * @Qualifier} when you specifically need the custom-partitioner variant.
   */
  @Bean
  @Primary
  public ProducerFactory<String, TransactionEvent> producerFactory() {
    log.debug("Creating primary producer factory with Avro serialization and idempotence");
    DefaultKafkaProducerFactory<String, TransactionEvent> factory =
        new DefaultKafkaProducerFactory<>(commonProducerProps());

    // Transactional producers enable Exactly-Once Semantics between Kafka topics.
    // Only activated when a transaction-id-prefix is set in application.yml.
    String prefix = kafkaProperties.getProducer().getTransactionIdPrefix();
    if (prefix != null && !prefix.isEmpty()) {
      factory.setTransactionIdPrefix(prefix);
      log.debug("Transactional producer enabled with prefix={}", prefix);
    }
    return factory;
  }

  /**
   * <b>Custom-Partitioner Producer Factory</b>
   *
   * <p><b>TUTORIAL:</b> Overriding the partitioner allows business-driven routing. The {@link
   * HighValueTransactionPartitioner} sends transactions above $10k to a dedicated partition,
   * allowing a high-priority consumer to process them first without affecting the main pipeline.
   *
   * <p>Uses a distinct transaction-ID prefix ({@code …metrics-}) so its transactional state is
   * independent of the primary factory — prevents producer-ID fencing between the two factories.
   */
  @Bean
  public ProducerFactory<String, TransactionEvent> customPartitionerProducerFactory() {
    log.debug("Creating custom-partitioner producer factory");
    Map<String, Object> props = commonProducerProps();
    props.put(
        ProducerConfig.PARTITIONER_CLASS_CONFIG, HighValueTransactionPartitioner.class.getName());
    DefaultKafkaProducerFactory<String, TransactionEvent> factory =
        new DefaultKafkaProducerFactory<>(props);
    String prefix = kafkaProperties.getProducer().getTransactionIdPrefix();
    if (prefix != null && !prefix.isEmpty()) {
      factory.setTransactionIdPrefix(prefix + "metrics-");
    }
    return factory;
  }

  // ─── Kafka Templates ──────────────────────────────────────────────────────────────────────────

  /**
   * <b>Primary KafkaTemplate</b>
   *
   * <p><b>TUTORIAL:</b> {@link KafkaTemplate} is the Spring equivalent of {@code JdbcTemplate} —
   * a thin, friendly wrapper over the raw Kafka producer. It handles serialization, error
   * callbacks, and transaction boundaries transparently. Inject this wherever you need to produce.
   */
  @Bean
  @Primary
  public KafkaTemplate<String, TransactionEvent> kafkaTemplate() {
    log.debug("Creating primary KafkaTemplate");
    return new KafkaTemplate<>(producerFactory());
  }

  /**
   * Specialist template that routes records using the custom partitioner.
   *
   * <p>Use {@code @Qualifier("customPartitioningKafkaTemplate")} at the injection site.
   */
  @Bean("customPartitioningKafkaTemplate")
  public KafkaTemplate<String, TransactionEvent> customPartitioningKafkaTemplate() {
    log.debug("Creating custom-partitioning KafkaTemplate");
    return new KafkaTemplate<>(customPartitionerProducerFactory());
  }

  // ─── Shared Property Builder ──────────────────────────────────────────────────────────────────

  /**
   * Builds the shared producer property map applied to all producer factories.
   *
   * <p>Package-private so sibling config classes (e.g., {@link KafkaRequestReplyConfig}) in the
   * same package can reuse it without duplication.
   */
  Map<String, Object> commonProducerProps() {
    // Seed from application.yml spring.kafka.producer.* properties
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

    // Idempotent producer: broker deduplicates retries using PID + sequence number
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    // Wait for ALL ISR acknowledgements — strongest durability guarantee
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    // Attach Correlation ID header to every outgoing record automatically
    props.put(
        ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, KafkaCorrelationIdInterceptor.class.getName());

    log.debug(
        "Built common producer props: acks=all, idempotence=true, interceptor={}",
        KafkaCorrelationIdInterceptor.class.getSimpleName());

    return props;
  }
}
