package com.example.springkafkapoc.config;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.observability.KafkaCorrelationIdExtractor;
import com.example.springkafkapoc.observability.KafkaCorrelationIdInterceptor;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * <b>Kafka Core Infrastructure Configuration</b>
 *
 * <p>Centralizes the creation of Producer/Consumer factories and standard templates.
 *
 * <p>Tutorial Tip: This class is the "Heart" of the Kafka integration. It handles:
 *
 * <ul>
 *   <li><b>Idempotency:</b> Enabled by default to prevent duplicate data for retries.
 *   <li><b>Serialization:</b> Uses Apache Avro for strict schema enforcement.
 *   <li><b>Observability:</b> Hooks in Correlation ID interceptors for tracing.
 * </ul>
 */
@Slf4j
@Configuration
public class KafkaCoreConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.properties.schema.registry.url:http://localhost:8081}")
  private String schemaRegistryUrl;

  @Value("${kafka.topic.replication-factor:1}")
  private short replicationFactor;

  @Value("${kafka.topic.pipeline-partitions:3}")
  private int pipelinePartitions;

  @Value("${kafka.topic.metrics-partitions:1}")
  private int metricsPartitions;

  @Value("${spring.kafka.producer.transaction-id-prefix:}")
  private String transactionIdPrefix;

  // -------------------------------------------------------------------------
  // Producer & Consumer Factories
  // -------------------------------------------------------------------------

  /**
   * TUTORIAL: The {@code ProducerFactory} defines how to create Kafka producers. We use a {@code
   * DefaultKafkaProducerFactory} supplied with our common properties. This is the {@code @Primary}
   * factory, meaning it will be used by default when injecting a ProducerFactory or building
   * standard templates.
   */
  @Bean
  @Primary
  public ProducerFactory<String, TransactionEvent> producerFactory() {
    DefaultKafkaProducerFactory<String, TransactionEvent> factory =
        new DefaultKafkaProducerFactory<>(commonProducerProps());
    if (transactionIdPrefix != null && !transactionIdPrefix.isEmpty()) {
      factory.setTransactionIdPrefix(transactionIdPrefix);
    }
    return factory;
  }

  /**
   * TUTORIAL: A specialized producer factory that overrides the default partitioner. We attach the
   * {@code HighValueTransactionPartitioner} so that transactions over a certain value are
   * explicitly routed to dedicated metrics partitions, optimizing downstream processing logic.
   */
  @Bean
  public ProducerFactory<String, TransactionEvent> customPartitionerProducerFactory() {
    Map<String, Object> props = commonProducerProps();
    props.put(
        ProducerConfig.PARTITIONER_CLASS_CONFIG, HighValueTransactionPartitioner.class.getName());
    DefaultKafkaProducerFactory<String, TransactionEvent> factory =
        new DefaultKafkaProducerFactory<>(props);
    if (transactionIdPrefix != null && !transactionIdPrefix.isEmpty()) {
      factory.setTransactionIdPrefix(transactionIdPrefix + "metrics-");
    }
    return factory;
  }

  private Map<String, Object> commonProducerProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put("schema.registry.url", schemaRegistryUrl);
    // Ensure exactly-once/idempotent semantics on the producer side
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    // Guarantee strongest durability; producer returns success only if leader and
    // ISR confirm receipt
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    // Inject custom interceptor to append trace contexts (Correlation ID) into
    // message headers
    props.put(
        ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, KafkaCorrelationIdInterceptor.class.getName());
    return props;
  }

  /**
   * TUTORIAL: The {@code KafkaTemplate} simplifies sending messages to Kafka topics. It wraps the
   * {@code ProducerFactory} and provides high-level operations like {@code send(topic, key,
   * value)}. It behaves much like Spring's JdbcTemplate or RestTemplate.
   */
  @Bean
  @Primary
  public KafkaTemplate<String, TransactionEvent> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  @Bean("customPartitioningKafkaTemplate")
  public KafkaTemplate<String, TransactionEvent> customPartitioningKafkaTemplate() {
    return new KafkaTemplate<>(customPartitionerProducerFactory());
  }

  /**
   * TUTORIAL: The {@code ConsumerFactory} creates Kafka consumers. Similar to the producer, it
   * defines the core connection and deserialization rules. Notice we configure it to deserialize
   * Avro back into specific Java classes natively.
   */
  @Bean
  public ConsumerFactory<String, Object> consumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put("schema.registry.url", schemaRegistryUrl);
    props.put("specific.avro.reader", "true");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); // Align with YAML default
    return new DefaultKafkaConsumerFactory<>(props);
  }

  /**
   * TUTORIAL: The {@code ConcurrentKafkaListenerContainerFactory} is the engine behind
   * {@code @KafkaListener} annotations. It spawns the consumer threads. We customize it here to: 1.
   * Add a global error handler for dealing with Exceptions during processing. 2. Insert our
   * Correlation ID extractor before our business logic runs. 3. Set Manual acknowledgement mode so
   * we control exactly when a message is marked 'read'.
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
    factory.setRecordInterceptor(correlationIdExtractor);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }

  /**
   * TUTORIAL: Similar to the above, but specifically configured for batch processing. Useful when
   * processing lots of small events simultaneously (e.g., buffering writes to BigQuery).
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, Object> batchKafkaListenerContainerFactory(
      ConsumerFactory<String, Object> consumerFactory, DefaultErrorHandler errorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, Object> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setCommonErrorHandler(errorHandler);
    factory.setBatchListener(true);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }

  // -------------------------------------------------------------------------
  // Request-Reply Configuration
  // -------------------------------------------------------------------------

  @Bean
  public ProducerFactory<String, TransactionEvent> replyProducerFactory() {
    // Request-reply doesn't need transactions; remove prefix to avoid fencing.
    return new DefaultKafkaProducerFactory<>(commonProducerProps());
  }

  @Bean
  public ConsumerFactory<String, String> replyConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "reply-consumer-group");
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> replyListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(replyConsumerFactory());
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }

  @Bean
  public ReplyingKafkaTemplate<String, TransactionEvent, String> replyingKafkaTemplate() {
    var replyContainer = replyListenerContainerFactory().createContainer("transaction-reply-topic");
    replyContainer.getContainerProperties().setGroupId("replying-template-group");
    return new ReplyingKafkaTemplate<>(replyProducerFactory(), replyContainer);
  }

  // -------------------------------------------------------------------------
  // Topic Beans
  // -------------------------------------------------------------------------

  @Bean
  public NewTopic rawTransactionsTopic() {
    return TopicBuilder.name(TopicConstants.RAW_TRANSACTIONS)
        .partitions(pipelinePartitions)
        .replicas(replicationFactor)
        .build();
  }

  @Bean
  public NewTopic processedTransactionsTopic() {
    return TopicBuilder.name(TopicConstants.PROCESSED_TRANSACTIONS)
        .partitions(pipelinePartitions)
        .replicas(replicationFactor)
        .build();
  }

  @Bean
  public NewTopic dailyAccountMetricsTopic() {
    return TopicBuilder.name(TopicConstants.DAILY_ACCOUNT_METRICS)
        .partitions(metricsPartitions)
        .replicas(replicationFactor)
        .build();
  }

  @Bean
  public NewTopic rawTransactionsDlt() {
    return TopicBuilder.name(TopicConstants.RAW_TRANSACTIONS_DLT)
        .partitions(pipelinePartitions)
        .replicas(replicationFactor)
        .build();
  }

  // -------------------------------------------------------------------------
  // Error Handling Strategy
  // -------------------------------------------------------------------------

  @Bean
  public DefaultErrorHandler errorHandler(KafkaTemplate<String, TransactionEvent> template) {
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(
            template,
            (r, e) ->
                new org.apache.kafka.common.TopicPartition(r.topic() + ".DLT", r.partition()));

    ExponentialBackOff backOff = new ExponentialBackOff(2000L, 2.0);
    backOff.setMaxElapsedTime(60000L); // 60s total window to survive DB re-boots
    backOff.setMaxInterval(20000L); // Cap single delay at 20s

    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
    handler.setLogLevel(org.springframework.kafka.KafkaException.Level.WARN);
    return handler;
  }
}
