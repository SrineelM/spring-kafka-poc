package com.example.springkafkapoc.config;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.observability.KafkaCorrelationIdExtractor;
import com.example.springkafkapoc.observability.KafkaCorrelationIdInterceptor;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
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
 * <b>Kafka Core Infrastructure Configuration — The Heart of the Integration</b>
 *
 * <p><b>TUTORIAL — Why centralize this?</b><br>
 * In a production system, you will have multiple producers, multiple consumer groups, and custom
 * error handling strategies. If each service builds its own {@code KafkaTemplate} or factory, you
 * end up with inconsistent settings scattered across the codebase — different ack modes, different
 * serializers, missing interceptors. Centralizing here guarantees that every producer and consumer
 * shares the same production-grade baseline.
 *
 * <p><b>What this class wires up:</b>
 *
 * <ul>
 *   <li><b>Producer Factories:</b> Default (for most use cases) and custom-partitioner variant.
 *   <li><b>Consumer Factory:</b> Shared Avro consumer with read-committed isolation.
 *   <li><b>Listener Container Factories:</b> Single-record and batch-mode, both with error
 *       handling and manual acknowledgement.
 *   <li><b>Topic Beans:</b> Declarative topic creation (idempotent — Kafka skips if already
 *       exists).
 *   <li><b>Error Handler:</b> Exponential-backoff retries → DLT routing on exhaustion.
 *   <li><b>Request-Reply Template:</b> Synchronous-style Kafka RPC pattern.
 * </ul>
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaCoreConfig {

  // KafkaProperties binds all spring.kafka.* properties from application.yml
  private final AppProperties appProperties;
  private final KafkaProperties kafkaProperties;

  // ─── Producer Factories ───────────────────────────────────────────────────────────────────────

  /**
   * <b>Primary Producer Factory</b>
   *
   * <p><b>TUTORIAL:</b> The {@link ProducerFactory} is the component that creates new Kafka
   * producer instances. Spring wraps the underlying Apache Kafka {@code KafkaProducer}, which is
   * thread-safe and long-lived. The factory manages the lifecycle of these producers.
   *
   * <p>Marking this {@code @Primary} means it is injected by default whenever a
   * {@code ProducerFactory<String, TransactionEvent>} is required without further qualification.
   *
   * <p><b>WHY:</b> Centralizing this ensures all producers in the app share the same security,
   * serialization, idempotence, and tracing settings.
   */
  @Bean
  @Primary
  public ProducerFactory<String, TransactionEvent> producerFactory() {
    DefaultKafkaProducerFactory<String, TransactionEvent> factory =
        new DefaultKafkaProducerFactory<>(commonProducerProps());

    // Wire Kafka transactions only if a transaction-id-prefix is set in application.yml.
    // Transactional producers enable Exactly-Once Semantics between Kafka topics.
    String prefix = kafkaProperties.getProducer().getTransactionIdPrefix();
    if (prefix != null && !prefix.isEmpty()) {
      factory.setTransactionIdPrefix(prefix);
    }
    return factory;
  }

  /**
   * <b>Custom-Partitioner Producer Factory</b>
   *
   * <p><b>TUTORIAL:</b> We override the partitioner here to apply business-driven routing. The
   * {@link HighValueTransactionPartitioner} sends transactions above $10k to a dedicated partition,
   * allowing a high-priority consumer to process them first.
   *
   * <p>This factory uses a different transaction ID prefix so its transactional producers are
   * independent of the primary factory's producers — avoiding producer ID fencing.
   */
  @Bean
  public ProducerFactory<String, TransactionEvent> customPartitionerProducerFactory() {
    Map<String, Object> props = commonProducerProps();
    // Replace the default partitioner with our business-aware routing logic
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

  /**
   * Shared producer property builder.
   *
   * <p><b>TUTORIAL — Key Producer Settings:</b>
   *
   * <ul>
   *   <li>{@code ENABLE_IDEMPOTENCE}: Ensures the broker deduplicates retries. Even if the
   *       producer retries the same record (due to a network timeout), the broker only appends it
   *       once. This requires {@code acks=all} and {@code retries > 0}.
   *   <li>{@code ACKS_CONFIG=all}: The producer waits for confirmation from the leader AND all
   *       In-Sync Replicas (ISR). The strongest durability guarantee — no data loss even if the
   *       leader crashes immediately after the ack.
   *   <li>{@code INTERCEPTOR_CLASSES}: Injects our {@link KafkaCorrelationIdInterceptor} into
   *       every outgoing record's headers. Invisible to business code — zero coupling.
   * </ul>
   */
  private Map<String, Object> commonProducerProps() {
    // Start with all spring.kafka.producer.* properties from application.yml
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));

    // String key serializer — transaction IDs are UUIDv7 strings
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    // Avro value serializer — communicates with Confluent Schema Registry for schema validation
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

    // Idempotent producer: the broker assigns a PID + sequence number to detect and drop retries
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    // Wait for all in-sync replicas to confirm — no data loss if leader fails after ack
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    // Automatically attach Correlation ID to every outgoing record's Kafka headers
    props.put(
        ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, KafkaCorrelationIdInterceptor.class.getName());

    return props;
  }

  // ─── Kafka Templates (high-level send API) ────────────────────────────────────────────────────

  /**
   * <b>Primary KafkaTemplate</b>
   *
   * <p><b>TUTORIAL:</b> {@link KafkaTemplate} is the Spring equivalent of {@code JdbcTemplate}
   * for Kafka. It wraps the {@code ProducerFactory} and exposes simple {@code send(topic, key,
   * value)} methods. It handles serialization, error callbacks, and transaction wrappers
   * transparently.
   */
  @Bean
  @Primary
  public KafkaTemplate<String, TransactionEvent> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  /**
   * Specialist template that routes records using the custom partitioner.
   *
   * <p>Injected by name — use {@code @Qualifier("customPartitioningKafkaTemplate")} at the
   * injection site to select this over the primary template.
   */
  @Bean("customPartitioningKafkaTemplate")
  public KafkaTemplate<String, TransactionEvent> customPartitioningKafkaTemplate() {
    return new KafkaTemplate<>(customPartitionerProducerFactory());
  }

  // ─── Consumer Factory ─────────────────────────────────────────────────────────────────────────

  /**
   * <b>Shared Consumer Factory</b>
   *
   * <p><b>TUTORIAL — Key Consumer Settings:</b>
   *
   * <ul>
   *   <li>{@code specific.avro.reader=true}: Tells the Confluent Avro deserializer to generate
   *       the compiled Java class ({@code TransactionEvent}) rather than a generic Avro
   *       {@code GenericRecord}. Required to access typed fields like {@code event.getAmount()}.
   *   <li>{@code MAX_POLL_RECORDS}: How many records the consumer fetches per {@code poll()} call.
   *       500 is a good default for balanced throughput. Increase for high-throughput batch sinks,
   *       decrease for latency-sensitive single-record processing.
   *   <li>{@code isolation.level=read_committed}: Prevents "Dirty Reads" — the consumer only sees
   *       records from transactions that were fully committed by the producer. Critical for
   *       Exactly-Once end-to-end guarantees.
   * </ul>
   */
  @Bean
  public ConsumerFactory<String, Object> consumerFactory() {
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));

    // String key — matches producer key serializer
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    // Confluent Avro deserializer — fetches schema from Schema Registry on first message
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.KafkaAvroDeserializer");

    // Generate typed Java classes (TransactionEvent) rather than GenericRecord
    props.put("specific.avro.reader", "true");

    // Max records fetched per poll — tune based on processing time per record
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

    // Read-committed: only see records from committed Kafka transactions
    // PRO TIP: Without this, a transactional producer that aborts would still have its
    // partial writes visible to this consumer — breaking your consistency guarantees!
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    return new DefaultKafkaConsumerFactory<>(props);
  }

  // ─── Listener Container Factories ────────────────────────────────────────────────────────────

  /**
   * <b>Single-Record Listener Container Factory (Default)</b>
   *
   * <p><b>TUTORIAL — Three critical wires applied here:</b>
   *
   * <ol>
   *   <li><b>Error Handler:</b> Our custom {@link DefaultErrorHandler} applies exponential-backoff
   *       retries then routes to the DLT. Listeners don't need to handle retries themselves.
   *   <li><b>Correlation ID Extractor:</b> Runs before every listener method to restore the
   *       trace context from Kafka headers into MDC. Enables end-to-end log correlation.
   *   <li><b>MANUAL_IMMEDIATE Ack Mode:</b> The consumer does NOT auto-commit offsets. Your code
   *       must call {@code ack.acknowledge()} explicitly. This is the key guard: offsets are
   *       only committed after your database write succeeds. A JVM crash before {@code ack}
   *       means the message is redelivered — no data loss possible.
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
    // Intercept each record before the listener runs to inject the correlation ID into MDC
    factory.setRecordInterceptor(correlationIdExtractor);
    // MANUAL_IMMEDIATE: we control offset commits — no silent data loss on crash
    // PRO TIP: Call ack.acknowledge() ONLY after your DB write has committed
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }

  /**
   * <b>Batch Listener Container Factory</b>
   *
   * <p><b>TUTORIAL — Batching trade-offs:</b>
   *
   * <ul>
   *   <li><b>Speed:</b> One DB transaction for N records = N× fewer round-trips. Throughput can
   *       be 10–100× higher than single-record mode.
   *   <li><b>Risk:</b> If the batch fails and retries, ALL records in the batch are redelivered —
   *       including the ones that already succeeded in the database. Idempotency checks inside the
   *       listener are therefore non-optional.
   *   <li><b>Error Isolation:</b> Spring Kafka's error handler will split a consistently-failing
   *       batch into single records and route each failed record to the DLT individually.
   * </ul>
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, Object> batchKafkaListenerContainerFactory(
      ConsumerFactory<String, Object> consumerFactory, DefaultErrorHandler errorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, Object> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setCommonErrorHandler(errorHandler);
    // Enable batch mode — listener receives List<ConsumerRecord> instead of a single record
    factory.setBatchListener(true);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }

  // ─── Request-Reply Pattern ────────────────────────────────────────────────────────────────────

  /**
   * <b>Request-Reply Support — Kafka as an RPC bus</b>
   *
   * <p><b>TUTORIAL:</b> The {@link ReplyingKafkaTemplate} implements the "Request-Reply"
   * messaging pattern: the caller publishes a request record to a topic and then blocks (within a
   * timeout) waiting for a reply to appear on a dedicated reply topic.
   *
   * <p><b>PRO TIP:</b> This is useful for "Confirm Before Proceed" flows (e.g., payment
   * authorization) but comes with a major cost: it couples the caller to a specific responder and
   * limits horizontal scalability. Avoid it in hot paths.
   */
  @Bean
  public ProducerFactory<String, TransactionEvent> replyProducerFactory() {
    // Request-reply producers do not need transactions — they are fire-and-forget
    return new DefaultKafkaProducerFactory<>(commonProducerProps());
  }

  /** Consumer factory for reading replies — replies are plain strings in this PoC. */
  @Bean
  public ConsumerFactory<String, String> replyConsumerFactory() {
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    // Dedicated consumer group for the replying template so it doesn't interfere with
    // the main consumer groups
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "reply-consumer-group");
    return new DefaultKafkaConsumerFactory<>(props);
  }

  /** Container factory for the reply listener. Registered internally by the template. */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> replyListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(replyConsumerFactory());
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }

  /** Assembles the full ReplyingKafkaTemplate by linking the producer and reply listener. */
  @Bean
  public ReplyingKafkaTemplate<String, TransactionEvent, String> replyingKafkaTemplate() {
    // Create a listener container specifically for the reply topic
    var replyContainer = replyListenerContainerFactory().createContainer("transaction-reply-topic");
    replyContainer.getContainerProperties().setGroupId("replying-template-group");
    return new ReplyingKafkaTemplate<>(replyProducerFactory(), replyContainer);
  }

  // ─── Topic Declarations ───────────────────────────────────────────────────────────────────────

  /**
   * Declarative topic creation.
   *
   * <p><b>TUTORIAL:</b> Spring Boot auto-configures a {@code KafkaAdmin} bean that uses these
   * {@code NewTopic} bean definitions to create topics at application startup via the Kafka Admin
   * API. The operation is idempotent — if the topic already exists with the same configuration,
   * nothing happens. This is a clean alternative to manual {@code kafka-topics.sh} scripts.
   *
   * <p><b>PRO TIP — Partition counts:</b> Partitions are permanent. You can only increase, never
   * decrease. Start with a count that matches your expected consumer parallelism (e.g., number of
   * pods × threads per pod).
   */
  @Bean
  public NewTopic rawTransactionsTopic() {
    return TopicBuilder.name(TopicConstants.RAW_TRANSACTIONS)
        .partitions(appProperties.getKafka().getPartitions().getPipeline())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  @Bean
  public NewTopic processedTransactionsTopic() {
    return TopicBuilder.name(TopicConstants.PROCESSED_TRANSACTIONS)
        .partitions(appProperties.getKafka().getPartitions().getPipeline())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  @Bean
  public NewTopic dailyAccountMetricsTopic() {
    return TopicBuilder.name(TopicConstants.DAILY_ACCOUNT_METRICS)
        .partitions(appProperties.getKafka().getPartitions().getMetrics())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  /** DLT must be created explicitly — it won't auto-exist just because we route to it. */
  @Bean
  public NewTopic rawTransactionsDlt() {
    return TopicBuilder.name(TopicConstants.RAW_TRANSACTIONS_DLT)
        .partitions(appProperties.getKafka().getPartitions().getPipeline())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  @Bean
  public NewTopic highValueTransactionsTopic() {
    return TopicBuilder.name(TopicConstants.HIGH_VALUE_TRANSACTIONS)
        .partitions(appProperties.getKafka().getPartitions().getPipeline())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  @Bean
  public NewTopic normalTransactionsTopic() {
    return TopicBuilder.name(TopicConstants.NORMAL_TRANSACTIONS)
        .partitions(appProperties.getKafka().getPartitions().getPipeline())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  @Bean
  public NewTopic allTransactionsAuditTopic() {
    return TopicBuilder.name(TopicConstants.ALL_TRANSACTIONS_AUDIT)
        .partitions(appProperties.getKafka().getPartitions().getPipeline())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  @Bean
  public NewTopic accountBalancesTopic() {
    return TopicBuilder.name(TopicConstants.ACCOUNT_BALANCES)
        .partitions(appProperties.getKafka().getPartitions().getPipeline())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  @Bean
  public NewTopic hourlyAccountMetricsTopic() {
    return TopicBuilder.name(TopicConstants.HOURLY_ACCOUNT_METRICS)
        .partitions(appProperties.getKafka().getPartitions().getMetrics())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  @Bean
  public NewTopic sessionActivityTopic() {
    return TopicBuilder.name(TopicConstants.SESSION_ACTIVITY)
        .partitions(appProperties.getKafka().getPartitions().getMetrics())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  /**
   * Fraud signals and processed-transactions MUST have the same partition count.
   *
   * <p><b>TUTORIAL:</b> Kafka Streams KStream-KStream joins require both input topics to be
   * "co-partitioned" — same number of partitions AND same key-partitioning logic. If partition
   * counts differ, the join produces incorrect results because related keys land in different
   * partitions.
   */
  @Bean
  public NewTopic fraudSignalsTopic() {
    return TopicBuilder.name(TopicConstants.FRAUD_SIGNALS)
        .partitions(appProperties.getKafka().getPartitions().getPipeline())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  @Bean
  public NewTopic fraudAlertsTopic() {
    return TopicBuilder.name(TopicConstants.FRAUD_ALERTS)
        .partitions(appProperties.getKafka().getPartitions().getPipeline())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  @Bean
  public NewTopic auditThresholdEventsTopic() {
    return TopicBuilder.name(TopicConstants.AUDIT_THRESHOLD_EVENTS)
        .partitions(appProperties.getKafka().getPartitions().getPipeline())
        .replicas(appProperties.getKafka().getReplicationFactor())
        .build();
  }

  // ─── Error Handler ────────────────────────────────────────────────────────────────────────────

  /**
   * <b>Retry → DLQ Error Handler Strategy</b>
   *
   * <p><b>TUTORIAL:</b> This bean is the backbone of our consumer-tier resilience. It wires three
   * concepts together:
   *
   * <p><b>1. Exponential Backoff (Retry Strategy)</b><br>
   * On any exception, retries with increasing delays: {@code 2s → 4s → 8s → 16s → 20s (capped)}
   * within a 60-second total window. This gives transient dependencies (DB restarts, network
   * blips) time to recover without crashing the consumer or causing a partition rebalance.
   *
   * <p><b>2. Dead Letter Topic (Exhausted Retry Strategy)</b><br>
   * When retries run out, {@link DeadLetterPublishingRecoverer} routes the failed record to
   * {@code <original-topic>.DLT} on the <em>same partition</em>. Same partition is crucial:
   * it preserves ordering context for forensic replay. Example:<br>
   * {@code raw-transactions → raw-transactions.DLT}
   *
   * <p><b>3. Non-Retryable Exclusions</b><br>
   * Some errors are permanent (e.g., deserialization errors — no amount of retrying will fix a
   * corrupt record). These are excluded in {@code @RetryableTopic.exclude} at the listener level,
   * bypassing backoff entirely and going straight to the DLT.
   *
   * <p><b>WARNING — Rebalance Risk:</b><br>
   * If the total backoff window ({@code maxElapsedTime} = 60s here) exceeds
   * {@code max.poll.interval.ms}, the broker will consider the consumer dead and trigger a
   * rebalance mid-retry. Always ensure {@code max.poll.interval.ms > maxElapsedTime}.
   * See {@code application.yml} for the corresponding consumer timeout settings.
   */
  @Bean
  public DefaultErrorHandler errorHandler(KafkaTemplate<String, TransactionEvent> template) {

    // On exhaustion: publish the failed record to <topic>.DLT on the same partition
    // Same partition preserves the offset order, making forensic investigation easier
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(
            template,
            (r, e) ->
                new org.apache.kafka.common.TopicPartition(r.topic() + ".DLT", r.partition()));

    // Exponential backoff: start at 2s, double each attempt, cap at 20s per attempt
    ExponentialBackOff backOff = new ExponentialBackOff(2000L, 2.0);
    backOff.setMaxElapsedTime(60000L);  // 60s total window — covers most DB/service restarts
    backOff.setMaxInterval(20000L);     // Cap a single delay at 20s to stay within poll interval

    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

    // Log at WARN not ERROR — retries are expected transients, not emergencies.
    // Reserving ERROR for records that ultimately land in the DLT.
    handler.setLogLevel(org.springframework.kafka.KafkaException.Level.WARN);

    return handler;
  }
}
