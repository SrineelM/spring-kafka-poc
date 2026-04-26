package com.example.springkafkapoc.config;

import static org.apache.kafka.streams.StreamsConfig.*;

import com.example.springkafkapoc.avro.TransactionEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

/**
 * <b>Kafka Streams Orchestration — The Engine Room</b>
 *
 * <p><b>TUTORIAL — Kafka Streams vs. @KafkaListener</b><br>
 * {@code @KafkaListener} is ideal for simple, record-by-record processing with full Spring
 * integration (transactions, retry, DLQ). Kafka Streams is a higher-level abstraction built for
 * <em>stateful</em> stream processing — aggregations, joins, windowing, and interactive queries.
 * Both are present in this project, each solving a different class of problem.
 *
 * <p><b>Responsibilities of this class:</b>
 *
 * <ul>
 *   <li><b>EXACTLY_ONCE_V2:</b> Essential for financial systems. Ensures that even if a node
 *       crashes and restarts, no balance is double-counted. The "V2" variant uses fewer broker
 *       transactions than the original, improving throughput.
 *   <li><b>Modular Topology Registration:</b> Rather than one giant topology method, we compose
 *       small, independently testable beans (Source, Fraud, Metrics, Sessions, Routing). Each can
 *       be unit-tested with {@code TopologyTestDriver} in isolation.
 *   <li><b>Poison-Pill Resilience:</b> The {@link
 *       com.example.springkafkapoc.streams.StreamsDeserializationErrorHandler} is registered here
 *       to prevent corrupt records from crashing stream threads.
 * </ul>
 *
 * <p><b>PRO TIP — State Directory in Production:</b><br>
 * Always point {@code state.dir} at a Persistent Volume in Kubernetes. Without it, every pod
 * restart triggers a "Cold Start" — the engine rebuilds all local RocksDB state from Kafka
 * changelogs, which can take minutes for large state stores and causes processing delays.
 */
@Slf4j
@Configuration
@EnableKafkaStreams  // Registers the StreamsBuilderFactoryBean and manages thread lifecycle
@RequiredArgsConstructor
public class KafkaStreamsConfig {

  private final AppProperties appProperties;
  private final KafkaProperties kafkaProperties;

  // ─── Topology Module Beans (injected from their own @Component classes) ───────────────────────

  // SourceTopology: validates, deduplicates, and re-keys the input stream
  private final com.example.springkafkapoc.streams.topology.SourceTopology sourceTopology;

  // FraudTopology: performs a temporal KStream-KStream join against the fraud-signals stream
  private final com.example.springkafkapoc.streams.topology.FraudTopology fraudTopology;

  // BalanceTopology: maintains real-time running account balances via KTable aggregation
  private final com.example.springkafkapoc.streams.topology.BalanceTopology balanceTopology;

  // MetricsTopology: computes 24h tumbling and 1h hopping window totals
  private final com.example.springkafkapoc.streams.topology.MetricsTopology metricsTopology;

  // SessionTopology: tracks user activity sessions with dynamic, inactivity-based windows
  private final com.example.springkafkapoc.streams.topology.SessionTopology sessionTopology;

  // RoutingTopology: branches the stream into high-value and standard sub-topics
  private final com.example.springkafkapoc.streams.topology.RoutingTopology routingTopology;

  /**
   * Declares the global Kafka Streams configuration.
   *
   * <p><b>TUTORIAL:</b> The bean name <em>must</em> match {@link
   * KafkaStreamsDefaultConfiguration#DEFAULT_STREAMS_CONFIG_BEAN_NAME} so that Spring picks it up
   * automatically. Any other name will cause Spring to use its auto-configured defaults instead.
   */
  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildStreamsProperties(null));

    // Unique identifier for this streaming application across the Kafka cluster.
    // All instances sharing the same ID form a single consumer group for load balancing.
    props.put(APPLICATION_ID_CONFIG, appProperties.getKafka().getStreams().getApplicationId());

    // Exactly-Once Semantics V2: the strongest processing guarantee.
    // Each input record is processed exactly once even across restarts.
    props.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);

    // Default SerDes used when not overridden at the individual KStream/KTable level
    props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

    // Local RocksDB state directory — mount a PersistentVolume here in Kubernetes
    props.put(STATE_DIR_CONFIG, appProperties.getKafka().getStreams().getStateDir());

    // In-memory write buffer for state stores before flushing to RocksDB (10 MB here)
    // Larger = fewer disk I/Os but higher memory use
    props.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 10 * 1024 * 1024L);

    // Number of parallel stream threads within this single JVM instance
    // PRO TIP: Set this equal to the partition count for maximum parallelism
    props.put(NUM_STREAM_THREADS_CONFIG, appProperties.getKafka().getStreams().getThreads());

    // ─── Production Resilience ──────────────────────────────────────────────────────────────────

    // Guards against "Poison Pills" (corrupt bytes that can't be deserialized).
    // Without this, one bad record crashes the stream thread indefinitely.
    props.put(
        DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        com.example.springkafkapoc.streams.StreamsDeserializationErrorHandler.class);

    // Default production exception handler: logs and continues if publishing a record fails.
    // In production, override this to route failed output records to a dedicated error topic.
    props.put(
        DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
        org.apache.kafka.streams.errors.DefaultProductionExceptionHandler.class);

    log.info("Kafka Streams initialized (Exactly-Once V2 & Modular Topologies Enabled)");
    return new KafkaStreamsConfiguration(props);
  }

  /**
   * Builds and registers the Source Topology as a Spring bean.
   *
   * <p><b>TUTORIAL:</b> The {@link StreamsBuilder} is the entry point for defining topology
   * operations. It's auto-injected by {@code @EnableKafkaStreams}. We pass it to our modular
   * topology classes, which append their processing steps to the shared builder.
   *
   * <p>The {@link com.example.springkafkapoc.streams.topology.SourceTopology.SourceContext}
   * returned here contains both the keyed and grouped streams, allowing downstream topologies
   * to share them without re-reading from Kafka (which would be wasteful).
   */
  @Bean
  public com.example.springkafkapoc.streams.topology.SourceTopology.SourceContext sourceContext(
      StreamsBuilder builder) {
    log.info("Registering Source + Deduplication Topology...");
    return sourceTopology.buildSource(builder);
  }

  /**
   * Wires all analytical sub-topologies together using the shared source context.
   *
   * <p><b>TUTORIAL — Why one shared context?</b><br>
   * The {@code SourceContext} holds a single {@code KGroupedStream} that all aggregation topologies
   * consume. Kafka Streams is smart enough to share the underlying topic reading — no re-partitions
   * or duplicate consumption occurs. This is called "branching from a single source node."
   *
   * @return the keyed transaction stream (useful for downstream interactive queries)
   */
  @Bean
  public KStream<String, TransactionEvent> analyticsTopology(
      StreamsBuilder builder,
      com.example.springkafkapoc.streams.topology.SourceTopology.SourceContext context) {
    log.info("Registering Analytics Topologies (Balances, Metrics, Sessions, Fraud, Routing)...");

    var groupedStream = context.getGroupedStream(); // used by aggregation topologies
    var keyedStream = context.getKeyedStream();     // used by join and routing topologies

    // Each topology appends its own processing steps to the shared StreamsBuilder
    balanceTopology.build(groupedStream);  // real-time balance KTable
    metricsTopology.build(groupedStream);  // 24h tumbling + 1h hopping windows
    sessionTopology.build(groupedStream);  // user-activity session windows
    fraudTopology.build(builder, keyedStream);  // stream-stream temporal join
    routingTopology.build(keyedStream);    // branch into high-value vs. standard topics

    // Return the keyed stream so it can be used by other beans (e.g., Interactive Queries)
    return keyedStream;
  }
}
