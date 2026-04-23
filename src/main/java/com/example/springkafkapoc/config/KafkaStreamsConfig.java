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
 * <b>Kafka Streams Orchestration (The Brain)</b>
 *
 * <p><b>TUTORIAL:</b> This class coordinates the entire streaming engine. It leverages {@link
 * EnableKafkaStreams} to automatically manage the lifecycle of the internal Stream threads.
 *
 * <p>Key Responsibilities:
 *
 * <ul>
 *   <li><b>EXACTLY_ONCE_V2:</b> Essential for financial systems. Ensures that if a node crashes,
 *       balances aren't double-counted upon restart.
 *   <li><b>Modular Registration:</b> Instead of one giant topology, we wire small, testable beans
 *       together (Source, Fraud, Metrics, etc.).
 *   <li><b>Resilience:</b> Configures error handlers (Deserialization/Production) to keep the
 *       engine running even if corrupt data arrives.
 * </ul>
 */
@Slf4j
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaStreamsConfig {

  private final AppProperties appProperties;
  private final KafkaProperties kafkaProperties;

  // Topology Modules
  private final com.example.springkafkapoc.streams.topology.SourceTopology sourceTopology;
  private final com.example.springkafkapoc.streams.topology.FraudTopology fraudTopology;
  private final com.example.springkafkapoc.streams.topology.BalanceTopology balanceTopology;
  private final com.example.springkafkapoc.streams.topology.MetricsTopology metricsTopology;
  private final com.example.springkafkapoc.streams.topology.SessionTopology sessionTopology;
  private final com.example.springkafkapoc.streams.topology.RoutingTopology routingTopology;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildStreamsProperties(null));

    props.put(APPLICATION_ID_CONFIG, appProperties.getKafka().getStreams().getApplicationId());
    props.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
    props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put(STATE_DIR_CONFIG, appProperties.getKafka().getStreams().getStateDir());
    props.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 10 * 1024 * 1024L);
    props.put(NUM_STREAM_THREADS_CONFIG, appProperties.getKafka().getStreams().getThreads());

    // --- Production Resilience ---
    props.put(
        DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        com.example.springkafkapoc.streams.StreamsDeserializationErrorHandler.class);
    props.put(
        DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
        org.apache.kafka.streams.errors.DefaultProductionExceptionHandler.class);

    log.info("Kafka Streams initialized (Exactly-Once & Modular Topologies Enabled)");
    return new KafkaStreamsConfiguration(props);
  }

  @Bean
  public com.example.springkafkapoc.streams.topology.SourceTopology.SourceContext sourceContext(
      StreamsBuilder builder) {
    log.info("Registering modular Source and Deduplication Topology...");
    return sourceTopology.buildSource(builder);
  }

  @Bean
  public KStream<String, TransactionEvent> analyticsTopology(
      StreamsBuilder builder,
      com.example.springkafkapoc.streams.topology.SourceTopology.SourceContext context) {
    log.info("Registering modular Analytics Topologies (Balances, Metrics, Sessions)...");

    var groupedStream = context.getGroupedStream();
    var keyedStream = context.getKeyedStream();

    // Wire individual modules
    balanceTopology.build(groupedStream);
    metricsTopology.build(groupedStream);
    sessionTopology.build(groupedStream);
    fraudTopology.build(builder, keyedStream);
    routingTopology.build(keyedStream);

    return keyedStream;
  }
}
