package com.example.springkafkapoc.config;

import static org.apache.kafka.streams.StreamsConfig.*;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

/**
 * <b>Kafka Streams Topology Configuration</b>
 *
 * <p>Initializes the Streams engine for real-time aggregation and joining.
 *
 * <p>Tutorial Tip: This class is only active because of {@link EnableKafkaStreams}. We use
 * <b>EXACTLY_ONCE_V2</b> processing to guarantee that financial totals are never double-counted,
 * even if a broker or node crashes.
 */
@Slf4j
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.properties.schema.registry.url:http://localhost:8081}")
  private String schemaRegistryUrl;

  @Value("${app.kafka.streams.threads:3}")
  private int numStreamThreads;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
    Map<String, Object> props = new HashMap<>();

    props.put(APPLICATION_ID_CONFIG, "spring-kafka-poc-streams");
    props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
    props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put("schema.registry.url", schemaRegistryUrl);
    props.put(STATE_DIR_CONFIG, "/tmp/kafka-streams-state");
    props.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 10 * 1024 * 1024L);
    props.put(NUM_STREAM_THREADS_CONFIG, numStreamThreads);

    log.info("Kafka Streams initialized (Exactly-Once Enabled)");
    return new KafkaStreamsConfiguration(props);
  }
}
