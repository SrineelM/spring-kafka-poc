package com.example.springkafkapoc.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.streams.topology.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/** <b>Unit test for the Modular Kafka Streams topology using TopologyTestDriver.</b> */
class AnalyticsTopologyTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, TransactionEvent> inputTopic;
  private TestOutputTopic<String, BigDecimal> balanceTopic;
  private TestInputTopic<String, String> accountTableTopic;
  private SpecificAvroSerde<TransactionEvent> avroSerde;
  private SerdeConfig serdeConfig;
  private MeterRegistry meterRegistry;

  @BeforeEach
  void setup() {
    KafkaProperties kafkaProperties = new KafkaProperties();
    kafkaProperties.getProperties().put("schema.registry.url", "mock://schema-registry");
    serdeConfig = new SerdeConfig(kafkaProperties);
    meterRegistry = new SimpleMeterRegistry();

    StreamsBuilder builder = new StreamsBuilder();

    // Wire Modular Topologies manually for the test
    SourceTopology sourceTopology = new SourceTopology(serdeConfig, meterRegistry);
    BalanceTopology balanceTopology = new BalanceTopology(serdeConfig);
    MetricsTopology metricsTopology = new MetricsTopology(serdeConfig);

    var context = sourceTopology.buildSource(builder);
    balanceTopology.build(context.getGroupedStream());
    metricsTopology.build(context.getGroupedStream());

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put("schema.registry.url", "mock://schema-registry");

    testDriver = new TopologyTestDriver(builder.build(), props);

    Serde<String> stringSerde = Serdes.String();
    avroSerde = serdeConfig.transactionEventSerde();

    inputTopic =
        testDriver.createInputTopic(
            TopicConstants.PROCESSED_TRANSACTIONS,
            stringSerde.serializer(),
            avroSerde.serializer());

    accountTableTopic =
        testDriver.createInputTopic(
            TopicConstants.ACCOUNT_REFERENCE, stringSerde.serializer(), stringSerde.serializer());

    Serde<BigDecimal> optimizedDecimalSerde = serdeConfig.optimizedBigDecimalSerde();

    balanceTopic =
        testDriver.createOutputTopic(
            TopicConstants.ACCOUNT_BALANCES,
            stringSerde.deserializer(),
            optimizedDecimalSerde.deserializer());
  }

  @AfterEach
  void tearDown() {
    if (avroSerde != null) avroSerde.close();
    if (testDriver != null) testDriver.close();
  }

  @Test
  void shouldAggregateBalancesPerAccount() {
    String accountId = "ACC-123";
    accountTableTopic.pipeInput(accountId, "Test Account");

    long baseTime = Instant.parse("2023-10-01T10:00:00Z").toEpochMilli();

    inputTopic.pipeInput(
        "txn-1", createEvent("txn-1", accountId, new BigDecimal("100.00"), baseTime));
    inputTopic.pipeInput(
        "txn-2", createEvent("txn-2", accountId, new BigDecimal("50.50"), baseTime + 1000));

    // Deduplication check: Send txn-1 again
    inputTopic.pipeInput(
        "txn-1-dup", createEvent("txn-1", accountId, new BigDecimal("100.00"), baseTime));

    assertThat(balanceTopic.readValue()).isEqualByComparingTo(new BigDecimal("100.00"));
    assertThat(balanceTopic.readValue()).isEqualByComparingTo(new BigDecimal("150.50"));

    // Since txn-1 was a duplicate, no third message should be in balanceTopic
    assertThat(balanceTopic.isEmpty()).isTrue();
  }

  private TransactionEvent createEvent(
      String transactionId, String accountId, BigDecimal amount, long timestamp) {
    return TransactionEvent.newBuilder()
        .setTransactionId(transactionId)
        .setAccountId(accountId)
        .setAmount(amount)
        .setTimestamp(timestamp)
        .setProcessingState("PROCESSED")
        .build();
  }
}
