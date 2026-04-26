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

/**
 * <b>Unit test for the Modular Kafka Streams topology using {@link TopologyTestDriver}.</b>
 *
 * <p><b>TUTORIAL: Why TopologyTestDriver?</b>
 *
 * <p>Testing Kafka Streams topologies against a real broker is slow and flaky. The {@link
 * TopologyTestDriver} is an in-process, synchronous test harness provided by the Kafka Streams
 * library. It:
 *
 * <ul>
 *   <li>Requires no broker, no Zookeeper, no network — runs in milliseconds.
 *   <li>Lets you pipe records into topics and read results synchronously.
 *   <li>Supports state stores and punctuators.
 * </ul>
 *
 * <p><b>TUTORIAL: Why {@code mock://schema-registry}?</b>
 *
 * <p>Avro Serdes normally talk to a real Schema Registry to encode/decode schemas. In tests, we use
 * the Confluent mock implementation ({@code mock://}) which stores schemas in-memory. This avoids
 * any external network dependency in unit tests.
 *
 * <p><b>What this test validates:</b>
 *
 * <ul>
 *   <li>Balance aggregation: that amounts are summed correctly per account.
 *   <li>Idempotency (Deduplication): that duplicate transaction IDs produce no extra output.
 * </ul>
 */
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
    // TUTORIAL: Build Kafka properties with mock schema registry.
    // KafkaProperties is Spring's config wrapper; we override just the schema registry URL.
    KafkaProperties kafkaProperties = new KafkaProperties();
    kafkaProperties.getProperties().put("schema.registry.url", "mock://schema-registry");
    serdeConfig = new SerdeConfig(kafkaProperties);
    meterRegistry = new SimpleMeterRegistry();

    StreamsBuilder builder = new StreamsBuilder();

    // Wire Modular Topologies manually for the test.
    // TUTORIAL: We instantiate only the modules relevant to what we are testing.
    // SessionTopology, FraudTopology and RoutingTopology are not wired here because
    // they are tested independently. This keeps test scope focused.
    SourceTopology sourceTopology = new SourceTopology(serdeConfig, meterRegistry);
    BalanceTopology balanceTopology = new BalanceTopology(serdeConfig);
    MetricsTopology metricsTopology = new MetricsTopology(serdeConfig);

    var context = sourceTopology.buildSource(builder);
    balanceTopology.build(context.getGroupedStream());
    metricsTopology.build(context.getGroupedStream());

    // TUTORIAL: StreamsConfig for the test driver.
    // APPLICATION_ID_CONFIG must be set but its value doesn't matter for unit tests.
    // BOOTSTRAP_SERVERS_CONFIG must be a non-empty placeholder; the driver never connects.
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // The mock schema registry must be in the StreamsConfig as well so that
    // internal Streams operations (e.g., changelog topics) can serialize/deserialize.
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
    // TUTORIAL: Pre-populate the GlobalKTable.
    // The GlobalKTable (ACCOUNT_REFERENCE) is used for enrichment joins. Even though we don't
    // assert on join output here, the SourceTopology references this store and it must exist.
    String accountId = "ACC-123";
    accountTableTopic.pipeInput(accountId, "Test Account");

    long baseTime = Instant.parse("2023-10-01T10:00:00Z").toEpochMilli();

    // PHASE 1: Send two unique transactions.
    // Expected: balance output for each, accumulating to 150.50.
    inputTopic.pipeInput(
        "txn-1", createEvent("txn-1", accountId, new BigDecimal("100.00"), baseTime));
    inputTopic.pipeInput(
        "txn-2", createEvent("txn-2", accountId, new BigDecimal("50.50"), baseTime + 1000));

    // PHASE 2: Send a duplicate of txn-1 (same transactionId, different Kafka key).
    // TUTORIAL: This validates the Idempotency / Deduplication logic in SourceTopology.
    // The DeduplicationProcessor stores seen transaction IDs in a RocksDB state store.
    // If the same ID arrives again (within the 24h TTL), it is dropped BEFORE the
    // aggregation step — so the balance must NOT increase.
    inputTopic.pipeInput(
        "txn-1-dup", createEvent("txn-1", accountId, new BigDecimal("100.00"), baseTime));

    // Assertions: txn-1 → 100.00, txn-2 → 150.50, txn-1-dup → (no output expected)
    assertThat(balanceTopic.readValue()).isEqualByComparingTo(new BigDecimal("100.00"));
    assertThat(balanceTopic.readValue()).isEqualByComparingTo(new BigDecimal("150.50"));

    // TUTORIAL: This is the core idempotency assertion.
    // If deduplication is broken, a third record (200.50) would appear here.
    assertThat(balanceTopic.isEmpty())
        .as("Duplicate txn-1 should be suppressed by DeduplicationProcessor")
        .isTrue();
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
