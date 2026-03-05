package com.example.springkafkapoc.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * <b>Unit test for the Kafka Streams topology using TopologyTestDriver.</b>
 *
 * <p>
 * <b>TUTORIAL: WHY TOPOLOGY TEST DRIVER?</b>
 * <ul>
 * <li><b>Speed:</b> It runs entirely in-memory without starting a real Kafka
 * broker or Zookeeper.
 * This makes your CI/CD pipeline lightning fast.</li>
 * <li><b>Determinism:</b> You control the wall-clock time (Virtual Time). This
 * makes testing
 * tricky windowed aggregations or suppression logic predictable and easy to
 * verify.</li>
 * <li><b>Isolation:</b> No network flakiness, no port conflicts, and no state
 * leakage between tests.</li>
 * </ul>
 */
class AnalyticsTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, TransactionEvent> inputTopic;
    private TestOutputTopic<String, BigDecimal> outputTopic;
    private TestInputTopic<String, String> accountTableTopic;
    private SpecificAvroSerde<TransactionEvent> avroSerde;

    @BeforeEach
    void setup() {
        // 1. Configure the topology
        AnalyticsTopology topology = new AnalyticsTopology();
        // TUTORIAL: Since we're not in a full Spring context, we manually inject
        // values that would normally come from application.yml
        ReflectionTestUtils.setField(topology, "schemaRegistryUrl", "mock://schema-registry");

        StreamsBuilder builder = new StreamsBuilder();
        topology.buildPipeline(builder);

        // 2. Configure the test driver
        // TUTORIAL: We use a 'dummy' bootstrap server because no network calls actually
        // happen.
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put("schema.registry.url", "mock://schema-registry");

        testDriver = new TopologyTestDriver(builder.build(), props);

        // 3. Setup input/output topics
        // TUTORIAL: We must use a Mock Schema Registry URL (mock://) to test Avro
        // without a real schema registry service running.
        Serde<String> stringSerde = Serdes.String();
        avroSerde = new SpecificAvroSerde<>();
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "mock://schema-registry");
        avroSerde.configure(serdeConfig, false);

        inputTopic = testDriver.createInputTopic(
                TopicConstants.PROCESSED_TRANSACTIONS, stringSerde.serializer(), avroSerde.serializer());

        accountTableTopic = testDriver.createInputTopic(
                TopicConstants.ACCOUNT_REFERENCE, stringSerde.serializer(), stringSerde.serializer());

        // Custom BigDecimal Serde for output
        // TUTORIAL: We match the serialization format used in the production topology
        // to ensure we can correctly deserialize the results in our test.
        Serde<BigDecimal> bigDecimalSerde = Serdes.serdeFrom(
                (topic, data) -> data == null ? null : data.toString().getBytes(StandardCharsets.UTF_8),
                (topic, data) -> data == null ? null : new BigDecimal(new String(data, StandardCharsets.UTF_8)));

        outputTopic = testDriver.createOutputTopic(
                TopicConstants.DAILY_ACCOUNT_METRICS, stringSerde.deserializer(), bigDecimalSerde.deserializer());
    }

    /**
     * TUTORIAL: Always close the TestDriver and Serdes to prevent memory leaks,
     * especially when running many tests in a large suite.
     */
    @AfterEach
    void tearDown() {
        if (avroSerde != null) {
            avroSerde.close();
        }
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void shouldAggregateDailySpendPerAccount() {
        // Given: An account exists in the GlobalKTable (Reference Data)
        String accountId = "ACC-123";
        accountTableTopic.pipeInput(accountId, "Test Account Description");

        // And: Multiple transactions occur for this account
        long baseTime = Instant.parse("2023-10-01T10:00:00Z").toEpochMilli();

        inputTopic.pipeInput(
                "transaction-1", createEvent("transaction-1", accountId, new BigDecimal("100.00"), baseTime));
        inputTopic.pipeInput(
                "transaction-2", createEvent("transaction-2", accountId, new BigDecimal("50.50"), baseTime + 1000));
        inputTopic.pipeInput(
                "transaction-3", createEvent("transaction-3", accountId, new BigDecimal("25.25"), baseTime + 2000));

        // When/Then: The topology emits an update for every input record (KTable
        // behavior)
        // We verify the cumulative aggregation: 100.00 -> 150.50 -> 175.75
        assertThat(outputTopic.readValue()).isEqualByComparingTo(new BigDecimal("100.00"));
        assertThat(outputTopic.readValue()).isEqualByComparingTo(new BigDecimal("150.50"));
        assertThat(outputTopic.readValue()).isEqualByComparingTo(new BigDecimal("175.75"));

        assertThat(outputTopic.isEmpty()).isTrue();
    }

    private TransactionEvent createEvent(String transactionId, String accountId, BigDecimal amount, long timestamp) {
        return TransactionEvent.newBuilder()
                .setTransactionId(transactionId)
                .setAccountId(accountId)
                .setAmount(amount)
                .setTimestamp(timestamp)
                .setProcessingState("PROCESSED")
                .build();
    }
}
