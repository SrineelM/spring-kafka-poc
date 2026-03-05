package com.example.springkafkapoc.streams;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * <b>Analytics & Enrichment Topology</b>
 *
 * <p>
 * <b>TUTORIAL:</b> This is the core "Intelligence" of the system, implemented
 * using <b>Kafka Streams</b>.
 * Unlike the standard Consumer API which processes records one-by-one, Kafka
 * Streams allows you to
 * define a functional "Processing Topology" (a graph of operations) that the
 * engine executes
 * with built-in scalability and fault tolerance.
 *
 * <p>
 * This class demonstrates several advanced stream-processing patterns:
 * <ul>
 * <li><b>Stream-Table Join:</b> Enriches real-time transaction events with
 * reference data (Account names).</li>
 * <li><b>Branching:</b> Dynamically routes messages to different topics based
 * on business logic (Fraud/High Value).</li>
 * <li><b>Stateful Aggregation:</b> Computes rolling totals (windows) for
 * account activity.</li>
 * <li><b>Interactive Queries:</b> Exposes the results of these computations via
 * Local State Stores,
 * which can be queried directly by our REST controllers (see
 * {@code AnalyticsQueryService}).</li>
 * </ul>
 */
@Slf4j
@Component
public class AnalyticsTopology {

    public static final String DAILY_ACCOUNT_AGGREGATES_STORE = "daily-account-aggregates-store";
    private static final BigDecimal HIGH_VALUE_THRESHOLD = new BigDecimal("10000");

    @Value("${spring.kafka.properties.schema.registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;

    /**
     * Builds the processing graph. Spring calls this automatically because
     * {@code @EnableKafkaStreams} is active in our config.
     */
    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        final SpecificAvroSerde<TransactionEvent> transactionSerde = buildAvroSerde();
        final Serde<BigDecimal> bigDecimalSerde = bigDecimalSerde();

        // 1. Consume the stream of processed transactions
        // TUTORIAL: Consumption from a topic is the entry point of the topology.
        // We specify the key/value Serdes (Serializer/Deserializers) explicitly here.
        KStream<String, TransactionEvent> transactionStream =
                builder.stream(TopicConstants.PROCESSED_TRANSACTIONS, Consumed.with(Serdes.String(), transactionSerde));

        // 2. Load Account metadata into a GlobalKTable.
        // WHY GLOBAL? A GlobalKTable is replicated to every instance of the app.
        // This allows us to join against it LOCAL-ONLY without any network shuffle.
        // In Kafka Streams, a KTable represents a "changelog stream" where only the
        // latest value for a key is kept, effectively turning the stream into a
        // database table.
        GlobalKTable<String, String> accountTable = builder.globalTable(
                TopicConstants.ACCOUNT_REFERENCE,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("account-reference-store"));

        // 3. ENRICH: Join transaction with its account description
        // TUTORIAL: A Stream-Table join is used to enrich a high-volume stream
        // with metadata from a slower-moving table. Because accountTable is a
        // GlobalKTable,
        // this join is "non-windowed" and happens instantly on the local node.
        KStream<String, TransactionEvent> enrichedStream = transactionStream.join(
                accountTable,
                (transactionKey, transactionEvent) ->
                        transactionEvent.getAccountId().toString(), // Join
                // Key
                // (maps
                // stream
                // key
                // to
                // table
                // key)
                (transactionEvent, accountDescription) -> {
                    log.debug(
                            "Enriched TXN {} for account: {}", transactionEvent.getTransactionId(), accountDescription);
                    // In a real app, you might update a field in the event here.
                    return transactionEvent;
                });

        // 4. BRANCH: Split high-value transactions for specialized monitoring
        // TUTORIAL: Branching is the modern way to replace multiple 'filter' steps.
        // It allows you to route records to different downstream handlers or topics
        // based on predicates. This is much more efficient than reading the same topic
        // multiple times with different consumers.
        enrichedStream
                .split()
                .branch(
                        (key, value) -> value.getAmount().compareTo(HIGH_VALUE_THRESHOLD) > 0,
                        Branched.withConsumer(stream ->
                                stream.to("high-value-transactions", Produced.with(Serdes.String(), transactionSerde))))
                .defaultBranch(Branched.withConsumer(
                        stream -> stream.to("normal-transactions", Produced.with(Serdes.String(), transactionSerde))));

        // 5. AGGREGATE: Compute 24-hour spending totals
        // TUTORIAL: Stateful processing is where Kafka Streams shines.
        // Here we:
        // a) Change the key to accountId (so all transactions for one account aggregate
        // together)
        // b) Group the records.
        // c) Apply a Window (Time-based grouping).
        // d) Aggregate (Maintain a running total).
        // e) Materialize (Save the result to a persistent State Store).
        enrichedStream
                .selectKey((transactionId, event) -> event.getAccountId().toString()) // Re-key by
                // Account
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(24))) // 24h Tumbling Window
                .aggregate(
                        () -> BigDecimal.ZERO, // Initializer: starting total
                        (accountId, event, runningTotal) -> runningTotal.add(event.getAmount()), // Adder:
                        // business
                        // logic
                        Materialized.<String, BigDecimal, WindowStore<Bytes, byte[]>>as(DAILY_ACCOUNT_AGGREGATES_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(bigDecimalSerde))
                .toStream()
                .map((windowedKey, totalAmount) -> {
                    // Create a composite key containing both account and window start time
                    String outputKey =
                            windowedKey.key() + "@" + windowedKey.window().start();
                    return new KeyValue<>(outputKey, totalAmount);
                })
                .to(TopicConstants.DAILY_ACCOUNT_METRICS, Produced.with(Serdes.String(), bigDecimalSerde));

        log.info("AnalyticsTopology built successfully.");
    }

    /**
     * Helper to configure Avro Deserialization with Schema Registry.
     */
    private SpecificAvroSerde<TransactionEvent> buildAvroSerde() {
        SpecificAvroSerde<TransactionEvent> serde = new SpecificAvroSerde<>();
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);
        serde.configure(serdeConfig, false);
        return serde;
    }

    /**
     * Helper to build a Serde for BigDecimal, which isn't provided by default.
     * We serialize BigDecimals as UTF-8 strings for maximal compatibility.
     */
    private Serde<BigDecimal> bigDecimalSerde() {
        return Serdes.serdeFrom(
                (topic, data) -> data == null ? null : data.toString().getBytes(StandardCharsets.UTF_8),
                (topic, data) -> data == null ? null : new BigDecimal(new String(data, StandardCharsets.UTF_8)));
    }
}
