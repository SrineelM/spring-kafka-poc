package com.example.springkafkapoc;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.service.DataIngestionService;
import java.math.BigDecimal;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * <b>End-to-End Pipeline Integration Test</b>
 *
 * <p><b>TUTORIAL: INTEGRATION TESTING PATTERNS</b>
 *
 * <ul>
 *   <li><b>@EmbeddedKafka:</b> Spins up a real, in-memory Kafka broker (and Zookeeper) locally on
 *       your machine during the test. No external infra required!
 *   <li><b>@SpringBootTest:</b> Loads the full Application Context, allowing us to test the
 *       interaction between all beans (Service -> Producer -> Kafka).
 *   <li><b>@DirtiesContext:</b> Ensures a fresh Spring context for this class, preventing
 *       side-effects from other tests that might have modified shared beans.
 * </ul>
 */
@SpringBootTest
@ActiveProfiles("local")
@EmbeddedKafka(
    partitions = 3,
    topics = {
      TopicConstants.RAW_TRANSACTIONS,
      TopicConstants.PROCESSED_TRANSACTIONS,
      TopicConstants.DAILY_ACCOUNT_METRICS,
      TopicConstants.HOURLY_ACCOUNT_METRICS,
      TopicConstants.ACCOUNT_REFERENCE,
      TopicConstants.ACCOUNT_BALANCES,
      TopicConstants.FRAUD_ALERTS,
      TopicConstants.SESSION_ACTIVITY,
      TopicConstants.HIGH_VALUE_TRANSACTIONS,
      TopicConstants.NORMAL_TRANSACTIONS,
      TopicConstants.ALL_TRANSACTIONS_AUDIT,
      TopicConstants.RAW_TRANSACTIONS_DLT,
      "transaction-reply-topic"
    },
    brokerProperties = {
      "transaction.state.log.replication.factor=1",
      "transaction.state.log.min.isr=1",
      "offsets.topic.replication.factor=1",
      "schema.registry.url=mock://integration-test-registry"
    })
@DirtiesContext
class KafkaPipelineIntegrationTest {

  /**
   * TUTORIAL: Testing asynchronous systems like Kafka is hard! We use a {@link BlockingQueue} to
   * "catch" messages as they arrive in our test listener. This allows our main test thread to wait
   * (block) until the message is received or a timeout occurs.
   */
  private static final LinkedBlockingQueue<ConsumerRecord<String, Object>> RECEIVED =
      new LinkedBlockingQueue<>();

  @org.junit.jupiter.api.BeforeEach
  void resetQueue() {
    RECEIVED.clear();
  }

  @Autowired private DataIngestionService dataIngestionService;

  /**
   * TUTORIAL: This is a dedicated "Test Consumer". It listens to the topic we are testing and pipes
   * every message into our BlockingQueue for verification.
   */
  @KafkaListener(
      topics = TopicConstants.RAW_TRANSACTIONS,
      groupId = "integration-test-group",
      containerFactory = "kafkaListenerContainerFactory")
  void testListener(ConsumerRecord<String, Object> record) {
    RECEIVED.add(record);
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void ingestTransaction_shouldPublishToRawTopic() throws Exception {
    // Given: A valid transaction request
    String accountId = "test-account-001";
    BigDecimal amount = BigDecimal.valueOf(500.0);

    // When: We call the ingestion service (which triggers the Kafka Producer)
    // .get() waits for the Producer's acknowledgement
    dataIngestionService.ingestTransaction(amount, accountId).get();

    // Then: We wait up to 5 seconds for the message to propagate through the
    // embedded broker
    // and hit our test listener.
    ConsumerRecord<String, Object> record = RECEIVED.poll(5, TimeUnit.SECONDS);

    assertThat(record).as("A record should be published to the raw-transactions topic").isNotNull();

    assertThat(record.value())
        .as("Record value should be a TransactionEvent Avro object")
        .isInstanceOf(TransactionEvent.class);

    TransactionEvent event = (TransactionEvent) record.value();
    assertThat(event.getAccountId().toString()).isEqualTo(accountId);

    // TUTORIAL: Even if the Avro schema uses logical types, we verify against
    // our original BigDecimal to ensure precision wasn't lost.
    assertThat(event.getAmount()).isEqualByComparingTo(amount);
    assertThat(event.getProcessingState().toString()).isEqualTo("INITIAL");
  }
}
