package com.example.springkafkapoc.service;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.observability.CorrelationIdContext;
import com.github.f4b6a3.uuid.UuidCreator;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyMessageFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

/**
 * <b>Advanced Kafka Producer Patterns</b>
 *
 * <p><b>TUTORIAL:</b> While the {@link DataIngestionService} handles the simple data "push", this
 * service demonstrates the <b>Rich Messaging Patterns</b> required in high-compliance or banking
 * environments.
 *
 * <p><b>Patterns Demonstrated:</b>
 *
 * <ul>
 *   <li><b>Manual Header Injection:</b> Using {@code ProducerRecord} to add metadata (Trace IDs,
 *       Source System names) <i>outside</i> the core Avro data schema. This allows infrastructure
 *       to read metadata without deserializing the entire business payload.
 *   <li><b>Request-Reply (RPC):</b> Implementing a blocking-wait for an asynchronous response. One
 *       service sends a "Request" message, and wait on a "Reply" topic for a "Response" from
 *       another service.
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaHeadersProducerService {

  private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
  private final ReplyingKafkaTemplate<String, TransactionEvent, String> replyingKafkaTemplate;

  private static final String HEADER_CORRELATION_ID = "correlationId";
  private static final String HEADER_SOURCE_SYSTEM = "X-Source-System";
  private static final String SOURCE_SYSTEM_NAME = "spring-kafka-poc";
  private static final int REPLY_TIMEOUT_SECONDS = 15;

  /**
   * Publishes a transaction event with manual header injection.
   *
   * <p>Tutorial Tip: We use <b>ProducerRecord</b> for fine-grained control. Headers allow you to
   * pass routing or trace data <i>without</i> modifying your Avro data schema.
   */
  public CompletableFuture<SendResult<String, TransactionEvent>> sendWithHeaders(
      BigDecimal amount, String accountId) {

    String transactionId = UuidCreator.getTimeOrderedEpoch().toString();

    // 1. Pull the ID from our ThreadLocal context (established by the HTTP Filter)
    String correlationId = CorrelationIdContext.getCorrelationId();

    TransactionEvent event =
        TransactionEvent.newBuilder()
            .setTransactionId(transactionId)
            .setAccountId(accountId)
            .setAmount(amount)
            .setTimestamp(Instant.now().toEpochMilli())
            .setProcessingState("INITIAL_HEADERS")
            .build();

    // 2. Create the low-level Record
    ProducerRecord<String, TransactionEvent> record =
        new ProducerRecord<>(TopicConstants.RAW_TRANSACTIONS, null, transactionId, event);

    // 3. Manually add headers
    if (correlationId != null) {
      record
          .headers()
          .add(
              new RecordHeader(
                  HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8)));
    }
    record
        .headers()
        .add(
            new RecordHeader(
                HEADER_SOURCE_SYSTEM, SOURCE_SYSTEM_NAME.getBytes(StandardCharsets.UTF_8)));

    log.info("Sending record with headers for TXN: {}", transactionId);
    return kafkaTemplate.send(record);
  }

  /**
   * Demonstrates the <b>Request-Reply (RPC)</b> pattern. The method blocks until a response is
   * received on a reply topic.
   *
   * <p>Tutorial Tip: DO NOT use this for high-throughput flows. It couples the producer to a
   * specific consumer response, which limits your horizontal scalability. It's best for
   * "Confirm-before-Proceed" loops.
   */
  public String sendAndAwaitReply(BigDecimal amount, String accountId) {
    String transactionId = UuidCreator.getTimeOrderedEpoch().toString();

    TransactionEvent event =
        TransactionEvent.newBuilder()
            .setTransactionId(transactionId)
            .setAccountId(accountId)
            .setAmount(amount)
            .setTimestamp(Instant.now().toEpochMilli())
            .setProcessingState("REQ_REPLY_AUTH")
            .build();

    // 1. Build a Spring Message with headers for the Replying template
    Message<TransactionEvent> requestMessage =
        MessageBuilder.withPayload(event)
            .setHeader(KafkaHeaders.TOPIC, TopicConstants.RAW_TRANSACTIONS)
            .setHeader(KafkaHeaders.REPLY_TOPIC, "transaction-reply-topic")
            .setHeader(HEADER_CORRELATION_ID, transactionId)
            .build();

    log.info("Awaiting reply for TXN: {}...", transactionId);

    try {
      // 2. Send and block (wait) for the reply
      RequestReplyMessageFuture<String, TransactionEvent> replyFuture =
          replyingKafkaTemplate.sendAndReceive(requestMessage);

      Message<?> replyMessage = replyFuture.get();
      return String.valueOf(replyMessage.getPayload());
    } catch (Exception ex) {
      log.error("Timed out waiting for Kafka reply: {}", ex.getMessage());
      throw new RuntimeException("Reply not received within timeout", ex);
    }
  }
}
