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
 * <b>Advanced Kafka Producer Patterns — Headers & Request-Reply</b>
 *
 * <p><b>TUTORIAL — When to use this service vs. DataIngestionService:</b><br>
 * {@link DataIngestionService} is the high-throughput, fire-and-forget producer for normal
 * ingestion. This service demonstrates two <em>advanced</em> producer patterns required in
 * compliance-sensitive or orchestration-heavy systems:
 *
 * <p><b>Pattern 1 — Manual Header Injection:</b><br>
 * Instead of relying on the {@link com.example.springkafkapoc.observability.KafkaCorrelationIdInterceptor}
 * to inject headers automatically, this service demonstrates building a {@link ProducerRecord}
 * manually and attaching headers directly. This is useful when you need to add headers that
 * are NOT part of your Avro schema — such as source system IDs, priority flags, or routing
 * hints that infrastructure components can read without deserializing the Avro payload.
 *
 * <p><b>WHY headers instead of schema fields?</b><br>
 * Headers are metadata — they travel alongside the payload but are not part of it. A router or
 * proxy can read headers without knowing the Avro schema. Adding these fields to the schema
 * would bloat the business contract with infrastructure concerns.
 *
 * <p><b>Pattern 2 — Request-Reply (Kafka as RPC):</b><br>
 * {@link ReplyingKafkaTemplate} implements a synchronous request-reply over Kafka: the producer
 * sends a request to a request topic and blocks until a response appears on a reply topic.
 *
 * <p><b>PRO TIP — When NOT to use Request-Reply:</b><br>
 * Request-reply couples the producer to a specific responder and blocks a thread for the entire
 * round-trip. This limits horizontal scalability and increases latency. Only use it for
 * "Confirm-Before-Proceed" flows where you genuinely cannot proceed without a synchronous answer
 * (e.g., payment authorization, fraud pre-check).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaHeadersProducerService {

  private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
  private final ReplyingKafkaTemplate<String, TransactionEvent, String> replyingKafkaTemplate;

  // ─── Header Constants ─────────────────────────────────────────────────────────────────────────

  // Standard correlation header — also added by KafkaCorrelationIdInterceptor globally
  private static final String HEADER_CORRELATION_ID = "correlationId";

  // Source system identifier — consumed by downstream services to track the request origin
  private static final String HEADER_SOURCE_SYSTEM = "X-Source-System";

  // The name of this service — used as the value of the X-Source-System header
  private static final String SOURCE_SYSTEM_NAME = "spring-kafka-poc";

  /**
   * Publishes a transaction event with manually injected Kafka headers.
   *
   * <p><b>TUTORIAL — ProducerRecord vs. KafkaTemplate.send(topic, key, value):</b><br>
   * {@code KafkaTemplate.send(topic, key, value)} is the high-level convenience API — it builds
   * the {@code ProducerRecord} for you. But it doesn't expose headers. To add headers, you must
   * build the {@link ProducerRecord} yourself and use {@code kafkaTemplate.send(record)}.
   *
   * <p><b>Headers in Avro:</b><br>
   * Headers are NOT part of the Avro schema — they exist in the Kafka transport layer. This
   * means they are NOT validated by the Schema Registry. Be careful not to rely on header values
   * for business logic; use them only for infrastructure routing and observability.
   *
   * @param amount    the transaction amount
   * @param accountId the account identifier
   * @return a future that completes with the Kafka {@link SendResult}
   */
  public CompletableFuture<SendResult<String, TransactionEvent>> sendWithHeaders(
      BigDecimal amount, String accountId) {

    // Generate a time-ordered UUID as the transaction ID (sortable, globally unique)
    String transactionId = UuidCreator.getTimeOrderedEpoch().toString();

    // Step 1: Retrieve the Correlation ID from ThreadLocal context.
    // This was set by CorrelationIdFilter when the HTTP request arrived.
    // If this call is not triggered by HTTP (e.g., a scheduled job), correlationId may be null.
    String correlationId = CorrelationIdContext.getCorrelationId();

    TransactionEvent event =
        TransactionEvent.newBuilder()
            .setTransactionId(transactionId)
            .setAccountId(accountId)
            .setAmount(amount)
            .setTimestamp(Instant.now().toEpochMilli())
            .setProcessingState("INITIAL_HEADERS") // Distinguish from normal ingestion in logs
            .build();

    // Step 2: Build the low-level ProducerRecord with explicit topic, key, and value.
    // Passing null for timestamp lets the broker assign a timestamp (LogAppendTime).
    ProducerRecord<String, TransactionEvent> record =
        new ProducerRecord<>(TopicConstants.RAW_TRANSACTIONS, null, transactionId, event);

    // Step 3: Inject the Correlation ID header if one exists.
    // Headers must be UTF-8 encoded bytes — Kafka stores header values as raw byte arrays.
    if (correlationId != null) {
      record.headers()
          .add(new RecordHeader(
              HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8)));
    }

    // Step 4: Always inject the source system header so downstream consumers know the origin.
    // Consumers can read this without deserializing the Avro payload (useful for routers/proxies).
    record.headers()
        .add(new RecordHeader(
            HEADER_SOURCE_SYSTEM, SOURCE_SYSTEM_NAME.getBytes(StandardCharsets.UTF_8)));

    log.info("Sending record with manual headers: transactionId={}", transactionId);
    return kafkaTemplate.send(record);
  }

  /**
   * Sends a transaction event and blocks until a response is received on the reply topic.
   *
   * <p><b>TUTORIAL — Request-Reply internals:</b><br>
   * {@link ReplyingKafkaTemplate} works as follows:
   * <ol>
   *   <li>Injects a {@code KafkaHeaders.REPLY_TOPIC} header into the outgoing message.
   *   <li>Injects a correlation ID header to match the reply to the request.
   *   <li>Publishes the message to the request topic.
   *   <li>The responding service consumes from the request topic, processes it, and publishes
   *       a response to the reply topic (using the REPLY_TOPIC header as the destination).
   *   <li>The template listens on the reply topic and resolves the future when the response
   *       arrives with a matching correlation ID.
   * </ol>
   *
   * <p><b>Thread blocking:</b> {@code replyFuture.get()} blocks the calling thread until the
   * reply arrives. In a reactive/async context, use {@code replyFuture.thenApply()} instead.
   *
   * @return the reply payload as a String, or throws if the reply times out
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

    // Build a Spring Message with the reply-topic header.
    // REPLY_TOPIC tells the responding service WHERE to publish its answer.
    // The responding service reads this header and produces its response to that topic.
    Message<TransactionEvent> requestMessage =
        MessageBuilder.withPayload(event)
            .setHeader(KafkaHeaders.TOPIC, TopicConstants.RAW_TRANSACTIONS)
            .setHeader(KafkaHeaders.REPLY_TOPIC, "transaction-reply-topic")
            .setHeader(HEADER_CORRELATION_ID, transactionId) // Used to match reply to request
            .build();

    log.info("Awaiting reply for transactionId={}...", transactionId);

    try {
      // Send the request and block until the reply arrives (within the template's timeout)
      RequestReplyMessageFuture<String, TransactionEvent> replyFuture =
          replyingKafkaTemplate.sendAndReceive(requestMessage);

      // .get() blocks — for async handling, use .get(timeout, unit) with explicit timeout
      Message<?> replyMessage = replyFuture.get();
      return String.valueOf(replyMessage.getPayload());

    } catch (Exception ex) {
      log.error("Timed out waiting for Kafka reply: {}", ex.getMessage());
      // Wrap and re-throw — callers should handle timeout gracefully (e.g., circuit breaker)
      throw new RuntimeException("Reply not received within timeout", ex);
    }
  }
}
