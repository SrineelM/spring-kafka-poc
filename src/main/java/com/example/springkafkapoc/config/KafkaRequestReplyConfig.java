package com.example.springkafkapoc.config;

import com.example.springkafkapoc.avro.TransactionEvent;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

/**
 * <b>Kafka Request-Reply Configuration — Synchronous RPC over Kafka</b>
 *
 * <p><b>TUTORIAL — The Request-Reply Pattern:</b><br>
 * {@link ReplyingKafkaTemplate} implements synchronous-style Kafka RPC:
 *
 * <ol>
 *   <li>Caller publishes a request record to an input topic.
 *   <li>A responder service processes the request and publishes a reply to a designated reply
 *       topic.
 *   <li>Caller blocks (within a configurable timeout) waiting for the reply.
 * </ol>
 *
 * <p><b>When to use it:</b><br>
 * Useful for "Confirm Before Proceed" flows (e.g., payment authorization, fraud pre-check).
 * However, it couples the caller to a specific responder and limits horizontal scalability —
 * <em>avoid in hot paths</em>. Prefer async event-driven designs for high-throughput scenarios.
 *
 * <p><b>Why a separate reply consumer factory?</b><br>
 * Replies in this PoC are plain strings (not Avro). Using the main Avro {@link ConsumerFactory}
 * for the reply listener would attempt to deserialize plain strings through the Avro schema
 * registry, causing a {@code SerializationException}. The reply factory uses {@code
 * StringDeserializer} for both key and value, kept isolated here so the main consumer factory
 * remains unchanged.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaRequestReplyConfig {

  private final KafkaProperties kafkaProperties;
  private final KafkaProducerConfig producerConfig;

  /**
   * The {@link ReplyingKafkaTemplate} ties together:
   *
   * <ul>
   *   <li>A producer factory (for sending requests) — reuses the shared producer properties from
   *       {@link KafkaProducerConfig#commonProducerProps()} to stay consistent with idempotency
   *       and interceptor settings.
   *   <li>A reply listener container — a dedicated listener that watches the reply topic and
   *       routes incoming replies back to the awaiting caller thread.
   * </ul>
   */
  @Bean
  public ReplyingKafkaTemplate<String, TransactionEvent, String> replyingKafkaTemplate() {
    log.debug(
        "Creating ReplyingKafkaTemplate: replyTopic=transaction-reply-topic,"
            + " replyGroup=replying-template-group");

    var replyContainer =
        replyListenerContainerFactory().createContainer("transaction-reply-topic");
    replyContainer.getContainerProperties().setGroupId("replying-template-group");

    return new ReplyingKafkaTemplate<>(replyProducerFactory(), replyContainer);
  }

  // ─── Private helpers — only used internally by replyingKafkaTemplate() ───────────────────────

  /**
   * Reply producer factory — reuses the same common properties (idempotence, acks=all, interceptor)
   * but has no transaction-id prefix because request-reply producers are fire-and-forget.
   */
  private ProducerFactory<String, TransactionEvent> replyProducerFactory() {
    log.debug("Creating reply producer factory (no transactions)");
    return new DefaultKafkaProducerFactory<>(producerConfig.commonProducerProps());
  }

  /** Reply consumer factory — uses {@code StringDeserializer} for plain-string replies. */
  private ConsumerFactory<String, String> replyConsumerFactory() {
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    // Dedicated consumer group — must not interfere with the main consumer groups
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "reply-consumer-group");

    log.debug("Creating reply consumer factory: group=reply-consumer-group");
    return new DefaultKafkaConsumerFactory<>(props);
  }

  /** Container factory for the reply listener — registered internally by the template. */
  private ConcurrentKafkaListenerContainerFactory<String, String> replyListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(replyConsumerFactory());
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }
}
