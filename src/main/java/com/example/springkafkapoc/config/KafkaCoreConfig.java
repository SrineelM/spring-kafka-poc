package com.example.springkafkapoc.config;

/**
 * <b>⚠ This class has been decomposed into focused, single-responsibility configs.</b>
 *
 * <p>Each concern now lives in its own configuration class for clarity and maintainability:
 *
 * <ul>
 *   <li>{@link KafkaProducerConfig} — Producer factories, {@code KafkaTemplate} beans, and the
 *       shared {@code commonProducerProps()} builder.
 *   <li>{@link KafkaConsumerConfig} — {@code ConsumerFactory}, single-record listener container
 *       factory, and batch listener container factory.
 *   <li>{@link KafkaErrorHandlerConfig} — {@code DefaultErrorHandler} with exponential-backoff
 *       retry and Dead Letter Topic routing.
 *   <li>{@link KafkaRequestReplyConfig} — {@code ReplyingKafkaTemplate} and its private reply
 *       consumer/producer factories.
 *   <li>{@link KafkaTopicConfig} — All {@code NewTopic} bean declarations.
 *   <li>{@link CooperativeStickyConsumerConfig} — Separate consumer factory and container factory
 *       for Cooperative Sticky partition assignment (KIP-429).
 * </ul>
 *
 * <p>This file is intentionally empty. It is kept as a navigation breadcrumb so that IDE
 * "Find Usages" of the old class name still leads developers to this index.
 */
public final class KafkaCoreConfig {

  private KafkaCoreConfig() {
    // Navigation class only — do not instantiate.
    // All beans have moved to the classes listed in the Javadoc above.
  }
}
