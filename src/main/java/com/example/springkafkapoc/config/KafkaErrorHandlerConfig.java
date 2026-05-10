package com.example.springkafkapoc.config;

import com.example.springkafkapoc.avro.TransactionEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * <b>Kafka Error Handler Configuration — Retry & Dead Letter Strategy</b>
 *
 * <p>This class owns the retry-and-DLT pipeline that is wired into every listener container
 * factory. Extracting it here means the retry policy is declared once and applied uniformly —
 * changing the backoff parameters here automatically affects both single-record and batch consumers.
 *
 * <p><b>TUTORIAL — Three layers of resilience wired together:</b>
 *
 * <ol>
 *   <li><b>Exponential Backoff (Retry):</b><br>
 *       On any exception, the error handler retries the same record with increasing delays:
 *       {@code 2s → 4s → 8s → 16s → 20s (capped)}, within a 60-second total window. This gives
 *       transient dependencies (DB restarts, network blips) time to recover without crashing the
 *       consumer or triggering a partition rebalance.
 *   <li><b>Dead Letter Topic (Exhausted Retries):</b><br>
 *       When retries are exhausted, {@link DeadLetterPublishingRecoverer} publishes the failed
 *       record to {@code <original-topic>.DLT} on the <em>same partition</em>. Routing to the
 *       same partition preserves offset ordering, making forensic investigation simpler:
 *       {@code raw-transactions → raw-transactions.DLT}
 *   <li><b>WARN-Level Logging for Retries:</b><br>
 *       Retries are transient events — logging them at ERROR would flood alerting systems.
 *       ERROR is reserved for records that ultimately land in the DLT after all retries fail.
 * </ol>
 *
 * <p><b>CRITICAL — Rebalance Risk:</b><br>
 * If the total backoff window ({@code maxElapsedTime} = 60s) exceeds {@code
 * max.poll.interval.ms}, the broker considers the consumer dead and triggers a rebalance
 * mid-retry. Always ensure {@code max.poll.interval.ms > maxElapsedTime}. The consumer
 * properties in {@link KafkaConsumerConfig} set {@code max.poll.interval.ms = 300000ms} (5 min),
 * which is well above the 60s backoff window.
 */
@Slf4j
@Configuration
public class KafkaErrorHandlerConfig {

  /**
   * Wires together the exponential-backoff retry strategy and the DLT recoverer into a single
   * {@link DefaultErrorHandler} that is shared by all listener container factories.
   *
   * @param kafkaTemplate the primary template — used internally by the DLT recoverer to publish
   *     failed records to the dead letter topic
   */
  @Bean
  public DefaultErrorHandler errorHandler(KafkaTemplate<String, TransactionEvent> kafkaTemplate) {

    // ── Dead Letter Recoverer ─────────────────────────────────────────────────────────────────────
    // Routes the failed record to <topic>.DLT on the same partition as the original.
    // Same partition is crucial: it preserves ordering context for forensic replay.
    // PRO TIP: If you need multiple DLT consumers or a central error bus, replace the
    //          destination function with a fixed topic name instead.
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            (record, ex) ->
                new TopicPartition(record.topic() + ".DLT", record.partition()));

    log.debug(
        "DLT recoverer configured: destination pattern=<topic>.DLT, same partition routing");

    // ── Exponential Backoff ───────────────────────────────────────────────────────────────────────
    // initialInterval=2000ms: first retry waits 2 seconds
    // multiplier=2.0: each subsequent delay doubles (2s → 4s → 8s → 16s → 20s)
    // maxInterval=20000ms: single delay capped at 20s (prevents 30-min waits)
    // maxElapsedTime=60000ms: abandon retries after 60s total (prevent max.poll.interval breach)
    ExponentialBackOff backOff = new ExponentialBackOff(2_000L, 2.0);
    backOff.setMaxElapsedTime(60_000L);
    backOff.setMaxInterval(20_000L);

    log.debug(
        "Exponential backoff configured: initial=2s, multiplier=2.0,"
            + " maxInterval=20s, maxElapsedTime=60s");

    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

    // Log retries at WARN, not ERROR — retries are expected transients.
    // Reserving ERROR for records that end up in the DLT after all retries fail.
    handler.setLogLevel(org.springframework.kafka.KafkaException.Level.WARN);

    return handler;
  }
}
