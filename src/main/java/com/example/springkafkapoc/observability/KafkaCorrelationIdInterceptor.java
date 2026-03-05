package com.example.springkafkapoc.observability;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * <b>Kafka Producer Interceptor</b>
 *
 * <p>
 * A passive way to attach observability metadata to every Kafka message.
 *
 * <p>
 * Tutorial Tip: This class implements the <b>Kafka Interceptor Pattern</b>.
 * Instead of writing {@code record.headers().add(...)} every time we send a
 * message, we register this globally. It automatically reaches into the
 * {@code CorrelationIdContext} and injects the ID into the <b>Kafka
 * Headers</b>.
 *
 * <p>
 * <b>WHY INTERCEPTORS?</b>
 * Interceptors are great for cross-cutting concerns like security, tracing,
 * or encryption. They are invisible to the developer writing the business
 * logic.
 */
@Slf4j
public class KafkaCorrelationIdInterceptor<K, V> implements ProducerInterceptor<K, V> {

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        String correlationId = CorrelationIdContext.getCorrelationId();

        if (correlationId != null) {
            log.trace("Injecting Correlation ID into Kafka record: {}", correlationId);
            // 1. Attach the Correlation ID to the Kafka record header
            record.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));
        }

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
