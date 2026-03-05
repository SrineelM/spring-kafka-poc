package com.example.springkafkapoc.observability;

import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

/**
 * <b>Kafka Consumer Ingress Interceptor</b>
 *
 * <p>
 * Restores the tracking context from Kafka headers back into the application.
 *
 * <p>
 * Tutorial Tip: This is part of <b>Full Lifecycle Traceability</b>.
 * When a Kafka record arrives, we extract the "correlationId" from its
 * headers and set it in the <b>ThreadLocal</b>. Now, every {@code log.info()}
 * call in your consumer will automatically show which HTTP request
 * triggered this processing.
 */
@Slf4j
@Component
public class KafkaCorrelationIdExtractor<K, V> implements RecordInterceptor<K, V> {

    @Override
    public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {

        // 1. Check for the header in the Kafka record
        Header header = record.headers().lastHeader("correlationId");

        if (header != null) {
            String correlationId = new String(header.value(), StandardCharsets.UTF_8);
            log.trace("Extracted Correlation ID from Kafka header: {}", correlationId);

            // 2. Set it into our ThreadLocal context
            CorrelationIdContext.setCorrelationId(correlationId);
        } else {
            // Generate a fresh ID if the record didn't have one (e.g. legacy or third-party
            // message)
            log.warn("Kafka record missing 'correlationId' header. Generating fresh ID.");
            CorrelationIdContext.setCorrelationId(java.util.UUID.randomUUID().toString());
        }

        return record;
    }

    /**
     * Called AFTER the listener method returns.
     * We MUST clear the context so it doesn't leak into the next record
     * processed by this thread.
     */
    @Override
    public void afterRecord(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        CorrelationIdContext.clear();
    }
}
