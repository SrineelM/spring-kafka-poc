package com.example.springkafkapoc.service;

import com.example.springkafkapoc.config.TopicConstants;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * <b>BigQuery Sink Service</b>
 *
 * <p>
 * <b>TUTORIAL:</b> This service acts as a "Sink" or an "Exit Point" for our
 * data pipeline.
 * It consumes aggregated metrics Produced by our {@link AnalyticsTopology} and
 * persists
 * them to Google BigQuery.
 *
 * <p>
 * <b>Resilience Patterns Demonstrated:</b>
 * This is arguably the most resilient part of the system! It handles
 * <b>Downstream Pressure</b>:
 * <ul>
 * <li><b>Circuit Breaker (Resilience4j):</b> Monitors the failure rate. If
 * BigQuery times out
 * repeatedly, the circuit "Opens," immediately failing future calls to protect
 * the app.</li>
 * <li><b>Self-Healing / Backpressure:</b> When the circuit opens, the
 * {@code fallbackSink}
 * is called. Crucially, it <b>PAUSES</b> the Kafka listener. This is the
 * ultimate backpressure;
 * we stop asking Kafka for data we know we can't write, preventing offset lag
 * from spiraling
 * into a massive retry mountain.</li>
 * <li><b>Proactive Recovery:</b> A {@code @Scheduled} health-check periodically
 * "pings"
 * the sink and resumes the consumer only when success is likely.</li>
 * </ul>
 */
@Slf4j
@Service
@EnableScheduling
public class BigQuerySinkService {

    private static final String LISTENER_ID = "bigquery-sink-listener";

    private final KafkaListenerEndpointRegistry registry;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    public BigQuerySinkService(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    /**
     * Processes a single aggregated metric record from Kafka.
     *
     * <p>
     * WHY String? The upstream
     * {@link com.example.springkafkapoc.streams.AnalyticsTopology}
     * uses a custom Serde for BigDecimal that serializes it as a plain string.
     * Therefore, this listener MUST consume a {@code String} to avoid a
     * {@code SerializationException}.
     */
    @KafkaListener(
            id = LISTENER_ID,
            topics = TopicConstants.DAILY_ACCOUNT_METRICS,
            groupId = "bigquery-sink-group",
            properties = {"value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"})
    @CircuitBreaker(name = "bigQueryCircuitBreaker", fallbackMethod = "fallbackSink")
    public void sinkToBigQuery(@Payload String rawTotalAmount) {
        BigDecimal totalAmount = new BigDecimal(rawTotalAmount);
        log.info("Received aggregate metric: {}. Writing to BigQuery...", totalAmount);

        // --- Simulated Sink Operations ---
        if (Math.random() > 0.90) { // 10% simulated failure
            throw new RuntimeException("BigQuery API Timeout");
        }

        // If we previously paused the consumer and it's working now, resume it.
        if (paused.compareAndSet(true, false)) {
            resumeConsumer();
        }

        log.info("Successfully flushed metric ({}) to BigQuery.", totalAmount);
    }

    /**
     * Resilience4j fallback for when the BigQuery sink becomes unresponsive.
     *
     * <p>
     * Tutorial Tip: Instead of just logging the error, we reach into the
     * {@code KafkaListenerEndpointRegistry} to <b>PAUSE</b> the consumer.
     * This stops the application from pulling new records it can't handle.
     */
    public void fallbackSink(String rawTotalAmount, Exception ex) {
        log.error("BigQuery sink unavailable. Pausing listener. Metric={}, cause={}", rawTotalAmount, ex.getMessage());

        if (paused.compareAndSet(false, true)) {
            var container = registry.getListenerContainer(LISTENER_ID);
            if (container != null && container.isRunning()) {
                container.pause();
                log.warn("Kafka listener '{}' paused automatically.", LISTENER_ID);
            }
        }
    }

    /**
     * Periodically executes a health check to resume the paused consumer.
     */
    @Scheduled(fixedDelayString = "${app.bigquery.health-check-interval-ms:30000}")
    public void resumeHealthCheck() {
        if (!paused.get()) {
            return;
        }

        log.info("BigQuery periodic recovery check...");
        try {
            boolean sinkHealthy = Math.random() > 0.3; // 70% simulated recovery
            if (sinkHealthy) {
                log.info("Sink is healthy again. Resuming listener.");
                resumeConsumer();
                paused.set(false);
            } else {
                log.warn("Sink still unresponsive. Consumer remains paused.");
            }
        } catch (Exception e) {
            log.error("Health check failed: {}", e.getMessage());
        }
    }

    private void resumeConsumer() {
        var container = registry.getListenerContainer(LISTENER_ID);
        if (container != null && container.isRunning()) {
            container.resume();
            log.info("Kafka listener '{}' resumed.", LISTENER_ID);
        }
    }
}
