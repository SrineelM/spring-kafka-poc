package com.example.springkafkapoc.service;

import com.example.springkafkapoc.config.TopicConstants;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * <b>BigQuery Sink Service</b>
 *
 * <p><b>TUTORIAL:</b> This service acts as a "Sink" or an "Exit Point" for our data pipeline. It
 * consumes aggregated metrics Produced by our {@link AnalyticsTopology} and persists them to Google
 * BigQuery.
 *
 * <p><b>Resilience Patterns Demonstrated:</b> This is arguably the most resilient part of the
 * system! It handles <b>Downstream Pressure</b>:
 *
 * <ul>
 *   <li><b>Circuit Breaker (Resilience4j):</b> Monitors the failure rate. If BigQuery times out
 *       repeatedly, the circuit "Opens," immediately failing future calls to protect the app.
 *   <li><b>Self-Healing / Backpressure:</b> When the circuit opens, the {@code fallbackSink} is
 *       called. Crucially, it <b>PAUSES</b> the Kafka listener. This is the ultimate backpressure;
 *       we stop asking Kafka for data we know we can't write, preventing offset lag from spiraling
 *       into a massive retry mountain.
 *   <li><b>Proactive Recovery:</b> A {@code @Scheduled} health-check periodically "pings" the sink
 *       and resumes the consumer only when success is likely.
 * </ul>
 */
@Slf4j
@Service
@EnableScheduling
public class BigQuerySinkService {

  private static final String LISTENER_ID = "bigquery-sink-listener";

  private final KafkaListenerEndpointRegistry registry;
  private final MeterRegistry meterRegistry;
  private final AtomicBoolean paused = new AtomicBoolean(false);

  private final Timer sinkTimer;
  private final Counter successCounter;
  private final Counter errorCounter;
  private final Counter totalVolumeCounter;
  private final AtomicLong pausedGauge = new AtomicLong(0);

  @Autowired
  public BigQuerySinkService(KafkaListenerEndpointRegistry registry, MeterRegistry meterRegistry) {
    this.registry = registry;
    this.meterRegistry = meterRegistry;

    this.sinkTimer =
        Timer.builder("bigquery.sink.time")
            .description("Time taken to write a record to BigQuery")
            .register(meterRegistry);
    this.successCounter =
        Counter.builder("bigquery.sink.success.count")
            .description("Number of successful writes to BigQuery")
            .register(meterRegistry);
    this.errorCounter =
        Counter.builder("bigquery.sink.error.count")
            .description("Number of failed writes to BigQuery")
            .register(meterRegistry);
    this.totalVolumeCounter =
        Counter.builder("bigquery.sink.volume.total")
            .description("Total dollar volume successfully written to BigQuery")
            .baseUnit("USD")
            .register(meterRegistry);

    meterRegistry.gauge("bigquery.sink.paused", pausedGauge);
  }

  /**
   * Processes a single aggregated metric record from Kafka.
   *
   * <p>WHY String? The upstream {@link com.example.springkafkapoc.streams.AnalyticsTopology} uses a
   * custom Serde for BigDecimal that serializes it as a plain string. Therefore, this listener MUST
   * consume a {@code String} to avoid a {@code SerializationException}.
   */
  @KafkaListener(
      id = LISTENER_ID,
      topics = TopicConstants.DAILY_ACCOUNT_METRICS,
      groupId = "bigquery-sink-group",
      properties = {"value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"})
  @CircuitBreaker(name = "bigQueryCircuitBreaker", fallbackMethod = "fallbackSink")
  public void sinkToBigQuery(@Payload String rawTotalAmount) {
    sinkTimer.record(
        () -> {
          BigDecimal totalAmount = new BigDecimal(rawTotalAmount);
          log.info("Received aggregate metric: {}. Writing to BigQuery...", totalAmount);

          // --- Simulated Sink Operations ---
          if (Math.random() > 0.90) { // 10% simulated failure
            errorCounter.increment();
            // PRO TIP: Throwing an exception here triggers the Circuit Breaker.
            throw new RuntimeException("BigQuery API Timeout");
          }

          // If we previously paused the consumer and it's working now, resume it.
          if (paused.compareAndSet(true, false)) {
            // PRO TIP: This is "Self-Healing". Once the sink is healthy,
            // we resume the flow of data automatically.
            resumeConsumer();
          }

          successCounter.increment();
          totalVolumeCounter.increment(totalAmount.doubleValue());
          log.info("Successfully flushed metric ({}) to BigQuery.", totalAmount);
        });
  }

  /**
   * Resilience4j fallback for when the BigQuery sink becomes unresponsive.
   *
   * <p>Tutorial Tip: Instead of just logging the error, we reach into the {@code
   * KafkaListenerEndpointRegistry} to <b>PAUSE</b> the consumer. This stops the application from
   * pulling new records it can't handle.
   */
  public void fallbackSink(String rawTotalAmount, Exception ex) {
    log.error(
        "BigQuery sink unavailable. Pausing listener. Metric={}, cause={}",
        rawTotalAmount,
        ex.getMessage());
    errorCounter.increment();

    if (paused.compareAndSet(false, true)) {
      pausedGauge.set(1);
      // We reach into the Spring Kafka registry to find our container
      var container = registry.getListenerContainer(LISTENER_ID);
      if (container != null && container.isRunning()) {
        // PRO TIP: Pausing the container is better than throwing exceptions.
        // It prevents "Retry Storms" where the consumer keeps trying and failing,
        // burning CPU and network for nothing.
        container.pause();
        log.warn("Kafka listener '{}' paused automatically.", LISTENER_ID);
      }
    }
  }

  /** Periodically executes a health check to resume the paused consumer. */
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
      pausedGauge.set(0);
      log.info("Kafka listener '{}' resumed.", LISTENER_ID);
    }
  }
}
