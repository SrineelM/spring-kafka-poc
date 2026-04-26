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
 * <b>BigQuery Sink Service — The Analytics Exit Point</b>
 *
 * <p><b>TUTORIAL — What is a "Sink" in stream processing?</b><br>
 * A sink is the final destination for processed data. This service consumes aggregated metrics
 * produced by the {@link com.example.springkafkapoc.streams.topology.MetricsTopology} from the
 * {@code daily-account-metrics} topic and writes them to Google BigQuery for dashboards and ad-hoc
 * analytics queries.
 *
 * <p><b>Three Resilience Patterns implemented here:</b>
 *
 * <p><b>1. Circuit Breaker (Resilience4j):</b><br>
 * Guards every BigQuery write. If 50% of calls fail within a sliding window of 10, the circuit
 * "opens" — all subsequent calls immediately invoke the fallback without attempting BigQuery. This
 * protects threads and prevents timeout pile-ups.
 *
 * <p><b>2. Backpressure via Consumer Pause:</b><br>
 * When the circuit opens, the fallback method PAUSES the Kafka listener. This stops the application
 * from polling new records it can't write. Without this, the consumer would keep fetching records,
 * fail, and build up a retry mountain — the "retry storm" anti-pattern.
 *
 * <p><b>3. Proactive Self-Healing:</b><br>
 * A scheduled health-check periodically tests whether BigQuery is available again. If it is, the
 * Kafka listener is RESUMED automatically — no human intervention required. This is "self-healing"
 * infrastructure.
 *
 * <p><b>WHY String payload?</b><br>
 * The upstream MetricsTopology uses the {@code optimizedBigDecimalSerde} for RocksDB, but outputs
 * to the Kafka topic using that same serde (scaled long bytes). However, when cross-consuming from
 * a non-Streams consumer, using a {@code StringDeserializer} is simpler and more explicit. The
 * value is parsed to {@link BigDecimal} here.
 */
@Slf4j
@Service
@EnableScheduling
public class BigQuerySinkService {

  // This ID must match the @KafkaListener id= below — used to control the container
  private static final String LISTENER_ID = "bigquery-sink-listener";

  // Spring Kafka registry — allows pausing/resuming listener containers programmatically
  private final KafkaListenerEndpointRegistry registry;
  private final MeterRegistry meterRegistry;

  // Thread-safe flag: true when the listener is paused due to a BigQuery outage
  private final AtomicBoolean paused = new AtomicBoolean(false);

  // ─── Metrics ──────────────────────────────────────────────────────────────────────────────────

  private final Timer sinkTimer; // Latency per write operation
  private final Counter successCounter; // Successful writes
  private final Counter errorCounter; // Failed writes (triggers circuit breaker)
  private final Counter totalVolumeCounter; // Total $ volume flushed to BigQuery
  private final AtomicLong pausedGauge = new AtomicLong(0); // 0=running, 1=paused (for Grafana)

  @Autowired
  public BigQuerySinkService(KafkaListenerEndpointRegistry registry, MeterRegistry meterRegistry) {
    this.registry = registry;
    this.meterRegistry = meterRegistry;

    this.sinkTimer =
        Timer.builder("bigquery.sink.time")
            .description("Time taken to write one record to BigQuery")
            .register(meterRegistry);
    this.successCounter =
        Counter.builder("bigquery.sink.success.count")
            .description("Successful writes to BigQuery")
            .register(meterRegistry);
    this.errorCounter =
        Counter.builder("bigquery.sink.error.count")
            .description("Failed writes to BigQuery — each failure counts toward circuit breaker")
            .register(meterRegistry);
    this.totalVolumeCounter =
        Counter.builder("bigquery.sink.volume.total")
            .description("Total dollar volume successfully written to BigQuery")
            .baseUnit("USD")
            .register(meterRegistry);

    // Gauge: live 0/1 flag — set to 1 when listener is paused, 0 when running
    meterRegistry.gauge("bigquery.sink.paused", pausedGauge);
  }

  /**
   * Consumes one aggregated metric from the daily-account-metrics topic and writes to BigQuery.
   *
   * <p><b>TUTORIAL — @CircuitBreaker:</b><br>
   * Resilience4j intercepts every call to this method. On failure, it increments its internal
   * failure counter. If the failure rate exceeds the configured threshold (50% over 10 calls), the
   * circuit opens and {@code fallbackSink} is called for all subsequent invocations — without
   * attempting the BigQuery write. The circuit stays open until the health-check scheduled task
   * proves BigQuery is healthy again.
   *
   * <p><b>WHY String and not a typed Avro/JSON payload?</b><br>
   * The upstream topology serializes BigDecimal using the compact scaled-long serde. We configure
   * this specific listener with a {@code StringDeserializer} override to consume the raw value as a
   * string, then parse it to BigDecimal here for clarity and testability.
   */
  @KafkaListener(
      id = LISTENER_ID,
      topics = TopicConstants.DAILY_ACCOUNT_METRICS,
      groupId = "bigquery-sink-group",
      // Override the default Avro deserializer — this topic carries BigDecimal as a string
      properties = {"value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"})
  @CircuitBreaker(name = "bigQueryCircuitBreaker", fallbackMethod = "fallbackSink")
  public void sinkToBigQuery(@Payload String rawTotalAmount) {
    sinkTimer.record(
        () -> {
          BigDecimal totalAmount = new BigDecimal(rawTotalAmount);
          log.info("Received metric: {}. Writing to BigQuery...", totalAmount);

          // ── Simulated BigQuery Write ──────────────────────────────────────────────────────
          // In production: use the BigQuery Storage Write API or the InsertAll (streaming insert)
          // API here. We simulate a 10% failure rate to demonstrate circuit breaker behaviour.
          if (Math.random() > 0.90) {
            errorCounter.increment();
            // Throwing here causes Resilience4j to count this as a failure.
            // After enough failures, the circuit opens and fallbackSink is called instead.
            throw new RuntimeException("BigQuery API Timeout (simulated)");
          }

          // If we were paused and the write just succeeded, resume the consumer automatically.
          // compareAndSet(true, false) is atomic — only one thread will execute this block.
          if (paused.compareAndSet(true, false)) {
            // PRO TIP: This is "self-healing" — the sink detects its own recovery and
            // resumes the data flow without waiting for a human to intervene.
            resumeConsumer();
          }

          successCounter.increment();
          totalVolumeCounter.increment(totalAmount.doubleValue());
          log.info("Successfully flushed {} to BigQuery.", totalAmount);
        });
  }

  /**
   * Resilience4j fallback — invoked when the circuit is OPEN or when a call fails.
   *
   * <p><b>TUTORIAL — Backpressure via Consumer Pause:</b><br>
   * Instead of just logging and returning, we actively pause the Kafka listener. This stops the
   * consumer from fetching more records from the broker. The records remain safely in Kafka (Kafka
   * retains them indefinitely by retention policy) until the consumer resumes.
   *
   * <p>Without this pause, the consumer would keep fetching records, failing to write them, and the
   * error rate would spiral — causing timeouts, thread starvation, and offset lag.
   */
  public void fallbackSink(String rawTotalAmount, Exception ex) {
    log.error(
        "BigQuery unavailable. Pausing listener. metric={}, cause={}",
        rawTotalAmount,
        ex.getMessage());
    errorCounter.increment();

    // compareAndSet(false, true) is atomic — ensures only the first failing call pauses
    if (paused.compareAndSet(false, true)) {
      pausedGauge.set(1); // Alert Grafana that the listener is paused
      var container = registry.getListenerContainer(LISTENER_ID);
      if (container != null && container.isRunning()) {
        // Pause the listener: it stops polling Kafka but remains "running" (can be resumed)
        // This is different from stop() which would require restart() to bring back
        container.pause();
        log.warn("Kafka listener '{}' paused — BigQuery is unreachable.", LISTENER_ID);
      }
    }
  }

  /**
   * Periodic recovery check — runs every {@code app.bigquery.health-check-interval-ms} (30s).
   *
   * <p><b>TUTORIAL:</b> This scheduled method is the "recovery probe." While the listener is
   * paused, this runs every 30 seconds and attempts a lightweight BigQuery health check. If
   * BigQuery is healthy, it resumes the consumer so normal data flow restores automatically.
   *
   * <p>This prevents the situation where BigQuery recovers at 3 AM but no engineer is awake to
   * manually restart the consumer — the system heals itself.
   */
  @Scheduled(fixedDelayString = "${app.bigquery.health-check-interval-ms:30000}")
  public void resumeHealthCheck() {
    // Fast-path exit: if the consumer is running normally, nothing to do
    if (!paused.get()) return;

    log.info("BigQuery health check — attempting recovery probe...");
    try {
      // Simulated health check: in production, send a lightweight query (e.g., SELECT 1)
      boolean sinkHealthy = Math.random() > 0.3; // 70% simulated recovery probability
      if (sinkHealthy) {
        log.info("BigQuery is healthy again. Resuming Kafka listener.");
        resumeConsumer();
        paused.set(false); // Clear the paused flag so sinkToBigQuery handles future records
      } else {
        log.warn("BigQuery still unresponsive. Consumer remains paused.");
      }
    } catch (Exception e) {
      log.error("Health check probe failed: {}", e.getMessage());
    }
  }

  /** Resumes the Kafka listener container and resets the paused gauge. */
  private void resumeConsumer() {
    var container = registry.getListenerContainer(LISTENER_ID);
    if (container != null && container.isRunning()) {
      container.resume(); // Resume polling — consumer re-joins the broker assignment
      pausedGauge.set(0); // Clear Grafana alert
      log.info("Kafka listener '{}' resumed.", LISTENER_ID);
    }
  }
}
