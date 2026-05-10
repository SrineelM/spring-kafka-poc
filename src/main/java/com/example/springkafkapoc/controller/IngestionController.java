package com.example.springkafkapoc.controller;

import com.example.springkafkapoc.service.DataIngestionService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * <b>REST Ingestion Controller — The HTTP Entry Gate</b>
 *
 * <p><b>TUTORIAL — Bridging REST and Kafka:</b><br>
 * This controller is the boundary between the synchronous HTTP world and the asynchronous event
 * streaming world. An HTTP client calls {@code POST /api/v1/transactions} and gets back a 202
 * Accepted response as soon as Kafka has durably accepted the record — not when it's been fully
 * processed. Processing happens downstream, asynchronously.
 *
 * <p><b>Non-Blocking Design:</b><br>
 * The method returns a {@link CompletableFuture}. Spring MVC (with async enabled) releases the
 * Tomcat thread immediately after handing work to the Kafka producer thread. When the broker ACKs,
 * the future completes and the HTTP response is sent. This allows a small server to handle
 * thousands of concurrent ingestion requests without thread starvation.
 *
 * <p><b>PRO TIP — Always validate at the boundary:</b><br>
 * Bean Validation ({@code @Valid}, {@code @NotNull}, {@code @Positive}) catches bad data at the
 * HTTP boundary — before it ever reaches the Avro serializer. Without this, a null amount causes a
 * NullPointerException deep in the Kafka producer, producing a cryptic error message that's hard to
 * correlate back to the HTTP request.
 *
 * <p><b>202 vs. 200:</b><br>
 * We return {@code 202 Accepted} (not 200 OK) because the request has been accepted for processing
 * but processing is not yet complete. The HTTP spec is clear: 202 means "I got it, I'm working on
 * it." 200 would incorrectly imply the work is done.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/transactions")
public class IngestionController {

  private final DataIngestionService ingestionService;
  private final MeterRegistry meterRegistry;

  // ─── Metrics ──────────────────────────────────────────────────────────────────────────────────

  private final Timer ingestionTimer; // End-to-end HTTP call latency (including Kafka ack)
  private final Counter totalVolumeCounter; // Total $ volume ingested via this endpoint
  private final Counter errorCounter; // Failed ingestion attempts (Kafka or internal)

  @Autowired
  public IngestionController(DataIngestionService ingestionService, MeterRegistry meterRegistry) {
    this.ingestionService = ingestionService;
    this.meterRegistry = meterRegistry;

    this.ingestionTimer =
        Timer.builder("ingestion.latency")
            .description("End-to-end REST ingestion latency including Kafka producer ack")
            .register(meterRegistry);
    this.totalVolumeCounter =
        Counter.builder("ingestion.volume.total")
            .description("Total dollar volume ingested via the REST endpoint")
            .baseUnit("USD")
            .register(meterRegistry);
    this.errorCounter =
        Counter.builder("ingestion.error.count")
            .description("Number of failed ingestion requests")
            .register(meterRegistry);
  }

  /**
   * <b>Inbound Request Record</b>
   *
   * <p><b>TUTORIAL — Java Records for Request Objects:</b><br>
   * A Java {@code record} (Java 16+) gives us an immutable data class with canonical constructor,
   * {@code equals()}, {@code hashCode()}, and {@code toString()} for free. This is cleaner than a
   * mutable POJO for request bodies — an HTTP request body should never be mutated after
   * deserialization.
   *
   * <p><b>Validation annotations:</b>
   *
   * <ul>
   *   <li>{@code @NotNull @Positive} on {@code amount}: rejects null and zero/negative values
   *       before the business layer ever sees the request.
   *   <li>{@code @NotBlank @Size(max=128)} on {@code accountId}: prevents empty strings and
   *       oversized IDs that could truncate in the database.
   * </ul>
   */
  public record IngestionRequest(
      @NotNull @Positive BigDecimal amount, @NotBlank @Size(max = 128) String accountId) {}

  /**
   * {@code POST /api/v1/transactions} — Ingest a new transaction.
   *
   * <p>The {@code @Valid} annotation triggers Bean Validation on the request body. If validation
   * fails, Spring automatically returns a 400 Bad Request with field-level error details.
   *
   * @param request the transaction payload (amount + accountId), validated by {@code @Valid}
   * @return 202 Accepted with the transaction ID on success, 400 Bad Request on validation failure,
   *     500 Internal Server Error on Kafka or serialization failure
   */
  @PostMapping
  public CompletableFuture<ResponseEntity<String>> ingest(
      @Valid @RequestBody IngestionRequest request) {

    // Capture start time for latency measurement
    long startNanos = System.nanoTime();

    log.debug("Ingestion request: accountId={}, amount={}", request.accountId(), request.amount());

    return ingestionService
        .ingestTransaction(request.amount(), request.accountId())
        .thenApply(
            result -> {
              // Record latency from HTTP request receipt to Kafka broker ACK
              ingestionTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);

              // Count the dollar volume for financial throughput monitoring
              totalVolumeCounter.increment(request.amount().doubleValue());

              // 202 Accepted: the record is durably in Kafka but not yet processed downstream
              return ResponseEntity.accepted()
                  .body("Transaction accepted: " + result.getProducerRecord().key());
            })
        .exceptionally(
            ex -> {
              // Record latency even on failure — important for P99 latency calculations
              ingestionTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
              errorCounter.increment();

              log.error(
                  "Ingestion failed: accountId={}, error={}", request.accountId(), ex.getMessage());

              return ResponseEntity.internalServerError()
                  .body("Ingestion failed: " + ex.getMessage());
            });
  }

  /**
   * {@code GET /api/v1/transactions/health} — Simple readiness probe.
   *
   * <p><b>TUTORIAL:</b> Load balancers (GCP Load Balancer, Kubernetes Ingress) periodically call a
   * health endpoint to decide whether to route traffic to this pod. A lightweight 200 OK here is
   * sufficient — it proves the web layer is up and Spring context is loaded. For deeper health
   * checks (DB connectivity, Kafka connectivity), use Spring Actuator's {@code /actuator/health}
   * endpoint instead.
   */
  @GetMapping("/health")
  public ResponseEntity<String> health() {
    return ResponseEntity.ok("Ingestion service is running");
  }
}
