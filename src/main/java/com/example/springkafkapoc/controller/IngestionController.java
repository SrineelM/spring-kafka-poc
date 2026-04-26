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
 * <b>REST Data Ingestion Controller</b>
 *
 * <p><b>TUTORIAL:</b> This is the "Front Gate" of our entire system. It bridges the synchronous
 * world of HTTP/REST with the asynchronous world of Event Streaming (Kafka).
 *
 * <p><b>Key Architecture Tip:</b> Notice how this controller returns a {@link CompletableFuture}.
 * 1. An HTTP request arrives. 2. We hand the work to the {@link DataIngestionService}. 3. We
 * <b>RELEASE</b> the Tomcat/Netty thread immediately. 4. When Kafka acknowledges the record, the
 * future completes and the HTTP response is sent.
 *
 * <p>This non-blocking approach allows one small server to handle thousands of concurrent ingestion
 * requests without thread starvation.
 *
 * <p><b>PRO TIP:</b> In high-throughput systems, always favor {@link CompletableFuture} or Reactive
 * types (Mono/Flux). Blocking an HTTP thread waiting for a database or broker to respond is the #1
 * cause of cascading failures in microservices.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/transactions")
public class IngestionController {

  private final DataIngestionService ingestionService;
  private final MeterRegistry meterRegistry;

  private final Timer ingestionTimer;
  private final Counter totalVolumeCounter;
  private final Counter errorCounter;

  @Autowired
  public IngestionController(DataIngestionService ingestionService, MeterRegistry meterRegistry) {
    this.ingestionService = ingestionService;
    this.meterRegistry = meterRegistry;

    this.ingestionTimer =
        Timer.builder("ingestion.latency")
            .description("Time taken for the ingestion REST call to complete")
            .register(meterRegistry);
    this.totalVolumeCounter =
        Counter.builder("ingestion.volume.total")
            .description("Total dollar volume ingested via REST")
            .baseUnit("USD")
            .register(meterRegistry);
    this.errorCounter =
        Counter.builder("ingestion.error.count")
            .description("Number of failed ingestion requests")
            .register(meterRegistry);
  }

  /**
   * Inbound payload. Using a Java {@code record} gives us immutability and a compact canonical
   * constructor for free.
   *
   * <p>WHY VALIDATION: We validate here at the boundary. If we accept a null amount, the Avro
   * serializer would later fail with a NullPointerException, causing a silent failure or a
   * hard-to-debug error deep in the pipeline. Failing fast is always better.
   *
   * <p><b>PRO TIP:</b> Bean Validation (@NotNull, @Positive) combined with {@code record} classes
   * is the modern way to ensure data integrity at the edge of your network.
   */
  public record IngestionRequest(
      @NotNull @Positive BigDecimal amount, @NotBlank @Size(max = 128) String accountId) {}

  /**
   * POST /api/v1/transactions
   *
   * <p>Asynchronous — returns a {@link CompletableFuture} so the Tomcat/Netty thread is released
   * immediately while Kafka send completes on a producer thread.
   *
   * @param request the transaction details from the HTTP body (JSON), validated by @Valid.
   * @return 202 Accepted with the created transaction ID, or 400 on validation failure, or 500 on
   *     internal failure.
   */
  @PostMapping
  public CompletableFuture<ResponseEntity<String>> ingest(
      @Valid @RequestBody IngestionRequest request) {
    long startTime = System.nanoTime();
    log.debug(
        "Received ingestion request for accountId={}, amount={}",
        request.accountId(),
        request.amount());

    return ingestionService
        .ingestTransaction(request.amount(), request.accountId())
        .thenApply(
            result -> {
              ingestionTimer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
              totalVolumeCounter.increment(request.amount().doubleValue());
              return ResponseEntity.accepted()
                  .body("Transaction accepted: " + result.getProducerRecord().key());
            })
        .exceptionally(
            ex -> {
              ingestionTimer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
              errorCounter.increment();
              log.error(
                  "Failed to ingest transaction for accountId={}: {}",
                  request.accountId(),
                  ex.getMessage());
              return ResponseEntity.internalServerError()
                  .body("Ingestion failed: " + ex.getMessage());
            });
  }

  /** Simple health probe for load-balancer readiness checks. */
  @GetMapping("/health")
  public ResponseEntity<String> health() {
    return ResponseEntity.ok("Ingestion service is running");
  }
}
