package com.example.springkafkapoc.config;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.aop.ObservedAspect;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <b>Modern Observability & Tracing Configuration</b>
 *
 * <p>Configures standard metrics (Micrometer) and distributed tracing (Observation API).
 *
 * <p>Tutorial Tip: In <b>Spring Boot 3</b>, the old "Spring Cloud Sleuth" has been replaced by the
 * <b>Micrometer Observation API</b>. This is a "Write Once, Observe Anywhere" approach.
 *
 * <p><b>What this provides:</b>
 *
 * <ul>
 *   <li><b>Tracing:</b> Automatic span creation for Kafka and HTTP calls.
 *   <li><b>Metrics:</b> Latency histograms, error counts, and backlog gauges.
 *   <li><b>AOP Support:</b> Use {@code @Observed} on any method to record its execution time and
 *       success rate automatically!
 * </ul>
 */
@Slf4j
@Configuration
public class ObservabilityConfig {

  /**
   * Required for using {@code @Observed} annotation on any service methods. Use this for
   * lightweight, non-intrusive monitoring of key business logic.
   */
  @Bean
  public ObservedAspect observedAspect(ObservationRegistry observationRegistry) {
    log.info("Micrometer @Observed support enabled (AspectJ)");
    return new ObservedAspect(observationRegistry);
  }
}
