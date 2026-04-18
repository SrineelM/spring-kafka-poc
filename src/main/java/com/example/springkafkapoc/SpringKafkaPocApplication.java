package com.example.springkafkapoc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main Entry Point for the Spring Boot Kafka Data Ingestion & Analytics Pipeline.
 *
 * <p>Requirements covered: - Spring Boot 3.3.4 - GCP Spanner - GCP Secret Manager - GCP BigQuery -
 * Kafka (Streams, Producer, Consumer)
 *
 * <p><b>TUTORIAL:</b> The {@code @SpringBootApplication} annotation is a convenience annotation
 * that adds all of the following:
 *
 * <ul>
 *   <li>{@code @Configuration}: Tags the class as a source of bean definitions for the application
 *       context.
 *   <li>{@code @EnableAutoConfiguration}: Tells Spring Boot to start adding beans based on
 *       classpath settings, other beans, and various property settings.
 *   <li>{@code @ComponentScan}: Tells Spring to look for other components, configurations, and
 *       services in the {@code com.example.springkafkapoc} package, allowing it to find the
 *       controllers and services.
 * </ul>
 */
@SpringBootApplication
public class SpringKafkaPocApplication {

  public static void main(String[] args) {
    // Enable asynchronous logging context selector globally for Log4j2 and LMAX
    // Disruptor. Using the modern log4j2.contextSelector property.
    // TUTORIAL: Async loggers provide higher throughput and lower latency, crucial
    // for high-volume streaming applications.
    System.setProperty(
        "log4j2.contextSelector", "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");

    SpringApplication.run(SpringKafkaPocApplication.class, args);
  }
}
