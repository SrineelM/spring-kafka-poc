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
@org.springframework.boot.context.properties.ConfigurationPropertiesScan
public class SpringKafkaPocApplication {

  public static void main(String[] args) {
    // TUTORIAL: We've removed the global AsyncLoggerContextSelector to allow
    // for "Mixed Mode" logging. This enables us to keep high-volume logs
    // asynchronous while ensuring critical ERROR logs are synchronous.

    SpringApplication.run(SpringKafkaPocApplication.class, args);
  }
}
