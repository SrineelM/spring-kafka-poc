package com.example.springkafkapoc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * <b>Spring Boot Masterclass — Entry Point</b>
 *
 * <p>This class serves as the command center for the entire data ingestion and analytics pipeline.
 * It orchestrates the lifecycle of the Spring application context, coordinating everything from
 * REST endpoints to Kafka Streams topologies.
 *
 * <p><b>TUTORIAL — The Power of @SpringBootApplication:</b> This is a "meta-annotation" that
 * combines three critical behaviors:
 *
 * <ul>
 *   <li><b>@Configuration:</b> Enables the class to define and register beans in the Spring
 *       context.
 *   <li><b>@EnableAutoConfiguration:</b> The "magic" of Spring Boot. It scans the classpath and
 *       automatically configures beans (like {@code KafkaTemplate} or {@code EntityManager}) based
 *       on the jars you've included in your {@code pom.xml}.
 *   <li><b>@ComponentScan:</b> Scans the current package and its sub-packages for components
 *       (@Service, @RestController, @Component), ensuring they are instantiated and managed by
 *       Spring.
 * </ul>
 *
 * <p><b>Type-Safe Configuration:</b> We use {@code @ConfigurationPropertiesScan} to automatically
 * discover and register our {@code AppProperties} class. This is the professional way to handle
 * configuration, moving away from scattered {@code @Value} annotations.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class SpringKafkaPocApplication {

  /**
   * Main method: The actual launch point of the JVM process.
   *
   * <p><b>TUTORIAL — Mixed-Mode Logging Control:</b> In a high-throughput system, you must be
   * deliberate about logging. By default, many developers set a global 'Async' flag. However, we've
   * opted for a <b>Mixed Mode</b> strategy.
   *
   * <p>Operational logs (INFO/DEBUG) are handled asynchronously for speed, but critical error logs
   * are handled synchronously to ensure they are flushed to disk before a potential JVM crash.
   */
  public static void main(String[] args) {
    SpringApplication.run(SpringKafkaPocApplication.class, args);
  }
}
