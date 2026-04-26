package com.example.springkafkapoc.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * <b>Type-Safe Application Configuration</b>
 *
 * <p><b>TUTORIAL — Why not just use {@code @Value}?</b><br>
 * {@code @Value("${some.prop}")} works fine for a handful of properties. But as a project grows,
 * scattered {@code @Value} annotations become impossible to maintain or refactor. This class
 * gathers every custom property under the {@code app.*} prefix into a single, strongly-typed POJO.
 *
 * <p><b>Benefits:</b>
 *
 * <ul>
 *   <li><b>Type Safety:</b> If you put a string where an int is expected, Spring fails at startup
 *       — not at runtime. Errors surface early.
 *   <li><b>IDE Autocomplete:</b> With the {@code spring-boot-configuration-processor} on the
 *       classpath, your IDE can suggest property names in {@code application.yml}.
 *   <li><b>Grouping:</b> Related settings (Kafka, BigQuery, Outbox) live together in a clean
 *       nested hierarchy rather than scattered {@code @Value} fields across 20 classes.
 * </ul>
 *
 * <p>The {@code @ConfigurationPropertiesScan} on the main application class tells Spring Boot to
 * discover and register this bean automatically.
 */
@Data
@Component
@ConfigurationProperties(prefix = "app")
public class AppProperties {

  // Top-level property groups — each is a nested static class below
  private final Kafka kafka = new Kafka();
  private final BigQuery bigQuery = new BigQuery();
  private final Outbox outbox = new Outbox();
  private final Database database = new Database();

  /** Kafka-specific tunables grouped together. */
  @Data
  public static class Kafka {

    // Partition counts are topic-specific; keeping them here avoids hardcoding in config classes
    private final Partitions partitions = new Partitions();

    // Replication factor for topic creation; set to 1 for local, 3+ in production
    private int replicationFactor = 1;

    // Streams-specific tunables (thread count, state store directory)
    private final Streams streams = new Streams();

    /** Per-topic partition counts. Expose here so topology builders can read them. */
    @Data
    public static class Partitions {
      // Number of partitions for the main processing pipeline topics
      private int pipeline = 3;
      // Fewer partitions for metrics topics (lower cardinality)
      private int metrics = 1;
    }

    /** Configuration knobs specific to the Kafka Streams engine. */
    @Data
    public static class Streams {
      // Number of parallel processing threads within a single Streams instance
      // PRO TIP: Set this to the number of partitions on your source topic for max parallelism
      private int threads = 3;
      private String applicationId = "spring-kafka-poc-streams";
      // PRO TIP: In Kubernetes, mount a PersistentVolume here so RocksDB survives pod restarts.
      // Without this, the app rebuilds its entire local state from Kafka changelogs on every boot.
      private String stateDir = "/tmp/kafka-streams-state";
    }
  }

  /** BigQuery sink tunables. */
  @Data
  public static class BigQuery {
    // How often (ms) the health-check scheduled task runs to see if the sink has recovered
    private long healthCheckIntervalMs = 30000;
  }

  /** Transactional Outbox poller tunables. */
  @Data
  public static class Outbox {
    // How long (ms) the distributed lock is held for one poll cycle.
    // Should be longer than the poll cycle itself so the lock doesn't expire mid-run.
    private int lockTtlMs = 30000;
  }

  /** Database routing flags. */
  @Data
  public static class Database {
    // When true, the DynamicPersistenceRouter will prefer Cloud Spanner over H2
    private boolean spannerEnabled = false;
  }
}
