package com.example.springkafkapoc.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * <b>Type-Safe Configuration (The Blueprint)</b>
 *
 * <p><b>TUTORIAL:</b> In small projects, you might use {@code @Value("${some.prop}")}. But as a
 * project grows, that becomes a maintenance nightmare.
 *
 * <p>Why use {@code @ConfigurationProperties}?
 *
 * <ul>
 *   <li><b>Type Safety:</b> If you put a string where a number is expected, the app fails early
 *       during startup.
 *   <li><b>IDE Support:</b> IDEs can provide autocomplete for these properties in your {@code
 *       application.yml}!
 *   <li><b>Grouping:</b> It keeps related settings (Kafka, DB, Outbox) together in a clean
 *       hierarchy.
 * </ul>
 */
@Data
@Component
@ConfigurationProperties(prefix = "app")
public class AppProperties {

  private final Kafka kafka = new Kafka();
  private final BigQuery bigQuery = new BigQuery();
  private final Outbox outbox = new Outbox();
  private final Database database = new Database();

  @Data
  public static class Kafka {
    private final Partitions partitions = new Partitions();
    private int replicationFactor = 1;
    private final Streams streams = new Streams();

    @Data
    public static class Partitions {
      private int pipeline = 3;
      private int metrics = 1;
    }

    @Data
    public static class Streams {
      private int threads = 3;
      private String applicationId = "spring-kafka-poc-streams";
      private String stateDir = "/tmp/kafka-streams-state";
    }
  }

  @Data
  public static class BigQuery {
    private long healthCheckIntervalMs = 30000;
  }

  @Data
  public static class Outbox {
    private int lockTtlMs = 30000;
  }

  @Data
  public static class Database {
    private boolean spannerEnabled = false;
  }
}
