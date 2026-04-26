package com.example.springkafkapoc.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.persistence.EntityManagerFactory;
import java.time.Duration;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.integration.jdbc.lock.DefaultLockRepository;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.integration.jdbc.lock.LockRepository;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * <b>Persistence Configuration — Transactions, Locking & Resilience</b>
 *
 * <p>This class centralizes all persistence-layer infrastructure beans. Three concerns live here:
 *
 * <p><b>1. Distributed Locking (JDBC-backed)</b><br>
 * We use Spring Integration's {@link JdbcLockRegistry} to coordinate which application instance
 * may poll the Outbox at any given moment. The lock state is stored in the {@code INT_LOCK} table,
 * so all instances in a cluster share the same view. Without this, two pods would simultaneously
 * read the same unprocessed outbox records and publish duplicates to Kafka.
 *
 * <p><b>2. Transaction Management</b><br>
 * The standard JPA {@link JpaTransactionManager} is declared here as the {@code @Primary} bean so
 * every {@code @Transactional} annotation in the codebase uses the correct manager.
 *
 * <p><b>3. Circuit Breaker (Resilience4j)</b><br>
 * The BigQuery circuit breaker guards the analytics sink. If the external service degrades,
 * the breaker "opens" and the fallback is invoked — which pauses the Kafka consumer instead of
 * hammering a broken endpoint.
 *
 * <p><b>PRO TIP:</b> Distributed locking is the secret to scaling outbox-processors safely. Without
 * it, multiple instances would attempt to publish the same record, causing duplicates and race
 * conditions.
 */
@Slf4j
@Configuration
public class PersistenceConfig {

  /**
   * Configures WHERE lock state is persisted.
   *
   * <p><b>TUTORIAL:</b> {@link DefaultLockRepository} creates or uses the {@code INT_LOCK} table.
   * The TTL (Time-To-Live) ensures locks don't remain held forever if a pod crashes mid-cycle —
   * another instance can acquire it after the TTL expires.
   */
  @Bean
  public LockRepository lockRepository(DataSource dataSource, AppProperties appProperties) {
    DefaultLockRepository repo = new DefaultLockRepository(dataSource);
    // Set TTL from config so it's tunable per environment without a code deploy
    repo.setTimeToLive(appProperties.getOutbox().getLockTtlMs());
    return repo;
  }

  /**
   * Exposes the distributed lock API to our services.
   *
   * <p><b>TUTORIAL:</b> Callers call {@code lockRegistry.obtain("my-key")} to get a {@link
   * java.util.concurrent.locks.Lock} object. They then call {@code tryLock()} — which is
   * non-blocking. If the lock is held by another instance, {@code tryLock()} returns {@code false}
   * immediately; the caller simply skips its work for this cycle.
   */
  @Bean
  public LockRegistry lockRegistry(LockRepository lockRepository) {
    return new JdbcLockRegistry(lockRepository);
  }

  /**
   * Declares the primary JPA transaction manager.
   *
   * <p><b>TUTORIAL:</b> Every {@code @Transactional} annotation is backed by a {@link
   * PlatformTransactionManager}. We explicitly declare it as {@code @Primary} here because the
   * project also has a Kafka transaction manager on the classpath; Spring needs to know which one
   * is the default for JPA operations.
   */
  @Bean
  @Primary
  public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
    return new JpaTransactionManager(entityManagerFactory);
  }

  /**
   * Programmatic Resilience4j Circuit Breaker for BigQuery.
   *
   * <p><b>TUTORIAL — How a Circuit Breaker works:</b>
   *
   * <ul>
   *   <li><b>CLOSED (normal):</b> Requests pass through. Failures are counted.
   *   <li><b>OPEN (degraded):</b> If the failure rate exceeds the threshold (50% here over a
   *       sliding window of 10 calls), the breaker opens. Further calls are rejected instantly
   *       (the fallback method is invoked) without even trying. This protects thread pools.
   *   <li><b>HALF-OPEN (recovery):</b> After {@code waitDurationInOpenState} (30s here), the
   *       breaker allows a limited number of test calls through. If they succeed, it closes again.
   * </ul>
   *
   * <p><b>PRO TIP:</b> The state-transition event listener logs every change. Hook this into your
   * alerting system (e.g., publish to a Slack webhook or increment a Prometheus counter) so
   * operations are notified the moment a dependency starts misbehaving.
   */
  @Bean
  public CircuitBreaker bigQueryCircuitBreaker(CircuitBreakerRegistry registry) {
    CircuitBreakerConfig config =
        CircuitBreakerConfig.custom()
            // Track the last 10 calls in a count-based sliding window
            .slidingWindowSize(10)
            // Open the circuit if ≥ 50% of those calls fail
            .failureRateThreshold(50.0f)
            // Stay OPEN for 30 seconds before attempting recovery (HALF-OPEN)
            .waitDurationInOpenState(Duration.ofSeconds(30))
            // Treat any exception as a failure (can be narrowed to specific types)
            .recordExceptions(Exception.class)
            .build();

    // Register the breaker under a named key matching @CircuitBreaker(name="bigQueryCircuitBreaker")
    CircuitBreaker cb = registry.circuitBreaker("bigQueryCircuitBreaker", config);

    // Log every state transition so operations can observe breaker behaviour in real time
    cb.getEventPublisher()
        .onStateTransition(
            event ->
                log.warn("BigQuery CircuitBreaker state change: {}", event.getStateTransition()));

    return cb;
  }
}
