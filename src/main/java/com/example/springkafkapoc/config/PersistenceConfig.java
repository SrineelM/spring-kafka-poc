package com.example.springkafkapoc.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.persistence.EntityManagerFactory;
import java.time.Duration;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.jdbc.lock.DefaultLockRepository;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.integration.jdbc.lock.LockRepository;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * <b>Persistence Configuration</b>
 *
 * <p>
 * This class centralizes all persistence-related beans.
 *
 * <p>
 * <b>Distributed Locking:</b> We use {@link JdbcLockRegistry} backed by a
 * database table
 * to ensure that only one instance of the Outbox processor runs at a time for a
 * given aggregate.
 *
 * <p>
 * <b>Transaction Management:</b> Standard JPA transaction management for data
 * integrity.
 *
 * <p>
 * <b>Resilience:</b> Programmatic Circuit Breaker for BigQuery writes to handle
 * outages gracefully.
 */
@Slf4j
@Configuration
public class PersistenceConfig {

    /**
     * TUTORIAL: The {@code LockRepository} specifies where lock states are stored.
     * Here, we use the default Spring Integration behavior which relies on a SQL
     * data source
     * (the 'INT_LOCK' table). This ensures that locks are distributed across
     * instances.
     */
    @Bean
    public LockRepository lockRepository(DataSource dataSource, @Value("${app.outbox.lock-ttl-ms:30000}") int ttlMs) {
        DefaultLockRepository repo = new DefaultLockRepository(dataSource);
        repo.setTimeToLive(ttlMs);
        return repo;
    }

    /**
     * TUTORIAL: The {@code LockRegistry} exposes the distributed lock API.
     * We use a JDBC-backed registry to obtain locks that all application nodes
     * respect.
     * This guarantees singleton execution paths where necessary (like Polling the
     * outbox).
     */
    @Bean
    public LockRegistry lockRegistry(LockRepository lockRepository) {
        return new JdbcLockRegistry(lockRepository);
    }

    /**
     * TUTORIAL: Defines the primary Transaction Manager.
     * Required for {@code @Transactional} annotations. When you save to the DB
     * within
     * a method, this manager wraps the operation in a SQL transaction, ensuring
     * atomic commits
     * or rollbacks on failure.
     */
    @Bean
    public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    /**
     * TUTORIAL: Defines a Resilience4j Circuit Breaker.
     * It guards the integration point to BigQuery. If standard operations fail
     * significantly
     * (e.g. 50% failure rate over a sliding window of 10), it 'opens' to
     * short-circuit
     * further requests immediately, returning errors without waiting to fail again.
     */
    @Bean
    public CircuitBreaker bigQueryCircuitBreaker(CircuitBreakerRegistry registry) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowSize(10)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .recordExceptions(Exception.class)
                .build();

        CircuitBreaker cb = registry.circuitBreaker("bigQueryCircuitBreaker", config);

        cb.getEventPublisher()
                .onStateTransition(
                        event -> log.warn("BigQuery CircuitBreaker state change: {}", event.getStateTransition()));

        return cb;
    }
}
