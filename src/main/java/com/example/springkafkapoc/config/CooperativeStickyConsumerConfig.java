package com.example.springkafkapoc.config;

import com.example.springkafkapoc.consumer.CooperativeStickyRebalanceListener;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * <b>Cooperative Sticky Consumer Configuration</b>
 *
 * <p><b>TUTORIAL — Eager vs. Cooperative Rebalance:</b><br>
 * The original Kafka rebalance protocol (Eager / Range / RoundRobin) is a "stop the world" event:
 *
 * <ol>
 *   <li>ALL consumers in the group revoke ALL their partitions simultaneously.
 *   <li>The group coordinator elects a leader.
 *   <li>The leader re-assigns ALL partitions from scratch.
 *   <li>Consumers resume from their new assignments.
 * </ol>
 *
 * During this stop-the-world pause, <em>zero</em> messages are consumed. In a 12-partition topic
 * with 3 consumers, a scale-up from 3→4 instances triggers a full revoke of all 12 partitions even
 * though only 3 need to move.
 *
 * <p><b>Cooperative Sticky Rebalance (ICKP protocol) solves this:</b>
 *
 * <ol>
 *   <li><b>Round 1:</b> Only the partitions that MUST move are revoked (incremental). Consumers
 *       continue processing all other partitions.
 *   <li><b>Round 2:</b> The freed partitions are assigned to the new consumer.
 * </ol>
 *
 * <p><b>Sticky Assignment:</b><br>
 * "Sticky" means the assignor tries to keep existing consumer-partition pairings stable. If
 * Consumer A owned partitions {0,1,2} before a rebalance, it will continue to own them afterward
 * (unless one must move). This minimises the cache-warming cost of partition reassignment.
 *
 * <p><b>Why this reduces downtime:</b><br>
 * In a 9-partition topic with 3 consumers (3 partitions each), adding a 4th consumer only requires
 * moving 2-3 partitions — not all 9. Processing on the remaining 6-7 partitions never stops.
 *
 * <p><b>Static Membership ({@code group.instance.id}):</b><br>
 * Combined with cooperative rebalancing, static membership eliminates rebalances for rolling
 * restarts. When a consumer with a known {@code group.instance.id} reconnects within the session
 * timeout window, the broker skips the rebalance entirely — the consumer simply reclaims its prior
 * partitions.
 *
 * <p><b>Production Recommendations:</b>
 *
 * <ul>
 *   <li>{@code session.timeout.ms=45000}: Time before the broker considers a consumer dead. Must be
 *       {@code >= 3 * heartbeat.interval.ms}. 45s allows for GC pauses without false eviction.
 *   <li>{@code heartbeat.interval.ms=15000}: Frequency of heartbeats to the group coordinator.
 *       Kept at 1/3 of session.timeout.ms per Kafka documentation.
 *   <li>{@code max.poll.interval.ms=300000}: Maximum time your listener code may run between {@code
 *       poll()} calls. Must be greater than your slowest batch processing time. If you exceed this,
 *       the broker considers the consumer dead mid-processing, causing a rebalance.
 *   <li>{@code concurrency=3}: Matches the number of topic partitions. One thread per partition is
 *       the optimal ratio for throughput with no idle threads.
 *   <li>{@code group.instance.id}: Set per-pod (e.g., via {@code POD_NAME} env var) for static
 *       membership to eliminate rolling-restart rebalances in Kubernetes.
 * </ul>
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class CooperativeStickyConsumerConfig {

  private final KafkaProperties kafkaProperties;
  private final AppProperties appProperties;

  /**
   * Consumer factory configured exclusively for Cooperative Sticky assignment.
   *
   * <p>This is a <em>separate</em> factory from the one in {@link KafkaCoreConfig} to avoid
   * conflicting partition assignors on the same consumer group. Consumers using Eager and
   * Cooperative assignors in the same group will cause a {@code InconsistentGroupProtocolException}
   * at startup.
   */
  @Bean("cooperativeStickyConsumerFactory")
  public ConsumerFactory<String, Object> cooperativeStickyConsumerFactory() {
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    // ── Core: Cooperative Sticky Assignor ────────────────────────────────────────────────────────
    // This single config line switches from the "stop-the-world" eager rebalance
    // to the incremental cooperative protocol. Only the partitions that MUST move
    // are revoked; all others continue processing without interruption.
    props.put(
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        CooperativeStickyAssignor.class.getName());

    // ── Rebalance Timing Tuning ───────────────────────────────────────────────────────────────────
    // session.timeout.ms: How long the broker waits before declaring a consumer dead.
    // For cooperative rebalance, a larger value reduces false-positive evictions during GC pauses.
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45_000);

    // heartbeat.interval.ms: Must be <= session.timeout.ms / 3.
    // At 15s heartbeats with a 45s session timeout, we get 3 missed heartbeats before eviction.
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 15_000);

    // max.poll.interval.ms: Maximum time your @KafkaListener method may block.
    // CRITICAL: If your listener takes longer than this, Kafka evicts the consumer,
    // triggering a rebalance MID-PROCESSING. Set this to 1.5× your max observed processing time.
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);

    // max.poll.records: Limits the number of records returned per poll().
    // Smaller batches = more predictable processing time = safer with max.poll.interval.ms.
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

    // ── Idempotent Processing ─────────────────────────────────────────────────────────────────────
    // disable auto-commit — offsets committed only after successful processing.
    // Combined with partition-level ordering guarantees, this prevents double-processing.
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // ── Static Membership ─────────────────────────────────────────────────────────────────────────
    // group.instance.id provides "static membership": when a consumer with this ID reconnects
    // within session.timeout.ms, the broker skips the rebalance and re-assigns its prior
    // partitions directly. Essential for Kubernetes rolling restarts with cooperative rebalance.
    // In production: derive from POD_NAME env var, e.g.:
    //   System.getenv().getOrDefault("POD_NAME", "cooperative-consumer-" + UUID.randomUUID())
    // Here we read from the app property so it can be overridden per-instance via config.
    String instanceId = System.getenv("POD_NAME");
    if (instanceId != null && !instanceId.isBlank()) {
      log.info(
          "Static membership enabled: group.instance.id={}", instanceId);
      props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
    } else {
      log.debug(
          "POD_NAME env var not set; static membership disabled. "
              + "Cooperative rebalance still active but rolling restarts will trigger rebalances.");
    }

    log.debug(
        "Cooperative sticky consumer factory created: assignor={}, session.timeout.ms={},"
            + " heartbeat.interval.ms={}, max.poll.interval.ms={}",
        CooperativeStickyAssignor.class.getSimpleName(),
        45_000,
        15_000,
        300_000);

    return new DefaultKafkaConsumerFactory<>(props);
  }

  /**
   * Listener container factory wired with:
   *
   * <ul>
   *   <li>The cooperative sticky consumer factory.
   *   <li>The rebalance listener that logs partition assignments/revocations.
   *   <li>MANUAL_IMMEDIATE ack mode for idempotent offset commits.
   *   <li>Concurrency matching the partition count for maximum parallelism.
   * </ul>
   */
  @Bean("cooperativeStickyContainerFactory")
  public ConcurrentKafkaListenerContainerFactory<String, Object>
      cooperativeStickyContainerFactory(
          CooperativeStickyRebalanceListener rebalanceListener) {

    ConcurrentKafkaListenerContainerFactory<String, Object> factory =
        new ConcurrentKafkaListenerContainerFactory<>();

    factory.setConsumerFactory(cooperativeStickyConsumerFactory());

    // Matches the number of partitions on the target topic.
    // PRO TIP: concurrency > partition count wastes threads (extra threads stay idle).
    //          concurrency < partition count leaves partitions underserved.
    int concurrency = appProperties.getKafka().getPartitions().getPipeline();
    factory.setConcurrency(concurrency);

    // Ack mode: commit offset only after our business logic completes successfully.
    // This is the critical guard against data loss on JVM crash.
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

    // Wire the rebalance listener — logs every partition assignment and revocation.
    // In cooperative rebalance, revocations are incremental: only moving partitions appear here.
    factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);

    log.debug(
        "Cooperative sticky container factory created: concurrency={}, ackMode=MANUAL_IMMEDIATE",
        concurrency);

    return factory;
  }
}
