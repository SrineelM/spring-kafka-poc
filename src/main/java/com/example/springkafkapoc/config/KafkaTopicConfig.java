package com.example.springkafkapoc.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * <b>Kafka Topic Configuration — Declarative Topic Registry</b>
 *
 * <p><b>TUTORIAL — Why declare topics as Spring beans?</b><br>
 * Spring Boot auto-configures a {@code KafkaAdmin} bean that uses {@link NewTopic} bean
 * definitions to create topics at startup via the Kafka Admin API. The operation is
 * <em>idempotent</em> — if the topic already exists with the same configuration, nothing happens.
 *
 * <p>This is a clean alternative to manual {@code kafka-topics.sh} scripts because:
 *
 * <ul>
 *   <li>Topic names are drawn from {@link TopicConstants} — one change propagates everywhere.
 *   <li>Partition counts and replication factors are controlled by {@link AppProperties} — tunable
 *       per environment without code changes.
 *   <li>New team members can open this file and immediately see the complete topic landscape.
 * </ul>
 *
 * <p><b>PRO TIP — Partition counts are permanent:</b><br>
 * You can only increase partition counts, never decrease. Start with a count that matches your
 * expected consumer parallelism (pods × threads-per-pod). For co-partitioned topics used in
 * KStream-KStream joins, ALL joined topics MUST have identical partition counts.
 *
 * <p><b>Topic Tiers in this pipeline:</b>
 *
 * <ol>
 *   <li><b>Ingestion:</b> Raw events enter, processed events exit.
 *   <li><b>Analytics:</b> Windowed metrics and session-aggregated outputs.
 *   <li><b>Routing:</b> High-value vs. normal transaction separation.
 *   <li><b>Fraud:</b> Join inputs and join outputs for the fraud detection topology.
 *   <li><b>Audit:</b> Complete audit trail and threshold breach events.
 *   <li><b>Resilience:</b> Dead Letter Topic for poison-pill forensic investigation.
 * </ol>
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {

  private final AppProperties appProperties;

  // ─── Ingestion Tier ───────────────────────────────────────────────────────────────────────────

  /** Raw events arriving from the REST API ingestion boundary. */
  @Bean
  public NewTopic rawTransactionsTopic() {
    return build(
        TopicConstants.RAW_TRANSACTIONS, appProperties.getKafka().getPartitions().getPipeline());
  }

  /**
   * Events successfully saved to the primary DB.
   *
   * <p>This is the source for all downstream analytical Kafka Streams topologies. Must be
   * co-partitioned with {@link TopicConstants#FRAUD_SIGNALS} for KStream-KStream joins.
   */
  @Bean
  public NewTopic processedTransactionsTopic() {
    return build(
        TopicConstants.PROCESSED_TRANSACTIONS,
        appProperties.getKafka().getPartitions().getPipeline());
  }

  // ─── Analytics Tier ──────────────────────────────────────────────────────────────────────────

  /** 24h Tumbling Window outputs — daily account spending totals. */
  @Bean
  public NewTopic dailyAccountMetricsTopic() {
    return build(
        TopicConstants.DAILY_ACCOUNT_METRICS,
        appProperties.getKafka().getPartitions().getMetrics());
  }

  /** 1h Hopping Window outputs — trailing hourly account spend view. */
  @Bean
  public NewTopic hourlyAccountMetricsTopic() {
    return build(
        TopicConstants.HOURLY_ACCOUNT_METRICS,
        appProperties.getKafka().getPartitions().getMetrics());
  }

  /** Sessionized activity burst windows. */
  @Bean
  public NewTopic sessionActivityTopic() {
    return build(
        TopicConstants.SESSION_ACTIVITY, appProperties.getKafka().getPartitions().getMetrics());
  }

  /** Cumulative account balance KTable changelog topic. */
  @Bean
  public NewTopic accountBalancesTopic() {
    return build(
        TopicConstants.ACCOUNT_BALANCES, appProperties.getKafka().getPartitions().getPipeline());
  }

  // ─── Routing Tier ────────────────────────────────────────────────────────────────────────────

  /** Transactions above the high-value threshold routed for premium processing. */
  @Bean
  public NewTopic highValueTransactionsTopic() {
    return build(
        TopicConstants.HIGH_VALUE_TRANSACTIONS,
        appProperties.getKafka().getPartitions().getPipeline());
  }

  /** Transactions below the high-value threshold routed for standard processing. */
  @Bean
  public NewTopic normalTransactionsTopic() {
    return build(
        TopicConstants.NORMAL_TRANSACTIONS,
        appProperties.getKafka().getPartitions().getPipeline());
  }

  // ─── Fraud Detection Tier ─────────────────────────────────────────────────────────────────────

  /**
   * Fraud signal inputs from external risk engines.
   *
   * <p><b>CRITICAL — Co-partitioning:</b> Must have identical partition count to {@link
   * TopicConstants#PROCESSED_TRANSACTIONS}. KStream-KStream joins require both topics to use the
   * same key-partitioning strategy and the same partition count. If they differ, records with the
   * same key land on different partitions and the join produces no results.
   */
  @Bean
  public NewTopic fraudSignalsTopic() {
    return build(
        TopicConstants.FRAUD_SIGNALS, appProperties.getKafka().getPartitions().getPipeline());
  }

  /** Fraud alert outputs emitted by the join topology when a signal matches a transaction. */
  @Bean
  public NewTopic fraudAlertsTopic() {
    return build(
        TopicConstants.FRAUD_ALERTS, appProperties.getKafka().getPartitions().getPipeline());
  }

  // ─── Audit Tier ───────────────────────────────────────────────────────────────────────────────

  /**
   * All transactions (high-value + normal) merged into a single audit stream by the routing
   * topology. Provides compliance systems with a complete, partition-ordered audit trail.
   */
  @Bean
  public NewTopic allTransactionsAuditTopic() {
    return build(
        TopicConstants.ALL_TRANSACTIONS_AUDIT,
        appProperties.getKafka().getPartitions().getPipeline());
  }

  /** Threshold breach events emitted when a transaction amount exceeds a configured limit. */
  @Bean
  public NewTopic auditThresholdEventsTopic() {
    return build(
        TopicConstants.AUDIT_THRESHOLD_EVENTS,
        appProperties.getKafka().getPartitions().getPipeline());
  }

  // ─── Resilience Tier ──────────────────────────────────────────────────────────────────────────

  /**
   * Dead Letter Topic for records that have exhausted all retry attempts.
   *
   * <p><b>IMPORTANT:</b> The DLT must be created explicitly — it does not auto-exist just because
   * the error handler routes to it. Partition count must match the source topic so that records
   * land on the same partition as the original (preserving offset ordering for forensic replay).
   */
  @Bean
  public NewTopic rawTransactionsDlt() {
    return build(
        TopicConstants.RAW_TRANSACTIONS_DLT,
        appProperties.getKafka().getPartitions().getPipeline());
  }

  // ─── Shared Builder ──────────────────────────────────────────────────────────────────────────

  /**
   * Centralises the {@link TopicBuilder} call so partition count and replication factor are applied
   * uniformly from {@link AppProperties}. Logging here surfaces topic creation at startup.
   */
  private NewTopic build(String name, int partitions) {
    int replicationFactor = appProperties.getKafka().getReplicationFactor();
    log.debug(
        "Declaring topic: name={}, partitions={}, replicationFactor={}",
        name,
        partitions,
        replicationFactor);
    return TopicBuilder.name(name).partitions(partitions).replicas(replicationFactor).build();
  }
}
