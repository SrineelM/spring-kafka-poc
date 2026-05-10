package com.example.springkafkapoc.consumer;

import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * <b>Cooperative Sticky Rebalance Listener — Partition Lifecycle Observer</b>
 *
 * <p>This listener is wired into the container factory and fires callbacks at every rebalance
 * event. It is the primary observability tool for understanding partition movement in your cluster.
 *
 * <p><b>TUTORIAL — What "Cooperative" means in practice:</b><br>
 * With the old Eager protocol ({@code RangeAssignor}, {@code RoundRobinAssignor}), both callbacks
 * below fire for ALL partitions, even the ones that are NOT moving. This is the "stop the world"
 * behaviour.
 *
 * <p>With {@code CooperativeStickyAssignor}, the callbacks fire for ONLY the partitions that are
 * actually changing hands. If you have 9 partitions across 3 consumers and add a 4th, you'll see
 * revocations for only 2-3 partitions — the rest continue processing without interruption.
 *
 * <p><b>Lifecycle during scale-up (3 consumers → 4 consumers):</b>
 *
 * <pre>
 * BEFORE:
 *   Consumer-A: partitions {0, 1, 2}
 *   Consumer-B: partitions {3, 4, 5}
 *   Consumer-C: partitions {6, 7, 8}
 *
 * REBALANCE ROUND 1 — Incremental Revocation:
 *   onPartitionsRevoked fires on Consumer-A for {2} only
 *   onPartitionsRevoked fires on Consumer-B for {5} only
 *   Consumer-A still processes {0, 1}  ← NO INTERRUPTION
 *   Consumer-B still processes {3, 4}  ← NO INTERRUPTION
 *   Consumer-C still processes {6,7,8} ← NO INTERRUPTION
 *
 * REBALANCE ROUND 2 — Assignment:
 *   Consumer-D joins and gets {2, 5}
 *   onPartitionsAssigned fires on Consumer-D for {2, 5}
 * </pre>
 *
 * <p><b>Logging strategy:</b> We log at INFO for production visibility and DEBUG for the full
 * partition list. Use {@code [seq=%sn]} in log aggregators to correlate before/after entries.
 */
@Slf4j
@Component
public class CooperativeStickyRebalanceListener implements ConsumerRebalanceListener {

  /**
   * Called BEFORE the consumer stops fetching from the revoked partitions.
   *
   * <p><b>TUTORIAL — What to do here:</b>
   *
   * <ul>
   *   <li>Flush any in-memory state for the revoked partitions (e.g., micro-batch buffers).
   *   <li>Commit offsets for in-flight records on the revoked partitions.
   *   <li>Log the revocation for incident analysis.
   * </ul>
   *
   * <p><b>CRITICAL:</b> With cooperative rebalance, this list will be SMALL (only the partitions
   * moving away). With eager rebalance this would be ALL partitions — a major difference.
   *
   * @param partitions the partitions being revoked from this consumer instance
   */
  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) {
      log.debug("onPartitionsRevoked: no partitions revoked (cooperative incremental round)");
      return;
    }

    log.info(
        "REBALANCE — onPartitionsRevoked: {} partition(s) being revoked. "
            + "Processing continues on all OTHER partitions. partitions={}",
        partitions.size(),
        partitions);

    // PRO TIP: In a production system, commit offsets here for in-flight records
    // on the revoked partitions to avoid reprocessing after reassignment.
    // Example (if you have access to the consumer):
    //   consumer.commitSync(currentOffsets(partitions));
    //
    // With MANUAL_IMMEDIATE ack mode in Spring Kafka, Spring handles this for you
    // automatically when it calls commitSync on revocation.
    log.debug(
        "REBALANCE — revoked partitions detail: count={}, partitions={}",
        partitions.size(),
        partitions);
  }

  /**
   * Called AFTER the consumer has been assigned new partitions and is ready to fetch from them.
   *
   * <p><b>TUTORIAL — What to do here:</b>
   *
   * <ul>
   *   <li>Seek to a specific offset if you need to replay from a checkpoint.
   *   <li>Warm up any partition-local caches (e.g., deduplication state).
   *   <li>Log the new assignment to track the steady-state partition distribution.
   * </ul>
   *
   * @param partitions the newly assigned partitions (may include previously held partitions in
   *     eager mode, only NEW partitions in cooperative mode)
   */
  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) {
      log.debug("onPartitionsAssigned: no new partitions assigned in this rebalance round");
      return;
    }

    log.info(
        "REBALANCE — onPartitionsAssigned: {} partition(s) assigned. "
            + "Consumer is now active on these partitions. partitions={}",
        partitions.size(),
        partitions);

    log.debug(
        "REBALANCE — assigned partitions detail: count={}, partitions={}",
        partitions.size(),
        partitions);
  }

  /**
   * Called when partitions are lost due to an unexpected consumer group rebalance (e.g., the
   * consumer did not have time to revoke gracefully — JVM OOM, network partition).
   *
   * <p><b>TUTORIAL:</b><br>
   * {@code onPartitionsLost} is different from {@code onPartitionsRevoked}: offsets may NOT have
   * been committed for the lost partitions. Another consumer will pick them up and reprocess from
   * the last committed offset. This is why idempotent processing is non-optional.
   *
   * @param partitions the partitions that were forcibly lost without a graceful revoke
   */
  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    log.warn(
        "REBALANCE — onPartitionsLost: {} partition(s) lost without graceful revoke. "
            + "Another consumer will reprocess these from the last committed offset. "
            + "Ensure your listener is idempotent! partitions={}",
        partitions.size(),
        partitions);

    log.debug(
        "REBALANCE — lost partitions detail: count={}, partitions={}",
        partitions.size(),
        partitions);
  }
}
