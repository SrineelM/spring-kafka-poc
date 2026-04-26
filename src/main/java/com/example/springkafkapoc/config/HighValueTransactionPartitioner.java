package com.example.springkafkapoc.config;

import java.math.BigDecimal;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * <b>Custom Kafka Partitioner — Business-Driven Routing</b>
 *
 * <p><b>TUTORIAL — Why a Custom Partitioner?</b><br>
 * By default, Kafka uses a hash of the record key to pick a partition (uniform distribution). This
 * is great for balanced load, but useless for priority routing. Here we override that behaviour.
 *
 * <p><b>What this does:</b><br>
 * High-value transactions (amount {@literal >} $10,000) are always sent to <em>Partition 0</em>.
 * This allows you to dedicate a high-priority consumer just to Partition 0 — giving premium
 * transactions faster, isolated processing.
 *
 * <p><b>PRO TIP:</b> Use this pattern sparingly. If 90% of your traffic is high-value, Partition 0
 * becomes a hot spot. Balance it with topic partition counts and consumer group sizing.
 */
@Slf4j
public class HighValueTransactionPartitioner implements Partitioner {

  // The dollar threshold above which a transaction is considered high-value
  private static final BigDecimal HIGH_VALUE_THRESHOLD = new BigDecimal("10000.00");

  // Partition 0 is reserved for high-priority, high-value records
  private static final int PRIORITY_PARTITION = 0;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

    // How many partitions does this topic have? We need this to compute the fallback hash.
    int numPartitions = cluster.partitionCountForTopic(topic);

    // Edge case: single-partition topic — all records go to 0 regardless
    if (numPartitions <= 1) return 0;

    // We only inspect records that are Avro SpecificRecord types (our TransactionEvent)
    if (value instanceof org.apache.avro.specific.SpecificRecord record) {

      // Use the Avro schema reflection to pull the 'amount' field value
      Object amountField = record.get(record.getSchema().getField("amount").pos());

      // If the amount exceeds our threshold, route to the priority partition
      if (amountField instanceof BigDecimal amount && amount.compareTo(HIGH_VALUE_THRESHOLD) > 0) {
        log.debug("PRIORITY Routing: {} > threshold — sending to partition {}", amount, PRIORITY_PARTITION);
        return PRIORITY_PARTITION;
      }
    }

    // For all normal transactions: spread across partitions 1..N using Kafka's murmur2 hash.
    // We deliberately skip Partition 0 (reserved for high-value) by using (N-1) as the modulus
    // and adding 1 to shift the result range to [1, N-1].
    int partition =
        (keyBytes != null)
            ? (Math.abs(org.apache.kafka.common.utils.Utils.murmur2(keyBytes))
                    % (numPartitions - 1))
                + 1
            : 1; // Fallback to partition 1 if no key is present

    return partition;
  }

  @Override
  public void close() {
    // No resources to release
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // No configuration needed for this partitioner
  }
}
