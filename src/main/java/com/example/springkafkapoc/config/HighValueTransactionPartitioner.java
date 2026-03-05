package com.example.springkafkapoc.config;

import java.math.BigDecimal;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * <b>Custom Kafka Partitioner</b>
 *
 * <p>
 * Routes priority transactions to a specific partition.
 *
 * <p>
 * Tutorial Tip: This demonstrates <b>Business-Driven Routing</b>.
 * High-value transactions are sent to Partition 0, while others are spread
 * across 1-N.
 * This allows you to have a dedicated "High-Priority" consumer monitoring
 * partition 0.
 */
@Slf4j
public class HighValueTransactionPartitioner implements Partitioner {

    private static final BigDecimal HIGH_VALUE_THRESHOLD = new BigDecimal("10000.00");
    private static final int PRIORITY_PARTITION = 0;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);
        if (numPartitions <= 1) return 0;

        if (value instanceof org.apache.avro.specific.SpecificRecord record) {
            Object amountField =
                    record.get(record.getSchema().getField("amount").pos());

            // Fix: Handle BigDecimal comparison
            if (amountField instanceof BigDecimal amount && amount.compareTo(HIGH_VALUE_THRESHOLD) > 0) {
                log.debug("PRIORITY Routing: {} > threshold", amount);
                return PRIORITY_PARTITION;
            }
        }

        // Stick to default hash-based routing for normal transactions
        int partition = (keyBytes != null)
                ? (Math.abs(org.apache.kafka.common.utils.Utils.murmur2(keyBytes)) % (numPartitions - 1)) + 1
                : 1;

        return partition;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
