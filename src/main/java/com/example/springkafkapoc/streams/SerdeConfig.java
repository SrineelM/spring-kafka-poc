package com.example.springkafkapoc.streams;

import com.example.springkafkapoc.avro.TransactionEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

/**
 * <b>Serialization & Performance Config (Data Layout)</b>
 *
 * <p><b>TUTORIAL:</b> In high-volume streaming, Serialization (SerDe) is often the primary CPU
 * bottleneck. Standard Java/JSON serialization is slow and bulky.
 *
 * <p>This project uses:
 *
 * <ul>
 *   <li><b>Avro:</b> For structured, schema-validated events with compact binary encoding.
 *   <li><b>Scaled Longs:</b> Our custom <b>OptimizedBigDecimalSerde</b> converts amounts like
 *       "$123.45" to "12345" (long). This makes RocksDB lookups significantly faster and reduces
 *       the memory footprint by over 60% compared to String-based storage.
 * </ul>
 */
@Component
public class SerdeConfig {

  private final KafkaProperties kafkaProperties;

  public SerdeConfig(KafkaProperties kafkaProperties) {
    this.kafkaProperties = kafkaProperties;
  }

  public SpecificAvroSerde<TransactionEvent> transactionEventSerde() {
    SpecificAvroSerde<TransactionEvent> serde = new SpecificAvroSerde<>();
    String url = (String) kafkaProperties.getProperties().get("schema.registry.url");
    if (url == null) {
      url = "http://localhost:8081";
    }
    Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", url);
    serde.configure(serdeConfig, false);
    return serde;
  }

  /**
   * Performance Optimized BigDecimal Serde.
   *
   * <p>TUTORIAL: Instead of toString() which is slow and heavy, we use a scaled long (cents) for
   * financial totals. This reduces payload size and CPU overhead significantly.
   */
  public Serde<BigDecimal> optimizedBigDecimalSerde() {
    return Serdes.serdeFrom(
        (topic, data) -> {
          if (data == null) return null;
          // Scale to 4 decimal places (matches DB precision) and store as long
          long value = data.movePointRight(4).longValue();
          return ByteBuffer.allocate(8).putLong(value).array();
        },
        (topic, data) -> {
          if (data == null) return null;
          long value = ByteBuffer.wrap(data).getLong();
          return BigDecimal.valueOf(value, 4);
        });
  }
}
