package com.example.springkafkapoc.streams;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * <b>Kafka Streams Dead Letter Queue (DLQ) Handler</b>
 *
 * <p><b>TUTORIAL:</b> In a standard consumer, Spring handles the DLQ. But in Kafka Streams, you
 * must provide a {@link DeserializationExceptionHandler}.
 *
 * <p>By default, if Kafka Streams encounters a record it cannot deserialize (e.g., corrupt bytes),
 * it <b>stops the world</b> — the entire stream thread crashes.
 *
 * <p>This implementation log the error and returns {@code FAIL}, or can be configured to {@code
 * CONTINUE} to skip the bad record. In production, you would typically send the raw bytes to a
 * "poison-pill" topic for forensics.
 */
@Slf4j
public class StreamsDeserializationErrorHandler implements DeserializationExceptionHandler {

  @Override
  public DeserializationHandlerResponse handle(
      ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {

    log.error(
        "CRITICAL: Deserialization error in stream! Skipping record. Offset: {}, Partition: {}, Error: {}",
        record.offset(),
        record.partition(),
        exception.getMessage());

    // TUTORIAL: We return CONTINUE to skip the bad record and keep the pipeline running.
    // In a bank, you'd probably send this to a dedicated "poison-pill" topic first.
    return DeserializationHandlerResponse.CONTINUE;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // No-op
  }
}
