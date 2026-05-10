package com.example.springkafkapoc.streams;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * <b>Streams Deserialization Error Handler — "Poison Pill" Shield</b>
 *
 * <p><b>TUTORIAL — Why Kafka Streams needs its own error handler:</b><br>
 * This class does NOT replace Spring's {@code @RetryableTopic} or {@code DefaultErrorHandler}.
 * Those apply only to {@code @KafkaListener}-based consumers. Kafka Streams has its own internal
 * error handling mechanism, configured via {@code DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS}.
 *
 * <p><b>The "Poison Pill" problem:</b><br>
 * A "poison pill" is a record whose bytes cannot be deserialized — perhaps because:
 *
 * <ul>
 *   <li>A producer published raw JSON while the consumer expects Avro.
 *   <li>The Schema Registry returned a newer, incompatible schema version.
 *   <li>The record was corrupted in transit or during storage.
 * </ul>
 *
 * Without a custom handler, any poison pill causes the stream thread to throw, Kafka Streams to
 * restart the thread (up to {@code num.stream.threads} retries), and eventually the entire
 * application shuts down. <b>One bad record kills the pipeline.</b>
 *
 * <p><b>Two strategies — choose based on your requirements:</b>
 *
 * <ul>
 *   <li>{@code FAIL} — Re-throw: halt the stream thread and alert loudly. Use this when 100% data
 *       fidelity is required and you cannot tolerate silent data loss.
 *   <li>{@code CONTINUE} — Skip and log: discard the bad record, move on. Use this when pipeline
 *       availability trumps complete data capture. <b>This is our choice.</b>
 * </ul>
 *
 * <p><b>PRO TIP — "Skip & Forensics" Pattern:</b><br>
 * In production, before returning {@code CONTINUE}, produce the raw {@code record.value()} bytes to
 * a dedicated poison-pill topic (e.g., {@code raw-transactions-STREAMS-POISON}) using an
 * independent {@code KafkaProducer}. This gives operations a chance to inspect and replay the bad
 * record after fixing the schema or producer.
 */
@Slf4j
public class StreamsDeserializationErrorHandler implements DeserializationExceptionHandler {

  /**
   * Called by Kafka Streams when a record cannot be deserialized.
   *
   * <p>We log the full location (topic, partition, offset) so operations can use {@code
   * kafka-console-consumer --topic X --partition Y --offset Z} to retrieve and inspect the raw
   * bytes manually.
   *
   * @param context the processor context (provides access to application ID, etc.)
   * @param record the raw bytes of the offending record — key and value are unreadable
   * @param exception the deserialization error (e.g., schema mismatch, corrupt Avro encoding)
   * @return {@code CONTINUE} to skip the record; {@code FAIL} would crash the stream thread
   */
  @Override
  public DeserializationHandlerResponse handle(
      ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {

    // Log at ERROR with full Kafka coordinates so operations can locate the record in the topic
    log.error(
        "[STREAMS-POISON-PILL] Skipping undeserializable record — pipeline continues. "
            + "topic={}, partition={}, offset={}, error={}",
        record.topic(),
        record.partition(),
        record.offset(),
        exception.getMessage());

    // TODO (production hardening): produce raw bytes to a poison-pill DLT topic here
    // so forensics teams can inspect and replay after fixing the schema mismatch.

    // CONTINUE: discard this record, commit its offset, and keep the pipeline running.
    // The next record in the partition will be processed normally.
    return DeserializationHandlerResponse.CONTINUE;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // No-op: this handler requires no additional configuration
  }
}
