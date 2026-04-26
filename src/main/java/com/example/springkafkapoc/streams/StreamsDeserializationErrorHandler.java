package com.example.springkafkapoc.streams;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * <b>Kafka Streams — Deserialization Error Handler (Streams-Tier DLQ)</b>
 *
 * <p><b>TUTORIAL: Why this class exists</b>
 *
 * <p>Kafka Streams is NOT the same as a standard {@code @KafkaListener}. Spring's built-in {@link
 * org.springframework.kafka.listener.DefaultErrorHandler} and {@code @RetryableTopic} do NOT apply
 * here. You must supply a {@link DeserializationExceptionHandler} to the Streams config.
 *
 * <p><b>What happens without this?</b><br>
 * If a "poison pill" record (corrupt bytes that cannot be parsed) arrives, Kafka Streams will throw
 * an exception and <b>crash the stream thread</b>. After enough retries, the whole application
 * exits. This is the "stops the world" problem.
 *
 * <p><b>The Two Choices:</b>
 *
 * <ul>
 *   <li>{@code FAIL} — Re-throw the exception and halt the stream thread. Use this if you cannot
 *       tolerate data loss and prefer to alert loudly.
 *   <li>{@code CONTINUE} — Log the error and skip the bad record. Use this when the pipeline's
 *       availability is more important than processing every single byte. <b>We use this.</b>
 * </ul>
 *
 * <p><b>Production Pattern ("Skip & Forensics"):</b><br>
 * In a real system, before returning {@code CONTINUE}, you would write the raw {@code
 * record.value()} bytes to a dedicated "poison-pill" topic (e.g., {@code
 * raw-transactions-STREAMS-DLT}) using a separate KafkaProducer. This preserves the bad record for
 * forensics and replay.
 */
@Slf4j
public class StreamsDeserializationErrorHandler implements DeserializationExceptionHandler {

  @Override
  public DeserializationHandlerResponse handle(
      ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {

    // Log all available metadata so operations can find the bad record in the topic.
    // In production: also produce the raw bytes to a poison-pill topic here.
    log.error(
        "[STREAMS-DLQ] Poison pill detected. Skipping record and continuing pipeline. "
            + "topic={}, partition={}, offset={}, error={}",
        record.topic(),
        record.partition(),
        record.offset(),
        exception.getMessage());

    // TUTORIAL: CONTINUE = skip this record, do NOT crash the thread.
    // The offset will be committed and this record is gone from the live stream.
    // A forensics copy should be written to a DLT topic before this line in production.
    return DeserializationHandlerResponse.CONTINUE;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // No-op
  }
}
