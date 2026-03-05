package com.example.springkafkapoc.service;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.github.f4b6a3.uuid.UuidCreator;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * <b>Data Ingestion Service</b>
 *
 * <p>
 * <b>TUTORIAL:</b> This is the "Front Door" of the application. It acts as the
 * primary
 * Producer into the Kafka ecosystem. In a real-world scenario, this might be
 * triggered
 * by a REST endpoint or a gRPC call.
 *
 * <p>
 * We use <b>Avro</b> for serialization. Avro provides a strict schema
 * (contract) between
 * services, which is essential in a production microservices environment. If
 * the schema
 * changes in an incompatible way (e.g., a required field is removed), the build
 * will fail,
 * preventing downstream consumers from crashing at runtime.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DataIngestionService {

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    /**
     * Ingests a raw transaction and publishes it to Kafka.
     *
     * @param amount    the transaction amount
     * @param accountId the account ID
     * @return a future that completes with the Kafka SendResult
     */
    public CompletableFuture<SendResult<String, TransactionEvent>> ingestTransaction(
            BigDecimal amount, String accountId) {

        // WHY UUIDv7: Highly performant, time-ordered UUIDs.
        String transactionId = UuidCreator.getTimeOrderedEpoch().toString();
        log.info("Ingesting transaction: id={}, account={}, amount={}", transactionId, accountId, amount);

        TransactionEvent event = TransactionEvent.newBuilder()
                .setTransactionId(transactionId)
                .setAccountId(accountId)
                .setAmount(amount)
                .setTimestamp(Instant.now().toEpochMilli())
                .setProcessingState("INITIAL")
                .build();

        // We use the transactionId as the Kafka KEY.
        // This ensures all events for the same transaction land in the same partition,
        // which guarantees ordering for that specific transaction.
        return kafkaTemplate
                .send(TopicConstants.RAW_TRANSACTIONS, transactionId, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info(
                                "Ingested successfully: id={}, partition={}, offset={}",
                                transactionId,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Ingestion failed: id={}, error={}", transactionId, ex.getMessage());
                    }
                });
    }
}
