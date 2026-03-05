package com.example.springkafkapoc.domain.model;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <b>Outbox Domain Model</b>
 *
 * <p>
 * <b>TUTORIAL:</b> This model represents a "Message to be Sent."
 * By saving this to our database in the <b>same transaction</b> as our business
 * record
 * (Transactional Outbox Pattern), we guarantee that for every successful DB
 * record,
 * there WILL be a matching Kafka message.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Outbox {
    private Long id;
    private String aggregateType;
    private String aggregateId;
    private String destinationTopic;
    private String payload;
    private Instant createdAt;
    private boolean processed;
}
