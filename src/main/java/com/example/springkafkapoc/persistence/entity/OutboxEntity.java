package com.example.springkafkapoc.persistence.entity;

import jakarta.persistence.*;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <b>Outbox Entity (The Message Buffer)</b>
 *
 * <p><b>TUTORIAL:</b> This table is the secret sauce for <b>Reliable Messaging</b>.
 *
 * <p>Instead of sending a message to Kafka directly (which could fail if Kafka is down), we save it
 * to this table in the <b>same transaction</b> as our business data.
 *
 * <p>A separate background "Poller" service then reads from this table and sends the message to
 * Kafka, marking it as {@code processed} only after Kafka acknowledges it. This guarantees
 * <b>At-Least-Once Delivery</b> even if the database or the application crashes mid-process.
 */
@Entity
@Table(name = "Outbox")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEntity {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(nullable = false, length = 64)
  private String aggregateType;

  @Column(nullable = false, length = 64)
  private String aggregateId;

  @Column(nullable = false, length = 128)
  private String destinationTopic;

  @Column(nullable = false, length = 4000)
  private String payload;

  @Column(nullable = false)
  private Instant createdAt;

  @Column(nullable = false)
  private boolean processed;
}
