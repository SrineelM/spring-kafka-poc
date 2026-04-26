package com.example.springkafkapoc.streams.topology;

import com.example.springkafkapoc.avro.TransactionEvent;
import com.example.springkafkapoc.config.TopicConstants;
import com.example.springkafkapoc.streams.SerdeConfig;
import java.math.BigDecimal;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

/**
 * <b>Routing & Branching Topology — Content-Based Stream Routing</b>
 *
 * <p><b>TUTORIAL — Content-Based Routing (CBR):</b><br>
 * Content-Based Routing is an Enterprise Integration Pattern (EIP). Instead of every consumer
 * reading the same topic and filtering internally (wasteful — every consumer does redundant
 * filtering CPU), CBR routes records at the source into specialized sub-topics. Downstream
 * consumers only subscribe to the topic relevant to them.
 *
 * <p><b>What this topology does:</b><br>
 * Splits the main transaction stream into three output topics:
 * <ol>
 *   <li><b>High-Value Transactions ({@literal >}$10,000):</b> Routed to a dedicated topic for
 *       compliance monitoring, manual review workflows, and high-priority consumer groups.
 *   <li><b>Standard Transactions (≤$10,000):</b> Routed to the normal processing topic.
 *   <li><b>All Transactions Audit:</b> EVERY transaction (high-value + standard) is merged
 *       and written here for complete audit trail continuity.
 * </ol>
 *
 * <p><b>TUTORIAL — Named Branching API:</b><br>
 * {@code .split(Named.as("txn-"))} creates a named split node in the topology. Each branch is
 * accessed from the returned map using the fully qualified name: prefix + branch alias. This is
 * preferable to the older array-based API ({@code branch()[0]}) because named branches are:
 * <ul>
 *   <li>Self-documenting in the topology description.
 *   <li>Accessible by name — no index-based magic numbers.
 *   <li>Easier to test with {@code TopologyTestDriver}.
 * </ul>
 *
 * <p><b>PRO TIP — Why merge for the audit topic?</b><br>
 * Calling {@code highValue.merge(standard)} creates a single unified view for the audit sink
 * without duplicating logic. The merged stream is NOT a new Kafka topic — it's an in-memory
 * combination of the two branches, then written as a single {@code .to()} call.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RoutingTopology {

  // $10,000 threshold — matches the HighValueTransactionPartitioner on the producer side
  private static final BigDecimal HIGH_VALUE_THRESHOLD = new BigDecimal("10000");

  private final SerdeConfig serdeConfig;

  /**
   * Builds the routing and branching topology.
   *
   * @param keyedTransactionStream the transaction stream keyed by accountId, from
   *                               {@link SourceTopology}
   */
  public void build(KStream<String, TransactionEvent> keyedTransactionStream) {
    var txnSerde = serdeConfig.transactionEventSerde();

    // ─── Split into Named Branches ────────────────────────────────────────────────────────────
    // Named.as("txn-") sets the branch name prefix.
    // Branched.as("high-value") → full key = "txn-high-value"
    // defaultBranch(Branched.as("standard")) → full key = "txn-standard"
    var branches =
        keyedTransactionStream
            .split(Named.as("txn-"))
            .branch(
                // Predicate: amount > $10,000 → HIGH VALUE branch
                (key, value) -> value.getAmount().compareTo(HIGH_VALUE_THRESHOLD) > 0,
                Branched.as("high-value"))
            // Default branch: everything that didn't match any predicate above
            .defaultBranch(Branched.as("standard"));

    // Access branches by their full name (prefix + alias)
    // requireNonNull is defensive — in practice these branches always exist given our predicates
    KStream<String, TransactionEvent> highValue =
        Objects.requireNonNull(branches.get("txn-high-value"),
            "high-value branch not found — check branch naming");
    KStream<String, TransactionEvent> standard =
        Objects.requireNonNull(branches.get("txn-standard"),
            "standard branch not found — check branch naming");

    // ─── Route Each Branch to Its Dedicated Topic ─────────────────────────────────────────────
    // High-value consumers (compliance, audit) only subscribe to HIGH_VALUE_TRANSACTIONS
    highValue.to(TopicConstants.HIGH_VALUE_TRANSACTIONS, Produced.with(Serdes.String(), txnSerde));
    // Standard consumers (normal processing) only subscribe to NORMAL_TRANSACTIONS
    standard.to(TopicConstants.NORMAL_TRANSACTIONS, Produced.with(Serdes.String(), txnSerde));

    // ─── Unified Audit Sink ───────────────────────────────────────────────────────────────────
    // merge(): combines two KStreams into one in-memory stream (no new Kafka topic created).
    // The merged stream writes every transaction (regardless of value) to the audit topic.
    // TUTORIAL: This gives compliance teams a single topic with 100% of all transactions,
    // while operational teams use the specialized topics for their workloads.
    highValue
        .merge(standard)
        .to(TopicConstants.ALL_TRANSACTIONS_AUDIT, Produced.with(Serdes.String(), txnSerde));

    log.debug("RoutingTopology wired: threshold={}", HIGH_VALUE_THRESHOLD);
  }
}
