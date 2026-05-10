# 🛡️ Operations Masterclass: Running at Scale

This guide provides the tactical knowledge required to deploy, monitor, and scale the `spring-kafka-poc` in a high-compliance cloud environment (GKE).

---

## 🏗️ 1. Deployment Strategy: GKE & Skaffold

We treat infrastructure as code. The deployment to Google Kubernetes Engine is automated and repeatable.

### 🔹 Orchestration
We use **Skaffold** to bridge the gap between development and production.
```bash
# Build with Jib and deploy to GKE
skaffold run -p gke
```

### 🔹 Resource Sizing (The "Golden Ratio")
For a system handling 50k events/sec, we recommend:
*   **Replicas:** 3 (one per Kafka partition).
*   **CPU:** 2 Cores per pod (Kafka Streams is CPU intensive during serialization).
*   **Memory:** 4GB (2GB for JVM Heap + 2GB for Off-Heap RocksDB usage).

---

## 🕋 2. Persistence & Schema Governance

In production, `ddl-auto` is set to `validate`. You must manage the schema manually.

### 🔹 Spanner DDL (External Consistency)
```sql
CREATE TABLE Transactions (
    txnId        STRING(64) NOT NULL,
    accountId    STRING(128) NOT NULL,
    amount       NUMERIC NOT NULL,
    timestamp    TIMESTAMP NOT NULL,
    status       STRING(32) NOT NULL,
    processedBy  STRING(64),
    sourcePartition INT64,
    sourceOffset INT64
) PRIMARY KEY (txnId);
```

### 🔹 Distributed Lock Table
The **Transactional Outbox** requires a shared lock table to prevent duplicate relaying in a multi-pod cluster.
```sql
CREATE TABLE INT_LOCK (
    LOCK_KEY CHAR(36) NOT NULL,
    REGION VARCHAR(100) NOT NULL,
    CLIENT_ID CHAR(36),
    CREATED_DATE TIMESTAMP NOT NULL,
    CONSTRAINT INT_LOCK_PK PRIMARY KEY (LOCK_KEY, REGION)
);
```

---

## 📡 3. Kafka Governance

### 🔹 Topic Configuration
| Topic Name | Partitions | Cleanup Policy | Purpose |
|------------|------------|----------------|---------|
| `raw-transactions` | 3 | delete | Ingestion entry point |
| `account-reference` | 3 | compact | Reference data for enrichment |
| `processed-transactions` | 3 | delete | Post-persistence stream |

### 🔹 Non-Blocking DLT Path
Spring Kafka automatically creates retry topics (e.g., `raw-transactions-retry-0`). Ensure your service account has **Create** permissions on the cluster to allow the app to provision these topics dynamically during its first failure event.

---

## 📈 4. The Monitoring Dashboard (SRE Guide)

### 🚨 Critical Alerts
1.  **DLQ Saturation:** `rate(transaction.dlq.count[5m]) > 0`. 
    *   *Action:* Inspect the DLT topic raw bytes. A "Poison Pill" is likely present.
2.  **Sink Backpressure:** `bigquery.sink.paused == 1`.
    *   *Action:* Check BigQuery API status. The pipeline has paused to prevent data loss.
3.  **Outbox Stagnation:** `outbox.backlog.size > 1000`.
    *   *Action:* Check if the `INT_LOCK` table is locked by a dead pod.

### 📊 Performance Tuning
If `kafka_consumer_lag` is increasing:
*   **Horizontal Scale:** Increase topic partitions and pod replicas.
*   **Vertical Scale:** Increase CPU allocation (improves Avro serialization speed).

---

## 🪵 5. Forensic Logging

Every log includes a **Global Sequence Number (`%sn`)**.
*   **The Pattern:** When debugging a race condition, sort by `timestamp` then `seq`. 
*   **The Result:** Even if logs arrive at the collector out-of-order, you can reconstruct the exact sequence of events that occurred within the same nanosecond.

---

*“Visibility is the prerequisite for control. Monitor everything.”*
