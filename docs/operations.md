# Operations & Production Deployment Guide

This guide covers the operational aspects of deploying and managing the `spring-kafka-poc` application in a production environment, focusing on Google Kubernetes Engine (GKE).

---

## 1. Deployment to GKE

Deployment to GKE is managed via Skaffold and a standard Dockerfile.

### Prerequisites
- A running GKE cluster.
- `gcloud` CLI configured to connect to your cluster.
- `skaffold` CLI installed.

### Build & Deploy
The project includes a `skaffold.yaml` that automates the build and deploy process.

```bash
# This command will build the Docker image using Jib, push it to your configured
# container registry, and apply the Kubernetes manifests.
skaffold run -p gke
```

### Kubernetes Manifests
You will need to create Kubernetes manifests (`k8s-deployment.yaml`, `k8s-service.yaml`, etc.) that define the deployment, service, secrets, and config maps. The deployment manifest should pass environment variables to configure the database and other external services.

**Example `k8s-deployment.yaml` snippet:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spring-kafka-poc
spec:
  replicas: 3 # Run multiple instances for high availability
  template:
    spec:
      containers:
      - name: app
        image: your-registry/spring-kafka-poc # Image built by Skaffold
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "gke"
        - name: GCP_PROJECT_ID
          value: "your-gcp-project"
        - name: SPANNER_INSTANCE_ID
          value: "your-spanner-instance"
        # ... other env vars for AlloyDB, Kafka, etc.
```

---

## 2. Database Configuration & Schema Management

The application supports H2 (local), Spanner (prod), and AlloyDB (prod).

### `ddl-auto` Strategy
- **`local` profile:** `create-drop` (schema is created on startup and dropped on shutdown).
- **`gke` (prod) profile:** `none`. **You are responsible for managing the production schema.**

### Schema Initialization
The application requires two tables to be created manually in the production database (Spanner or AlloyDB) before the first run.

**1. `Transactions` Table:**
```sql
-- Spanner DDL
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

-- AlloyDB (PostgreSQL) DDL
CREATE TABLE Transactions (
    txn_id        VARCHAR(64) PRIMARY KEY,
    account_id    VARCHAR(128) NOT NULL,
    amount        NUMERIC(18, 4) NOT NULL,
    "timestamp"   TIMESTAMPTZ NOT NULL,
    status        VARCHAR(32) NOT NULL,
    processed_by  VARCHAR(64),
    source_partition INT,
    source_offset BIGINT
);
```

**2. `Outbox` Table:**
```sql
-- Spanner DDL
CREATE TABLE Outbox (
    id           INT64 NOT NULL,
    aggregateType STRING(255),
    aggregateId  STRING(255),
    topic        STRING(255),
    payload      STRING(MAX),
    createdAt    TIMESTAMP
) PRIMARY KEY (id);

-- AlloyDB (PostgreSQL) DDL
CREATE TABLE Outbox (
    id             BIGSERIAL PRIMARY KEY,
    aggregate_type VARCHAR(255),
    aggregate_id   VARCHAR(255),
    topic          VARCHAR(255),
    payload        TEXT,
    created_at     TIMESTAMPTZ
);
```

**3. `INT_LOCK` Table (for Distributed Lock):**
Spring Integration's `JdbcLockRegistry` will automatically create a table named `INT_LOCK` on its first run if it doesn't exist. It's recommended to create it manually in production.

```sql
-- DDL for most standard SQL databases
CREATE TABLE INT_LOCK  (
	LOCK_KEY CHAR(36) NOT NULL,
	REGION VARCHAR(100) NOT NULL,
	CLIENT_ID CHAR(36),
	CREATED_DATE TIMESTAMP NOT NULL,
	constraint INT_LOCK_PK primary key (LOCK_KEY, REGION)
);
```

---

## 3. Kafka Topic Management

The application uses several topics. While `KafkaTopicConfig` can auto-create them in local development, in production, topics should be created manually with appropriate partition counts and replication factors.

- `raw-transactions`
- `processed-transactions`
- `account-reference` (compacted)
- `daily-account-metrics`
- `high-value-transactions`
- `normal-transactions`

**Retry and DLT Topics:**
The `@RetryableTopic` feature will automatically create the following topics based on the main topic name. For `raw-transactions`, it will create:
- `raw-transactions-retry-0` (for the first retry attempt)
- `raw-transactions-retry-1` (for the second retry attempt)
- `raw-transactions-dlt` (the Dead Letter Topic)

Ensure your Kafka user has permissions to create these topics, or create them beforehand.

---

## 4. JVM Tuning for Production

For running in GKE, use the following JVM arguments in your `Dockerfile` or Kubernetes deployment manifest for optimal performance and stability.

```
-Xms2g -Xmx2g          # Symmetric heap to prevent resize pauses
-XX:+UseG1GC            # G1 Garbage Collector for predictable pause times
-XX:MaxGCPauseMillis=100   # Target pause time well below Kafka's session timeout
-XX:+UseStringDeduplication  # Saves significant heap on text-heavy data like Avro
-XX:+ExitOnOutOfMemoryError  # Fail fast and let Kubernetes restart the pod
-Djava.security.egd=file:/dev/./urandom # Non-blocking entropy source
```

**Example in Dockerfile:**
```dockerfile
FROM openjdk:21-slim
VOLUME /tmp
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java", "-Xms2g", "-Xmx2g", "-XX:+UseG1GC", "-jar", "/app.jar"]
```

---

---

## 6. Mixed-Mode Logging & Observability

The application uses a **Mixed-Mode Logging** strategy with **Log4j2** to balance high-performance throughput with production-grade reliability.

### Strategy
1.  **Asynchronous (INFO, WARN, DEBUG)**: Standard runtime logs are routed through an `AsyncAppender`. This ensures that logging operations do not block the high-volume Kafka processing threads, maximizing application throughput.
2.  **Synchronous (ERROR)**: Critical errors are routed **directly** to the console/disk appender. This ensures that the calling thread waits for the log to be written, guaranteeing that error details are captured even if the application crashes immediately after.

### Configuration
- **No Global Async**: The `AsyncLoggerContextSelector` is disabled to allow for this granular level-based control.
- **Routing**: This is achieved in `log4j2.xml` using an `<Async>` wrapper with a `ThresholdFilter` that denies `ERROR` level logs, forcing them to take the synchronous path.

### Handling Out-of-Order Logs
Even with a mix of synchronous and asynchronous paths, the application includes a **Global Sequence Number (`seq=%sn`)** in every log line.

**How to reconstruct the correct order:**
1.  **Sort by `timestamp`**: Primary sort.
2.  **Sort by `seq`**: Secondary sort for definitive ordering of events that occurred within the same millisecond or were logged across the sync/async divide.

### Distributed Tracing
`traceId` and `spanId` continue to be injected into the MDC, allowing full correlation regardless of whether the log was processed synchronously or asynchronously.

---

## 7. Operational & Business Metrics

The application is instrumented with **Micrometer** and exposes a rich set of metrics via the `/actuator/prometheus` endpoint. These are categorized into **Operational Metrics** (system health) and **Business Metrics** (transactional data).

### 📈 Business Metrics (Financial Visibility)
| Metric Name | Type | Description |
|-------------|------|-------------|
| `ingestion.volume.total` | Counter | Total USD volume ingested via the REST API. |
| `bigquery.sink.volume.total` | Counter | Total USD volume successfully written to BigQuery. |
| `transaction.batch.processed.count` | Counter | Total records successfully persisted to the database. |
| `streams.deduplication.duplicates.count` | Counter | Number of duplicate records intercepted by KStreams. |

### 🛠️ Operational Metrics (Pipeline Health)
| Metric Name | Type | Description |
|-------------|------|-------------|
| `ingestion.latency` | Timer | Time taken for the REST ingestion call (incl. Kafka producer ACKs). |
| `outbox.backlog.size` | Gauge | Current number of unprocessed messages in the Outbox table. |
| `outbox.published.count` | Counter | Total messages successfully shipped from Outbox to Kafka. |
| `outbox.lock.failures` | Counter | High values indicate lock contention between multiple pod instances. |
| `bigquery.sink.paused` | Gauge | `1` if the sink consumer is paused due to downstream backpressure. |
| `transaction.dlq.count` | Counter | Critical Metric: Total messages that failed all retries and landed in DLQ. |
| `streams.source.malformed.count` | Counter | Number of corrupt/invalid records dropped at the stream entry point. |

### 🚦 Recommended Alerts
1. **DLQ Alert**: `rate(transaction.dlq.count[5m]) > 0`. Indicates a persistent failure or "Poison Pill" that requires manual investigation.
2. **Outbox Lag**: `outbox.backlog.size > 5000`. Indicates that the Outbox poller cannot keep up with the ingestion rate or the database lock is stuck.
3. **Sink Backpressure**: `bigquery.sink.paused == 1`. Indicates that the downstream BigQuery service is failing, and the pipeline has entered "protective mode."
4. **Consumer Lag**: Monitor standard Kafka metrics: `kafka_consumer_lag`.
