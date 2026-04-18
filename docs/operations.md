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

## 6. Async Logging & Observability

The application uses **Log4j2 with LMAX Disruptor** for high-performance asynchronous logging. This ensures that logging operations do not block the main processing threads (Kafka consumers/producers).

### Configuration
- **Context Selector**: The `Log4jContextSelector` is set to `AsyncLoggerContextSelector` in `SpringKafkaPocApplication.java`.
- **Buffer Size**: Default Disruptor ring buffer size is used (adjustable via `-Dlog4j2.asyncLoggerRingBufferSize`).
- **Wait Strategy**: Uses a non-blocking wait strategy by default for maximum throughput.

### Handling Out-of-Order Logs
In high-throughput environments, multiple threads may produce logs almost simultaneously. While the Disruptor preserves the arrival order, some log aggregators (like GCP Cloud Logging) or log viewers might display them out of order due to timestamp collisions.

To solve this, the application includes a **Global Sequence Number (`seq=%sn`)** in every log line.

**How to reconstruct the correct order:**
1.  **Sort by `timestamp`**: First level of sorting.
2.  **Sort by `seq`**: If timestamps are identical, the sequence number guaranteed by Log4j2 before entering the async queue defines the absolute order.

**Log Pattern:**
```
%d{ISO8601} [%t] %level %logger [seq=%sn, traceId=%X, spanId=%X] - %msg
```

### Distributed Tracing
`traceId` and `spanId` are automatically injected into the MDC by Micrometer Tracing. These are included in the log pattern to allow correlation across service boundaries and with GCP Cloud Trace.
