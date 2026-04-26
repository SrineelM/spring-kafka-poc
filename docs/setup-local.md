# 🛠️ Local Development Masterclass: Setting Up the Sandbox

This guide details how to bootstrap the `spring-kafka-poc` infrastructure and run the application in a local "Zero-Trust" environment.

---

## 1. Prerequisites

*   **Java 21 (LTS):** The core runtime. Download from [Adoptium](https://adoptium.net/).
*   **Maven 3.9+:** For build and dependency management.
*   **Docker & Docker Compose:** To run the Kafka stack.
*   **Postman or cURL:** For triggering ingestion.

---

## 2. Infrastructure: The Local Cluster

We use a Docker-managed Kafka stack that mimics a production environment.

```bash
# Start the stack
docker-compose up -d
```

**Services Exposed:**
*   **Kafka Broker:** `localhost:9092`
*   **Schema Registry:** `localhost:8081` (Avro schema validation)
*   **Zookeeper:** `localhost:2181`
*   **AKHQ (Optional):** If configured, visit `localhost:8085` to view topics visually.

---

## 3. Running the Application

### 🔹 The "Local" Baseline
The `local` profile uses an **in-memory H2 database** and **auto-seeds** the system with sample transactions.

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

### 🔹 High-Throughput Batch Mode
To test the batch processing logic (500 records per poll), activate the `batch` profile:

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=local,batch
```

---

## 4. Verification: The "Pro-Level" Audit

Once the app is running, perform these checks to ensure everything is working correctly:

### 1️⃣ REST Ingestion (The Gateway)
Submit a transaction and look for the **Correlation ID** in the console logs.
```bash
curl -X POST http://localhost:8080/api/v1/transactions \
-H "Content-Type: application/json" \
-d '{"amount": 5000.00, "accountId": "ACC-PRO-123"}'
```
*   **Check logs for:** `[correlationId=...]`
*   **Check result:** `202 Accepted`

### 2️⃣ Database Inspection (The Persistence)
Access the H2 Console at `http://localhost:8080/h2-console`.
*   **JDBC URL:** `jdbc:h2:mem:transactiondb`
*   **User:** `sa` | **Pass:** `password`
*   **Run:** `SELECT * FROM TRANSACTIONS;` — you should see your record.
*   **Run:** `SELECT * FROM AUDIT_LOG;` — you should see the `RECEIVED` and `PROCESSED` events.

### 3️⃣ Real-Time Analytics (The Stream)
Query the Kafka Streams state store directly via Interactive Queries:
```bash
curl http://localhost:8080/api/v1/analytics/daily-total/ACC-PRO-123
```
*   **Expected:** The running total for that account, fetched from RocksDB.

---

## 5. Troubleshooting

*   **Kafka Connection Refused:** Ensure `docker-compose ps` shows all containers as `Up`.
*   **Schema Registry Error:** The app waits for the registry. If it fails, restart the registry container: `docker-compose restart schema-registry`.
*   **Port Conflict:** If port `8080` is taken, run with `-Dserver.port=8081`.

---

*“A developer is only as good as their feedback loop. Master your local environment.”*
