# Local Development Setup Guide

This guide provides instructions for setting up and running the `spring-kafka-poc` application on a local machine.

## 1. Prerequisites

- **Java 21:** The project is built on Java 21.
- **Maven:** The project uses Maven for dependency management and build automation.
- **Docker:** A local Kafka cluster is run via Docker Compose.

## 2. Starting the Kafka Cluster

The project includes a `docker-compose.yml` file to spin up a local Kafka cluster, including Zookeeper, a Kafka broker, and a Schema Registry.

```bash
# From the project root directory
docker-compose up -d
```

This will start the following services:
- **Zookeeper:** `localhost:2181`
- **Kafka Broker:** `localhost:9092`
- **Schema Registry:** `localhost:8081`

## 3. Running the Application

The application is a standard Spring Boot project and can be run from your IDE or via the Maven wrapper.

### From Your IDE
- Import the project as a Maven project.
- Run the `SpringKafkaPocApplication` main class.

### From the Command Line
```bash
# This will build the project and run all tests
./mvnw clean install

# This will run the application
./mvnw spring-boot:run
```

The application will start with the `local` profile active by default, which configures it to connect to the local Kafka cluster and use an in-memory H2 database.

## 4. Activating Profiles

The application uses Spring profiles to control its behavior.

- **`local` (default):** Uses H2 database, single-record Kafka consumer.
- **`batch`:** Activates the batch Kafka consumer for higher throughput.

To run with the batch consumer active:
```bash
# From the command line
./mvnw spring-boot:run -Dspring-boot.run.profiles=local,batch

# Or set the environment variable
export SPRING_PROFILES_ACTIVE=local,batch
./mvnw spring-boot:run
```

## 5. Verifying the Setup

Once the application is running, you can interact with it via its REST endpoints.

**Send a transaction:**
```bash
curl -X POST http://localhost:8080/api/v1/transactions \
-H "Content-Type: application/json" \
-d '{"amount": 123.45, "accountId": "ACC-TEST-001"}'
```

**Query the audit trail:**
```bash
curl http://localhost:8080/api/v1/audit/transaction/{transactionId}
```

**Query the real-time analytics:**
```bash
curl http://localhost:8080/api/v1/analytics/daily-total/ACC-TEST-001
```

## 6. Database Schema
When running with the `local` profile, the application uses an in-memory H2 database. The schema for the `Transactions`, `Outbox`, `AuditLog`, and `INT_LOCK` tables is created automatically on startup (`spring.jpa.hibernate.ddl-auto=create-drop`).

You can access the H2 console at `http://localhost:8080/h2-console` to inspect the database.
- **JDBC URL:** `jdbc:h2:mem:testdb`
- **User Name:** `sa`
- **Password:** (leave blank)
