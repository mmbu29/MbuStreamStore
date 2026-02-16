MbuStreamStore: Real-Time Event Pipeline
A high-performance data engineering pipeline that demonstrates how to ingest, stream, and persist retail transactions using Python, Kafka, and PostgreSQL.
Architecture Overview
The system follows a modern "Source-to-Sink" streaming pattern:
1. Ingestion Layer: Python Producer generates randomized JSON order events.
2. Messaging Layer: Kafka (KRaft mode) acts as a fault-tolerant distributed log.
3. Persistence Layer: Python Consumer (DB Writer) validates data and performs idempotent writes.
4. Storage Layer: PostgreSQL 17 containerized with persistent Docker volumes.
Getting Started
1. Prerequisites
- Docker & Docker Desktop
- Python 3.10+
- psycopg2 and confluent-kafka libraries
2. Infrastructure Setup
Spin up the Kafka broker and PostgreSQL database using Docker Compose:
```
docker compose up -d
```
3. Database Preparation
Initialize the schema (Note: db_writer.py will also attempt to do this automatically):
```
docker exec -it mbu_postgres psql -U postgres -d stream_store -c "ALTER TABLE orders ADD COLUMN price NUMERIC(10, 2);"
```
4. Running the Pipeline
Open two terminals and run:
- Terminal A (The Sink): python db_writer.py
- Terminal B (The Source): python order_producer.py
Infrastructure Details
Service
Port
Technology
Role
Kafka
9092
Confluent 7.8.3
Message Broker
PostgreSQL
5432
Postgres 17
Permanent Storage
Useful Commands
```
psql -h localhost -p 5432 -U postgres -d stream_store -c "SELECT * FROM orders ORDER BY created_at DESC;"
```
Check Kafka Broker Health:
```
docker logs kafka
```
Wipe Environment (Fresh Start):
docker compose down -v
- Idempotency: Uses ON CONFLICT DO NOTHING to prevent duplicate records.
- Fault Tolerance: Kafka retains messages even if the Database Writer goes offline.
- Scalability: The architecture allows for multiple consumers to read from the same Kafka topic.
