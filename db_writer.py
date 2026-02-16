# ==============================================================================
# SCRIPT: db_writer.py
# ROLE: Data Sink / Persistence Layer
# DESCRIPTION:
#   This script acts as the Data Sink for the MbuStreamStore architecture.
#   While the live tracker provides real-time visibility, this script is
#   responsible for converting transient Kafka events into permanent,
#   queryable records within a PostgreSQL relational database.
#
# KEY FEATURES:
#   - Idempotent writes (ON CONFLICT DO NOTHING)
#   - Poison pill handling for malformed JSON
#   - Automatic schema initialization
# ==============================================================================

from confluent_kafka import Consumer
import psycopg2
import json
import sys

# 1. Database Connection Configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "stream_store",
    "user": "postgres",
    "password": "pgadmin"
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print(f"✅ Connected to Docker PostgreSQL on port {DB_CONFIG['port']}")

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id UUID PRIMARY KEY,
            username TEXT,
            item TEXT,
            quantity INTEGER,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

except Exception as e:
    print(f"❌ Database connection failed: {e}")
    sys.exit(1)

# 2. Kafka Consumer Setup
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'db-writer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['orders'])

# 3. Processing Loop
try:
    print("🚀 DB Writer is listening for Kafka events...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Kafka Error: {msg.error()}")
            continue

        # --- NEW: Defensive JSON Decoding ---
        try:
            raw_payload = msg.value().decode('utf-8')
            order = json.loads(raw_payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as parse_err:
            print(f"💀 Poison Pill Detected! Skipping invalid message: {parse_err}")
            continue  # Move to next message instead of crashing

        try:
            # Insert into Postgres
            cursor.execute(
                """
                INSERT INTO orders (order_id, username, item, quantity) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT (order_id) DO NOTHING
                """,
                (order['order_id'], order.get('user'), order.get('item'), order.get('quantity'))
            )
            conn.commit()
            print(f"💾 Saved order {order['order_id']} ({order.get('item', 'unknown')}) to PostgreSQL")

        except Exception as db_err:
            print(f"⚠️ Database Error: {db_err}")
            conn.rollback()

except KeyboardInterrupt:
    print("🛑 Stopping DB Writer...")
finally:
    consumer.close()
    cursor.close()
    conn.close()

