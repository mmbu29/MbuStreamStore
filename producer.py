# ==============================================================================
# SCRIPT: order_producer.py
# ROLE: Data Ingestion / Event Producer
# DESCRIPTION:
#   This script serves as the entry point for the MbuStreamStore architecture.
#   It simulates user activity by generating structured order events,
#   assigning unique UUIDs for idempotency, and publishing the data to
#   the Kafka 'orders' topic.
#
# KEY FEATURES:
#   - Unique Event ID generation (UUID v4)
#   - Asynchronous message delivery with delivery callbacks
#   - Schema-compliant JSON serialization
# ==============================================================================

import json
import uuid

from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode('utf-8')}")
        print(f"✅ Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")

order = {
    "order_id": str(uuid.uuid4()),
    "user": "Joe",
    "item": "wine",
    "quantity": 10
}

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)

producer.flush()

