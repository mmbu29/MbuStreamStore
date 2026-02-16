# ==============================================================================
# SCRIPT: order_tracker.py
# ROLE: Real-Time Observability / Console Logger
# DESCRIPTION:
#   This script serves as the live monitoring component of the MbuStreamStore.
#   It provides immediate, human-readable visibility into the Kafka 'orders'
#   stream. Unlike the DB Writer, this service focuses on low-latency
#   tracking and terminal-based debugging.
#
# KEY FEATURES:
#   - Real-time stream decoding
#   - Independent Consumer Group (order-tracker)
#   - Lightweight, non-persistent monitoring
# ==============================================================================

import json

from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("🟢 Consumer is running and subscribed to orders topic")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Error:", msg.error())
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(f"📦 Received order: {order['quantity']} x {order['item']} from {order['user']}")
except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()
