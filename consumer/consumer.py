import os
import json
import random
import time
from confluent_kafka import Consumer, KafkaError  # <-- IMPORT CHANGE

# --- Configuration ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPICS = ["orders", "payments", "inventory_updates", "user_activity"]

def create_consumer():
    """Creates a Kafka consumer using the Confluent library."""
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'observability-demo-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    print("✅ Confluent Kafka Consumer created successfully.")
    return consumer

if __name__ == "__main__":
    consumer = create_consumer()
    consumer.subscribe(TOPICS) # <-- Subscribe to topics
    print(f"👂 Consumer is now listening to topics: {', '.join(TOPICS)}")

    try:
        # The new consumer loop polls for messages continuously
        while True:
            msg = consumer.poll(timeout=1.0) # Poll for a message with a 1-second timeout

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"Reached end of partition for {msg.topic()} [{msg.partition()}]")
                else:
                    print(f"Consumer error: {msg.error()}")
                continue

            # Message received successfully
            message_value = msg.value().decode('utf-8')
            print(f"\n📬 Received message from topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()}")

            try:
                data = json.loads(message_value)
                print(f"   ✅ Successfully parsed JSON: {data}")
                
                print("   ⚙️  Simulating data processing...")
                time.sleep(random.uniform(0.05, 0.2))
                print("   ✅ Processing complete.")

            except json.JSONDecodeError:
                print(f"   ❌ ERROR: Failed to decode JSON.")
                print(f"   ➡️  Raw message value: '{message_value}'")
            
            except Exception as e:
                print(f"   🔥 An unexpected error occurred: {e}")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
