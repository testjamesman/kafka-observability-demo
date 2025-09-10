import os
import json
import random
import time
from kafka import KafkaConsumer

# --- Configuration ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPICS = ["orders", "payments", "inventory_updates", "user_activity"]

def create_consumer():
    """Creates a Kafka consumer, retrying until it connects."""
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BROKER,
                group_id='observability-demo-group',
                auto_offset_reset='earliest', # Start reading at the earliest message
                value_deserializer=lambda x: x.decode('utf-8') # Decode messages from bytes to string
            )
            print("✅ Kafka Consumer connected successfully.")
            return consumer
        except Exception as e:
            print(f"🔥 Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    consumer = create_consumer()
    print(f"👂 Consumer is now listening to topics: {', '.join(TOPICS)}")

    for message in consumer:
        print(f"\n📬 Received message from topic: {message.topic}, partition: {message.partition}, offset: {message.offset}")

        # --- Defensive Programming: Handle potential malformed JSON ---
        try:
            # Try to parse the message value as JSON
            data = json.loads(message.value)
            print(f"   ✅ Successfully parsed JSON: {data}")
            
            # Simulate some processing time
            print("   ⚙️  Simulating data processing...")
            time.sleep(random.uniform(0.05, 0.2)) # Simulate I/O or CPU work
            print("   ✅ Processing complete.")

        except json.JSONDecodeError:
            # This block executes if json.loads() fails
            print(f"   ❌ ERROR: Failed to decode JSON.")
            print(f"   ➡️  Raw message value: '{message.value}'")
            # In a real application, you might send this to a dead-letter queue
        
        except Exception as e:
            # Catch any other unexpected errors during processing
            print(f"   🔥 An unexpected error occurred: {e}")
