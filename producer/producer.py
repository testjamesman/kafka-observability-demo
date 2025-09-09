import os
import json
import time
import random
from faker import Faker
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
MESSAGES_PER_SECOND = int(os.environ.get("MESSAGES_PER_SECOND", 10))
ERROR_RATE = float(os.environ.get("ERROR_RATE", 0.05)) # 5% error rate by default

# Topics
TOPICS = ["orders", "payments", "inventory_updates", "user_activity"]

# Initialize Faker for generating mock data
fake = Faker()

def create_producer():
    """Creates a Kafka producer, retrying until it connects."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: str(v).encode('utf-8') # Send messages as strings
            )
            print("✅ Kafka Producer connected successfully.")
            return producer
        except Exception as e:
            print(f"🔥 Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def generate_order_data():
    """Generates a fake e-commerce order."""
    return {
        "order_id": fake.uuid4(),
        "user_id": fake.random_int(min=1000, max=9999),
        "items": [
            {"item_id": fake.uuid4(), "quantity": fake.random_int(min=1, max=5)}
            for _ in range(random.randint(1, 3))
        ],
        "total_value": round(random.uniform(10.0, 500.0), 2),
        "timestamp": fake.iso8601(),
    }

def generate_payment_data():
    """Generates fake payment data."""
    return {
        "payment_id": fake.uuid4(),
        "order_id": fake.uuid4(),
        "status": random.choice(["success", "failed", "pending"]),
        "amount": round(random.uniform(10.0, 500.0), 2),
        "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"]),
        "timestamp": fake.iso8601(),
    }

def generate_inventory_update():
    """Generates a fake inventory update."""
    return {
        "item_id": fake.uuid4(),
        "change": random.randint(-10, -1),
        "new_stock_level": random.randint(0, 100),
        "warehouse_id": f"WH{random.randint(1,5)}",
        "timestamp": fake.iso8601(),
    }

def generate_user_activity():
    """Generates a fake user activity event."""
    return {
        "user_id": fake.random_int(min=1000, max=9999),
        "activity_type": random.choice(["page_view", "search", "add_to_cart", "login"]),
        "page_url": fake.uri(),
        "session_id": fake.uuid4(),
        "timestamp": fake.iso8601(),
    }

# Mapping of topics to their data generation functions
DATA_GENERATORS = {
    "orders": generate_order_data,
    "payments": generate_payment_data,
    "inventory_updates": generate_inventory_update,
    "user_activity": generate_user_activity,
}

if __name__ == "__main__":
    producer = create_producer()
    sleep_interval = 1.0 / MESSAGES_PER_SECOND

    print(f"🚀 Starting load generation: {MESSAGES_PER_SECOND} msg/sec, Error Rate: {ERROR_RATE*100}%")

    while True:
        topic = random.choice(TOPICS)
        
        # Decide whether to send a malformed message
        if random.random() < ERROR_RATE:
            # Send a malformed JSON string
            message = "this-is-not-a-valid-json-string"
            print(f"💥 Intentionally sending malformed message to topic: {topic}")
        else:
            # Send a valid JSON message
            data = DATA_GENERATORS[topic]()
            message = json.dumps(data)
            print(f"✉️  Sending message to topic '{topic}': {message[:80]}...")

        # Send the message (either good or bad)
        producer.send(topic, value=message)
        producer.flush()

        time.sleep(sleep_interval)
