import os
import json
import time
import random
from faker import Faker
from confluent_kafka import Producer  # <-- IMPORT CHANGE

# --- Configuration ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
MESSAGES_PER_SECOND = int(os.environ.get("MESSAGES_PER_SECOND", 10))
ERROR_RATE = float(os.environ.get("ERROR_RATE", 0.05))

# Topics
TOPICS = ["orders", "payments", "inventory_updates", "user_activity"]

# Initialize Faker for generating mock data
fake = Faker()

# --- NEW: Delivery report callback for the producer ---
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        # Message delivered successfully
        pass

def create_producer():
    """Creates a Kafka producer using the Confluent library."""
    # The Confluent producer takes a dictionary of configuration settings.
    conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(conf)
    print("✅ Confluent Kafka Producer created successfully.")
    return producer

# (The data generation functions remain unchanged)
def generate_order_data():
    return {"order_id": fake.uuid4(), "user_id": fake.random_int(min=1000, max=9999), "items": [{"item_id": fake.uuid4(), "quantity": fake.random_int(min=1, max=5)} for _ in range(random.randint(1, 3))], "total_value": round(random.uniform(10.0, 500.0), 2), "timestamp": fake.iso8601()}
def generate_payment_data():
    return {"payment_id": fake.uuid4(), "order_id": fake.uuid4(), "status": random.choice(["success", "failed", "pending"]), "amount": round(random.uniform(10.0, 500.0), 2), "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"]), "timestamp": fake.iso8601()}
def generate_inventory_update():
    return {"item_id": fake.uuid4(), "change": random.randint(-10, -1), "new_stock_level": random.randint(0, 100), "warehouse_id": f"WH{random.randint(1,5)}", "timestamp": fake.iso8601()}
def generate_user_activity():
    return {"user_id": fake.random_int(min=1000, max=9999), "activity_type": random.choice(["page_view", "search", "add_to_cart", "login"]), "page_url": fake.uri(), "session_id": fake.uuid4(), "timestamp": fake.iso8601()}

DATA_GENERATORS = {"orders": generate_order_data, "payments": generate_payment_data, "inventory_updates": generate_inventory_update, "user_activity": generate_user_activity}

if __name__ == "__main__":
    producer = create_producer()
    sleep_interval = 1.0 / MESSAGES_PER_SECOND
    print(f"🚀 Starting load generation: {MESSAGES_PER_SECOND} msg/sec, Error Rate: {ERROR_RATE*100}%")

    while True:
        # --- THIS SECTION IS REWRITTEN FOR THE CONFLUENT CLIENT ---
        producer.poll(0) # Serve delivery reports (callbacks) from previous produce() calls

        topic = random.choice(TOPICS)
        
        if random.random() < ERROR_RATE:
            message = "this-is-not-a-valid-json-string"
            print(f"💥 Intentionally sending malformed message to topic: {topic}")
        else:
            data = DATA_GENERATORS[topic]()
            message = json.dumps(data)
            print(f"✉️  Sending message to topic '{topic}': {message[:80]}...")

        # The produce method is non-blocking. Pass the delivery_report callback.
        producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
        # -----------------------------------------------------------

        time.sleep(sleep_interval)

    # producer.flush() # Call this on exit to ensure all messages are sent.