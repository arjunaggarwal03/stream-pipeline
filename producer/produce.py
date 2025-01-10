import json
import time
import random
from kafka import KafkaProducer

# Adjust if your Kafka is reachable at a different host/port
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "sensor_data"

def generate_data():
    """Generate a random sensor reading."""
    return {
        "sensor_id": random.randint(1, 5),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "timestamp": time.time()
    }

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
print(f"Producing messages to Kafka topic '{TOPIC_NAME}'. Press Ctrl+C to stop.")

try:
    while True:
        data_point = generate_data()
        producer.send(TOPIC_NAME, json.dumps(data_point).encode('utf-8'))
        print("Sent:", data_point)
        time.sleep(1)  # throttle messages (1 per second)
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.close()
