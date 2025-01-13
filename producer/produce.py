import json
import time
import random
from kafka import KafkaProducer
import sys

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

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print(f"Producing messages to Kafka topic '{TOPIC_NAME}'. Press Ctrl+C to stop.")

try:
    while True:
        for sensor_id in range(1, 6):
            data = generate_data()
            producer.send('sensor_data', data)
            print(f"Sent: {data}")
        time.sleep(1)
except KeyboardInterrupt:
    print("\nGracefully shutting down producer...")
    producer.close()
    sys.exit(0)
except Exception as e:
    print(f"Error in producer: {str(e)}")
    producer.close()
    sys.exit(1)
