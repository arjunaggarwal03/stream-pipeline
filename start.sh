#!/bin/bash

echo "Starting up the streaming pipeline..."

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# 1. Stop any existing containers and clean volumes
echo "Cleaning up existing containers..."
docker compose down -v

# 2. Start docker containers (Kafka, Zookeeper, Cassandra)
echo "Starting docker containers..."
docker compose up -d

# Create checkpoint directory
echo "Creating checkpoint directory..."
mkdir -p /tmp/spark_checkpoints/sensor_pipeline

# 3. Wait for Cassandra to be ready
echo "Waiting for Cassandra to initialize..."
until docker exec cassandra cqlsh -e "describe keyspaces" > /dev/null 2>&1; do
  echo "Waiting for Cassandra..."
  sleep 5
done
echo "Cassandra is ready!"

# 4. Initialize Cassandra schema
echo "Initializing Cassandra schema..."
docker exec -i cassandra cqlsh < init-scripts/init.cql

# 5. Start the Kafka producer in the background
echo "Starting Kafka producer..."
python producer/produce.py &
PRODUCER_PID=$!
if [ $? -ne 0 ]; then
    echo "Failed to start producer"
    exit 1
fi

# 6. Start the Spark streaming job
echo "Starting Spark streaming pipeline..."
spark-submit \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0" \
  --conf "spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions" \
  spark_streaming/pipeline.py &
SPARK_PID=$!
if [ $? -ne 0 ]; then
    echo "Failed to start Spark job"
    exit 1
fi

# Start the FastAPI server
echo "Starting API server..."
uvicorn api.app:app --reload --port 8000 &
API_PID=$!
if [ $? -ne 0 ]; then
    echo "Failed to start API server"
    exit 1
fi

echo "All services started. Press Ctrl+C to stop..."

# Cleanup on script termination
cleanup() {
    echo "Shutting down..."
    echo "Stopping producer..."
    kill $PRODUCER_PID 2>/dev/null || true
    echo "Stopping Spark job..."
    kill $SPARK_PID 2>/dev/null || true
    echo "Stopping API server..."
    kill $API_PID 2>/dev/null || true
    echo "Stopping containers..."
    docker compose down
    echo "Cleanup complete"
    exit 0
}

# Ensure cleanup runs on script termination
trap cleanup EXIT
trap cleanup SIGINT SIGTERM

# Wait for all background processes
wait $PRODUCER_PID $SPARK_PID $API_PID 