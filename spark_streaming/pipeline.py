import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from redis import Redis
import json
from datetime import datetime

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "sensor_data"
CASSANDRA_HOST = "localhost"

# Initialize Redis client
redis_client = Redis(host='localhost', port=6379)
CACHE_TTL = 3600  # 1 hour in seconds

def process_batch(batch_df, batch_id):
    try:
        if batch_df.count() > 0:
            # Write to Cassandra
            write_df = batch_df.withColumn("window_start", col("window_start").cast("timestamp")) \
                             .withColumn("window_end", col("window_end").cast("timestamp"))
            
            write_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="sensor_aggregates", keyspace="sensor_keyspace") \
                .save()
            
            # Cache in Redis
            rows = write_df.collect()
            for row in rows:
                cache_key = f"sensor:{row.sensor_id}:latest"
                cache_value = json.dumps({
                    'sensor_id': row.sensor_id,
                    'window_start': row.window_start.isoformat(),
                    'window_end': row.window_end.isoformat(),
                    'avg_temp': row.avg_temp,
                    'cached_at': datetime.now().isoformat()
                })
                redis_client.setex(cache_key, CACHE_TTL, cache_value)
                
            print(f"Successfully processed batch {batch_id}")
    except Exception as e:
        print(f"Error in batch {batch_id}: {str(e)}")
        raise e

def main():
    print("Starting Spark Streaming Pipeline...")
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("SensorStreamingPipeline") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .config("spark.cassandra.connection.localDC", "datacenter1") \
        .getOrCreate()

    def cleanup(sig=None, frame=None):
        print("Stopping Spark Streaming...")
        spark.stop()
        sys.exit(0)

    import signal
    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)

    # Define schema for incoming data
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("timestamp", DoubleType(), True)
    ])

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # Process the stream
    json_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))

    # Window aggregation
    windowed_df = json_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            col("sensor_id"),
            window(col("timestamp"), "1 minute")
        ) \
        .avg("temperature") \
        .select(
            col("sensor_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg(temperature)").alias("avg_temp")
        )

    # Write to Cassandra using foreachBatch
    query = windowed_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoints/sensor_pipeline") \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        cleanup()
    except Exception as e:
        print(f"Error in streaming: {str(e)}")
        cleanup()

if __name__ == "__main__":
    main()
