import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Adjust for your environment if needed
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "sensor_data"
CASSANDRA_HOST = "localhost"

spark = SparkSession.builder \
    .appName("SensorStreamingPipeline") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .getOrCreate()

# Define the schema of the incoming JSON data
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

# 1. Read data from Kafka topic
df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .load())

# 2. Convert the binary 'value' field into JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

# 3. Simple windowed aggregation: average temperature per sensor in 1-minute windows
agg_df = (json_df
            .groupBy(
                col("sensor_id"),
                window(col("timestamp").cast("timestamp"), "1 minute")
            )
            .avg("temperature")
            .select(
                col("sensor_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("avg(temperature)").alias("avg_temp")
            ))

# 4. Write to Cassandra
#    First, create a keyspace/table if you haven't already:
#      CREATE KEYSPACE sensor_keyspace 
#        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
#
#      CREATE TABLE sensor_keyspace.sensor_aggregates (
#        sensor_id text,
#        window_start timestamp,
#        window_end timestamp,
#        avg_temp double,
#        PRIMARY KEY (sensor_id, window_start)
#      );
#
#    Spark-Cassandra connector must be added to Spark if not included by default.
query = (agg_df
            .writeStream
            .format("org.apache.spark.sql.cassandra")
            .options(table="sensor_aggregates", keyspace="sensor_keyspace")
            .option("checkpointLocation", "/tmp/spark_checkpoints/sensor_pipeline")
            .outputMode("update")
            .start())

# 5. Start the streaming query and wait for termination
query.awaitTermination()
