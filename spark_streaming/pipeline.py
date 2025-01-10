import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, from_unixtime
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
        .option("failOnDataLoss", "false")
        .option("startingOffsets", "latest")
        .load())

# 2. Convert the binary 'value' field into JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

# First cast timestamp to proper type
json_df_with_timestamp = json_df \
    .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))

# Add watermark to handle late data
json_df_with_watermark = json_df_with_timestamp \
    .withWatermark("timestamp", "1 minute")

# 3. Simple windowed aggregation: average temperature per sensor in 1-minute windows
agg_df = (json_df_with_watermark
            .groupBy(
                col("sensor_id"),
                window(col("timestamp"), "1 minute")
            )
            .avg("temperature")
            .select(
                col("sensor_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("avg(temperature)").alias("avg_temp")
            ))

# 4. Write to Cassandra
query = (agg_df
            .writeStream
            .format("org.apache.spark.sql.cassandra")
            .options(table="sensor_aggregates", keyspace="sensor_keyspace")
            .option("checkpointLocation", "/tmp/spark_checkpoints/sensor_pipeline")
            .outputMode("append")
            .start())

# 5. Start the streaming query and wait for termination
query.awaitTermination()
