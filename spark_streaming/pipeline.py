import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, explode, map_keys, map_values,
    array_max, array_min, to_timestamp, struct, lit, map_entries, min_by, max_by
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    MapType, TimestampType
)
from redis import Redis
import json
from datetime import datetime
import logging

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "crypto_prices"
CASSANDRA_HOST = "localhost"

# Initialize Redis client
redis_client = Redis(host='localhost', port=6379)
CACHE_TTL = 3600  # 1 hour in seconds

# Define minimum profit threshold (in percentage)
MIN_PROFIT_THRESHOLD = 0.01  # 0.5%

# Initialize logger
logger = logging.getLogger(__name__)

def process_batch(batch_df, batch_id):
    try:
        if batch_df.count() > 0:
            logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
            
            # Write to Cassandra
            write_df = batch_df \
                .withColumn("timestamp", col("timestamp").cast("timestamp"))
            
            # Debug log the data being written
            write_df.show()
            
            write_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="arbitrage_opportunities", keyspace="crypto_keyspace") \
                .save()
            
            # Cache profitable opportunities in Redis
            rows = write_df.filter(col("profit_pct") >= MIN_PROFIT_THRESHOLD).collect()
            logger.info(f"Found {len(rows)} profitable opportunities")
            
            for row in rows:
                cache_key = f"arbitrage:{row.trading_pair}:{row.timestamp}"
                cache_value = json.dumps({
                    'timestamp': row.timestamp.isoformat(),
                    'trading_pair': row.trading_pair,
                    'buy_exchange': row.buy_exchange,
                    'sell_exchange': row.sell_exchange,
                    'buy_price': row.buy_price,
                    'sell_price': row.sell_price,
                    'profit_pct': row.profit_pct,
                    'volume_limit': row.volume_limit
                })
                redis_client.setex(cache_key, CACHE_TTL, cache_value)
                
            logger.info(f"Successfully processed batch {batch_id}")
    except Exception as e:
        logger.error(f"Error in batch {batch_id}: {str(e)}", exc_info=True)
        raise e

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CryptoArbitrageDetection") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .config("spark.cassandra.connection.localDC", "datacenter1") \
        .getOrCreate()

    # Define schema for incoming data
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("trading_pair", StringType(), True),
        StructField("prices", MapType(StringType(), DoubleType()), True),
        StructField("max_diff", DoubleType(), True),
        StructField("max_diff_pct", DoubleType(), True)
    ])

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON data
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Find arbitrage opportunities
    opportunities_df = parsed_df \
        .withWatermark("timestamp", "1 minute") \
        .select(
            col("timestamp"),
            col("trading_pair"),
            explode(map_entries(col("prices"))).alias("price_entry")
        ) \
        .groupBy("timestamp", "trading_pair") \
        .agg(
            min_by(struct("price_entry.*"), "price_entry.value").alias("min_price_struct"),
            max_by(struct("price_entry.*"), "price_entry.value").alias("max_price_struct")
        ) \
        .select(
            to_timestamp("timestamp").alias("timestamp"),
            col("trading_pair"),
            col("min_price_struct.key").alias("buy_exchange"),
            col("max_price_struct.key").alias("sell_exchange"),
            col("min_price_struct.value").alias("buy_price"),
            col("max_price_struct.value").alias("sell_price"),
            ((col("max_price_struct.value") - col("min_price_struct.value")) / 
             col("min_price_struct.value") * 100).alias("profit_pct")
        ) \
        .filter(col("profit_pct") > MIN_PROFIT_THRESHOLD)

    # Add volume limit (placeholder for now)
    final_df = opportunities_df.withColumn("volume_limit", lit(0.0))

    # Write to Cassandra using foreachBatch
    query = final_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/spark_checkpoints/crypto_pipeline") \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        spark.stop()
    except Exception as e:
        print(f"Error in streaming: {str(e)}")
        spark.stop()

if __name__ == "__main__":
    main()
