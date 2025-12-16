from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# ------------------------
# Config (copied directly)
# ------------------------
KAFKA_BROKER = "localhost:9092"

KAFKA_TOPIC_RAW = "market_trades_raw"
KAFKA_TOPIC_CLEAN = "market_trades_cleaned"

# ------------------------
# Spark session
# ------------------------
spark = (
    SparkSession.builder
    .appName("CleanCoinbaseTrades")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ------------------------
# Schema for incoming trades
# ------------------------
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("trade_id", LongType(), True),
    StructField("trade_time", StringType(), True),
    StructField("exchange", StringType(), True)
])

# ------------------------
# Read raw trades from Kafka
# ------------------------
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC_RAW)
    .option("startingOffsets", "earliest")
    .load()
)

# ------------------------
# Parse JSON from Kafka 'value'
# ------------------------
parsed_df = (
    raw_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# ------------------------
# Filter and transform
# ------------------------
clean_df = (
    parsed_df
    .filter((col("price") > 0) & (col("quantity") > 0))
    .withColumn("event_time", to_timestamp(col("trade_time")))
)

# ------------------------
# Write cleaned trades back to Kafka
# ------------------------
query = (
    clean_df
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", KAFKA_TOPIC_CLEAN)
    .option("checkpointLocation", "checkpoints/clean")
    .start()
)


query.awaitTermination()





