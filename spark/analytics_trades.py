# analytics_trades.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, max, min, sum, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
import os

# ------------------------
# CONFIGURATION
# ------------------------
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC_CLEAN = "market_trades_cleaned"

S3_BUCKET_PATH = "s3a://smart-streaming-analytics/analytics/"
S3_CHECKPOINT_PATH = "s3a://smart-streaming-analytics/checkpoints/analytics/"

# ------------------------
# SPARK SESSION
# ------------------------
spark = (
    SparkSession.builder
    .appName("MarketAnalyticsToS3")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Explicitly set all S3A timeouts in milliseconds
    .config("spark.hadoop.fs.s3a.connection.timeout", 60000)
    .config("spark.hadoop.fs.s3a.socket.timeout", 60000)
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", 60000)
    .config("spark.hadoop.fs.s3a.attempts.maximum", 10)
    .config("spark.hadoop.fs.s3a.retry.limit", 10)
    .config("spark.hadoop.fs.s3a.paging.maximum", 1000)
    # AWS credentials (use environment variables or fill manually)
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ------------------------
# DEFINE SCHEMA
# ------------------------
schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("trade_id", LongType()),
    StructField("trade_time", StringType()),
    StructField("exchange", StringType()),
    StructField("event_time", TimestampType())
])

# ------------------------
# READ CLEANED TRADES FROM KAFKA
# ------------------------
trades_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC_CLEAN)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = trades_df.selectExpr("CAST(value AS STRING) as json")
df = parsed_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# ------------------------
# ANALYTICS: 1-MINUTE OHLCV
# ------------------------
analytics_df = (
    df
    .withWatermark("event_time", "2 minutes")  # watermark for late data
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol")
    )
    .agg(
        avg("price").alias("avg_price"),
        max("price").alias("high"),
        min("price").alias("low"),
        sum("quantity").alias("volume")
    )
)

# ------------------------
# WRITE STREAM
# ------------------------
WRITE_TO_CONSOLE = False  # toggle True for testing

if WRITE_TO_CONSOLE:
    query = (
        analytics_df
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
    )
else:
    query = (
        analytics_df
        .writeStream
        .format("parquet")
        .option("path", S3_BUCKET_PATH)
        .option("checkpointLocation", S3_CHECKPOINT_PATH)
        .partitionBy("symbol")
        .outputMode("append")
        .start()
    )

query.awaitTermination()
























# # analytics_trades.py
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, window, avg, max, min, sum, from_json
# from pyspark.sql.types import (
#     StructType, StructField, StringType,
#     DoubleType, LongType, TimestampType
# )
# import os

# # ------------------------
# # CONFIGURATION
# # ------------------------
# KAFKA_BROKER = "localhost:9092"
# KAFKA_TOPIC_CLEAN = "market_trades_cleaned"

# S3_BUCKET_PATH = "s3a://smart-streaming-analytics/analytics/"
# S3_CHECKPOINT_PATH = "s3a://smart-streaming-analytics/checkpoints/analytics/"

# # ------------------------
# # SPARK SESSION
# # ------------------------
# spark = (
#     SparkSession.builder
#     .appName("MarketAnalyticsToS3")
#     .config(
#         "spark.jars.packages",
#         ",".join([
#             "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
#             "org.apache.hadoop:hadoop-aws:3.4.1",
#             "com.amazonaws:aws-java-sdk-bundle:1.12.262"
#         ])
#     )
#     # --- S3A FILESYSTEM ---
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     # --- Timeouts as strings in milliseconds ---
#     .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
#     .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
#     .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
#     .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
#     .config("spark.hadoop.fs.s3a.retry.limit", "10")
#     .config("spark.hadoop.fs.s3a.paging.maximum", "1000")
#     # --- AWS Credentials ---
#     .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
#     .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")

# # ------------------------
# # DEFINE SCHEMA
# # ------------------------
# schema = StructType([
#     StructField("symbol", StringType()),
#     StructField("price", DoubleType()),
#     StructField("quantity", DoubleType()),
#     StructField("trade_id", LongType()),
#     StructField("trade_time", StringType()),
#     StructField("exchange", StringType()),
#     StructField("event_time", TimestampType())
# ])

# # ------------------------
# # READ CLEANED TRADES FROM KAFKA
# # ------------------------
# trades_df = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", KAFKA_BROKER)
#     .option("subscribe", KAFKA_TOPIC_CLEAN)
#     .option("startingOffsets", "latest")
#     .load()
# )

# parsed_df = trades_df.selectExpr("CAST(value AS STRING) AS json")

# df = (
#     parsed_df
#     .select(from_json(col("json"), schema).alias("data"))
#     .select("data.*")
# )

# # ------------------------
# # ANALYTICS: 15-SECOND OHLCV (testing)
# # ------------------------
# analytics_df = (
#     df
#     .withWatermark("event_time", "10 seconds")  # faster watermark for testing
#     .groupBy(
#         window(col("event_time"), "15 seconds"),  # smaller window for fast testing
#         col("symbol")
#     )
#     .agg(
#         avg("price").alias("avg_price"),
#         max("price").alias("high"),
#         min("price").alias("low"),
#         sum("quantity").alias("volume")
#     )
# )

# # ------------------------
# # WRITE STREAM
# # ------------------------
# WRITE_TO_CONSOLE = False  # set True to debug without S3

# if WRITE_TO_CONSOLE:
#     query = (
#         analytics_df
#         .writeStream
#         .outputMode("update")
#         .format("console")
#         .option("truncate", False)
#         .trigger(processingTime="30 seconds")  # micro-batch every 5 seconds
#         .start()
#     )
# else:
#     query = (
#         analytics_df
#         .writeStream
#         .format("parquet")
#         .option("path", S3_BUCKET_PATH)
#         .option("checkpointLocation", S3_CHECKPOINT_PATH)
#         .partitionBy("symbol")
#         .outputMode("append")
#         .trigger(processingTime="30 seconds")  # micro-batch every 5 seconds
#         .start()
#     )

# query.awaitTermination()










































































# # analytics_trades.py
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, window, avg, max, min, sum, from_json
# from pyspark.sql.types import (
#     StructType, StructField, StringType,
#     DoubleType, LongType, TimestampType
# )
# import os

# # ------------------------
# # CONFIGURATION
# # ------------------------
# KAFKA_BROKER = "localhost:9092"
# KAFKA_TOPIC_CLEAN = "market_trades_cleaned"

# S3_BUCKET_PATH = "s3a://smart-streaming-analytics/analytics/"
# S3_CHECKPOINT_PATH = "s3a://smart-streaming-analytics/checkpoints/analytics/"

# # ------------------------
# # SPARK SESSION
# # ------------------------
# spark = (
#     SparkSession.builder
#     .appName("MarketAnalyticsToS3")

#     # S3A filesystem
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

#     # Timeouts (milliseconds, numeric strings)
#     .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
#     .config("spark.hadoop.fs.s3a.socket.timeout", "60000")

#     # AWS credentials (from env)
#     .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
#     .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))

#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")

# # ------------------------
# # SCHEMA
# # ------------------------
# schema = StructType([
#     StructField("symbol", StringType()),
#     StructField("price", DoubleType()),
#     StructField("quantity", DoubleType()),
#     StructField("trade_id", LongType()),
#     StructField("trade_time", StringType()),
#     StructField("exchange", StringType()),
#     StructField("event_time", TimestampType())
# ])

# # ------------------------
# # READ FROM KAFKA
# # ------------------------
# trades_df = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", KAFKA_BROKER)
#     .option("subscribe", KAFKA_TOPIC_CLEAN)
#     .option("startingOffsets", "latest")
#     .load()
# )

# df = (
#     trades_df
#     .selectExpr("CAST(value AS STRING) AS json")
#     .select(from_json(col("json"), schema).alias("data"))
#     .select("data.*")
# )

# # ------------------------
# # 1-MINUTE OHLCV
# # ------------------------
# analytics_df = (
#     df
#     .withWatermark("event_time", "2 minutes")
#     .groupBy(
#         window(col("event_time"), "1 minute"),
#         col("symbol")
#     )
#     .agg(
#         avg("price").alias("avg_price"),
#         max("price").alias("high"),
#         min("price").alias("low"),
#         sum("quantity").alias("volume")
#     )
# )

# # ------------------------
# # WRITE STREAM
# # ------------------------
# WRITE_TO_CONSOLE = False  # set True to test without S3

# if WRITE_TO_CONSOLE:
#     query = (
#         analytics_df.writeStream
#         .outputMode("update")
#         .format("console")
#         .option("truncate", False)
#         .start()
#     )
# else:
#     query = (
#         analytics_df.writeStream
#         .format("parquet")
#         .option("path", S3_BUCKET_PATH)
#         .option("checkpointLocation", S3_CHECKPOINT_PATH)
#         .partitionBy("symbol")
#         .outputMode("append")
#         .start()
#     )

# query.awaitTermination()



























