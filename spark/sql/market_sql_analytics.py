from pyspark.sql import SparkSession

# -------------------------------
# Spark Session
# -------------------------------
spark = (
    SparkSession.builder
    .appName("MarketSQLAnalytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# Load Parquet Analytics
# -------------------------------
analytics_df = spark.read.parquet(
    "storage/analytics/parquet"
)

# Register temp view
analytics_df.createOrReplaceTempView("market_analytics")

print("\n✅ market_analytics view created\n")

# -------------------------------
# 1️⃣ Latest BTC Price Snapshot
# -------------------------------
print("📌 Latest BTC Snapshot")
spark.sql("""
    SELECT
        symbol,
        window.start AS window_start,
        window.end   AS window_end,
        avg_price,
        high,
        low,
        volume
    FROM market_analytics
    WHERE symbol = 'BTC-USD'
    ORDER BY window.start DESC
    LIMIT 5
""").show(truncate=False)

# -------------------------------
# 2️⃣ Highest Volume Windows
# -------------------------------
print("📌 Highest Volume Windows")
spark.sql("""
    SELECT
        window.start AS window_start,
        window.end   AS window_end,
        volume
    FROM market_analytics
    WHERE symbol = 'BTC-USD'
    ORDER BY volume DESC
    LIMIT 5
""").show(truncate=False)

# -------------------------------
# 3️⃣ Daily VWAP
# -------------------------------
print("📌 Daily VWAP")
spark.sql("""
    SELECT
        symbol,
        date,
        SUM(avg_price * volume) / SUM(volume) AS vwap
    FROM market_analytics
    WHERE symbol = 'BTC-USD'
    GROUP BY symbol, date
    ORDER BY date DESC
""").show(truncate=False)

# -------------------------------
# 4️⃣ Volatility (High - Low)
# -------------------------------
print("📌 Most Volatile Windows")
spark.sql("""
    SELECT
        window.start AS window_start,
        (high - low) AS volatility
    FROM market_analytics
    WHERE symbol = 'BTC-USD'
    ORDER BY volatility DESC
    LIMIT 5
""").show(truncate=False)

