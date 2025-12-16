import duckdb

# Connect to DuckDB (in-memory)
con = duckdb.connect()

# Query the Parquet files in S3
# Access struct fields using dot notation, and alias the struct first
query = """
SELECT
    symbol,
    "window".start AS window_start,
    "window".end AS window_end,
    avg_price,
    high,
    low,
    volume
FROM read_parquet('s3://smart-streaming-analytics/analytics/*/*.parquet')
LIMIT 10
"""

# Execute the query and get the result as a Pandas DataFrame
df = con.execute(query).df()

# Print the DataFrame
print(df)
























# # test.py
# import duckdb
# import streamlit as st
# import pandas as pd

# # ------------------------
# # Connect to DuckDB
# # ------------------------
# con = duckdb.connect()

# # ------------------------
# # Read Spark OHLCV Parquet from S3
# # ------------------------
# # Use ** backticks ** for `window` because it's a reserved keyword
# query = """
# SELECT
#     symbol,
#     `window`.start AS window_start,
#     `window`.end AS window_end,
#     avg_price,
#     high,
#     low,
#     volume
# FROM read_parquet('s3://smart-streaming-analytics/analytics/*/*.parquet')
# """

# # Execute the query
# df = con.execute(query).df()

# # ------------------------
# # Show first 10 rows
# # ------------------------
# st.write("First 10 rows of OHLCV data:")
# st.dataframe(df.head(10))

# # ------------------------
# # Simple visualization for BTC-USD
# # ------------------------
# btc_df = df[df['symbol'] == 'BTC-USD'].copy()
# btc_df['window_start'] = pd.to_datetime(btc_df['window_start'])
# btc_df.set_index('window_start', inplace=True)

# st.line_chart(btc_df[['avg_price', 'high', 'low']])

# st.bar_chart(btc_df['volume'])
