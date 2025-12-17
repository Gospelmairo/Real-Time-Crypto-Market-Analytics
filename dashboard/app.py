import streamlit as st
import duckdb
import plotly.express as px

# -----------------------------
# Page Config
# -----------------------------
st.set_page_config(
    page_title="Crypto Streaming Analytics",
    layout="wide"
)

st.title("📈 Real-Time Crypto Market Analytics")

# -----------------------------
# DuckDB Connection (SAFE)
# -----------------------------
@st.cache_resource
def get_connection():
    con = duckdb.connect()
    # REQUIRED for S3
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    return con

con = get_connection()

# -----------------------------
# Sidebar Filters
# -----------------------------
st.sidebar.header("Filters")

@st.cache_data(ttl=60)
def load_symbols():
    return con.execute("""
        SELECT DISTINCT symbol
        FROM read_parquet('s3://smart-streaming-analytics/analytics/**/*.parquet')
        ORDER BY symbol
    """).df()

symbols_df = load_symbols()

if symbols_df.empty:
    st.warning("No symbols found yet. Waiting for analytics data...")
    st.stop()

symbols = symbols_df["symbol"].tolist()

selected_symbol = st.sidebar.selectbox(
    "Select Symbol",
    symbols
)

# -----------------------------
# Load Analytics Data (CACHED)
# -----------------------------
@st.cache_data(ttl=30)
def load_data(symbol):
    query = f"""
    SELECT
        symbol,
        struct_extract("window", 'start') AS window_start,
        struct_extract("window", 'end')   AS window_end,
        avg_price,
        high,
        low,
        volume
    FROM read_parquet('s3://smart-streaming-analytics/analytics/**/*.parquet')
    WHERE symbol = '{symbol}'
    ORDER BY window_start DESC
    LIMIT 500
    """
    return con.execute(query).df()

df = load_data(selected_symbol)

# -----------------------------
# Guard: Empty Data
# -----------------------------
if df.empty:
    st.warning(f"No data found yet for {selected_symbol}.")
    st.stop()

# -----------------------------
# Metrics
# -----------------------------
latest = df.iloc[0]

c1, c2, c3, c4 = st.columns(4)

c1.metric("Avg Price", f"${latest.avg_price:,.2f}")
c2.metric("High", f"${latest.high:,.2f}")
c3.metric("Low", f"${latest.low:,.2f}")
c4.metric("Volume", f"{latest.volume:,.4f}")

# -----------------------------
# Price Chart
# -----------------------------
st.subheader("Average Price Over Time")

fig_price = px.line(
    df.sort_values("window_start"),
    x="window_start",
    y="avg_price",
    title=f"{selected_symbol} — Average Price"
)

st.plotly_chart(fig_price, use_container_width=True)

# -----------------------------
# Volume Chart
# -----------------------------
st.subheader("Volume Over Time")

fig_vol = px.bar(
    df.sort_values("window_start"),
    x="window_start",
    y="volume",
    title=f"{selected_symbol} — Volume"
)

st.plotly_chart(fig_vol, use_container_width=True)

# -----------------------------
# Raw Data
# -----------------------------
with st.expander("Show Raw Data"):
    st.dataframe(df, use_container_width=True)








