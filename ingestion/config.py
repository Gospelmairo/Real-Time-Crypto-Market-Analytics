import os


REAL_IP = "192.168.1.195" 

# Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", f"{REAL_IP}:9092")
KAFKA_TOPIC = os.getenv("TOPIC_RAW", "market_trades_raw")

# Coinbase (no geo restriction)
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
SYMBOL = "BTC-USD"




