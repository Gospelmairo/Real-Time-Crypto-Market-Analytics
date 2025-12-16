import json
import websocket
from kafka import KafkaProducer
from config import KAFKA_BROKER, KAFKA_TOPIC

WS_URL = "wss://ws-feed.exchange.coinbase.com"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def on_open(ws):
    print("Connected to Coinbase WebSocket")
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD", "LTC-USD", "ADA-USD", "SOL-USD"],
        "channels": ["matches"]
    }
    ws.send(json.dumps(subscribe_msg))

def on_message(ws, message):
    data = json.loads(message)

    if data.get("type") != "match":
        return

    trade = {
        "symbol": data["product_id"],
        "price": float(data["price"]),
        "quantity": float(data["size"]),
        "trade_id": data["trade_id"],
        "trade_time": data["time"],
        "exchange": "coinbase"
    }

    producer.send(KAFKA_TOPIC, trade)
    producer.flush()
    print("Sent:", trade)

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, *args):
    print("WebSocket closed")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

