import requests
from datetime import datetime

def fetch_btc_price():
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "5m",
        "limit": 5
    }
    data = requests.get(url, params=params).json()
    closes = [float(k[4]) for k in data]
    return closes

def decide(closes):
    avg = sum(closes) / len(closes)
    last = closes[-1]

    if last > avg * 1.002:
        return "YES"
    elif last < avg * 0.998:
        return "NO"
    else:
        return "HOLD"

if __name__ == "__main__":
    closes = fetch_btc_price()
    decision = decide(closes)
    print(datetime.utcnow(), decision)
