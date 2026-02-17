import requests

def fetch_btc_price():
    # CoinGecko: last ~250 points at 5-minute granularity for 1 day
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {
        "vs_currency": "usd",
        "days": "1",
        "interval": "5m"
    }

    r = requests.get(url, params=params, timeout=20)

    # If Cloud provider hits a limit, don’t crash-loop
    if r.status_code != 200:
        print("COINGECKO_HTTP_STATUS:", r.status_code)
        print("COINGECKO_BODY:", r.text[:500])
        return None

    data = r.json()
    prices = data.get("prices", [])

    # prices: [[timestamp_ms, price], ...]
    if not isinstance(prices, list) or len(prices) < 5:
        print("COINGECKO_BAD_PRICES_SHAPE:", str(data)[:500])
        return None

    closes = [float(p[1]) for p in prices[-5:]]
    return closes


    response = requests.get(url, params=params)
    data = response.json()

    # Debug print to see what Railway is receiving
    print("API RESPONSE:", data)

    # Validate response
    if not isinstance(data, list):
        raise Exception(f"Unexpected API response: {data}")

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
