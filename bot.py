import os
import json
import requests
from flask import Flask

app = Flask(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com/markets"

# ----------------------------
# Helpers
# ----------------------------

def _maybe_json_list(x):
    """
    Gamma sometimes returns outcomes/outcomePrices as JSON-encoded strings.
    This safely converts them to lists.
    """
    if x is None:
        return None

    if isinstance(x, list):
        return x

    if isinstance(x, str):
        try:
            parsed = json.loads(x)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return None

    return None


def fetch_markets(limit=50):
    """
    Fetch active markets from Gamma API
    """
    try:
        response = requests.get(
            GAMMA_URL,
            params={"limit": limit, "active": "true"},
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print("Error fetching markets:", e)
        return []


def extract_yes_no_prices(markets):
    """
    Extract YES/NO prices safely.
    Guaranteed no unbound variable errors.
    """
    results = []

    for m in markets:

        if not isinstance(m, dict):
            continue

        # Always initialize
        p_yes = None
        p_no = None

        question = m.get("question") or m.get("title")

        outcomes = _maybe_json_list(m.get("outcomes"))
        prices = _maybe_json_list(m.get("outcomePrices"))

        if not outcomes or not prices:
            continue

        if len(outcomes) != len(prices):
            continue

        for name, price in zip(outcomes, prices):
            try:
                price = float(price)
            except Exception:
                continue

            name_clean = str(name).strip().lower()

            if name_clean == "yes":
                p_yes = price
            elif name_clean == "no":
                p_no = price

        if p_yes is not None and p_no is not None:
            results.append({
                "question": question,
                "p_yes": p_yes,
                "p_no": p_no
            })

    return results


# ----------------------------
# Simple Edge Scanner
# ----------------------------

def scan_for_edges(markets):
    """
    Example logic:
    Flag markets where YES + NO != 1 (pricing inefficiency)
    """
    edges = []

    for m in markets:
        total = m["p_yes"] + m["p_no"]

        if abs(total - 1) > 0.02:  # 2% threshold
            edges.append({
                "question": m["question"],
                "p_yes": m["p_yes"],
                "p_no": m["p_no"],
                "total": round(total, 4)
            })

    return edges


# ----------------------------
# Routes
# ----------------------------

@app.route("/")
def home():
    return "Johnny 5 is alive 🤖"


@app.route("/scan")
def scan():
    markets = fetch_markets(limit=100)
    parsed = extract_yes_no_prices(markets)
    edges = scan_for_edges(parsed)

    return {
        "markets_scanned": len(parsed),
        "edges_found": len(edges),
        "edges": edges[:10]
    }


# ----------------------------
# Railway Start
# ----------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
