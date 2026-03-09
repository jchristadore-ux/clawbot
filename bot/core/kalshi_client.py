"""
core/kalshi_client.py — Kalshi REST API client (read operations only).
All order placement goes through the Bun gateway (core/execution_engine.py).

Why separate reads from writes:
- Reads use RSA signing handled here in Python
- Writes go through the Bun gateway which handles PEM formatting more reliably
- Isolates the signing complexity; keeps this module testable
"""
from __future__ import annotations

import base64
import binascii
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from .notifier import log_event
from .scheduler import utc_iso


class KalshiClient:
    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": f"johnny5/{cfg.BOT_VERSION}"})
        self._private_key = None
        self._load_key()

    # ── Key loading ───────────────────────────────────────────────────────────

    def _load_key(self) -> None:
        pem = self._resolve_pem()
        if not pem:
            log_event("BOOT_WARN", {"msg": "no_private_key_found — read requests will be unauthenticated"})
            return
        try:
            self._private_key = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
            log_event("BOOT_INFO", {"msg": "private_key_loaded_ok"})
        except Exception as exc:
            log_event("BOOT_WARN", {"msg": f"private_key_load_failed: {str(exc)[:200]}"})

    def _resolve_pem(self) -> str:
        """Try B64 → inline PEM → file path. Return clean PEM string or empty."""
        cfg = self.cfg

        # Priority 1: base64-encoded (most Railway-safe)
        if cfg.KALSHI_PRIVATE_KEY_B64.strip():
            try:
                decoded = base64.b64decode(cfg.KALSHI_PRIVATE_KEY_B64.strip()).decode("utf-8")
                if "BEGIN" in decoded:
                    return self._normalize_pem(decoded)
            except (binascii.Error, UnicodeDecodeError):
                pass

        # Priority 2: inline PEM
        if cfg.KALSHI_PRIVATE_KEY_PEM.strip():
            return self._normalize_pem(cfg.KALSHI_PRIVATE_KEY_PEM)

        # Priority 3: file path or inline key in PATH var
        raw = cfg.KALSHI_PRIVATE_KEY_PATH.strip()
        if not raw:
            return ""
        if "BEGIN" in raw:
            return self._normalize_pem(raw)
        try:
            p = Path(raw)
            if len(raw) < 512 and p.exists():
                return self._normalize_pem(p.read_text(encoding="utf-8"))
        except Exception:
            pass
        try:
            decoded = base64.b64decode(raw, validate=True).decode("utf-8")
            if "BEGIN" in decoded:
                return self._normalize_pem(decoded)
        except (binascii.Error, UnicodeDecodeError):
            pass
        return ""

    @staticmethod
    def _normalize_pem(raw: str) -> str:
        """Fix common PEM formatting issues (collapsed newlines, escaped \\n, quotes)."""
        raw = raw.strip().strip('"').strip("'")
        if "\\n" in raw:
            return raw.replace("\\n", "\n").strip()
        # Collapsed by Railway UI: "-----BEGIN RSA PRIVATE KEY----- MIIEo..."
        import re
        m = re.match(r'^(-----BEGIN [^-]+-----)\s+([\s\S]+?)\s+(-----END [^-]+-----)$', raw)
        if m and "\n" not in m.group(2):
            header, body, footer = m.groups()
            body_clean = re.sub(r'\s+', '', body)
            wrapped = "\n".join(body_clean[i:i+64] for i in range(0, len(body_clean), 64))
            return f"{header}\n{wrapped}\n{footer}\n"
        return raw

    # ── Auth headers ──────────────────────────────────────────────────────────

    def _auth_headers(self, method: str, path: str) -> Dict[str, str]:
        base = {"Content-Type": "application/json"}
        if not (self.cfg.KALSHI_API_KEY_ID and self._private_key):
            return base
        ts_ms = str(int(time.time() * 1000))
        msg = f"{ts_ms}{method.upper()}{path}".encode("utf-8")
        sig = self._private_key.sign(msg, padding.PKCS1v15(), hashes.SHA256())
        return {
            **base,
            "KALSHI-ACCESS-KEY": self.cfg.KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
        }

    # ── HTTP ──────────────────────────────────────────────────────────────────

    def _get(self, path: str, params: Optional[Dict] = None, retries: int = 2) -> Dict[str, Any]:
        url = f"{self.cfg.KALSHI_BASE_URL}{path}"
        last_exc: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                r = self.session.get(
                    url,
                    headers=self._auth_headers("GET", path),
                    params=params,
                    timeout=self.cfg.REQUEST_TIMEOUT,
                )
                if r.status_code == 429:
                    retry_after = float(r.headers.get("Retry-After", 5))
                    log_event("RATE_LIMITED", {"retry_after": retry_after, "attempt": attempt})
                    time.sleep(retry_after)
                    continue
                if r.status_code >= 500:
                    log_event("KALSHI_5XX", {"status": r.status_code, "attempt": attempt})
                    time.sleep(2 ** attempt)
                    continue
                if r.status_code >= 400:
                    raise RuntimeError(f"Kalshi HTTP {r.status_code} GET {path}: {r.text[:300]}")
                out = r.json()
                if not isinstance(out, dict):
                    raise RuntimeError(f"Kalshi non-dict response on {path}")
                return out
            except RuntimeError:
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < retries:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"Kalshi GET {path} failed after {retries+1} attempts: {last_exc}")

    # ── Public API ────────────────────────────────────────────────────────────

    def list_open_markets(self) -> List[Dict[str, Any]]:
        """List open markets for the configured series ticker."""
        try:
            data = self._get(
                "/trade-api/v2/markets",
                params={"series_ticker": self.cfg.SERIES_TICKER, "status": "open"},
            )
            markets = data.get("markets", [])
            if not isinstance(markets, list):
                return []
            return markets
        except Exception as exc:
            log_event("KALSHI_LIST_ERR", {"err": str(exc)[:200]}, throttle_key="list_err", throttle_s=30)
            return []

    def get_orderbook(self, ticker: str) -> Dict[str, Any]:
        """Get full orderbook for a market ticker."""
        return self._get(f"/trade-api/v2/markets/{ticker}/orderbook")

    def get_market(self, ticker: str) -> Dict[str, Any]:
        """Get single market details."""
        return self._get(f"/trade-api/v2/markets/{ticker}")

    def get_exchange_status(self) -> Dict[str, Any]:
        """Check if Kalshi exchange is up. Used for health checks."""
        return self._get("/trade-api/v2/exchange/status")

    def get_portfolio_balance(self) -> Optional[float]:
        """Fetch real account balance in dollars. Returns None on error."""
        try:
            data = self._get("/trade-api/v2/portfolio/balance")
            cents = data.get("balance") or data.get("available_balance")
            if cents is not None:
                return float(cents) / 100.0
            return None
        except Exception as exc:
            log_event("BALANCE_ERR", {"err": str(exc)[:150]}, throttle_key="bal_err", throttle_s=60)
            return None
