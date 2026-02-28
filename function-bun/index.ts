import { Hono } from "hono";
import { ClobClient } from "@polymarket/clob-client";

const PORT = Number(process.env.PORT || 3000);

const {
  POLY_CLOB_HOST = "https://clob.polymarket.com",
  POLY_API_KEY,
  POLY_API_SECRET,
  POLY_API_PASSPHRASE,
  LOG_LEVEL = "info",
} = process.env;

const log = {
  info: (...args: any[]) => (LOG_LEVEL !== "silent" ? console.log(...args) : undefined),
  warn: (...args: any[]) => (LOG_LEVEL !== "silent" ? console.warn(...args) : undefined),
  error: (...args: any[]) => (LOG_LEVEL !== "silent" ? console.error(...args) : undefined),
};

function asString(x: any): string | null {
  if (x == null) return null;
  const s = typeof x === "string" ? x : String(x);
  const t = s.trim();
  return t.length ? t : null;
}

function asFiniteNumber(x: any): number | null {
  if (x == null) return null;
  const n = typeof x === "number" ? x : Number(x);
  return Number.isFinite(n) ? n : null;
}

type OrderSide = "BUY" | "SELL";
type OrderType = "GTC" | "IOC" | "FOK";

function normalizeSide(x: any): OrderSide | null {
  const s = asString(x)?.toUpperCase();
  if (s === "BUY" || s === "SELL") return s;
  return null;
}

function normalizeOrderType(x: any): OrderType {
  const s = asString(x)?.toUpperCase();
  if (s === "IOC" || s === "FOK" || s === "GTC") return s;
  return "GTC";
}

function redact(s: string | undefined) {
  if (!s) return "";
  if (s.length <= 6) return "***";
  return `${s.slice(0, 3)}***${s.slice(-3)}`;
}

const app = new Hono();

let client: any = null;
function getClient() {
  if (client) return client;

  if (!POLY_API_KEY || !POLY_API_SECRET || !POLY_API_PASSPHRASE) {
    log.warn(
      "Missing Polymarket creds (POLY_API_KEY/SECRET/PASSPHRASE). key=%s secret=%s pass=%s",
      redact(POLY_API_KEY),
      redact(POLY_API_SECRET),
      redact(POLY_API_PASSPHRASE)
    );
  }

  client = new ClobClient(POLY_CLOB_HOST, {
    apiKey: POLY_API_KEY!,
    apiSecret: POLY_API_SECRET!,
    apiPassphrase: POLY_API_PASSPHRASE!,
  });

  return client;
}

// Root health
app.get("/", (c) =>
  c.json({
    ok: true,
    service: "function-bun",
  })
);

// **Add standard health route so bot default works**
app.get("/health", (c) =>
  c.json({
    ok: true,
    service: "function-bun",
  })
);

// Keep __ping (some versions use it)
app.post("/__ping", async (c) => {
  return c.json({
    ok: true,
    service: "function-bun",
    clobHost: POLY_CLOB_HOST,
    now: new Date().toISOString(),
    hasCreds: Boolean(POLY_API_KEY && POLY_API_SECRET && POLY_API_PASSPHRASE),
  });
});

// Order endpoint
app.post("/order", async (c) => {
  let body: any = null;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ ok: false, error: "Invalid JSON body" }, 400);
  }

  const tokenId =
    asString(body?.token_id) ||
    asString(body?.tokenId) ||
    asString(body?.clob_token_id) ||
    asString(body?.clobTokenId);

  const side = normalizeSide(body?.side);
  const price = asFiniteNumber(body?.price);
  const size = asFiniteNumber(body?.size);
  const orderType = normalizeOrderType(body?.order_type ?? body?.orderType ?? body?.type);

  if (!tokenId) return c.json({ ok: false, error: "Missing token_id" }, 400);
  if (!side) return c.json({ ok: false, error: 'Missing/invalid side (must be "BUY" or "SELL")' }, 400);
  if (price == null) return c.json({ ok: false, error: "Missing/invalid price" }, 400);
  if (size == null) return c.json({ ok: false, error: "Missing/invalid size" }, 400);

  if (!POLY_API_KEY || !POLY_API_SECRET || !POLY_API_PASSPHRASE) {
    return c.json({ ok: false, error: "Server missing Polymarket API credentials" }, 500);
  }

  try {
    const clob = getClient();

    // Try a few payload shapes to handle library drift
    const payloads: any[] = [
      { token_id: tokenId, side, price: String(price), size: String(size), type: orderType },
      { tokenId, side, price: String(price), size: String(size), type: orderType },
      { token_id: tokenId, side, price: String(price), size: String(size), orderType },
    ];

    let lastErr: any = null;
    for (const p of payloads) {
      try {
        const resp = await clob.createOrder(p);
        return c.json({ ok: true, resp });
      } catch (e) {
        lastErr = e;
      }
    }

    throw lastErr ?? new Error("createOrder failed (unknown)");
  } catch (e: any) {
    const msg = e?.message ? String(e.message) : String(e);
    return c.json(
      {
        ok: false,
        error: "CLOB order failed",
        details: msg.slice(0, 1200),
        token_id: tokenId,
      },
      502
    );
  }
});

Bun.serve({
  port: PORT,
  hostname: "0.0.0.0",
  fetch: app.fetch,
});
