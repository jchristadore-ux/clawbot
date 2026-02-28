import { Hono } from "hono";
import { ClobClient } from "@polymarket/clob-client";

// Railway sets PORT automatically for web services
const PORT = Number(process.env.PORT || 3000);

const {
  POLY_CLOB_HOST = "https://clob.polymarket.com",
  POLY_API_KEY,
  POLY_API_SECRET,
  POLY_API_PASSPHRASE,
  // Optional guardrails
  REQUIRE_ORDERBOOK = "true", // if true, preflight-check orderbook exists for token_id
  LOG_LEVEL = "info",
} = process.env;

const log = {
  info: (...args: any[]) => (LOG_LEVEL !== "silent" ? console.log(...args) : undefined),
  warn: (...args: any[]) => (LOG_LEVEL !== "silent" ? console.warn(...args) : undefined),
  error: (...args: any[]) => (LOG_LEVEL !== "silent" ? console.error(...args) : undefined),
};

function envBool(x: string | undefined, fallback = false) {
  if (x == null) return fallback;
  return ["1", "true", "t", "yes", "y", "on"].includes(String(x).trim().toLowerCase());
}

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

// Create one client instance (don’t rebuild per request)
let client: any = null;
function getClient() {
  if (client) return client;

  if (!POLY_API_KEY || !POLY_API_SECRET || !POLY_API_PASSPHRASE) {
    // We still start the service so health checks work, but /order will fail clearly.
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

// Health (simple)
app.get("/", (c) =>
  c.json({
    ok: true,
    service: "function-bun",
  })
);

// Health (detailed, used by bot)
app.post("/__ping", async (c) => {
  return c.json({
    ok: true,
    service: "function-bun",
    clobHost: POLY_CLOB_HOST,
    now: new Date().toISOString(),
    hasCreds: Boolean(POLY_API_KEY && POLY_API_SECRET && POLY_API_PASSPHRASE),
  });
});

/**
 * Preflight guard:
 * If token_id is wrong (common “CLOB market issue”), CLOB returns:
 *   {"error":"No orderbook exists for the requested token id"}
 * We surface that early with a clearer 404-style response.
 */
async function assertOrderbookExists(tokenId: string) {
  const c = getClient();

  // clob-client method names can vary; try the most common ones.
  const candidates = [
    () => (typeof c.getOrderBook === "function" ? c.getOrderBook(tokenId) : undefined),
    () => (typeof c.getOrderbook === "function" ? c.getOrderbook(tokenId) : undefined),
    () => (typeof c.getOrderBookSummary === "function" ? c.getOrderBookSummary(tokenId) : undefined),
    () => (typeof c.getOrderbookSummary === "function" ? c.getOrderbookSummary(tokenId) : undefined),
  ];

  let lastErr: any = null;
  for (const fn of candidates) {
    try {
      const out = await fn();
      if (out !== undefined) return; // success
    } catch (e) {
      lastErr = e;
    }
  }

  // If we couldn’t call anything, don’t hard fail—just allow createOrder to speak.
  if (lastErr == null) return;

  // If we got an error from a real endpoint call, raise it.
  throw lastErr;
}

/**
 * clob-client payload shape drift shim.
 * Some versions expect token_id vs tokenId; type vs orderType; etc.
 */
async function createOrderCompat(args: {
  tokenId: string;
  side: OrderSide;
  price: number;
  size: number;
  orderType: OrderType;
}) {
  const c = getClient();

  const payloads: any[] = [
    // Most common (snake_case)
    {
      token_id: args.tokenId,
      side: args.side,
      price: String(args.price),
      size: String(args.size),
      type: args.orderType,
    },
    // camelCase tokenId
    {
      tokenId: args.tokenId,
      side: args.side,
      price: String(args.price),
      size: String(args.size),
      type: args.orderType,
    },
    // some libs use orderType
    {
      token_id: args.tokenId,
      side: args.side,
      price: String(args.price),
      size: String(args.size),
      orderType: args.orderType,
    },
  ];

  let lastErr: any = null;
  for (const p of payloads) {
    try {
      return await c.createOrder(p);
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr ?? new Error("createOrder failed (unknown)");
}

/**
 * Place an order on behalf of your bot.
 * Expect: { token_id, side: "BUY"|"SELL", price, size, order_type? }
 */
app.post("/order", async (c) => {
  let body: any = null;
  try {
    body = await c.req.json();
  } catch (e) {
    return c.json({ ok: false, error: "Invalid JSON body" }, 400);
  }

  // Accept a few aliases to make the Python caller resilient.
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
    return c.json(
      {
        ok: false,
        error: "Server missing Polymarket API credentials",
      },
      500
    );
  }

  // Optional: preflight check to catch the “No orderbook exists for token id” issue cleanly.
  if (envBool(REQUIRE_ORDERBOOK, true)) {
    try {
      await assertOrderbookExists(tokenId);
    } catch (e: any) {
      const msg = e?.message ? String(e.message) : String(e);
      // Surface as a 404-ish error because the token is effectively “not tradable / not found”.
      return c.json(
        {
          ok: false,
          error: "Orderbook preflight failed (token_id may be wrong or market not live on CLOB)",
          details: msg.slice(0, 800),
          token_id: tokenId,
        },
        404
      );
    }
  }

  try {
    const resp = await createOrderCompat({
      tokenId,
      side,
      price,
      size,
      orderType,
    });

    return c.json({ ok: true, resp });
  } catch (e: any) {
    const msg = e?.message ? String(e.message) : String(e);

    // Common failure mode we want to explicitly call out
    const looksLikeNoOrderbook =
      msg.toLowerCase().includes("no orderbook exists") ||
      msg.toLowerCase().includes("orderbook") ||
      msg.toLowerCase().includes("token id");

    return c.json(
      {
        ok: false,
        error: "CLOB order failed",
        details: msg.slice(0, 1200),
        token_id: tokenId,
        hint: looksLikeNoOrderbook
          ? "This usually means the token_id is wrong for the current rolling market window, or the orderbook isn’t available."
          : undefined,
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
