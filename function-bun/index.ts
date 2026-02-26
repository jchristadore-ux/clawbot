import { Hono } from "hono";
import { ClobClient } from "@polymarket/clob-client";

// Railway sets PORT automatically for web services
const port = Number(process.env.PORT || 3000);

const {
  POLY_CLOB_HOST = "https://clob.polymarket.com",
  POLY_API_KEY,
  POLY_API_SECRET,
  POLY_API_PASSPHRASE,
} = process.env;

if (!POLY_API_KEY || !POLY_API_SECRET || !POLY_API_PASSPHRASE) {
  console.warn("Missing Builder API creds (POLY_API_KEY/SECRET/PASSPHRASE).");
}

const app = new Hono();

// Health
app.get("/", (c) => c.json({ ok: true }));

/**
 * Place an order on behalf of your bot.
 * This is a thin wrapper so your Python cron job can call it.
 */
app.post("/order", async (c) => {
  const body = await c.req.json();

  // Expect: { token_id, side: "BUY"|"SELL", price, size, order_type? }
  const { token_id, side, price, size, order_type = "GTC" } = body || {};
  if (!token_id || !side || price == null || size == null) {
    return c.json({ ok: false, error: "Missing required fields" }, 400);
  }

  const client = new ClobClient(POLY_CLOB_HOST, {
    apiKey: POLY_API_KEY!,
    apiSecret: POLY_API_SECRET!,
    apiPassphrase: POLY_API_PASSPHRASE!,
  });

  // NOTE: clob-client order payload shape can vary by version.
  // The point here is: Bun owns the authenticated order submit.
  const resp = await client.createOrder({
    token_id,
    side,
    price: String(price),
    size: String(size),
    type: order_type,
  });

  return c.json({ ok: true, resp });
});

Bun.serve({
  port: Number(process.env.PORT || 3000),
  hostname: "0.0.0.0",
  fetch: app.fetch,
});
