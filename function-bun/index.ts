import { Hono } from "hono";
import { createSign, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

const PORT = Number(process.env.PORT || 3000);
const KALSHI_BASE_URL = (process.env.KALSHI_BASE_URL || "https://api.elections.kalshi.com").replace(/\/$/, "");
const KALSHI_KEY_ID = (process.env.KALSHI_API_KEY_ID || "").trim();
const KALSHI_PRIVATE_KEY_PEM = (process.env.KALSHI_PRIVATE_KEY_PEM || "").trim();
const KALSHI_PRIVATE_KEY_PATH = (process.env.KALSHI_PRIVATE_KEY_PATH || "").trim();

type Side = "YES" | "NO";
type Action = "buy" | "sell";

function asString(x: unknown): string | null {
  if (typeof x !== "string") return null;
  const s = x.trim();
  return s.length ? s : null;
}

function asNumber(x: unknown): number | null {
  const n = typeof x === "number" ? x : Number(x);
  return Number.isFinite(n) ? n : null;
}

async function loadPrivateKey(): Promise<string | null> {
  // Preferred: direct PEM
  if (KALSHI_PRIVATE_KEY_PEM) return KALSHI_PRIVATE_KEY_PEM;

  // Next: path or inline
  if (!KALSHI_PRIVATE_KEY_PATH) return null;

  if (KALSHI_PRIVATE_KEY_PATH.includes("BEGIN")) return KALSHI_PRIVATE_KEY_PATH;

  // File path
  try {
    if (KALSHI_PRIVATE_KEY_PATH.length < 512) {
      const text = await readFile(KALSHI_PRIVATE_KEY_PATH, "utf-8");
      if (text.includes("BEGIN")) return text.trim();
    }
  } catch {
    // ignore
  }

  // Base64 PEM
  try {
    const decoded = Buffer.from(KALSHI_PRIVATE_KEY_PATH, "base64").toString("utf-8");
    if (decoded.includes("BEGIN")) return decoded.trim();
  } catch {
    // ignore
  }

  return null;
}

async function signedKalshiHeaders(
  method: string,
  path: string,
  bodyString?: string
): Promise<Record<string, string>> {

  const key = await loadPrivateKey();
  if (!KALSHI_KEY_ID || !key) return {};

  const ts = Date.now().toString();

  const payload = `${ts}${method.toUpperCase()}${path}${bodyString ?? ""}`;

  const signer = createSign("RSA-SHA256");

  // IMPORTANT: sign UTF8 buffer
  signer.update(Buffer.from(payload, "utf8"));

  signer.end();

  const signature = signer.sign(key, "base64");

  return {
    "KALSHI-ACCESS-KEY": KALSHI_KEY_ID,
    "KALSHI-ACCESS-TIMESTAMP": ts,
    "KALSHI-ACCESS-SIGNATURE": signature,
    "Content-Type": "application/json",
  };
}

const app = new Hono();

app.get("/", (c) =>
  c.json({
    ok: true,
    service: "kalshi-order-gateway",
    hasCredentials: Boolean(KALSHI_KEY_ID && (KALSHI_PRIVATE_KEY_PEM || KALSHI_PRIVATE_KEY_PATH)),
  }),
);

app.get("/health", async (c) => {
  try {
    const path = "/trade-api/v2/exchange/status";
    const headers = await signedKalshiHeaders("GET", path);

    if (!headers["KALSHI-ACCESS-KEY"]) {
      return c.json({ ok: false, error: "missing Kalshi credentials" }, 500);
    }

    const resp = await fetch(`${KALSHI_BASE_URL}${path}`, { method: "GET", headers });
    const body = await resp.json();
    return c.json({ ok: resp.ok, upstream_status: resp.status, kalshi: body }, resp.ok ? 200 : 502);
  } catch (error) {
    return c.json({ ok: false, error: String(error) }, 500);
  }
});

app.post("/order", async (c) => {
  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ ok: false, error: "invalid JSON body" }, 400);
  }

  const ticker = asString(body?.ticker);
  const side = (asString(body?.side)?.toUpperCase() as Side | null) ?? null;
  const action = (asString(body?.action)?.toLowerCase() as Action | null) ?? "buy";
  const type = asString(body?.type)?.toLowerCase() || "limit";

  const count = asNumber(body?.count ?? body?.contracts);
  const yesPrice = asNumber(body?.yes_price ?? body?.yesPrice);
  const noPrice = asNumber(body?.no_price ?? body?.noPrice);

  if (!ticker) return c.json({ ok: false, error: "ticker is required" }, 400);
  if (side !== "YES" && side !== "NO") return c.json({ ok: false, error: 'side must be "YES" or "NO"' }, 400);
  if (!count || count <= 0) return c.json({ ok: false, error: "count/contracts must be > 0" }, 400);
  if (action !== "buy" && action !== "sell") return c.json({ ok: false, error: 'action must be "buy" or "sell"' }, 400);

  const orderPayload: Record<string, unknown> = {
    ticker,
    action,
    type,
    side: side.toLowerCase(),
    count: Math.floor(count),
    client_order_id: asString(body?.client_order_id) || randomUUID(),
  };

  if (side === "YES") {
    if (yesPrice == null) return c.json({ ok: false, error: "yes_price is required for YES orders" }, 400);
    orderPayload.yes_price = Math.max(1, Math.min(99, Math.round(yesPrice)));
  } else {
    if (noPrice == null) return c.json({ ok: false, error: "no_price is required for NO orders" }, 400);
    orderPayload.no_price = Math.max(1, Math.min(99, Math.round(noPrice)));
  }

  try {
    const path = "/trade-api/v2/portfolio/orders";

    // Sign the exact string we will send
    const bodyString = JSON.stringify(orderPayload);
    const headers = await signedKalshiHeaders("POST", path, bodyString);

    if (!headers["KALSHI-ACCESS-KEY"]) {
      return c.json({ ok: false, error: "missing Kalshi credentials" }, 500);
    }

    const resp = await fetch(`${KALSHI_BASE_URL}${path}`, {
      method: "POST",
      headers,
      body: bodyString,
    });

    const text = await resp.text();
    let parsed: unknown = text;
    try {
      parsed = JSON.parse(text);
    } catch {
      // keep raw string
    }

    return c.json(
      {
        ok: resp.ok,
        upstream_status: resp.status,
        request: orderPayload,
        response: parsed,
      },
      resp.ok ? 200 : 502,
    );
  } catch (error) {
    return c.json({ ok: false, error: String(error) }, 500);
  }
});

Bun.serve({
  hostname: "0.0.0.0",
  port: PORT,
  fetch: app.fetch,
});
