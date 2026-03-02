diff --git a/function-bun/index.ts b/function-bun/index.ts
index 80dc888ec400ec3727565727d91eeb5d0f4aa4e2..5d293840493f8c5a136128145b82c39862701148 100644
--- a/function-bun/index.ts
+++ b/function-bun/index.ts
@@ -1,173 +1,152 @@
 import { Hono } from "hono";
-import { ClobClient } from "@polymarket/clob-client";
+import { createSign, randomUUID } from "node:crypto";
 
 const PORT = Number(process.env.PORT || 3000);
+const KALSHI_BASE_URL = (process.env.KALSHI_BASE_URL || "https://api.elections.kalshi.com").replace(/\/$/, "");
+const KALSHI_KEY_ID = process.env.KALSHI_API_KEY_ID || "";
+const KALSHI_PRIVATE_KEY_PEM = process.env.KALSHI_PRIVATE_KEY_PEM || "";
+const KALSHI_PRIVATE_KEY_PATH = process.env.KALSHI_PRIVATE_KEY_PATH || "";
 
-const {
-  POLY_CLOB_HOST = "https://clob.polymarket.com",
-  POLY_API_KEY,
-  POLY_API_SECRET,
-  POLY_API_PASSPHRASE,
-  LOG_LEVEL = "info",
-} = process.env;
-
-const log = {
-  info: (...args: any[]) => (LOG_LEVEL !== "silent" ? console.log(...args) : undefined),
-  warn: (...args: any[]) => (LOG_LEVEL !== "silent" ? console.warn(...args) : undefined),
-  error: (...args: any[]) => (LOG_LEVEL !== "silent" ? console.error(...args) : undefined),
-};
-
-function asString(x: any): string | null {
-  if (x == null) return null;
-  const s = typeof x === "string" ? x : String(x);
-  const t = s.trim();
-  return t.length ? t : null;
+type Side = "YES" | "NO";
+
+function asString(x: unknown): string | null {
+  if (typeof x !== "string") return null;
+  const s = x.trim();
+  return s.length ? s : null;
 }
 
-function asFiniteNumber(x: any): number | null {
-  if (x == null) return null;
+function asNumber(x: unknown): number | null {
   const n = typeof x === "number" ? x : Number(x);
   return Number.isFinite(n) ? n : null;
 }
 
-type OrderSide = "BUY" | "SELL";
-type OrderType = "GTC" | "IOC" | "FOK";
-
-function normalizeSide(x: any): OrderSide | null {
-  const s = asString(x)?.toUpperCase();
-  if (s === "BUY" || s === "SELL") return s;
-  return null;
-}
-
-function normalizeOrderType(x: any): OrderType {
-  const s = asString(x)?.toUpperCase();
-  if (s === "IOC" || s === "FOK" || s === "GTC") return s;
-  return "GTC";
+async function loadPrivateKey(): Promise<string | null> {
+  if (KALSHI_PRIVATE_KEY_PEM.trim()) return KALSHI_PRIVATE_KEY_PEM;
+  if (!KALSHI_PRIVATE_KEY_PATH) return null;
+  try {
+    const f = Bun.file(KALSHI_PRIVATE_KEY_PATH);
+    return await f.text();
+  } catch {
+    return null;
+  }
 }
 
-function redact(s: string | undefined) {
-  if (!s) return "";
-  if (s.length <= 6) return "***";
-  return `${s.slice(0, 3)}***${s.slice(-3)}`;
+async function signedKalshiHeaders(method: string, path: string): Promise<Record<string, string>> {
+  const key = await loadPrivateKey();
+  if (!KALSHI_KEY_ID || !key) return {};
+
+  const ts = Date.now().toString();
+  const payload = `${ts}${method.toUpperCase()}${path}`;
+  const signer = createSign("RSA-SHA256");
+  signer.update(payload);
+  signer.end();
+  const signature = signer.sign(key, "base64");
+
+  return {
+    "KALSHI-ACCESS-KEY": KALSHI_KEY_ID,
+    "KALSHI-ACCESS-TIMESTAMP": ts,
+    "KALSHI-ACCESS-SIGNATURE": signature,
+    "Content-Type": "application/json",
+  };
 }
 
 const app = new Hono();
 
-let client: any = null;
-function getClient() {
-  if (client) return client;
-
-  if (!POLY_API_KEY || !POLY_API_SECRET || !POLY_API_PASSPHRASE) {
-    log.warn(
-      "Missing Polymarket creds (POLY_API_KEY/SECRET/PASSPHRASE). key=%s secret=%s pass=%s",
-      redact(POLY_API_KEY),
-      redact(POLY_API_SECRET),
-      redact(POLY_API_PASSPHRASE)
-    );
-  }
-
-  client = new ClobClient(POLY_CLOB_HOST, {
-    apiKey: POLY_API_KEY!,
-    apiSecret: POLY_API_SECRET!,
-    apiPassphrase: POLY_API_PASSPHRASE!,
-  });
-
-  return client;
-}
-
-// Root health
 app.get("/", (c) =>
   c.json({
     ok: true,
-    service: "function-bun",
-  })
+    service: "kalshi-order-gateway",
+    hasCredentials: Boolean(KALSHI_KEY_ID && (KALSHI_PRIVATE_KEY_PEM || KALSHI_PRIVATE_KEY_PATH)),
+  }),
 );
 
-// **Add standard health route so bot default works**
-app.get("/health", (c) =>
-  c.json({
-    ok: true,
-    service: "function-bun",
-  })
-);
-
-// Keep __ping (some versions use it)
-app.post("/__ping", async (c) => {
-  return c.json({
-    ok: true,
-    service: "function-bun",
-    clobHost: POLY_CLOB_HOST,
-    now: new Date().toISOString(),
-    hasCreds: Boolean(POLY_API_KEY && POLY_API_SECRET && POLY_API_PASSPHRASE),
-  });
+app.get("/health", async (c) => {
+  try {
+    const path = "/trade-api/v2/exchange/status";
+    const headers = await signedKalshiHeaders("GET", path);
+    const resp = await fetch(`${KALSHI_BASE_URL}${path}`, { method: "GET", headers });
+    const body = await resp.json();
+    return c.json({ ok: resp.ok, upstream_status: resp.status, kalshi: body }, resp.ok ? 200 : 502);
+  } catch (error) {
+    return c.json({ ok: false, error: String(error) }, 500);
+  }
 });
 
-// Order endpoint
 app.post("/order", async (c) => {
-  let body: any = null;
+  let body: any;
   try {
     body = await c.req.json();
   } catch {
-    return c.json({ ok: false, error: "Invalid JSON body" }, 400);
+    return c.json({ ok: false, error: "invalid JSON body" }, 400);
   }
 
-  const tokenId =
-    asString(body?.token_id) ||
-    asString(body?.tokenId) ||
-    asString(body?.clob_token_id) ||
-    asString(body?.clobTokenId);
-
-  const side = normalizeSide(body?.side);
-  const price = asFiniteNumber(body?.price);
-  const size = asFiniteNumber(body?.size);
-  const orderType = normalizeOrderType(body?.order_type ?? body?.orderType ?? body?.type);
-
-  if (!tokenId) return c.json({ ok: false, error: "Missing token_id" }, 400);
-  if (!side) return c.json({ ok: false, error: 'Missing/invalid side (must be "BUY" or "SELL")' }, 400);
-  if (price == null) return c.json({ ok: false, error: "Missing/invalid price" }, 400);
-  if (size == null) return c.json({ ok: false, error: "Missing/invalid size" }, 400);
-
-  if (!POLY_API_KEY || !POLY_API_SECRET || !POLY_API_PASSPHRASE) {
-    return c.json({ ok: false, error: "Server missing Polymarket API credentials" }, 500);
+  const ticker = asString(body?.ticker);
+  const side = asString(body?.side)?.toUpperCase() as Side | undefined;
+  const count = asNumber(body?.count ?? body?.contracts);
+  const yesPrice = asNumber(body?.yes_price ?? body?.yesPrice);
+  const noPrice = asNumber(body?.no_price ?? body?.noPrice);
+  const action = asString(body?.action)?.toLowerCase() || "buy";
+  const type = asString(body?.type)?.toLowerCase() || "limit";
+
+  if (!ticker) return c.json({ ok: false, error: "ticker is required" }, 400);
+  if (side !== "YES" && side !== "NO") return c.json({ ok: false, error: 'side must be "YES" or "NO"' }, 400);
+  if (!count || count <= 0) return c.json({ ok: false, error: "count/contracts must be > 0" }, 400);
+
+  const orderPayload: Record<string, unknown> = {
+    ticker,
+    action,
+    type,
+    side: side.toLowerCase(),
+    count: Math.floor(count),
+    client_order_id: asString(body?.client_order_id) || randomUUID(),
+  };
+
+  if (side === "YES") {
+    if (yesPrice == null) return c.json({ ok: false, error: "yes_price is required for YES orders" }, 400);
+    orderPayload.yes_price = Math.max(1, Math.min(99, Math.round(yesPrice)));
+  } else {
+    if (noPrice == null) return c.json({ ok: false, error: "no_price is required for NO orders" }, 400);
+    orderPayload.no_price = Math.max(1, Math.min(99, Math.round(noPrice)));
   }
 
   try {
-    const clob = getClient();
-
-    // Try a few payload shapes to handle library drift
-    const payloads: any[] = [
-      { token_id: tokenId, side, price: String(price), size: String(size), type: orderType },
-      { tokenId, side, price: String(price), size: String(size), type: orderType },
-      { token_id: tokenId, side, price: String(price), size: String(size), orderType },
-    ];
-
-    let lastErr: any = null;
-    for (const p of payloads) {
-      try {
-        const resp = await clob.createOrder(p);
-        return c.json({ ok: true, resp });
-      } catch (e) {
-        lastErr = e;
-      }
+    const path = "/trade-api/v2/portfolio/orders";
+    const headers = await signedKalshiHeaders("POST", path);
+
+    if (!headers["KALSHI-ACCESS-KEY"]) {
+      return c.json({ ok: false, error: "missing Kalshi credentials" }, 500);
+    }
+
+    const resp = await fetch(`${KALSHI_BASE_URL}${path}`, {
+      method: "POST",
+      headers,
+      body: JSON.stringify(orderPayload),
+    });
+
+    const text = await resp.text();
+    let parsed: unknown = text;
+    try {
+      parsed = JSON.parse(text);
+    } catch {
+      // keep raw string
     }
 
-    throw lastErr ?? new Error("createOrder failed (unknown)");
-  } catch (e: any) {
-    const msg = e?.message ? String(e.message) : String(e);
     return c.json(
       {
-        ok: false,
-        error: "CLOB order failed",
-        details: msg.slice(0, 1200),
-        token_id: tokenId,
+        ok: resp.ok,
+        upstream_status: resp.status,
+        request: orderPayload,
+        response: parsed,
       },
-      502
+      resp.ok ? 200 : 502,
     );
+  } catch (error) {
+    return c.json({ ok: false, error: String(error) }, 500);
   }
 });
 
 Bun.serve({
-  port: PORT,
   hostname: "0.0.0.0",
+  port: PORT,
   fetch: app.fetch,
 });
