import { Hono } from "hono";
import { createSign, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import postgres from "postgres";

const PORT = Number(process.env.PORT || 3000);
const KALSHI_BASE_URL = (process.env.KALSHI_BASE_URL || "https://api.elections.kalshi.com").replace(/\/$/, "");
const KALSHI_KEY_ID = process.env.KALSHI_API_KEY_ID || "";
const KALSHI_PRIVATE_KEY_PEM  = process.env.KALSHI_PRIVATE_KEY_PEM  || "";
const KALSHI_PRIVATE_KEY_PATH = process.env.KALSHI_PRIVATE_KEY_PATH || "";
// NEW: accept a plain base64-encoded PEM — no newline issues in Railway env vars
const KALSHI_PRIVATE_KEY_B64  = process.env.KALSHI_PRIVATE_KEY_B64  || "";

type Side = "YES" | "NO";

function asString(x: unknown): string | null {
  if (typeof x !== "string") return null;
  const s = x.trim();
  return s.length ? s : null;
}

function asNumber(x: unknown): number | null {
  const n = typeof x === "number" ? x : Number(x);
  return Number.isFinite(n) ? n : null;
}

/**
 * Reconstruct a valid PEM string from whatever format the key is stored in.
 *
 * Railway's UI collapses multi-line env var values: it replaces actual newlines
 * with spaces, which breaks Node's PEM parser.  This function handles:
 *   1. Proper multi-line PEM (ideal, works as-is)
 *   2. base64-encoded PEM  (KALSHI_PRIVATE_KEY_B64 — recommended for Railway)
 *   3. \\n-escaped PEM      (some Railway configs paste this way)
 *   4. Space-collapsed PEM (Railway UI paste bug — we reconstruct the newlines)
 */
function normalizePem(raw: string): string {
  raw = raw.trim().replace(/^["']|["']$/g, ""); // strip surrounding quotes

  // 1. Already has newlines and looks like PEM
  if (raw.includes("\n") && raw.includes("BEGIN")) return raw;

  // 2. \\n escaped
  if (raw.includes("\\n")) return raw.replace(/\\n/g, "\n");

  // 3. Space-collapsed PEM: "-----BEGIN RSA PRIVATE KEY----- MIIEo..."
  //    Reconstruct by pulling header, body, footer and re-wrapping base64
  const collapsed = raw.match(/^(-----BEGIN [^-]+-----)\s+([\s\S]+?)\s+(-----END [^-]+-----)$/);
  if (collapsed) {
    const header = collapsed[1];
    const body   = collapsed[2].replace(/\s+/g, ""); // strip all whitespace from base64
    const footer = collapsed[3];
    const wrapped = body.match(/.{1,64}/g)!.join("\n");
    return `${header}\n${wrapped}\n${footer}\n`;
  }

  return raw;
}

async function loadPrivateKey(): Promise<string | null> {
  // Priority 1: base64-encoded single-line value (most reliable for Railway)
  if (KALSHI_PRIVATE_KEY_B64.trim()) {
    try {
      const decoded = Buffer.from(KALSHI_PRIVATE_KEY_B64.trim(), "base64").toString("utf-8");
      if (decoded.includes("BEGIN")) return normalizePem(decoded);
    } catch { /* fallthrough */ }
  }

  // Priority 2: direct PEM in env var (may have formatting issues)
  if (KALSHI_PRIVATE_KEY_PEM.trim()) {
    return normalizePem(KALSHI_PRIVATE_KEY_PEM.trim());
  }

  // Priority 3: path or inline key
  const raw = KALSHI_PRIVATE_KEY_PATH.trim();
  if (!raw) return null;

  if (raw.includes("BEGIN")) return normalizePem(raw);

  // Try file path
  try {
    if (raw.length < 512) {
      const text = await readFile(raw, "utf-8");
      if (text.includes("BEGIN")) return normalizePem(text);
    }
  } catch { /* fallthrough */ }

  // Try base64 in PATH var
  try {
    const decoded = Buffer.from(raw, "base64").toString("utf-8");
    if (decoded.includes("BEGIN")) return normalizePem(decoded);
  } catch { /* fallthrough */ }

  return null;
}

async function signedKalshiHeaders(method: string, path: string): Promise<Record<string, string>> {
  const key = await loadPrivateKey();
  if (!KALSHI_KEY_ID || !key) {
    console.error("SIGNING_FAIL: missing key_id or private_key");
    return {};
  }

  const ts = Date.now().toString();
  const payload = `${ts}${method.toUpperCase()}${path}`;

  try {
    const signer = createSign("RSA-SHA256");
    signer.update(payload);
    signer.end();
    const signature = signer.sign(key, "base64");
    return {
      "KALSHI-ACCESS-KEY":       KALSHI_KEY_ID,
      "KALSHI-ACCESS-TIMESTAMP": ts,
      "KALSHI-ACCESS-SIGNATURE": signature,
      "Content-Type":            "application/json",
    };
  } catch (err) {
    console.error("SIGNING_ERROR:", String(err));
    return {};
  }
}

const app = new Hono();

app.get("/", (c) =>
  c.json({
    ok: true,
    service: "kalshi-order-gateway",
    hasKeyId: Boolean(KALSHI_KEY_ID),
    hasB64Key: Boolean(KALSHI_PRIVATE_KEY_B64.trim()),
    hasPemKey: Boolean(KALSHI_PRIVATE_KEY_PEM.trim()),
    hasPathKey: Boolean(KALSHI_PRIVATE_KEY_PATH.trim()),
  }),
);

app.get("/health", async (c) => {
  try {
    const path = "/trade-api/v2/exchange/status";
    const headers = await signedKalshiHeaders("GET", path);
    const resp = await fetch(`${KALSHI_BASE_URL}${path}`, { method: "GET", headers });
    const body = await resp.json();
    return c.json({ ok: resp.ok, upstream_status: resp.status, kalshi: body }, resp.ok ? 200 : 502);
  } catch (error) {
    return c.json({ ok: false, error: String(error) }, 500);
  }
});

// ── /debug-signing — test signing without placing a real order ────────────────
app.get("/debug-signing", async (c) => {
  const key = await loadPrivateKey();
  if (!key) return c.json({ ok: false, error: "no private key loaded" }, 500);
  
  const snippet = key.split("\n").slice(0, 3).join(" | ");
  const hasBegin = key.includes("BEGIN RSA PRIVATE KEY");
  const lineCount = key.split("\n").length;
  
  // Test actual signing
  try {
    const { createSign } = await import("node:crypto");
    const ts = Date.now().toString();
    const payload = `${ts}GET/trade-api/v2/exchange/status`;
    const signer = createSign("RSA-SHA256");
    signer.update(payload);
    signer.end();
    const sig = signer.sign(key, "base64");
    return c.json({
      ok: true,
      key_id: KALSHI_KEY_ID,
      pem_lines: lineCount,
      pem_has_begin: hasBegin,
      pem_first_lines: snippet,
      sig_length: sig.length,
      sig_preview: sig.slice(0, 20) + "...",
    }, 200, { "Access-Control-Allow-Origin": "*" });
  } catch (err) {
    return c.json({
      ok: false,
      error: String(err),
      pem_lines: lineCount,
      pem_has_begin: hasBegin,
      pem_first_lines: snippet,
    }, 500, { "Access-Control-Allow-Origin": "*" });
  }
});

app.post("/order", async (c) => {
  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ ok: false, error: "invalid JSON body" }, 400);
  }

  const ticker   = asString(body?.ticker);
  const side     = asString(body?.side)?.toUpperCase() as Side | undefined;
  const count    = asNumber(body?.count ?? body?.contracts);
  const yesPrice = asNumber(body?.yes_price ?? body?.yesPrice);
  const noPrice  = asNumber(body?.no_price  ?? body?.noPrice);
  const action   = asString(body?.action)?.toLowerCase()  || "buy";
  const type     = asString(body?.type)?.toLowerCase()    || "fok";

  if (!ticker) return c.json({ ok: false, error: "ticker is required" }, 400);
  if (side !== "YES" && side !== "NO") return c.json({ ok: false, error: 'side must be "YES" or "NO"' }, 400);
  if (!count || count <= 0) return c.json({ ok: false, error: "count/contracts must be > 0" }, 400);

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
    const headers = await signedKalshiHeaders("POST", path);

    if (!headers["KALSHI-ACCESS-KEY"]) {
      return c.json({ ok: false, error: "missing Kalshi credentials — check KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_B64" }, 500);
    }

    const resp = await fetch(`${KALSHI_BASE_URL}${path}`, {
      method: "POST",
      headers,
      body: JSON.stringify(orderPayload),
    });

    const text = await resp.text();
    let parsed: unknown = text;
    try { parsed = JSON.parse(text); } catch { /* keep raw string */ }

    return c.json(
      { ok: resp.ok, upstream_status: resp.status, request: orderPayload, response: parsed },
      resp.ok ? 200 : 502,
    );
  } catch (error) {
    return c.json({ ok: false, error: String(error) }, 500);
  }
});

// ── /setup-db ─────────────────────────────────────────────────────────────────
const DATABASE_URL = process.env.DATABASE_URL || "";

app.get("/setup-db", async (c) => {
  if (!DATABASE_URL) return c.json({ ok: false, error: "DATABASE_URL not set" }, 500);
  try {
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });
    await sql`CREATE TABLE IF NOT EXISTS j5_equity (
      key TEXT PRIMARY KEY DEFAULT 'main',
      cash NUMERIC(12,4) NOT NULL DEFAULT 0,
      lifetime_pnl NUMERIC(12,4) NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )`;
    await sql`CREATE TABLE IF NOT EXISTS bot_updates (
      id         SERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      version    TEXT NOT NULL,
      code       TEXT NOT NULL,
      applied    BOOLEAN DEFAULT FALSE,
      applied_at TIMESTAMPTZ
    )`;
    await sql`CREATE TABLE IF NOT EXISTS bot_logs (
      id       BIGSERIAL PRIMARY KEY,
      ts       TIMESTAMPTZ DEFAULT NOW(),
      message  TEXT NOT NULL,
      event    TEXT,
      severity TEXT DEFAULT 'info'
    )`;
    await sql`CREATE INDEX IF NOT EXISTS bot_logs_ts_idx ON bot_logs (ts DESC)`;
    const rows    = await sql`SELECT COUNT(*) as count FROM bot_updates`;
    const logRows = await sql`SELECT COUNT(*) as count FROM bot_logs`;
    await sql.end();
    return c.json({ ok: true, message: "tables ready", bot_updates_rows: rows[0].count, bot_logs_rows: logRows[0].count });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

// ── /logs ─────────────────────────────────────────────────────────────────────
app.get("/logs", async (c) => {
  if (!DATABASE_URL) return c.json({ ok: false, error: "DATABASE_URL not set" }, 500);
  try {
    const limit = Math.min(500, Number(c.req.query("limit") || 300));
    const since = c.req.query("since");
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });
    let logs: any[];
    if (since) {
      logs = await sql`SELECT id, ts AS timestamp, message, event, severity FROM bot_logs WHERE ts > ${since}::timestamptz ORDER BY ts ASC LIMIT ${limit}`;
    } else {
      logs = await sql`SELECT id, ts AS timestamp, message, event, severity FROM bot_logs ORDER BY ts DESC LIMIT ${limit}`;
      logs = logs.reverse();
    }
    await sql.end();
    return c.json({ ok: true, count: logs.length, logs }, 200, { "Access-Control-Allow-Origin": "*" });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

app.post("/ingest-logs", async (c) => {
  if (!DATABASE_URL) return c.json({ ok: false, error: "DATABASE_URL not set" }, 500);
  let body: any;
  try { body = await c.req.json(); } catch { return c.json({ ok: false, error: "bad JSON" }, 400); }
  if (!Array.isArray(body) || body.length === 0) return c.json({ ok: true, inserted: 0 });
  try {
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });
    await sql`CREATE TABLE IF NOT EXISTS bot_logs (id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ DEFAULT NOW(), message TEXT NOT NULL, event TEXT, severity TEXT DEFAULT 'info')`;
    await sql`CREATE INDEX IF NOT EXISTS bot_logs_ts_idx ON bot_logs (ts DESC)`;
    const rows = body.map((r: any) => ({
      message:  String(r.message || "").slice(0, 2000),
      event:    String(r.event   || "").slice(0, 64),
      severity: String(r.severity || "info").slice(0, 20),
    }));
    await sql`INSERT INTO bot_logs ${sql(rows, "message", "event", "severity")}`;
    await sql.end();
    return c.json({ ok: true, inserted: rows.length }, 200, { "Access-Control-Allow-Origin": "*" });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

// ── /balance ──────────────────────────────────────────────────────────────────
app.get("/balance", async (c) => {
  try {
    const path = "/trade-api/v2/portfolio/balance";
    const headers = await signedKalshiHeaders("GET", path);
    if (!headers["KALSHI-ACCESS-KEY"]) return c.json({ ok: false, error: "missing credentials" }, 500);
    const resp = await fetch(`${KALSHI_BASE_URL}${path}`, { method: "GET", headers });
    const body = await resp.json() as any;
    const cents   = body?.balance ?? body?.available_balance ?? null;
    const dollars = cents !== null ? cents / 100 : null;
    return c.json({ ok: resp.ok, balance: cents, balance_dollars: dollars, raw: body },
      resp.ok ? 200 : 502, { "Access-Control-Allow-Origin": "*" });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

// ── /equity ───────────────────────────────────────────────────────────────────
app.get("/equity", async (c) => {
  if (!DATABASE_URL) return c.json({ ok: false, error: "no DATABASE_URL" }, 500);
  try {
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });
    const rows = await sql`SELECT cash, lifetime_pnl, updated_at FROM j5_equity WHERE key = 'main' LIMIT 1`;
    await sql.end();
    if (rows.length === 0) return c.json({ ok: true, exists: false, cash: null, lifetime_pnl: null });
    return c.json({ ok: true, exists: true, cash: Number(rows[0].cash), lifetime_pnl: Number(rows[0].lifetime_pnl), updated_at: rows[0].updated_at },
      200, { "Access-Control-Allow-Origin": "*" });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

app.get("/init-equity", async (c) => {
  if (!DATABASE_URL) return c.json({ ok: false, error: "no DATABASE_URL" }, 500);
  try {
    const cash        = Number(c.req.query("cash"));
    const lifetime_pnl = Number(c.req.query("lifetime_pnl") ?? "0");
    if (isNaN(cash) || cash < 0) return c.json({ ok: false, error: "provide ?cash=23.43 in the URL" }, 400);
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });
    await sql`INSERT INTO j5_equity (key, cash, lifetime_pnl, updated_at) VALUES ('main', ${cash}, ${lifetime_pnl}, NOW())
      ON CONFLICT (key) DO UPDATE SET cash = EXCLUDED.cash, lifetime_pnl = EXCLUDED.lifetime_pnl, updated_at = EXCLUDED.updated_at`;
    await sql.end();
    return c.json({ ok: true, cash, lifetime_pnl, msg: "equity set" }, 200, { "Access-Control-Allow-Origin": "*" });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

app.post("/set-equity", async (c) => {
  if (!DATABASE_URL) return c.json({ ok: false, error: "no DATABASE_URL" }, 500);
  try {
    const body       = await c.req.json() as any;
    const cash       = Number(body?.cash);
    const lifetime_pnl = body?.lifetime_pnl !== undefined ? Number(body.lifetime_pnl) : 0;
    if (isNaN(cash) || cash < 0) return c.json({ ok: false, error: "cash must be non-negative" }, 400);
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });
    await sql`INSERT INTO j5_equity (key, cash, lifetime_pnl, updated_at) VALUES ('main', ${cash}, ${lifetime_pnl}, NOW())
      ON CONFLICT (key) DO UPDATE SET cash = EXCLUDED.cash, lifetime_pnl = EXCLUDED.lifetime_pnl, updated_at = EXCLUDED.updated_at`;
    await sql.end();
    return c.json({ ok: true, cash, lifetime_pnl, msg: "equity updated" }, 200, { "Access-Control-Allow-Origin": "*" });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

// ── /hot-reload-check / /hot-reload-applied ───────────────────────────────────
app.get("/hot-reload-check", async (c) => {
  if (!DATABASE_URL) return c.json({ pending: false });
  try {
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });
    const rows = await sql`SELECT id, version, code FROM bot_updates WHERE applied = false ORDER BY created_at DESC LIMIT 1`;
    await sql.end();
    if (rows.length === 0) return c.json({ pending: false }, 200, { "Access-Control-Allow-Origin": "*" });
    return c.json({ pending: true, id: rows[0].id, version: rows[0].version, code: rows[0].code },
      200, { "Access-Control-Allow-Origin": "*" });
  } catch (err) {
    return c.json({ pending: false, error: String(err) });
  }
});

app.post("/hot-reload-applied", async (c) => {
  if (!DATABASE_URL) return c.json({ ok: false });
  try {
    const body = await c.req.json() as any;
    const id = body?.id;
    if (!id) return c.json({ ok: false, error: "missing id" }, 400);
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });
    await sql`UPDATE bot_updates SET applied = true, applied_at = NOW() WHERE id = ${id}`;
    await sql.end();
    return c.json({ ok: true }, 200, { "Access-Control-Allow-Origin": "*" });
  } catch (err) {
    return c.json({ ok: false, error: String(err) });
  }
});

// ── CORS preflight ────────────────────────────────────────────────────────────
const cors = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type" };
app.options("*", (c) => c.text("", 204, cors));

Bun.serve({ hostname: "0.0.0.0", port: PORT, fetch: app.fetch });
console.log(`kalshi-order-gateway running on port ${PORT}`);
