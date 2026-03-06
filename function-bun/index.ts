import { Hono } from "hono";
import { createSign, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import postgres from "postgres";

const PORT = Number(process.env.PORT || 3000);
const KALSHI_BASE_URL = (process.env.KALSHI_BASE_URL || "https://api.elections.kalshi.com").replace(/\/$/, "");
const KALSHI_KEY_ID = process.env.KALSHI_API_KEY_ID || "";
const KALSHI_PRIVATE_KEY_PEM = process.env.KALSHI_PRIVATE_KEY_PEM || "";
const KALSHI_PRIVATE_KEY_PATH = process.env.KALSHI_PRIVATE_KEY_PATH || "";

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

async function loadPrivateKey(): Promise<string | null> {
  if (KALSHI_PRIVATE_KEY_PEM.trim()) return KALSHI_PRIVATE_KEY_PEM.trim();

  const raw = KALSHI_PRIVATE_KEY_PATH.trim();
  if (!raw) return null;

  if (raw.includes("BEGIN")) return raw;

  try {
    if (raw.length < 512) {
      const text = await readFile(raw, "utf-8");
      if (text.includes("BEGIN")) return text;
    }
  } catch {
    // fallthrough
  }

  try {
    const decoded = Buffer.from(raw, "base64").toString("utf-8");
    if (decoded.includes("BEGIN")) return decoded;
  } catch {
    // fallthrough
  }

  return null;
}

async function signedKalshiHeaders(method: string, path: string): Promise<Record<string, string>> {
  const key = await loadPrivateKey();
  if (!KALSHI_KEY_ID || !key) return {};

  const ts = Date.now().toString();
  const payload = `${ts}${method.toUpperCase()}${path}`;
  const signer = createSign("RSA-SHA256");
  signer.update(payload);
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
  const side = asString(body?.side)?.toUpperCase() as Side | undefined;
  const count = asNumber(body?.count ?? body?.contracts);
  const yesPrice = asNumber(body?.yes_price ?? body?.yesPrice);
  const noPrice = asNumber(body?.no_price ?? body?.noPrice);
  const action = asString(body?.action)?.toLowerCase() || "buy";
  const type = asString(body?.type)?.toLowerCase() || "limit";

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
      return c.json({ ok: false, error: "missing Kalshi credentials" }, 500);
    }

    const resp = await fetch(`${KALSHI_BASE_URL}${path}`, {
      method: "POST",
      headers,
      body: JSON.stringify(orderPayload),
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

// ── /setup-db — creates bot_updates table, hit once in browser ───────────────
const DATABASE_URL = process.env.DATABASE_URL || "";

app.get("/setup-db", async (c) => {
  if (!DATABASE_URL) {
    return c.json({ ok: false, error: "DATABASE_URL env var not set on this service" }, 500);
  }

  try {
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });

    await sql`
      CREATE TABLE IF NOT EXISTS j5_equity (
        key TEXT PRIMARY KEY DEFAULT 'main',
        cash NUMERIC(12,4) NOT NULL DEFAULT 0,
        lifetime_pnl NUMERIC(12,4) NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS bot_updates (
        id          SERIAL PRIMARY KEY,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        version     TEXT NOT NULL,
        code        TEXT NOT NULL,
        applied     BOOLEAN DEFAULT FALSE,
        applied_at  TIMESTAMPTZ
      )
    `;

    await sql`
      CREATE TABLE IF NOT EXISTS bot_logs (
        id          BIGSERIAL PRIMARY KEY,
        ts          TIMESTAMPTZ DEFAULT NOW(),
        message     TEXT NOT NULL,
        event       TEXT,
        severity    TEXT DEFAULT 'info'
      )
    `;

    await sql`CREATE INDEX IF NOT EXISTS bot_logs_ts_idx ON bot_logs (ts DESC)`;

    const rows = await sql`SELECT COUNT(*) as count FROM bot_updates`;
    const logRows = await sql`SELECT COUNT(*) as count FROM bot_logs`;
    await sql.end();

    return c.json({
      ok: true,
      message: "bot_updates + bot_logs tables ready",
      bot_updates_rows: rows[0].count,
      bot_logs_rows: logRows[0].count,
    });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

// ── /logs — proxies Railway GraphQL to avoid browser CORS ────────────────────
const RAILWAY_TOKEN = process.env.RAILWAY_TOKEN || "";
const RAILWAY_PROJECT_ID = process.env.RAILWAY_PROJECT_ID || "469c280f-68f1-4293-beab-19dceeedde7e";
const RAILWAY_SERVICE_ID = process.env.RAILWAY_SERVICE_ID || "0a405cac-8993-49bc-8589-e7349e380e48";
const RAILWAY_GQL = "https://backboard.railway.app/graphql/v2";

app.get("/logs", async (c) => {
  if (!DATABASE_URL) {
    return c.json({ ok: false, error: "DATABASE_URL not set" }, 500);
  }

  try {
    const limit = Math.min(500, Number(c.req.query("limit") || 300));
    const since = c.req.query("since"); // ISO timestamp for polling
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });

    let logs: any[];
    if (since) {
      logs = await sql`
        SELECT id, ts AS timestamp, message, event, severity
        FROM bot_logs
        WHERE ts > ${since}::timestamptz
        ORDER BY ts ASC
        LIMIT ${limit}
      `;
    } else {
      logs = await sql`
        SELECT id, ts AS timestamp, message, event, severity
        FROM bot_logs
        ORDER BY ts DESC
        LIMIT ${limit}
      `;
      logs = logs.reverse();
    }

    await sql.end();

    return c.json({ ok: true, count: logs.length, logs }, 200, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET",
    });
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

    // Ensure table exists
    await sql`
      CREATE TABLE IF NOT EXISTS bot_logs (
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMPTZ DEFAULT NOW(),
        message TEXT NOT NULL,
        event TEXT,
        severity TEXT DEFAULT 'info'
      )
    `;
    await sql`CREATE INDEX IF NOT EXISTS bot_logs_ts_idx ON bot_logs (ts DESC)`;

    const rows = body.map((r: any) => ({
      message: String(r.message || "").slice(0, 2000),
      event: String(r.event || "").slice(0, 64),
      severity: String(r.severity || "info").slice(0, 20),
    }));

    await sql`INSERT INTO bot_logs ${sql(rows, "message", "event", "severity")}`;
    await sql.end();

    return c.json({ ok: true, inserted: rows.length }, 200, {
      "Access-Control-Allow-Origin": "*",
    });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

// ── /balance — fetch real Kalshi balance (cents → dollars) ─────────────────
app.get("/balance", async (c) => {
  try {
    const path = "/trade-api/v2/portfolio/balance";
    const headers = await signedKalshiHeaders("GET", path);
    if (!headers["KALSHI-ACCESS-KEY"]) {
      return c.json({ ok: false, error: "missing Kalshi credentials" }, 500);
    }
    const resp = await fetch(`${KALSHI_BASE_URL}${path}`, { method: "GET", headers });
    const body = await resp.json() as any;
    // Kalshi returns balance in cents
    const cents = body?.balance ?? body?.available_balance ?? null;
    const dollars = cents !== null ? cents / 100 : null;
    return c.json({ ok: resp.ok, balance: cents, balance_dollars: dollars, raw: body }, 
      resp.ok ? 200 : 502, { "Access-Control-Allow-Origin": "*" });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

// ── /equity — read current equity from Postgres ──────────────────────────────
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

// ── /set-equity — manual override: set cash and optionally lifetime_pnl ──────
app.post("/set-equity", async (c) => {
  if (!DATABASE_URL) return c.json({ ok: false, error: "no DATABASE_URL" }, 500);
  try {
    const body = await c.req.json() as any;
    const cash = Number(body?.cash);
    const lifetime_pnl = body?.lifetime_pnl !== undefined ? Number(body.lifetime_pnl) : 0;
    if (isNaN(cash) || cash < 0) return c.json({ ok: false, error: "cash must be a non-negative number" }, 400);
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });
    await sql`
      INSERT INTO j5_equity (key, cash, lifetime_pnl, updated_at)
      VALUES ('main', ${cash}, ${lifetime_pnl}, NOW())
      ON CONFLICT (key) DO UPDATE
        SET cash = EXCLUDED.cash,
            lifetime_pnl = EXCLUDED.lifetime_pnl,
            updated_at = EXCLUDED.updated_at
    `;
    await sql.end();
    return c.json({ ok: true, cash, lifetime_pnl, msg: "equity updated — bot will use this on next restart" },
      200, { "Access-Control-Allow-Origin": "*" });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

app.options("/balance", (c) => c.text("", 204, { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, OPTIONS" }));
app.options("/equity", (c) => c.text("", 204, { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, OPTIONS" }));
app.options("/set-equity", (c) => c.text("", 204, { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST, OPTIONS" }));

// ── /hot-reload-check — bot polls this to get pending code updates ───────────
app.get("/hot-reload-check", async (c) => {
  if (!DATABASE_URL) return c.json({ pending: false });
  try {
    const sql = postgres(DATABASE_URL, { ssl: "prefer", max: 1, idle_timeout: 5 });
    const rows = await sql`
      SELECT id, version, code FROM bot_updates
      WHERE applied = false
      ORDER BY created_at DESC
      LIMIT 1
    `;
    await sql.end();
    if (rows.length === 0) return c.json({ pending: false }, 200, { "Access-Control-Allow-Origin": "*" });
    const row = rows[0];
    return c.json({ pending: true, id: row.id, version: row.version, code: row.code }, 200, {
      "Access-Control-Allow-Origin": "*",
    });
  } catch (err) {
    return c.json({ pending: false, error: String(err) });
  }
});

// ── /hot-reload-applied — bot calls this after applying update ───────────────
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

app.options("/hot-reload-check", (c) => c.text("", 204, { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, OPTIONS" }));
app.options("/hot-reload-applied", (c) => c.text("", 204, { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST, OPTIONS" }));

app.options("/ingest-logs", (c) => c.text("", 204, {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
}));

app.options("/logs", (c) => c.text("", 204, {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
}));

Bun.serve({
  hostname: "0.0.0.0",
  port: PORT,
  fetch: app.fetch,
});
