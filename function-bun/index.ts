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
      CREATE TABLE IF NOT EXISTS bot_updates (
        id          SERIAL PRIMARY KEY,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        version     TEXT NOT NULL,
        code        TEXT NOT NULL,
        applied     BOOLEAN DEFAULT FALSE,
        applied_at  TIMESTAMPTZ
      )
    `;

    const rows = await sql`SELECT COUNT(*) as count FROM bot_updates`;
    await sql.end();

    return c.json({
      ok: true,
      message: "bot_updates table created (or already existed)",
      row_count: rows[0].count,
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
  if (!RAILWAY_TOKEN) {
    return c.json({ ok: false, error: "RAILWAY_TOKEN not set on this service" }, 500);
  }

  try {
    // Step 1: get latest deployment ID
    const r1 = await fetch(RAILWAY_GQL, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${RAILWAY_TOKEN}` },
      body: JSON.stringify({
        query: `query { deployments(input: { projectId: "${RAILWAY_PROJECT_ID}", serviceId: "${RAILWAY_SERVICE_ID}" }, first: 1) { edges { node { id status createdAt } } } }`,
      }),
    });
    const d1 = await r1.json() as any;
    if (d1.errors) return c.json({ ok: false, error: d1.errors[0]?.message }, 500);

    const dep = d1?.data?.deployments?.edges?.[0]?.node;
    if (!dep) return c.json({ ok: false, error: "No deployment found" }, 404);

    // Step 2: get logs for that deployment
    const r2 = await fetch(RAILWAY_GQL, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${RAILWAY_TOKEN}` },
      body: JSON.stringify({
        query: `query { deploymentLogs(deploymentId: "${dep.id}") { timestamp message severity } }`,
      }),
    });
    const d2 = await r2.json() as any;
    if (d2.errors) return c.json({ ok: false, error: d2.errors[0]?.message }, 500);

    const logs = d2?.data?.deploymentLogs || [];

    return c.json({
      ok: true,
      deployment_id: dep.id,
      deployment_status: dep.status,
      count: logs.length,
      logs,
    }, 200, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET",
    });
  } catch (err) {
    return c.json({ ok: false, error: String(err) }, 500);
  }
});

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
