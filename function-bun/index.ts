import { Hono } from "hono";

const app = new Hono();

app.get("/", (c) => c.json({ ok: true }));

export default {
  port: Number(process.env.PORT || 3000),
  fetch: app.fetch,
};
