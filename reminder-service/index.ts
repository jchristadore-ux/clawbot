/**
 * johnny5-reminder-service
 *
 * Standalone Railway service. Wakes up every 60 seconds, checks if today
 * has a scheduled log drop, and fires reminders at 9am ET via:
 *   - Telegram (same bot token as Johnny5)
 *   - Email (Gmail SMTP via nodemailer)
 *
 * Environment variables required:
 *   TELEGRAM_BOT_TOKEN   — same as Johnny5
 *   TELEGRAM_CHAT_ID     — same as Johnny5
 *   GMAIL_USER           — Gmail address sending the reminder
 *   GMAIL_APP_PASSWORD   — Gmail App Password (not your login password)
 *   REMINDER_EMAIL       — destination email (jchristadore@gmail.com)
 *   TZ                   — set to "America/New_York" in Railway
 */

import nodemailer from "nodemailer";
import { createServer } from "http";

// ── Schedule ──────────────────────────────────────────────────────────────────
// Each entry: { date: "YYYY-MM-DD", phase, what, action }
const SCHEDULE = [
  {
    date: "2026-03-11",
    phase: "Phase 1",
    dayLabel: "Wednesday",
    action: "First Full Session Log Drop",
    what: "Send the first 30 lines of boot log + full session log since deploy.\nI will check: v12 confirmed running, equity loaded, ALL entry prices 25-75c, z agreeing with direction on every trade.",
  },
  {
    date: "2026-03-13",
    phase: "Phase 1",
    dayLabel: "Friday",
    action: "Mid-Week CSV Export",
    what: "Send CSV export covering all trades since Mar 11.\nI will verify: no lottery trades, contract sizes ≤5, z direction alignment on every entry.",
  },
  {
    date: "2026-03-16",
    phase: "⚠️  PHASE 1 GATE",
    dayLabel: "Monday",
    action: "Phase 1 Pass/Fail Decision",
    what: "Send full CSV + log dump.\nI will run the Phase 1 gate check and tell you: PASS → proceed to Phase 2, or FAIL → stop and fix.",
    isGate: true,
  },
  {
    date: "2026-03-19",
    phase: "Phase 2",
    dayLabel: "Thursday",
    action: "Mid-Week CSV — Trade Count Check",
    what: "Send CSV export.\nI will check trade count and early win rate. Should have 15-20 trades by now.",
  },
  {
    date: "2026-03-21",
    phase: "Phase 2",
    dayLabel: "Saturday",
    action: "CSV — Avg Win vs Avg Loss Check",
    what: "Send CSV export.\nI will check avg win vs avg loss and distribution. Should have 25-35 trades.",
  },
  {
    date: "2026-03-23",
    phase: "⚠️  PHASE 2 GATE",
    dayLabel: "Monday",
    action: "Phase 2 Pass/Fail — 50-Trade Statistical Check",
    what: "Send full CSV.\nI will run 50-trade stats: win rate, avg win/loss ratio, jackpot-strip test. Pass → Phase 3.",
    isGate: true,
  },
  {
    date: "2026-03-26",
    phase: "Phase 3",
    dayLabel: "Thursday",
    action: "CSV — 100-Trade Stats",
    what: "Send CSV export.\nI will check 100-trade stats and shape of the PnL curve (smooth = edge, lumpy = variance).",
  },
  {
    date: "2026-03-28",
    phase: "Phase 3",
    dayLabel: "Saturday",
    action: "CSV — Jackpot Strip Test",
    what: "Send CSV export.\nKey question: is PnL positive after removing the single best trade? This is the most important check of Phase 3.",
  },
  {
    date: "2026-03-30",
    phase: "⚠️  PHASE 3 GATE",
    dayLabel: "Monday",
    action: "Phase 3 Pass/Fail — Full Green-Light Checklist",
    what: "Send full CSV + log dump.\nI will run the complete green-light checklist against 150+ trades. Pass → Phase 4 (final week).",
    isGate: true,
  },
  {
    date: "2026-04-02",
    phase: "Phase 4",
    dayLabel: "Thursday",
    action: "Drift Check CSV",
    what: "Send CSV export.\nPhase 4 is dress rehearsal — no changes. I will verify nothing has drifted from Phase 3 behavior.",
  },
  {
    date: "2026-04-04",
    phase: "Phase 4",
    dayLabel: "Saturday",
    action: "Manual Trade Review — Every Entry Inspected",
    what: "Send full CSV.\nWe will go through every trade from the past 7 days together. For each one: does the entry make sense? Would you take this trade?",
  },
  {
    date: "2026-04-06",
    phase: "🚨 PHASE 4 GATE — FINAL GO/NO-GO",
    dayLabel: "Monday",
    action: "FINAL GO / NO-GO DECISION",
    what: "Send full CSV + log dump.\nThis is the final gate. I will run every check from all 4 phases. If all pass: GO LIVE tomorrow (Apr 7). If any fail: extend paper.",
    isGate: true,
    isFinal: true,
  },
  {
    date: "2026-04-07",
    phase: "✅  GO LIVE",
    dayLabel: "Tuesday",
    action: "FLIP LIVE_MODE=true",
    what: "Set LIVE_MODE=true in Railway. Within 2 hours, send me the first 5 live trade fills.\nI will verify: FOK orders filling, equity syncing, Telegram firing, no ghost positions.",
    isLive: true,
  },
];

// ── State: track what has already fired today ─────────────────────────────────
const firedToday = new Set();

// ── Helpers ───────────────────────────────────────────────────────────────────

function todayET() {
  // Returns "YYYY-MM-DD" in Eastern Time
  return new Date().toLocaleDateString("en-CA", { timeZone: "America/New_York" });
}

function nowHourET() {
  return parseInt(
    new Date().toLocaleString("en-US", { timeZone: "America/New_York", hour: "numeric", hour12: false }),
    10,
  );
}

function getRemindersForToday() {
  const today = todayET();
  return SCHEDULE.filter((s) => s.date === today);
}

// ── Telegram ──────────────────────────────────────────────────────────────────

async function sendTelegram(message) {
  const token = (process.env.TELEGRAM_BOT_TOKEN || "").trim();
  const chatId = (process.env.TELEGRAM_CHAT_ID || "").trim();
  if (!token || !chatId) {
    console.warn("TELEGRAM not configured — skipping");
    return;
  }
  const url = `https://api.telegram.org/bot${token}/sendMessage`;
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text: message, parse_mode: "HTML" }),
  });
  if (!resp.ok) {
    console.error("Telegram error:", resp.status, await resp.text());
  } else {
    console.log("Telegram sent OK");
  }
}

// ── Email ─────────────────────────────────────────────────────────────────────

async function sendEmail(subject, htmlBody) {
  const user = (process.env.GMAIL_USER || "").trim();
  const pass = (process.env.GMAIL_APP_PASSWORD || "").trim();
  const to   = (process.env.REMINDER_EMAIL || "jchristadore@gmail.com").trim();

  if (!user || !pass) {
    console.warn("GMAIL not configured — skipping email");
    return;
  }

  const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: { user, pass },
  });

  await transporter.sendMail({
    from: `"Johnny5 Reminder" <${user}>`,
    to,
    subject,
    html: htmlBody,
  });
  console.log("Email sent OK →", to);
}

// ── Message builders ──────────────────────────────────────────────────────────

function buildTelegramMessage(entry) {
  const emoji = entry.isLive ? "🚀" : entry.isFinal ? "🚨" : entry.isGate ? "⚠️" : "📋";
  const lines = [
    `${emoji} <b>JOHNNY5 — ${entry.phase}</b>`,
    `📅 <b>${entry.dayLabel}, ${entry.date}</b>`,
    ``,
    `<b>${entry.action}</b>`,
    ``,
    entry.what.replace(/\n/g, "\n"),
    ``,
    entry.isGate || entry.isLive
      ? `<b>⏰ ACTION REQUIRED TODAY — send to Claude when ready.</b>`
      : `<b>⏰ Reminder: send your log/CSV to Claude today.</b>`,
  ];
  return lines.join("\n");
}

function buildEmailHTML(entry) {
  const color = entry.isLive ? "#1A6B3A" : entry.isFinal ? "#8B1A1A" : entry.isGate ? "#7A5200" : "#1B3A6B";
  const bgColor = entry.isLive ? "#D6F0E0" : entry.isFinal ? "#FAE0E0" : entry.isGate ? "#FEF5E0" : "#D6E4F7";

  const whatHtml = entry.what
    .split("\n")
    .map((l) => l.trim() ? `<p style="margin:4px 0">${l}</p>` : "<br>")
    .join("");

  return `
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px;color:#222">
  <div style="background:${color};color:#fff;padding:16px 20px;border-radius:6px 6px 0 0">
    <div style="font-size:13px;opacity:0.8">JOHNNY5 v12 — Go-Live Plan</div>
    <div style="font-size:22px;font-weight:bold;margin-top:4px">${entry.phase}</div>
  </div>
  <div style="background:${bgColor};padding:12px 20px;border-left:4px solid ${color}">
    <div style="font-size:13px;color:#555">${entry.dayLabel}, ${entry.date}</div>
    <div style="font-size:17px;font-weight:bold;color:${color};margin-top:4px">${entry.action}</div>
  </div>
  <div style="background:#f9f9f9;padding:16px 20px;border:1px solid #e0e0e0">
    <div style="font-weight:bold;margin-bottom:8px;color:#333">What to send Claude:</div>
    ${whatHtml}
  </div>
  <div style="background:${color};color:#fff;padding:12px 20px;border-radius:0 0 6px 6px;font-weight:bold;font-size:14px">
    ⏰ Send your log/CSV to Claude at claude.ai when ready — Claude will analyze and update your plan.
  </div>
  <div style="margin-top:16px;font-size:11px;color:#999;text-align:center">
    Johnny5 Reminder Service • This is an automated message
  </div>
</body>
</html>`;
}

// ── Main loop ─────────────────────────────────────────────────────────────────

async function checkAndFire() {
  const hour = nowHourET();
  const today = todayET();

  // Fire at 9am ET
  if (hour !== 9) return;

  const reminders = getRemindersForToday();
  if (reminders.length === 0) return;

  for (const entry of reminders) {
    const key = `${entry.date}`;
    if (firedToday.has(key)) continue; // already fired today

    console.log(`[${new Date().toISOString()}] Firing reminder for ${entry.date} — ${entry.action}`);

    const tgMsg = buildTelegramMessage(entry);
    const emailSubject = `[Johnny5] ${entry.phase} — ${entry.action} (${entry.date})`;
    const emailHtml = buildEmailHTML(entry);

    await Promise.allSettled([
      sendTelegram(tgMsg),
      sendEmail(emailSubject, emailHtml),
    ]);

    firedToday.add(key);
    console.log(`[${new Date().toISOString()}] Reminder fired for ${entry.date}`);
  }
}

// ── Health server (Railway requires a responding port) ─────────────────────────

const PORT = parseInt(process.env.PORT || "3001", 10);
createServer((req, res) => {
  const today = todayET();
  const upcoming = SCHEDULE.filter((s) => s.date >= today).slice(0, 3);
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    ok: true,
    service: "johnny5-reminder-service",
    currentTimeET: new Date().toLocaleString("en-US", { timeZone: "America/New_York" }),
    todayET: today,
    nextReminders: upcoming.map((s) => ({ date: s.date, phase: s.phase, action: s.action })),
    firedToday: [...firedToday],
  }, null, 2));
}).listen(PORT, () => {
  console.log(`[reminder-service] Health server on port ${PORT}`);
});

// ── Start ─────────────────────────────────────────────────────────────────────

console.log(`[reminder-service] Started. Checking every 60s. Current ET time: ${
  new Date().toLocaleString("en-US", { timeZone: "America/New_York" })
}`);

// Run immediately on boot (catches a 9am boot edge case)
checkAndFire().catch(console.error);

// Then every 60 seconds
setInterval(() => {
  checkAndFire().catch(console.error);
}, 60_000);
