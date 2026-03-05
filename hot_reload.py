"""
hot_reload.py — Drop this file next to bot.py on Railway.
Bot polls Postgres for updated source code every HOT_RELOAD_INTERVAL seconds.
When a new version is found, it writes bot.py and re-execs the process.

Setup:
  1. Add this file to your repo alongside bot.py
  2. Add to bot.py main loop: `from hot_reload import check_hot_reload; check_hot_reload()`
  3. Run once to create the table:
     CREATE TABLE IF NOT EXISTS bot_updates (
       id SERIAL PRIMARY KEY,
       created_at TIMESTAMPTZ DEFAULT NOW(),
       version TEXT NOT NULL,
       code TEXT NOT NULL,
       applied BOOLEAN DEFAULT FALSE,
       applied_at TIMESTAMPTZ
     );

To push new code without redeploying:
  INSERT INTO bot_updates (version, code) VALUES ('v6', '<your new bot.py source>');

The bot will pick it up within HOT_RELOAD_INTERVAL seconds and restart itself.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# How often to check for updates (seconds)
HOT_RELOAD_INTERVAL = int(os.getenv("HOT_RELOAD_INTERVAL", "60"))
HOT_RELOAD_ENABLED = os.getenv("HOT_RELOAD_ENABLED", "true").strip().lower() in ("1", "true", "yes")
HOT_RELOAD_TABLE = os.getenv("HOT_RELOAD_TABLE", "bot_updates")

_last_check: float = 0.0
_current_version: Optional[str] = None


def _get_db_conn():
    """Get a psycopg2 connection. Returns None if unavailable."""
    try:
        import psycopg2
    except ImportError:
        return None

    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        return None

    try:
        conn = psycopg2.connect(db_url, connect_timeout=5)
        conn.autocommit = True
        return conn
    except Exception:
        return None


def _ensure_table(conn) -> bool:
    """Create the bot_updates table if it doesn't exist."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {HOT_RELOAD_TABLE} (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    version TEXT NOT NULL,
                    code TEXT NOT NULL,
                    applied BOOLEAN DEFAULT FALSE,
                    applied_at TIMESTAMPTZ
                )
            """)
        return True
    except Exception:
        return False


def _fetch_pending_update(conn) -> Optional[dict]:
    """Fetch the latest unapplied update, if any."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT id, version, code
                FROM {HOT_RELOAD_TABLE}
                WHERE applied = FALSE
                ORDER BY created_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                return {"id": row[0], "version": row[1], "code": row[2]}
    except Exception:
        pass
    return None


def _mark_applied(conn, update_id: int) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {HOT_RELOAD_TABLE}
                SET applied = TRUE, applied_at = NOW()
                WHERE id = %s
            """, (update_id,))
    except Exception:
        pass


def _log(msg: str) -> None:
    print(json.dumps({
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event": "HOT_RELOAD",
        "msg": msg,
    }), flush=True)


def check_hot_reload() -> None:
    """
    Call this from your main loop. If a new code version is found in Postgres,
    writes bot.py and re-execs this process with the new code.
    """
    global _last_check, _current_version

    if not HOT_RELOAD_ENABLED:
        return

    now = time.time()
    if now - _last_check < HOT_RELOAD_INTERVAL:
        return
    _last_check = now

    conn = _get_db_conn()
    if not conn:
        return

    try:
        _ensure_table(conn)
        update = _fetch_pending_update(conn)
        if not update:
            return

        version = update["version"]
        code = update["code"]
        update_id = update["id"]

        if version == _current_version:
            # Already running this version — mark applied and skip
            _mark_applied(conn, update_id)
            return

        _log(f"new_version_found version={version} id={update_id} — applying")

        # Write new bot.py next to this file
        bot_path = Path(__file__).parent / "bot.py"
        bot_path.write_text(code, encoding="utf-8")
        _log(f"wrote {bot_path} ({len(code)} bytes)")

        # Mark applied before re-exec so we don't loop
        _mark_applied(conn, update_id)
        conn.close()

        _log(f"re-execing process as version={version}")
        # Re-exec this process with the same argv
        os.execv(sys.executable, [sys.executable] + sys.argv)

    except Exception as e:
        _log(f"error: {str(e)[:200]}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_current_version() -> Optional[str]:
    return _current_version


def set_current_version(v: str) -> None:
    global _current_version
    _current_version = v
