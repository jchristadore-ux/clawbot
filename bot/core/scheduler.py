"""
core/scheduler.py — Health server + timing utilities.

Health server: Railway needs a responding HTTP endpoint to keep the container healthy.
utc_iso: consistent timestamp formatting used throughout.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def start_health_server(port: int = 3000) -> None:
    """Start a background HTTP server on PORT to satisfy Railway health checks."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            path = (self.path or "").lower().split("?")[0]
            if path in ("/health", "/", "/healthz", "/healthcheck"):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"ok": True, "service": "johnny5", "ts": utc_iso()}).encode()
                )
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt: str, *args: Any) -> None:
            return  # suppress access logs

    server = HTTPServer(("0.0.0.0", port), Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True, name="health-server")
    t.start()
