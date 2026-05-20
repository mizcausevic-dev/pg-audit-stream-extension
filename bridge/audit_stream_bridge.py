"""audit_stream_bridge — LISTEN on Postgres NOTIFY, POST to audit-stream-py.

Connects to a Postgres database, LISTENs on the ``audit_stream`` channel
(populated by the audit_stream Postgres extension), and POSTs each event
to the audit-stream-py REST endpoint at ``AUDIT_STREAM_URL``.

Best-effort semantics: a failed POST is logged and skipped. The bridge
never raises on a single bad event. Designed to run as a long-lived
sidecar; restart-tolerant.

Environment variables:

  DATABASE_URL       Postgres connection string (required).
  AUDIT_STREAM_URL   audit-stream-py POST endpoint, e.g. http://host:8093/events (required).
  POLL_TIMEOUT       Seconds between select() wakeups when idle. Default 5.
  LOG_LEVEL          Python logging level. Default INFO.

Usage:

  AUDIT_STREAM_URL=http://localhost:8093/events \\
  DATABASE_URL=postgresql://user:pass@localhost/db \\
  python -m audit_stream_bridge
"""

from __future__ import annotations

import json
import logging
import os
import select
import signal
import sys
from typing import Any

import httpx
import psycopg

logger = logging.getLogger("audit_stream_bridge")

CHANNEL = "audit_stream"
HTTP_TIMEOUT_SECONDS = 5.0


def post_event(client: httpx.Client, url: str, event: dict[str, Any]) -> None:
    """POST one event to audit-stream-py. Never raises."""
    try:
        response = client.post(url, json=event)
        if response.status_code >= 400:
            logger.warning(
                "audit-stream POST %s returned HTTP %s (body: %s)",
                url,
                response.status_code,
                response.text[:200],
            )
    except Exception as exc:  # noqa: BLE001 — best-effort, never raised
        logger.warning("audit-stream POST failed (best-effort, not raised): %s", exc)


def run(database_url: str, audit_stream_url: str, poll_timeout: float = 5.0) -> int:
    """Connect, LISTEN, and forward forever. Returns a process exit code."""
    logger.info(
        "connecting to Postgres at %s ; forwarding to %s",
        database_url.split("@")[-1],
        audit_stream_url,
    )

    running = True

    def _shutdown(signum: int, _frame: object) -> None:
        nonlocal running
        logger.info("received signal %s, shutting down", signum)
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    with (
        psycopg.connect(database_url, autocommit=True) as conn,
        httpx.Client(timeout=HTTP_TIMEOUT_SECONDS) as client,
    ):
        with conn.cursor() as cur:
            cur.execute(f"LISTEN {CHANNEL};")  # noqa: S608 — channel is constant

        logger.info("listening on channel '%s'", CHANNEL)

        while running:
            # Block until something happens on the connection or the poll timeout expires.
            ready, _, _ = select.select([conn], [], [], poll_timeout)
            if not ready:
                continue

            for notify in conn.notifies():
                try:
                    event = json.loads(notify.payload)
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "could not parse NOTIFY payload as JSON (%s); skipping. raw=%s",
                        exc,
                        notify.payload[:200],
                    )
                    continue
                post_event(client, audit_stream_url, event)

    logger.info("bridge stopped cleanly")
    return 0


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    database_url = os.environ.get("DATABASE_URL")
    audit_stream_url = os.environ.get("AUDIT_STREAM_URL")
    poll_timeout = float(os.environ.get("POLL_TIMEOUT", "5"))

    if not database_url:
        logger.error("DATABASE_URL is not set")
        return 2
    if not audit_stream_url:
        logger.error("AUDIT_STREAM_URL is not set")
        return 2

    return run(database_url, audit_stream_url, poll_timeout)


if __name__ == "__main__":
    sys.exit(main())
