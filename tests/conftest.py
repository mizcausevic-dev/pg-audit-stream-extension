"""Pytest fixtures: real Postgres connection + extension install / teardown.

Tests assume a Postgres 14+ instance is reachable via the PG_TEST_URL env var
(set in CI by the postgres service container). The fixture installs the
extension's SQL directly from the repo root (no `make install` required), so
the same tests run against any Postgres without PGXS / superuser tooling.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EXTENSION_SQL = REPO_ROOT / "audit_stream--0.1.0.sql"


def _install_extension(conn: psycopg.Connection) -> None:
    """Install the extension's SQL directly into the connected database.

    Uses the standalone schema-creation path (CREATE SCHEMA + run the SQL file
    contents under that search_path) so tests run on any Postgres without
    needing the .control file at the OS level. Skip the ``\\echo … \\quit``
    guard line at the top of the SQL file when running outside CREATE EXTENSION.
    """
    raw = EXTENSION_SQL.read_text(encoding="utf-8")
    lines = [
        line
        for line in raw.splitlines()
        # Skip the CREATE EXTENSION guard line at the top.
        if not (line.startswith("\\echo") or line.startswith("\\quit"))
    ]
    body = "\n".join(lines)

    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS audit_stream CASCADE;")
        cur.execute("CREATE SCHEMA audit_stream;")
        cur.execute("SET search_path TO audit_stream, public;")
        cur.execute(body)


@pytest.fixture
def db() -> Iterator[psycopg.Connection]:
    url = os.environ.get("PG_TEST_URL")
    if not url:
        pytest.skip("PG_TEST_URL not set — skipping live-Postgres tests")

    conn = psycopg.connect(url, autocommit=True)
    try:
        _install_extension(conn)
        yield conn
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS audit_stream CASCADE;")
            cur.execute("DROP TABLE IF EXISTS test_decisions;")
        conn.close()
