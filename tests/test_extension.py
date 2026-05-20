"""End-to-end tests against a real Postgres instance.

Exercises:
  - audit_stream.watch / unwatch / list_watches
  - NOTIFY payload shape on INSERT, UPDATE, DELETE
  - Source override (custom producer name)
  - Watch removal stops emissions
  - Non-existent table raises
"""

from __future__ import annotations

import json
import select
import time
from typing import Any

import psycopg
import pytest


def _drain_notifies(conn: psycopg.Connection, timeout: float = 2.0) -> list[dict[str, Any]]:
    """Block up to `timeout` seconds collecting NOTIFY payloads on this connection."""
    end = time.monotonic() + timeout
    events: list[dict[str, Any]] = []
    while True:
        remaining = end - time.monotonic()
        if remaining <= 0:
            break
        ready, _, _ = select.select([conn], [], [], remaining)
        if not ready:
            break
        for notify in conn.notifies():
            events.append(json.loads(notify.payload))
        if events:  # First payload(s) arrived — give a tiny extra window then return.
            time.sleep(0.05)
            break
    return events


@pytest.fixture
def watched_table(db: psycopg.Connection):
    """A small table watched as 'decision_card_status_changed' on source='test'."""
    with db.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_decisions;")
        cur.execute(
            "CREATE TABLE test_decisions (id TEXT PRIMARY KEY, status TEXT NOT NULL, vendor TEXT);"
        )
        cur.execute(
            "SELECT audit_stream.watch('test_decisions', 'decision_card_status_changed', 'test');"
        )
        cur.execute("LISTEN audit_stream;")
    yield db
    with db.cursor() as cur:
        cur.execute("SELECT audit_stream.unwatch('test_decisions');")


def test_watch_registers_in_list(db: psycopg.Connection) -> None:
    with db.cursor() as cur:
        cur.execute("CREATE TABLE foo (id INT PRIMARY KEY);")
        cur.execute("SELECT audit_stream.watch('foo', 'something_changed');")
        cur.execute("SELECT table_name, event_kind, source FROM audit_stream.list_watches();")
        rows = cur.fetchall()
    assert rows == [("public.foo", "something_changed", "pg-audit-stream-extension")]


def test_watch_rejects_unknown_table(db: psycopg.Connection) -> None:
    with db.cursor() as cur, pytest.raises(psycopg.errors.RaiseException):
        cur.execute("SELECT audit_stream.watch('does_not_exist', 'x');")


def test_insert_emits_event(watched_table: psycopg.Connection) -> None:
    with watched_table.cursor() as cur:
        cur.execute(
            "INSERT INTO test_decisions (id, status, vendor) "
            "VALUES ('DEC-001', 'approved', 'acme');"
        )

    events = _drain_notifies(watched_table)
    assert len(events) == 1
    ev = events[0]
    assert ev["kind"] == "decision_card_status_changed"
    assert ev["source"] == "test"
    assert ev["payload"]["table"] == "public.test_decisions"
    assert ev["payload"]["operation"] == "INSERT"
    assert ev["payload"]["new"]["id"] == "DEC-001"
    assert ev["payload"]["new"]["status"] == "approved"
    assert "old" not in ev["payload"]


def test_update_emits_event_with_old_and_new(watched_table: psycopg.Connection) -> None:
    with watched_table.cursor() as cur:
        cur.execute(
            "INSERT INTO test_decisions (id, status, vendor) VALUES ('DEC-002', 'pending', 'acme');"
        )
        _drain_notifies(watched_table)  # discard the insert
        cur.execute("UPDATE test_decisions SET status = 'approved' WHERE id = 'DEC-002';")

    events = _drain_notifies(watched_table)
    assert len(events) == 1
    ev = events[0]
    assert ev["payload"]["operation"] == "UPDATE"
    assert ev["payload"]["old"]["status"] == "pending"
    assert ev["payload"]["new"]["status"] == "approved"


def test_delete_emits_event_with_only_old(watched_table: psycopg.Connection) -> None:
    with watched_table.cursor() as cur:
        cur.execute(
            "INSERT INTO test_decisions (id, status, vendor) "
            "VALUES ('DEC-003', 'rejected', 'acme');"
        )
        _drain_notifies(watched_table)
        cur.execute("DELETE FROM test_decisions WHERE id = 'DEC-003';")

    events = _drain_notifies(watched_table)
    assert len(events) == 1
    ev = events[0]
    assert ev["payload"]["operation"] == "DELETE"
    assert ev["payload"]["old"]["id"] == "DEC-003"
    assert "new" not in ev["payload"]


def test_unwatch_stops_emissions(watched_table: psycopg.Connection) -> None:
    with watched_table.cursor() as cur:
        cur.execute("SELECT audit_stream.unwatch('test_decisions');")
        cur.execute(
            "INSERT INTO test_decisions (id, status, vendor) "
            "VALUES ('DEC-004', 'approved', 'acme');"
        )

    events = _drain_notifies(watched_table, timeout=1.0)
    assert events == []


def test_unwatch_returns_false_for_unknown(db: psycopg.Connection) -> None:
    with db.cursor() as cur:
        cur.execute("SELECT audit_stream.unwatch('never_watched');")
        result = cur.fetchone()
    assert result == (False,)
