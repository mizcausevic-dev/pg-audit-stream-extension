-- Example: wire your application's `decisions` table into the audit-stream spine.
--
-- After this script runs, every INSERT/UPDATE/DELETE on public.decisions emits a
-- `decision_card_status_changed` NOTIFY on the audit_stream channel. Start the
-- bridge daemon (bridge/audit_stream_bridge.py) and every NOTIFY becomes a real
-- audit-stream-py event with a hash-chained, tamper-evident receipt.

CREATE EXTENSION IF NOT EXISTS audit_stream;

-- Example application table.
CREATE TABLE IF NOT EXISTS decisions (
    id          TEXT PRIMARY KEY,
    status      TEXT NOT NULL,
    vendor      TEXT NOT NULL,
    buyer       TEXT NOT NULL,
    rationale   TEXT,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Register the watch. From this point onward every DML on `decisions` fires a
-- NOTIFY with kind=decision_card_status_changed and source=procurement-api.
SELECT audit_stream.watch(
    p_table      => 'decisions',
    p_event_kind => 'decision_card_status_changed',
    p_source     => 'procurement-api'
);

-- Verify the watch is in place.
SELECT * FROM audit_stream.list_watches();

-- Try it.
INSERT INTO decisions (id, status, vendor, buyer, rationale)
VALUES ('DEC-2026-001', 'approved-with-conditions', 'acme-tutor', 'springfield-isd', 'See AUP §3.2.');

UPDATE decisions SET status = 'withdrawn', rationale = 'Vendor changed model behavior.' WHERE id = 'DEC-2026-001';

DELETE FROM decisions WHERE id = 'DEC-2026-001';

-- To stop emitting events on this table:
-- SELECT audit_stream.unwatch('decisions');
