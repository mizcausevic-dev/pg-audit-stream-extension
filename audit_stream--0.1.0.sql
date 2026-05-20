-- audit_stream extension v0.1.0
-- Emits audit-stream-py-compatible governance events on watched table changes
-- via pg_notify on the 'audit_stream' channel. A small Python bridge daemon
-- (bridge/audit_stream_bridge.py) LISTENs and POSTs each event to the
-- audit-stream-py REST endpoint, preserving the same best-effort,
-- never-raised semantics every other Suite producer follows.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION audit_stream" to load this file. \quit

-- =========================================================================
-- Metadata table: tracks which (schema, table) pairs are watched
-- =========================================================================

CREATE TABLE watches (
    table_name TEXT PRIMARY KEY,                                       -- always stored as 'schema.table'
    event_kind TEXT NOT NULL CHECK (length(event_kind) BETWEEN 1 AND 128),
    source TEXT NOT NULL DEFAULT 'pg-audit-stream-extension',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE watches IS
    'Per-table audit-stream watches. One row per watched (schema, table) pair.';

-- =========================================================================
-- Internal: the trigger function that emits the event
-- =========================================================================

CREATE FUNCTION _emit_event() RETURNS TRIGGER AS $$
DECLARE
    full_table TEXT;
    watch_row  audit_stream.watches%ROWTYPE;
    payload    JSONB;
    row_data   JSONB;
BEGIN
    full_table := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;

    SELECT * INTO watch_row
    FROM audit_stream.watches
    WHERE table_name = full_table;

    -- Watch may have been removed between trigger fire and lookup; skip silently.
    IF NOT FOUND THEN
        RETURN COALESCE(NEW, OLD);
    END IF;

    -- to_jsonb(NULL::record) raises; guard each side.
    IF (TG_OP = 'DELETE') THEN
        row_data := jsonb_build_object('old', to_jsonb(OLD));
    ELSIF (TG_OP = 'INSERT') THEN
        row_data := jsonb_build_object('new', to_jsonb(NEW));
    ELSE  -- UPDATE
        row_data := jsonb_build_object('new', to_jsonb(NEW), 'old', to_jsonb(OLD));
    END IF;

    payload := jsonb_build_object(
        'kind',   watch_row.event_kind,
        'source', watch_row.source,
        'payload', jsonb_build_object(
            'table',     full_table,
            'operation', TG_OP
        ) || row_data
    );

    -- pg_notify caps the payload at 8000 bytes. Truncate row_data if oversize.
    IF octet_length(payload::TEXT) > 7800 THEN
        payload := jsonb_set(
            payload,
            '{payload}',
            jsonb_build_object(
                'table',     full_table,
                'operation', TG_OP,
                'truncated', true,
                'note',      'row data omitted; exceeds 8000-byte NOTIFY limit'
            )
        );
    END IF;

    PERFORM pg_notify('audit_stream', payload::TEXT);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION _emit_event() IS
    'Internal trigger function. Builds the audit-stream PublishRequest envelope as JSONB and pg_notify()s on the audit_stream channel. Best-effort; failures inside the trigger raise (the originating DML reverts).';

-- =========================================================================
-- Public API: watch / unwatch / list_watches
-- =========================================================================

CREATE FUNCTION watch(
    p_table      TEXT,
    p_event_kind TEXT,
    p_source     TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    schema_name TEXT;
    table_only  TEXT;
    full_table  TEXT;
BEGIN
    -- Resolve the table identifier. Accept either 'schema.table' or 'table' (=> public.table).
    IF position('.' IN p_table) > 0 THEN
        schema_name := split_part(p_table, '.', 1);
        table_only  := split_part(p_table, '.', 2);
    ELSE
        schema_name := 'public';
        table_only  := p_table;
    END IF;

    -- Verify the table actually exists.
    PERFORM 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = schema_name
      AND c.relname = table_only
      AND c.relkind = 'r';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'audit_stream.watch: table %.% does not exist', schema_name, table_only;
    END IF;

    full_table := schema_name || '.' || table_only;

    -- Upsert the watch record.
    INSERT INTO audit_stream.watches (table_name, event_kind, source)
    VALUES (full_table, p_event_kind, COALESCE(p_source, 'pg-audit-stream-extension'))
    ON CONFLICT (table_name) DO UPDATE
        SET event_kind = EXCLUDED.event_kind,
            source     = EXCLUDED.source;

    -- (Re-)create the trigger.
    EXECUTE format(
        'DROP TRIGGER IF EXISTS audit_stream_emit ON %I.%I',
        schema_name, table_only
    );
    EXECUTE format(
        'CREATE TRIGGER audit_stream_emit '
        || 'AFTER INSERT OR UPDATE OR DELETE ON %I.%I '
        || 'FOR EACH ROW EXECUTE FUNCTION audit_stream._emit_event()',
        schema_name, table_only
    );

    RETURN full_table;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION watch(TEXT, TEXT, TEXT) IS
    'Register a watch. Creates an AFTER INSERT/UPDATE/DELETE trigger on the named table that emits the given event_kind on the audit_stream NOTIFY channel.';

CREATE FUNCTION unwatch(p_table TEXT) RETURNS BOOLEAN AS $$
DECLARE
    schema_name TEXT;
    table_only  TEXT;
    full_table  TEXT;
    found_count INTEGER;
BEGIN
    IF position('.' IN p_table) > 0 THEN
        schema_name := split_part(p_table, '.', 1);
        table_only  := split_part(p_table, '.', 2);
    ELSE
        schema_name := 'public';
        table_only  := p_table;
    END IF;

    full_table := schema_name || '.' || table_only;

    -- Drop the trigger first (best-effort — table may have been dropped already).
    BEGIN
        EXECUTE format(
            'DROP TRIGGER IF EXISTS audit_stream_emit ON %I.%I',
            schema_name, table_only
        );
    EXCEPTION
        WHEN undefined_table THEN
            -- Table was dropped; trigger is already gone. Continue to clean up the watch row.
            NULL;
    END;

    -- Remove the watch row.
    DELETE FROM audit_stream.watches WHERE table_name = full_table;
    GET DIAGNOSTICS found_count = ROW_COUNT;

    RETURN found_count > 0;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION unwatch(TEXT) IS
    'Remove a watch. Drops the trigger and the watches-table row. Returns TRUE if a row was removed, FALSE if the table was not being watched.';

CREATE FUNCTION list_watches()
RETURNS TABLE(
    table_name TEXT,
    event_kind TEXT,
    source     TEXT,
    created_at TIMESTAMPTZ
) AS $$
    SELECT table_name, event_kind, source, created_at
    FROM audit_stream.watches
    ORDER BY created_at;
$$ LANGUAGE sql;

COMMENT ON FUNCTION list_watches() IS
    'List every currently registered watch, oldest first.';
