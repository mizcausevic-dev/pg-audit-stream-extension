# pg-audit-stream-extension

> A Postgres extension that turns any table-level CRUD into an [`audit-stream-py`](https://github.com/mizcausevic-dev/audit-stream-py)-compatible governance event — without writing one line of application code.

```sql
CREATE EXTENSION audit_stream;
SELECT audit_stream.watch('decisions', 'decision_card_status_changed', 'procurement-api');
```

From that point onward, every `INSERT` / `UPDATE` / `DELETE` on the `decisions` table fires a `pg_notify` on the `audit_stream` channel carrying a hash-chain-ready `PublishRequest` envelope. A small Python bridge daemon (`bridge/`) `LISTEN`s on that channel and forwards each event to the [audit-stream-py](https://github.com/mizcausevic-dev/audit-stream-py) HTTP endpoint, which appends it to the tamper-evident spine.

**The result**: database-tier governance enforcement that participates in the same event log as every other [Kinetic Gain Protocol Suite](https://github.com/mizcausevic-dev/kinetic-gain-protocol-suite) producer. No app-level instrumentation. No middleware. No leaks if your service is the one that goes down.

## Why this exists

Every other audit-stream producer is a service that POSTs events from its own request path. That's fine for services we own. It's a gap for the data those services touch: if a row gets `DELETE`d via a one-off psql session, no event ever lands. The Suite's tamper-evident promise quietly breaks.

This extension closes that gap. The database itself becomes a producer. Direct DML, application DML, `pg_dump`-replay, anything — every change to a watched table emits an event. The bridge daemon handles forwarding so the database stays out of the network path.

## Architecture

```
┌──────────────────────────────────┐         ┌───────────────────────────────┐
│  Your Postgres database          │         │  audit_stream_bridge          │
│                                  │         │  (Python LISTEN daemon)       │
│  ┌──────────────┐                │         │                               │
│  │ decisions    │   INSERT/      │         │  LISTEN audit_stream;         │
│  │ table        │   UPDATE/      │         │                               │
│  │              │   DELETE       │         │     │                         │
│  └──────┬───────┘                │         │     │ httpx.post(...)         │
│         │                        │         │     ▼                         │
│         ▼                        │         └─────┬─────────────────────────┘
│  ┌──────────────────────┐        │               │
│  │ audit_stream._emit() │        │               │
│  │ trigger function     │        │               ▼
│  └──────┬───────────────┘        │       ┌───────────────────┐
│         │                        │       │  audit-stream-py  │
│         │ pg_notify(             │       │  (hash-chained    │
│         │   'audit_stream',      │       │   tamper-evident  │
│         │   <PublishRequest>)    │       │   spine)          │
│         ▼                        │       └───────────────────┘
│  ┌──────────────────────┐        │
│  │ audit_stream channel │────────┼───────► (LISTEN connection)
│  └──────────────────────┘        │
└──────────────────────────────────┘
```

The trigger is `AFTER INSERT OR UPDATE OR DELETE FOR EACH ROW`. Payloads carry the `audit-stream-py` `PublishRequest` shape with the new row, the old row (or both for UPDATE), and the operation kind. The bridge is a thin loop — no business logic; failures POSTing to audit-stream-py are logged and skipped per the Suite's best-effort contract.

## Installation

### Option A — proper extension (recommended)

Requires `pg_config` + standard PGXS toolchain on the Postgres server:

```bash
git clone https://github.com/mizcausevic-dev/pg-audit-stream-extension
cd pg-audit-stream-extension
sudo make install
```

Then in your database:

```sql
CREATE EXTENSION audit_stream;
```

### Option B — schema-as-script (no PGXS needed)

If you don't have OS-level access to the Postgres server, or you're on a managed service like RDS / Cloud SQL / Supabase:

```bash
psql -d your_database -f install.sql
```

This creates the `audit_stream` schema and all functions directly. No `CREATE EXTENSION` required.

### Option C — Docker / managed Postgres

Drop `audit_stream--0.1.0.sql` into your image's `/docker-entrypoint-initdb.d/` and wrap it with the schema creation:

```sh
echo "CREATE SCHEMA audit_stream; SET search_path TO audit_stream, public;" > /docker-entrypoint-initdb.d/00-audit-stream.sql
cp audit_stream--0.1.0.sql /docker-entrypoint-initdb.d/01-audit-stream.sql
```

## The bridge daemon

```bash
pip install -e .
AUDIT_STREAM_URL=http://audit-stream:8093/events \
DATABASE_URL=postgresql://user:pass@localhost/your_db \
audit-stream-bridge
```

The bridge runs as a sidecar to your Postgres instance. It opens one long-lived connection, `LISTEN`s on the `audit_stream` channel, and forwards each `NOTIFY` payload to the audit-stream-py endpoint. Best-effort POST semantics — never raises on a single bad event.

Environment:

| Variable | Required | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | yes | Postgres connection string |
| `AUDIT_STREAM_URL` | yes | audit-stream-py POST endpoint (e.g. `http://host:8093/events`) |
| `POLL_TIMEOUT` | no (default 5) | Idle `select()` timeout in seconds |
| `LOG_LEVEL` | no (default `INFO`) | Python logging level |

## API reference

### `audit_stream.watch(table, event_kind, source = NULL) → TEXT`

Register a watch on a table. Creates an `AFTER INSERT OR UPDATE OR DELETE` trigger named `audit_stream_emit`. Returns the fully-qualified `schema.table` it watched.

```sql
-- 'public.decisions' is implied when no schema is given.
SELECT audit_stream.watch('decisions', 'decision_card_status_changed', 'procurement-api');

-- Cross-schema works too.
SELECT audit_stream.watch('billing.invoices', 'invoice_created', 'vapor-saas-billing-engine');

-- Calling watch() again on a watched table updates the event_kind/source in-place.
```

The `event_kind` should be one of the [`audit-stream-py` `EventKind` Literal](https://github.com/mizcausevic-dev/audit-stream-py/blob/main/src/audit_stream/models.py) values; the spine will reject any kind not in the Literal at publish time.

### `audit_stream.unwatch(table) → BOOLEAN`

Stops emitting events for the table. Drops the trigger and removes the row from `audit_stream.watches`. Returns `TRUE` if a watch was removed, `FALSE` if the table wasn't being watched.

```sql
SELECT audit_stream.unwatch('decisions');
```

Idempotent. Survives the table being dropped first.

### `audit_stream.list_watches() → TABLE(table_name, event_kind, source, created_at)`

```sql
SELECT * FROM audit_stream.list_watches();
```

## NOTIFY payload shape

```jsonc
{
  "kind":   "decision_card_status_changed",
  "source": "procurement-api",
  "payload": {
    "table":     "public.decisions",
    "operation": "UPDATE",
    "new": { "id": "DEC-001", "status": "approved", "vendor": "acme", ... },
    "old": { "id": "DEC-001", "status": "pending",  "vendor": "acme", ... }
  }
}
```

`INSERT` carries `new` only; `DELETE` carries `old` only; `UPDATE` carries both. If the JSON exceeds Postgres's 8000-byte `NOTIFY` ceiling, the row data is dropped and replaced with `{ "truncated": true, "note": ... }` — the spine still receives the event with the table + operation, just without the row contents. (For high-fan-out tables, write a custom trigger that emits only the columns you care about.)

## Composes with

| Concern | Repo |
| --- | --- |
| The tamper-evident spine | [`audit-stream-py`](https://github.com/mizcausevic-dev/audit-stream-py) |
| Other producers in the spine | `procurement-decision-api`, `policy-as-code-engine`, `data-contract-registry`, `aeo-validator-service`, `incident-correlation-rs`, `hash-attestation-rs`, `mcp-permission-broker` |
| The Suite the events describe | [`kinetic-gain-protocol-suite`](https://github.com/mizcausevic-dev/kinetic-gain-protocol-suite) |

## Status

**v0.1.0** — PL/pgSQL only. PG14/15/16/17 supported. Python 3.11/3.12/3.13 supported for the bridge. CI green across the full matrix.

Roadmap:

- **v0.2**: column-level filtering (`audit_stream.watch_columns(...)`) so high-frequency tables can emit only on relevant columns
- **v0.3**: optional C implementation of the trigger function for hot paths (when PL/pgSQL JSON construction becomes the bottleneck)
- **v0.4**: native logical-replication output plugin as an alternative to NOTIFY (lifts the 8000-byte ceiling, no bridge daemon needed)

## License

MIT.
