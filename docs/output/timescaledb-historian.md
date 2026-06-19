# TimescaleDB Historian Output

Archives one UNS data contract into TimescaleDB using the UMH Historian schema. The
plugin owns the schema bootstrap, the value/attribute writes, metadata de-duplication,
and the datatype/conflict guards, so a bridge write flow is just an input and this output
— no JavaScript processor or hand-written `sql_raw`.

## Prerequisites

- PostgreSQL 13+ with the TimescaleDB and `ltree` extensions available.
- A non-superuser owner role, created once before the bridge starts (the bridge logs in
  as this role and cannot create it itself):

  ```sql
  CREATE ROLE umh_owner WITH LOGIN PASSWORD 'change-me';
  GRANT CREATE, CONNECT ON DATABASE umh TO umh_owner;
  GRANT CREATE ON SCHEMA public TO umh_owner;
  ```

## Configuration

| Field | Required | Default | Description |
|---|---|---|---|
| `host` | yes | — | TimescaleDB/Postgres host. |
| `port` | no | `5432` | Port. |
| `database` | no | `umh` | Database name. |
| `username` | no | `umh_owner` | Login role. |
| `password` | yes | — | Role password (plaintext in config; redacted in logs). |
| `sslmode` | no | `require` | `require` \| `disable` \| `verify-full`. |
| `sslrootcert` / `sslcert` / `sslkey` | no | `""` | TLS cert paths inside the container. |
| `data_contract` | yes | — | Bare lowercase contract name, e.g. `pump`; no leading `_`, no `_vN` suffix. |
| `metadata_keys_all` | no | `true` | Store every metadata key except structural/high-churn keys. |
| `metadata_keys` | no | `[]` | Allowlist used only when `metadata_keys_all=false`. |
| `compress_after` | no | `168h` | Compress chunks older than this. |
| `retention` | no | `""` | Drop chunks older than this; empty keeps data forever. |

## What it writes

For `data_contract: pump`, the plugin creates and writes two hypertables:

- **`value_pump`** — one row per `(tag, millisecond)`. Numbers and booleans land in
  `value_num`, strings and JSON in `value_text`.
- **`attribute_pump`** — the message metadata as a JSON object, queryable via
  `attribute->>'key'` and `attribute @> '{...}'`.

`get_topic_id(location_path, virtual_path, data_contract, tag_name)` resolves a tag to its
`topic_id` for ad-hoc and Grafana queries.

## Behavior

- **Startup check.** `Connect()` verifies the server version and bootstraps the schema, so
  an unreachable, too-old, or misconfigured database fails the bridge at startup instead of
  running silently against it.
- **Idempotent replays.** An identical value at the same `(tag, ts)` is absorbed.
- **Conflict and datatype guards halt the bridge.** A *different* value at the same
  `(tag, ts)`, or a tag whose datatype flips (numeric ↔ text), RAISEs and stops the bridge
  until the source is fixed — a deliberate choice so corrupt history is never written.
  This includes a tag emitting two distinct values within one millisecond, which the
  millisecond UNS timestamp cannot distinguish from a real conflict.
- **Malformed messages are dropped, not nacked.** Wrong `data_contract`, missing
  `location_path`/`tag_name`, an empty location path, a non-finite number, or an
  unparseable timestamp drop the message and increment the `messages_dropped` metric
  (labelled by `reason`), so one bad message never stalls the stream.
- **Metadata de-duplication.** An attribute row is rewritten only when its key set changes,
  via an in-process fingerprint cache (per-process, bounded by tag cardinality).

## Quick example

```yaml
output:
  timescaledb_historian:
    host: timescaledb.example.com
    password: change-me
    data_contract: pump
```

A complete, runnable bridge example (including the UNS source form) is in
[`config/timescaledb-historian-example.yaml`](../../config/timescaledb-historian-example.yaml).
