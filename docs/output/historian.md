# TimescaleDB Historian Output

Saves one UNS data contract into TimescaleDB using the UMH Historian schema. The
plugin owns the schema bootstrap, the value/attribute writes, metadata de-duplication,
and the datatype/conflict guards, so a bridge write flow is just an input and this output.
No JavaScript processor or hand-written `sql_raw` is needed.

## Prerequisites

- PostgreSQL 13+ with the TimescaleDB and `ltree` extensions available.
- A non-superuser owner role, created once before the bridge starts (the bridge logs in
  as this role and cannot create it itself). It creates and owns the dedicated `umh` schema
  via the database-level grant, so no privilege on `public` is needed:

  ```sql
  CREATE ROLE umh_owner WITH LOGIN PASSWORD 'change-me';
  GRANT CREATE, CONNECT ON DATABASE umh TO umh_owner;
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

All objects live in a dedicated `umh` schema. For `data_contract: pump`, the plugin creates
and writes two hypertables:

- **`umh.value_pump`** — one row per `(tag, millisecond)`. Numbers and booleans land in
  `value_num`, strings and JSON in `value_text`.
- **`umh.attribute_pump`** — the message metadata as a JSON object, queryable via
  `attribute->>'key'` and `attribute @> '{...}'`.

`umh.get_topic_id(location_path, virtual_path, data_contract, tag_name)` resolves a tag to
its `topic_id` for ad-hoc and Grafana queries.

## Behavior

- **Startup check.** `Connect()` verifies the server version and bootstraps the schema, so
  an unreachable, too-old, or misconfigured database fails the bridge at startup rather than
  writing to a misconfigured database unnoticed.
- **Idempotent replays.** An identical value at the same `(tag, ts)` is absorbed.
- **Conflict and datatype guards halt the bridge.** A *different* value at the same
  `(tag, ts)`, or a tag whose datatype flips (numeric ↔ text), RAISEs and stops the bridge
  until the source is fixed. This is deliberate: corrupt history is never written. It
  includes a tag emitting two distinct values within one millisecond, which the millisecond
  UNS timestamp cannot distinguish from a real conflict, so this contract is unsuitable for
  tags that emit distinct values faster than 1 kHz.
- **Malformed messages are dropped, not nacked.** Wrong `data_contract`, missing
  `location_path`/`tag_name`, an empty location path, a non-finite number, or an
  unparseable timestamp drop the message and increment the `historian_messages_dropped` metric
  (labelled by `reason`), so one bad message never stalls the stream.
- **Metadata de-duplication.** An attribute row is rewritten only when its key set changes,
  via an in-process, LRU-bounded fingerprint cache. The cache is cleared on restart, so
  each tag re-emits its metadata once after a restart (absorbed idempotently).

## Metrics

On top of benthos's built-in output metrics (`output_sent`, `output_error`,
`output_latency_ns`), the plugin emits:

- `historian_value_rows_written` — value rows upserted (counted after the batch commits).
- `historian_attribute_rows_written` — attribute rows upserted; the gap below the value-row
  count is metadata de-duplication at work.
- `historian_messages_dropped` (labelled by `reason`) — messages dropped before any write.
- `historian_dedup_cache_size` — current dedup-cache entry count.

## Numeric precision

`value_num` is `DOUBLE PRECISION`. That is exact for sensor floats but loses precision for
integer counters above 2^53 (~9e15) and for exact decimals. Route such tags to a text data
contract instead, where the value is stored verbatim in `value_text`.

## Location identity

The location is canonicalized into an `ltree` path: every character outside `[A-Za-z0-9_]`
becomes `_`, each label is truncated to 255 characters, and empty labels are dropped. So
`enterprise.line-1`, `enterprise.line_1`, and `enterprise.line@1` all resolve to the **same**
`topic_id` and share one time-series. Distinguish sources by their path segments, not by
punctuation alone.

## Schema and compatibility

The plugin owns the schema: it bootstraps the baseline DDL into the `umh` schema
idempotently on first connect and **never alters an already-created
`umh.value_<contract>` / `umh.attribute_<contract>` table**. A breaking schema change ships
as a new contract (new tables), never an in-place migration. (`ltree` stays in `public`,
its conventional shared home.)

The baseline is a port of the Management Console TimescaleDB Historian template and writes
the same tables. To avoid schema drift, a given contract/database must be written by exactly
**one** writer type — the plugin **or** the template, never both.

## Quick example

```yaml
input:
  uns:
    umh_topics:
      - '^umh\.v1\..*\._pump.*'
output:
  historian:
    host: timescaledb.example.com
    password: change-me
    data_contract: pump
```

To deploy a bridge against this output from the Management Console, use the
**Historian** template in the Add Bridge wizard.
