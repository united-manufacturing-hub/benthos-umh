# TimescaleDB Historian Output

Saves one UNS data contract into TimescaleDB using the UMH Historian schema. The
plugin owns the schema bootstrap, the value/attribute writes, metadata de-duplication,
and the datatype/conflict guards, so a bridge write flow is just an input and this output.
No JavaScript processor or hand-written `sql_raw` is needed.

## Prerequisites

- PostgreSQL 16+ with the TimescaleDB and `ltree` extensions available (16+ so `ltree` labels accept hyphens).
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
| `allow_unvalidated_data` | no | `false` | Store data from an **unversioned** contract (e.g. `_historian`), accepting that its datatypes are never schema-checked. No effect on a versioned contract, where a bypassed schema is always rejected. |
| `data_contract_name` | yes | — | Bare lowercase contract name, e.g. `pump`; no leading `_`, no `_vN` suffix. Stored in `umh.tag.data_contract_name` in its UNS form with a leading underscore (`_pump`), matching the topic's data-contract segment. |
| `metadata_keys_all` | no | `true` | Store every metadata key except structural/high-churn keys and any `metadata_keys_exclude` match. |
| `metadata_keys` | no | `[]` | Allowlist used only when `metadata_keys_all=false`. |
| `metadata_keys_exclude` | no | `[]` | Blacklist applied only when `metadata_keys_all=true`. Each entry is an exact key name or a trailing-`*` prefix (e.g. `opcua_*`); matches are dropped on top of the built-in exclusions. A bare `*` drops everything. Ignored in allowlist mode. |
| `compress_after` | no | `168h` | Compress chunks older than this. **Applied once at first bootstrap** — changing it in the bridge afterward has no effect (see [Changing compression or retention](#changing-compression-or-retention)). |
| `retention` | no | `""` | Drop chunks older than this; empty keeps data forever. **Applied once at first bootstrap** — changing it in the bridge afterward has no effect (see [Changing compression or retention](#changing-compression-or-retention)). |
| `batching` | no | — | benthos batch policy (`count` / `period` / `byte_size`). The whole batch is written in one transaction, so larger batches raise throughput; e.g. `count: 1000`, `period: 1s`. |
| `max_in_flight` | no | `8` | Batches written to the database concurrently. Throughput scales with this and with batch size (see Throughput below). |
| `write_timeout` | no | `""` | Per-batch write timeout as a Go duration (e.g. `30s`). Empty/`0s` means no timeout (a write hung on a lock or half-open connection blocks until the context is cancelled). When set, a timed-out batch is held for retry (NACK), never dropped; set it above the largest expected batch commit time. |

## What it writes

All objects live in a dedicated `umh` schema. For `data_contract_name: pump`, the plugin creates
and writes two hypertables:

- **`umh.value_pump`** — one row per `(topic_id, ts)`, where `ts` is a `timestamptz`. Numbers
  and booleans land in `value_num`, strings and JSON in `value_text`.
- **`umh.attribute_pump`** — the message metadata as a JSON object, queryable via
  `attribute->>'key'` and `attribute @> '{...}'`.

`umh.get_topic_id(location_path, virtual_path, data_contract, tag_name)` resolves a tag to
its `topic_id` for ad-hoc and Grafana queries.

> **Note on the contract name.** You configure the bare form (`pump`), which is used verbatim
> in the table names (`umh.value_pump`, `umh.attribute_pump`). The `umh.tag.data_contract_name`
> *column*, however, stores the UNS form with a leading underscore (`_pump`) to match the
> topic's data-contract segment. This mirrors the ManagementConsole Historian template, so a
> database written by either resolves identically through `get_topic_id`.

## Reading the data

The value table stores a surrogate `topic_id`, not the location/tag names. To go from a value
row back to its identity, join through `umh.topic` to `umh.tag` and `umh.location`:

```
umh.value_pump (topic_id, ts, value_num, value_text)
       │ topic_id
       ▼
umh.topic (topic_id, location_id, tag_id)
       │ tag_id            │ location_id
       ▼                   ▼
umh.tag (tag_id, name,   umh.location (location_id, path)
         virtual_path,
         data_contract_name)
```

Two things trip up hand-written queries:

- The value timestamp column is **`ts`** (a `timestamptz`), not `timestamp` or `time`.
- A tag with no virtual path stores `virtual_path` as the **empty string `''`**, never `NULL`.
  Passing `NULL` to `get_topic_id` matches nothing and returns an empty result silently.

`umh.get_topic_id(location_path, virtual_path, data_contract, tag_name)` hides that join for
single-tag lookups. Its `data_contract` argument is forgiving — `pump`, `_pump`, and `_pump_v1`
all resolve to the same tag — so you don't have to remember the exact underscore/version form.

```sql
-- Latest value of one tag. Use '' (not NULL) when the tag has no virtual path.
SELECT ts, value_num, value_text
FROM   umh.value_pump
WHERE  topic_id = umh.get_topic_id('enterprise.site.area.line', '', 'pump', 'temperature')
ORDER  BY ts DESC
LIMIT  1;

-- Values of one tag over a time window (drop-in for a Grafana panel; the
-- WHERE ts line is what Grafana's $__timeFilter(ts) macro expands to).
SELECT ts, value_num
FROM   umh.value_pump
WHERE  topic_id = umh.get_topic_id('enterprise.site.area.line', '', 'pump', 'temperature')
  AND  ts BETWEEN now() - INTERVAL '1 hour' AND now()
ORDER  BY ts;

-- Current value of every tag in the contract, with names resolved.
SELECT DISTINCT ON (v.topic_id)
       l.path::text AS location, g.virtual_path, g.name AS tag, v.ts, v.value_num, v.value_text
FROM   umh.value_pump v
JOIN   umh.topic    t ON t.topic_id    = v.topic_id
JOIN   umh.tag      g ON g.tag_id      = t.tag_id
JOIN   umh.location l ON l.location_id = t.location_id
ORDER  BY v.topic_id, v.ts DESC;

-- Resolve a tag to its numbers: value row → topic → tag/location.
SELECT l.path::text AS location, g.name AS tag, v.ts, v.value_num
FROM   umh.value_pump v
JOIN   umh.topic    t ON t.topic_id    = v.topic_id
JOIN   umh.tag      g ON g.tag_id      = t.tag_id
JOIN   umh.location l ON l.location_id = t.location_id
WHERE  g.name = 'temperature'
ORDER  BY v.ts DESC;
```

> **`DISTINCT ON` and high tag counts.** The "current value of every tag" query above scans the
> history of every topic to find each one's newest row. That is fine for hundreds of tags but
> gets expensive as the tag count and history grow. For a dashboard that refreshes it often,
> back it with a TimescaleDB continuous aggregate holding `last(value_num, ts)` per `topic_id`
> and query that instead.

## Behavior

- **Startup check.** `Connect()` verifies the server version and bootstraps the schema, so
  an unreachable, too-old, or misconfigured database fails the bridge at startup rather than
  writing to a misconfigured database unnoticed.
- **Idempotent replays.** An identical value at the same `(tag, ts)` is absorbed.
- **Topic resolution is read-first.** A topic already in the database is resolved with a lookup that assigns no new id, so the internal surrogate ids advance only when a genuinely new topic is created — not per message, and restarts do not bump them.
- **Conflict and datatype guards drop the offending row.** A *different* value at the same
  `(tag, ts)`, or a tag whose datatype flips (numeric ↔ text), is rejected by the database and
  the row is dropped rather than overwriting history — the rest of the batch is still written
  (see [Error handling](#error-handling)). This includes a tag emitting two distinct values
  within one millisecond, which the millisecond UNS timestamp cannot distinguish from a real
  conflict, so this contract is unsuitable for tags that emit distinct values faster than 1 kHz.
- **Only schema-validated data is stored.** A **versioned** contract (`_pump_v1`) carrying
  `data_contract_bypassed=true`, which the `uns` output sets when the registry was unreachable or no
  schema is registered, is dropped as `contract_bypassed` with no way to override it. An
  **unversioned** contract (`_historian`) can never be validated, so it is dropped as
  `contract_unvalidated` unless you set `allow_unvalidated_data: true` and accept unchecked
  datatypes.
- **The payload must be exactly `{value, timestamp_ms}`.** A missing field is dropped as
  `missing_value` or `missing_timestamp`, an extra top-level field as `not_timeseries`, which is what
  refuses a relational record. Neither rule is configurable. The
  [tag processor](../processing/tag-processor.md) fills `timestamp_ms`; the
  [Node-RED JavaScript processor](../processing/node-red-javascript-processor.md) does not, so a
  write flow that reshapes the payload has to carry it across.
- **A malformed message is dropped, never nacked.** An unparseable `umh_topic`
  (`invalid_topic`), a non-finite number (`unclassifiable_value`), an unreadable timestamp
  (`bad_timestamp`) and a payload that is not a JSON object (`not_structured`, `not_object`) are
  skipped; the rest of the batch is written. Each reason logs one error per batch with its share of
  the batch, an example topic and its fix, which umh-core reads as a degraded bridge. The log clears
  once the bad messages stop and the last error ages out of umh-core's window.
- **A wrong data contract refuses the whole batch.** A batch holding any message whose data-contract
  segment is not the configured `data_contract_name` is NACKed from the first batch on: nothing in it
  is written, not even its matching messages, and `output_sent` never counts it, so write throughput
  reports rows stored. The error names the contracts that arrived and the `umh_topics` pattern to
  narrow to, throttled to once every 2 minutes; the `uns` input logs its own error per refused batch,
  unthrottled.

  A NACKed batch is not replayed: the `uns` input leaves its offsets uncommitted and the next ACKed
  batch commits past them, so the refused messages are lost. Fix the subscription, then redeploy.
- **Subscribe only to this contract.** Anything wider errors the bridge. Use
  `^umh\.v1(?:\.[^._][^.]*)+\._<contract>(_v\d+)?\..+$`: any location depth, both the bare and `_vN`
  forms, and no match on a virtual-path segment sharing the contract's name.
- **Metadata de-duplication.** An attribute row is rewritten only when its key set changes,
  via an in-process, LRU-bounded fingerprint cache. The cache is process-local and cleared on
  restart, so the plugin re-emits at most one attribute row per topic per restart: the first
  post-restart message lands at a new timestamp, so its identical-metadata row is written as a
  new `(topic_id, ts)` row rather than being absorbed by the conflict guard.

## Error handling

A write failure is handled by *what caused it*, so a single bad tag never stalls the stream:

- **Transient** (connection loss, serialization/deadlock, lock contention, operator
  intervention, and any error without a SQLSTATE) — the batch is retried until it succeeds.
  A DB restart mid-stream loses nothing: held messages replay and identical `(topic_id, ts)`
  rows are absorbed.
- **Standing fault** (missing table privilege, disk full, an unrecognized error) — retried
  too (good data is never dropped over a fixable problem), but logged at error level. The
  bridge does not progress until an operator fixes the cause, then resumes losslessly.
- **Poison** (a value that can never be written: an append-only conflict, a datatype flip, a
  constraint violation) — the offending row is dropped and counted on
  `historian_rows_poisoned` (labelled by `sqlstate` and `phase`), with an error log naming
  the tag. The rest of the batch is written. Retrying a poison row can never succeed, so
  dropping it is what keeps every other tag flowing.

Only poison rows are ever dropped on a write error. Oversized text is a separate case: a
`value_text` longer than the row limit is clipped and counted on `historian_values_truncated`
(previously silent).

`Connect` also verifies the login role can `INSERT` into the contract's tables (a
`has_table_privilege` check). A role that reaches the database but cannot write to it fails the
bridge at startup with a named error, instead of connecting and then stalling on every write.

## Runbook: poisoned tags

**Find them.** A non-zero `historian_rows_poisoned` counter means rows are being dropped.
The error log names each one: `dropped poison row at <phase> for contract=… location=…
virtual_path=… tag=… (sqlstate=…)`. `phase=resolve` with `sqlstate=P0001` is almost always a
**datatype flip**; `phase=value` with `P0001` is an **append-only conflict** (two different
values at the same millisecond).

**Datatype flip / accidental first type.** A tag's type is fixed by its first stored value:
one stray string (e.g. `"N/A"`) locks the tag to text, and later numeric readings are then
rejected. This only arises on generic contracts like `_historian` that carry no upstream type
validation; a modelled contract validates types before the historian ever sees them. Confirm
the established type, then decide:

```sql
-- what type is this tag locked to?
SELECT value_type FROM umh.tag
WHERE name = 'temperature' AND virtual_path = '' AND data_contract_name = '_historian';
```

To reset a tag that was locked to the wrong type, delete its stored value history and its tag
row so the next message re-establishes the type (this discards that tag's history for the
contract — take a copy first if you need it):

```sql
-- resolve the topic, delete its values, then remove the topic + tag so the type is no longer pinned
WITH tid AS (SELECT umh.get_topic_id('enterprise.site.area.line', '', 'historian', 'temperature') AS id)
DELETE FROM umh.value_historian WHERE topic_id = (SELECT id FROM tid);
-- then delete the matching rows in umh.topic and umh.tag.
```

**Append-only conflict.** The source emitted two different values at the same millisecond
timestamp. On bridges the downsampler collapses duplicate timestamps per series before the
historian sees them; if you hit this, the source is producing faster than 1 kHz on one tag —
not representable by the millisecond UNS timestamp and unsuitable for this contract.

**Prevention.** Pin the intended type on fixed contracts (don't let an accidental first sample
define it), and route text or high-precision counters to a text contract rather than mixing
types on one tag.

> **Generic contracts (`_historian`/`_raw`).** These don't pin a type, so a type change happens in
> the field and is not a defect. With `allow_unvalidated_data: true` the tag resolves on its natural
> key alone and the change is accepted: one `topic_id` then holds numeric rows in `value_num` and
> text rows in `value_text`, with `umh.tag.value_type` recording only the first type seen. Read such
> a tag with `coalesce(value_num::text, value_text)`. Without the flag nothing is stored at all:
> every unversioned message is dropped as `contract_unvalidated` before it reaches a write, flip or
> not. A datatype flip is poison only on a versioned contract, whose schema pins the type.

## Throughput

Each batch is written in one transaction: the distinct topics are resolved once, then value
and attribute rows are inserted by `topic_id`. Two knobs scale write throughput, and both help
independently:

- **`batching`** — a larger batch amortizes the single per-batch commit over more rows. Set a
  `count` / `period` policy (e.g. `count: 1000`, `period: 1s`); without one the output writes
  whatever the pipeline delivers per transaction.
- **`max_in_flight`** — more batches written concurrently. Because topics are resolved in
  short-lived statements (not held for the whole batch), concurrent batches do not serialize on
  the shared dimension rows, so throughput scales with this.

The defaults (`max_in_flight: 8` and a `count: 1000` / `period: 1s` batch policy) comfortably
exceed a typical per-bridge load. Raise `max_in_flight` (and the connection pool with it) or the
batch size for higher-throughput streams.

## Metrics

On top of benthos's built-in output metrics (`output_sent`, `output_error`,
`output_latency_ns`), the plugin emits:

- `historian_value_rows_written` — value rows upserted (counted after the batch commits).
- `historian_attribute_rows_written` — attribute rows upserted; the gap below the value-row
  count is metadata de-duplication at work.
- `messages_dropped` (labelled by `reason`) — messages dropped before any write. The name has no
  plugin prefix because every benthos-umh plugin uses the same drop counter, which is how umh-core
  subtracts it from `output_sent`.
- `historian_dedup_cache_size` — current dedup-cache entry count.

## Numeric precision

`value_num` is `DOUBLE PRECISION`. That is exact for sensor floats but loses precision for
integer counters above 2^53 (~9e15) and for exact decimals. Route such tags to a text data
contract instead, where the value is stored verbatim in `value_text`.

## Location identity

The location is canonicalized into an `ltree` path: every character outside `[A-Za-z0-9_-]`
becomes `_`, each label is truncated to 255 characters, and empty labels are dropped. Hyphens
are kept (PostgreSQL 16+ `ltree` labels accept them), so `enterprise.line-1` and
`enterprise.line_1` are **distinct** paths, each with its own `topic_id`. Other punctuation
still folds: `enterprise.line@1` becomes `enterprise.line_1` and shares its identity. Distinguish
sources by their path segments, not by punctuation that folds.

## Schema and compatibility

The plugin owns the schema: it bootstraps the baseline DDL into the `umh` schema
idempotently on first connect and **never alters an already-created
`umh.value_<contract>` / `umh.attribute_<contract>` table**. A breaking schema change ships
as a new contract (new tables), never an in-place migration. (`ltree` stays in `public`,
its conventional shared home.)

The baseline is a port of the Management Console TimescaleDB Historian template and writes
the same tables. To avoid schema drift, a given contract/database must be written by exactly
**one** writer type — the plugin **or** the template, never both.

## Changing compression or retention

`compress_after` and `retention` are applied **once, at first bootstrap**, and are deliberately
not re-applied when the bridge restarts. Editing them in the bridge config therefore has **no
effect** on a database that already has the tables. This is intentional: a config edit (or a form
change in the Management Console) should not silently change how production history is compressed
or — for retention — **deleted**. On restart the bridge logs a warning if the applied policy
differs from the config, so drift stays visible, but it does not act on it.

To change them on an existing database, update the TimescaleDB policies directly, on **both**
hypertables for the contract. For a contract named `pump`:

```sql
-- retention: keep 30 days (repeat for umh.attribute_pump)
SELECT remove_retention_policy('umh.value_pump', if_exists => true);
SELECT add_retention_policy('umh.value_pump', INTERVAL '30 days');

-- compression: compress chunks older than 7 days (repeat for umh.attribute_pump)
SELECT remove_compression_policy('umh.value_pump', if_exists => true);
SELECT add_compression_policy('umh.value_pump', INTERVAL '7 days');
```

To stop dropping data entirely (the `retention: ""` default), remove the retention policy and do
not re-add it. Changing either policy takes effect immediately and never needs a bridge restart.

After changing a policy on the database, set the **same** value in the bridge config too. The
config value is still what a fresh bootstrap of a new database uses, and matching it silences the
drift warning on restart.

## Quick example

```yaml
input:
  uns:
    umh_topics:
      # Substitute your own contract for "pump", here and in data_contract_name below.
      # Both must name the same contract: a batch carrying any other one is refused whole.
      - '^umh\.v1(?:\.[^._][^.]*)+\._pump(_v\d+)?\..+$'
output:
  historian:
    host: timescaledb.example.com
    password: change-me
    data_contract_name: pump
```

To deploy a bridge against this output from the Management Console, use the
**Historian** template in the Add Bridge wizard.
