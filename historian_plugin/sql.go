// Copyright 2026 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package historian_plugin

import (
	"fmt"
	"strings"
	"time"
)

// Port of the ManagementConsole timescaledb-historian template. All objects live in the umh
// schema; ltree stays in public so the unqualified LTREE type and ::ltree casts resolve on the
// default search_path. CONTRACT_SLOT is replaced by the validated contract name
// (^[a-z0-9_]+$, injection-safe).

const bootstrapTemplate = `BEGIN;
SELECT pg_advisory_xact_lock(hashtext('uns_to_timescale_bootstrap'));
CREATE EXTENSION IF NOT EXISTS ltree WITH SCHEMA public;
CREATE SCHEMA IF NOT EXISTS umh;
-- Migration ledger. Everything below is the immutable BASELINE: never edit a CREATE to change an
-- existing object (a database that ran it will not re-run it); append forward-only steps in MIGRATIONS.
CREATE TABLE IF NOT EXISTS umh.schema_migrations (
  version    INT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
DO $$
BEGIN
  CREATE TYPE umh.value_type AS ENUM ('numeric', 'text');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
-- Dimension tables use fillfactor 90: the ON CONFLICT DO UPDATE upserts on the create/flip path
-- rewrite indexed values in place (HOT, self-pruned). The value/attribute hypertables keep the default.
CREATE TABLE IF NOT EXISTS umh.location (
  location_id BIGSERIAL PRIMARY KEY,
  path        LTREE NOT NULL,
  UNIQUE (path)
) WITH (fillfactor = 90);
CREATE INDEX IF NOT EXISTS idx_location_path_gist ON umh.location USING GIST (path);
CREATE TABLE IF NOT EXISTS umh.tag (
  tag_id        BIGSERIAL PRIMARY KEY,
  name          TEXT       NOT NULL,
  virtual_path  TEXT       NOT NULL,
  data_contract_name TEXT  NOT NULL,
  value_type    umh.value_type NOT NULL,
  UNIQUE (virtual_path, name, data_contract_name)
) WITH (fillfactor = 90);
CREATE OR REPLACE FUNCTION umh.tag_value_type_guard()
RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
  IF NEW.value_type IS DISTINCT FROM OLD.value_type THEN
    RAISE EXCEPTION 'tag datatype changed for virtual_path=% name=% contract=%: stored % but received % (one tag must always produce the same datatype)',
      OLD.virtual_path, OLD.name, OLD.data_contract_name, OLD.value_type, NEW.value_type;
  END IF;
  RETURN NEW;
END $guard$;
DROP TRIGGER IF EXISTS trg_tag_value_type_guard ON umh.tag;
CREATE TRIGGER trg_tag_value_type_guard
  BEFORE UPDATE ON umh.tag
  FOR EACH ROW EXECUTE FUNCTION umh.tag_value_type_guard();
-- raise_pk_conflict and tag_value_type_guard (above) are the ONLY objects here permitted to RAISE
-- (SQLSTATE P0001). The plugin classifies P0001 as poison and drops the offending row, so any new
-- trigger/function/migration that needs to signal a *retryable* fault must use a proper SQLSTATE,
-- never a bare RAISE -- see the P0001 invariant in errclass.go.
CREATE OR REPLACE FUNCTION umh.raise_pk_conflict(p_msg text, p_ret anyelement)
RETURNS anyelement LANGUAGE plpgsql AS $rpc$
BEGIN
  RAISE EXCEPTION '%', p_msg;
END $rpc$;
CREATE TABLE IF NOT EXISTS umh.topic (
  topic_id    BIGSERIAL PRIMARY KEY,
  location_id BIGINT NOT NULL REFERENCES umh.location(location_id),
  tag_id      BIGINT NOT NULL REFERENCES umh.tag(tag_id),
  UNIQUE (location_id, tag_id)
) WITH (fillfactor = 90);
-- topic_id is deliberately not an FK on the hypertables: a per-chunk FK takes a
-- ShareRowExclusiveLock on topic and deadlocks concurrent writers.
CREATE TABLE IF NOT EXISTS umh.value_CONTRACT_SLOT (
  topic_id   BIGINT           NOT NULL,
  ts         TIMESTAMPTZ      NOT NULL,
  value_num  DOUBLE PRECISION,
  value_text TEXT,
  PRIMARY KEY (topic_id, ts),
  CONSTRAINT ck_value_num_text_exclusive
    CHECK ((value_num IS NULL) <> (value_text IS NULL)),
  CONSTRAINT ck_value_text_max_size
    CHECK (octet_length(value_text) <= 65536)
);
SELECT create_hypertable('umh.value_CONTRACT_SLOT', 'ts', chunk_time_interval => VALUE_CHUNK_SLOT, if_not_exists => TRUE);
VALUE_POLICY_SLOT
CREATE TABLE IF NOT EXISTS umh.attribute_CONTRACT_SLOT (
  topic_id  BIGINT      NOT NULL,
  ts        TIMESTAMPTZ NOT NULL,
  attribute JSONB       NOT NULL,
  PRIMARY KEY (topic_id, ts)
);
SELECT create_hypertable('umh.attribute_CONTRACT_SLOT', 'ts', chunk_time_interval => ATTRIBUTE_CHUNK_SLOT, if_not_exists => TRUE);
ATTR_POLICY_SLOT
CREATE OR REPLACE FUNCTION umh.to_ltree_path(p_location_path text)
RETURNS ltree
LANGUAGE sql IMMUTABLE
AS $ltree$
  SELECT string_agg(q.lbl, '.' ORDER BY q.ord)::ltree
    FROM (SELECT left(regexp_replace(s.seg, '[^A-Za-z0-9_-]', '_', 'g'), 255) AS lbl, s.ord
            FROM unnest(string_to_array(p_location_path, '.')) WITH ORDINALITY AS s(seg, ord)) q
   WHERE length(q.lbl) > 0;
$ltree$;
CREATE OR REPLACE FUNCTION umh.get_topic_id(
  p_location_path text,
  p_virtual_path  text,
  p_data_contract text,
  p_tag_name      text
)
RETURNS bigint
LANGUAGE sql STABLE SECURITY INVOKER SET search_path = umh, public, pg_temp
AS $fn$
  SELECT t.topic_id
    FROM umh.location l
    JOIN umh.tag   g ON g.virtual_path = p_virtual_path
                AND g.name = p_tag_name
                AND g.data_contract_name =
                    '_' || regexp_replace(regexp_replace(p_data_contract, '_v\d+$', ''), '^_', '')
    JOIN umh.topic t ON t.location_id = l.location_id AND t.tag_id = g.tag_id
   WHERE l.path = umh.to_ltree_path(p_location_path);
$fn$;
-- ===================== MIGRATIONS =====================
-- Forward-only, run-once steps applied after the baseline; each is ledger-gated and runs inside
-- the bootstrap advisory lock, so an older instance sharing the database can never revert a newer one.
MIGRATIONS_SLOT
-- ======================================================
COMMIT;`

const dimensionCTEFmt = `WITH loc AS (
  INSERT INTO umh.location (path) VALUES (umh.to_ltree_path($1))
  ON CONFLICT (path) DO UPDATE SET path = EXCLUDED.path
  RETURNING location_id
),
tg AS (
  INSERT INTO umh.tag (name, virtual_path, data_contract_name, value_type)
  VALUES ($4, $3, $2, $5::umh.value_type)
  ON CONFLICT (virtual_path, name, data_contract_name) DO UPDATE SET value_type = %s
  RETURNING tag_id
),
tp AS (
  INSERT INTO umh.topic (location_id, tag_id)
  SELECT loc.location_id, tg.tag_id FROM loc CROSS JOIN tg
  ON CONFLICT (location_id, tag_id) DO UPDATE SET location_id = EXCLUDED.location_id
  RETURNING topic_id
)
`

var dimensionCTE = fmt.Sprintf(dimensionCTEFmt, "EXCLUDED.value_type")

// topicResolveSQL upserts the location/tag/topic dimension rows and returns topic_id. Run once
// per distinct topic in its own autocommit statement so the shared location-row lock releases
// at once; held for the whole value write it would serialize concurrent batches.
// A Phase-2 RAISE leaves these upserts committed, orphaning a topic row with no value: wasted
// rows, reused on retry, not a correctness bug.
var topicResolveSQL = dimensionCTE + `SELECT topic_id FROM tp;`

var topicResolveKeepTypeSQL = fmt.Sprintf(dimensionCTEFmt, "umh.tag.value_type") + `SELECT topic_id FROM tp;`

// topicLookupSQL resolves an existing topic via a read (no sequence burn); on a miss the caller
// falls through to topicResolveSQL. value_type is in the WHERE deliberately: a datatype flip
// misses here and hits the guarded upsert, which RAISEs. A value_type-agnostic lookup would match
// the natural key and silently bypass tag_value_type_guard.
const topicLookupSQL = `SELECT t.topic_id
FROM umh.topic t
JOIN umh.location l ON l.location_id = t.location_id
JOIN umh.tag      g ON g.tag_id      = t.tag_id
WHERE l.path = umh.to_ltree_path($1)
  AND g.data_contract_name = $2
  AND g.virtual_path       = $3
  AND g.name               = $4
  AND g.value_type         = $5::umh.value_type;`

const topicLookupAnyTypeSQL = `SELECT t.topic_id
FROM umh.topic t
JOIN umh.location l ON l.location_id = t.location_id
JOIN umh.tag      g ON g.tag_id      = t.tag_id
WHERE l.path = umh.to_ltree_path($1)
  AND g.data_contract_name = $2
  AND g.virtual_path       = $3
  AND g.name               = $4;`

// valueInsert / attributeInsert write one row against an already-resolved topic_id (passed as
// $1), so the value-write phase only touches distinct (topic_id, ts) rows and concurrent
// batches do not contend. The ON CONFLICT guard is identical to the dimension-joined form.
const valueInsert = `INSERT INTO umh.value_CONTRACT_SLOT AS v (topic_id, ts, value_num, value_text)
VALUES ($1, $2::timestamptz, $3::double precision, $4)
ON CONFLICT (topic_id, ts) DO UPDATE
  SET value_num = CASE
    WHEN v.value_num  IS DISTINCT FROM EXCLUDED.value_num
      OR v.value_text IS DISTINCT FROM EXCLUDED.value_text
    THEN umh.raise_pk_conflict(
           format('value conflict at topic_id=%s ts=%s: stored (num=%s, text=%s) but received (num=%s, text=%s)',
                  v.topic_id, v.ts, v.value_num, v.value_text, EXCLUDED.value_num, EXCLUDED.value_text),
           NULL::double precision)
    ELSE v.value_num
  END;`

const attributeInsert = `INSERT INTO umh.attribute_CONTRACT_SLOT AS a (topic_id, ts, attribute)
VALUES ($1, $2::timestamptz, $3::jsonb)
ON CONFLICT (topic_id, ts) DO UPDATE
  SET attribute = CASE
    WHEN a.attribute IS DISTINCT FROM EXCLUDED.attribute
    THEN umh.raise_pk_conflict(
           format('attribute conflict at topic_id=%s ts=%s: stored %s but received %s',
                  a.topic_id, a.ts, a.attribute, EXCLUDED.attribute),
           NULL::jsonb)
    ELSE a.attribute
  END;`

// valueInsertBatch / attributeInsertBatch are the fast-path multi-row form: one statement inserts a
// whole batch by unnesting parallel arrays (fixed 4/3 params regardless of row count, so no 65535
// parameter cap). The arrays are the natural pgx encodings -- bigint[], text[], double precision[]
// -- and ts/attribute are cast per row in the SELECT to sidestep text[]->timestamptz[]/jsonb[] array
// casts. The per-row ON CONFLICT guard is identical to the single-row form; the caller must not pass
// the same (topic_id, ts) twice (Postgres raises 21000 -- "cannot affect row a second time" -- which
// the caller classifies as poison and re-runs isolated).
const valueInsertBatch = `INSERT INTO umh.value_CONTRACT_SLOT AS v (topic_id, ts, value_num, value_text)
SELECT u.topic_id, u.ts::timestamptz, u.value_num, u.value_text
  FROM unnest($1::bigint[], $2::text[], $3::double precision[], $4::text[])
       AS u(topic_id, ts, value_num, value_text)
ON CONFLICT (topic_id, ts) DO UPDATE
  SET value_num = CASE
    WHEN v.value_num  IS DISTINCT FROM EXCLUDED.value_num
      OR v.value_text IS DISTINCT FROM EXCLUDED.value_text
    THEN umh.raise_pk_conflict(
           format('value conflict at topic_id=%s ts=%s: stored (num=%s, text=%s) but received (num=%s, text=%s)',
                  v.topic_id, v.ts, v.value_num, v.value_text, EXCLUDED.value_num, EXCLUDED.value_text),
           NULL::double precision)
    ELSE v.value_num
  END;`

const attributeInsertBatch = `INSERT INTO umh.attribute_CONTRACT_SLOT AS a (topic_id, ts, attribute)
SELECT u.topic_id, u.ts::timestamptz, u.attribute::jsonb
  FROM unnest($1::bigint[], $2::text[], $3::text[]) AS u(topic_id, ts, attribute)
ON CONFLICT (topic_id, ts) DO UPDATE
  SET attribute = CASE
    WHEN a.attribute IS DISTINCT FROM EXCLUDED.attribute
    THEN umh.raise_pk_conflict(
           format('attribute conflict at topic_id=%s ts=%s: stored %s but received %s',
                  a.topic_id, a.ts, a.attribute, EXCLUDED.attribute),
           NULL::jsonb)
    ELSE a.attribute
  END;`

// migration is one forward-only schema step. sql must be idempotent (CREATE/ALTER ... IF NOT
// EXISTS): a fresh database already carrying the change in its baseline still records the version,
// so the DDL must no-op there. sql must not contain the tag "$mig$" (it is wrapped in DO $mig$).
type migration struct {
	version int
	sql     string
}

// schemaMigrations is the forward-only migration ledger; version 1 is the baseline created above.
// To evolve the schema, APPEND the next version with idempotent DDL -- never edit a baseline or an
// existing entry, because a database that already ran it will not re-run it.
//
// Example: {version: 2, sql: "ALTER TABLE umh.value_CONTRACT_SLOT ADD COLUMN IF NOT EXISTS quality SMALLINT;"}
var schemaMigrations = []migration{
	{version: 1}, // baseline above; recorded as the initial schema version
}

// highestMigrationVersion is the schema version a freshly bootstrapped database ends at.
func highestMigrationVersion() int {
	v := 0
	for _, m := range schemaMigrations {
		if m.version > v {
			v = m.version
		}
	}
	return v
}

// migrationsBlock renders the MIGRATIONS section: each step is ledger-gated to apply at most once,
// inside the bootstrap's BEGIN/COMMIT + advisory lock, so the upgrade is atomic.
func migrationsBlock() string {
	var b strings.Builder
	for _, m := range schemaMigrations {
		fmt.Fprintf(&b, "DO $mig$\nBEGIN\n  IF NOT EXISTS (SELECT 1 FROM umh.schema_migrations WHERE version = %[1]d) THEN\n", m.version)
		if s := strings.TrimSpace(m.sql); s != "" {
			b.WriteString("    " + s + "\n")
		}
		fmt.Fprintf(&b, "    INSERT INTO umh.schema_migrations (version) VALUES (%[1]d);\n  END IF;\nEND $mig$;\n", m.version)
	}
	return b.String()
}

func sub(sql string, contract string) string {
	return strings.ReplaceAll(sql, "CONTRACT_SLOT", contract)
}

// policyBlock builds the compression/retention setup for one hypertable. Each check reads the
// catalog for this hypertable, not umh.schema_migrations: that ledger holds one row per database
// while the hypertables are per contract, so gating on it skipped every contract after the first.
// The checks rely on the advisory lock this template takes at the top spanning them.
//
// The ALTER is checked separately because ALTER TABLE ... SET takes AccessExclusiveLock, and it
// leaves an operator's own compress_segmentby / compress_orderby alone. if_not_exists on the adds
// covers a check that reads false while the policy exists, which would otherwise abort the whole
// bootstrap. A deleted policy and one that never existed are the same catalog state, so a removal
// does not survive a restart.
func policyBlock(hypertableName string, compressAfter time.Duration, retention time.Duration, retentionSet bool) string {
	qualifiedTable := "umh." + hypertableName
	compressionEnabled := compressionEnabledSQL(hypertableName)
	compressionPolicyExists := policyJobExistsSQL("policy_compression", hypertableName)

	var b strings.Builder
	fmt.Fprintf(&b, `DO $pol$
BEGIN
  IF NOT %s THEN
    ALTER TABLE %s SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'topic_id',
      timescaledb.compress_orderby   = 'ts DESC'
    );
  END IF;
  IF NOT %s THEN
    PERFORM add_compression_policy('%s', %s, if_not_exists => TRUE);
  END IF;
`, compressionEnabled, qualifiedTable, compressionPolicyExists, qualifiedTable, intervalSQL(compressAfter))
	if retentionSet {
		retentionPolicyExists := policyJobExistsSQL("policy_retention", hypertableName)
		fmt.Fprintf(&b, `  IF NOT %s THEN
    PERFORM add_retention_policy('%s', %s, if_not_exists => TRUE);
  END IF;
`, retentionPolicyExists, qualifiedTable, intervalSQL(retention))
	}
	b.WriteString("END $pol$;")
	return b.String()
}

// policyJobExistsSQL and compressionEnabledSQL inline the hypertable name because a DO block takes
// no parameters. procName is an internal constant, and the name reaching them is the CONTRACT_SLOT
// template placeholder, replaced later by sub() with a contract ValidateContract has already matched
// against ^[a-z0-9_]+$ -- so neither is a path for user input.
func policyJobExistsSQL(procName string, hypertableName string) string {
	return fmt.Sprintf("EXISTS (SELECT 1 FROM timescaledb_information.jobs WHERE proc_name = '%s' AND hypertable_schema = 'umh' AND hypertable_name = '%s')", procName, hypertableName)
}

func compressionEnabledSQL(hypertableName string) string {
	return fmt.Sprintf("EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_schema = 'umh' AND hypertable_name = '%s' AND compression_enabled)", hypertableName)
}

// policyIntervalSQL returns a query for the interval (in seconds) of a TimescaleDB policy job on
// a umh hypertable, or NULL when no such policy exists. The scalar subquery always yields exactly
// one row (never ErrNoRows), so a nil scan means "no policy / catalog unavailable". procName and
// configKey are internal constants, not user input.
func policyIntervalSQL(procName string, configKey string) string {
	return fmt.Sprintf(`SELECT (
  SELECT EXTRACT(EPOCH FROM (config->>'%s')::interval)::bigint
    FROM timescaledb_information.jobs
   WHERE proc_name = '%s' AND hypertable_schema = 'umh' AND hypertable_name = $1
   LIMIT 1)`, configKey, procName)
}

// hypertableChunkCountSQL counts the chunks of one umh hypertable: a policy created on an empty
// table destroys nothing, one created on a table with history does.
const hypertableChunkCountSQL = `SELECT count(*) FROM timescaledb_information.chunks
 WHERE hypertable_schema = 'umh' AND hypertable_name = $1`

// policyDriftWarnings compares configured compression/retention intervals against the ones actually
// applied in the database and returns a warning per divergence. An applied interval is nil when no
// such policy is scheduled, and only that: a lookup that failed is an error the caller reports, so
// this no longer treats a nil compression interval as "introspection is broken" and stays silent on
// the retention checks because of it. retentionWant is nil when retention is unset in config (keep
// forever), which is the one nil that means "wanted absent" rather than "is absent".
func policyDriftWarnings(compressWant int64, appliedComp *int64, retentionWant *int64, appliedRet *int64) []string {
	var warns []string
	switch {
	case appliedComp == nil:
		warns = append(warns, fmt.Sprintf("configured compress_after (%ds) is not applied in the database", compressWant))
	case *appliedComp != compressWant:
		warns = append(warns, fmt.Sprintf("configured compress_after (%ds) does not match the compression policy applied in the database (%ds)", compressWant, *appliedComp))
	}
	switch {
	case retentionWant != nil && appliedRet == nil:
		warns = append(warns, fmt.Sprintf("configured retention (%ds) is not applied in the database", *retentionWant))
	case retentionWant != nil && *appliedRet != *retentionWant:
		warns = append(warns, fmt.Sprintf("configured retention (%ds) does not match the retention policy applied in the database (%ds)", *retentionWant, *appliedRet))
	case retentionWant == nil && appliedRet != nil:
		warns = append(warns, fmt.Sprintf("retention is unset in config but a retention policy (%ds) is applied in the database", *appliedRet))
	}
	return warns
}

func intervalSQL(d time.Duration) string {
	return fmt.Sprintf("INTERVAL '%d seconds'", int64(d.Seconds()))
}

type bootstrapConfig struct {
	contract       string
	compressAfter  time.Duration
	retention      time.Duration
	retentionSet   bool
	valueChunk     time.Duration
	attributeChunk time.Duration
}

// chunkIntervalSQL reads the chunk width applied to a umh hypertable, in seconds. A scalar subquery,
// so it always yields exactly one row and a nil scan means "not a hypertable here" rather than
// ErrNoRows.
func chunkIntervalSQL() string {
	return `SELECT (
  SELECT EXTRACT(EPOCH FROM time_interval)::bigint
    FROM timescaledb_information.dimensions
   WHERE hypertable_schema = 'umh' AND hypertable_name = $1 AND column_name = 'ts'
   LIMIT 1)`
}

// chunkDriftWarnings compares the configured chunk intervals against the ones the tables were
// actually created with (in seconds; nil = unreadable, so no warning rather than a false one).
func chunkDriftWarnings(valueWant int64, appliedValue *int64, attributeWant int64, appliedAttribute *int64) []string {
	var warns []string
	if appliedValue != nil && *appliedValue != valueWant {
		warns = append(warns, fmt.Sprintf("configured value_chunk_interval (%ds) does not match the chunk interval the value hypertable was created with (%ds)", valueWant, *appliedValue))
	}
	if appliedAttribute != nil && *appliedAttribute != attributeWant {
		warns = append(warns, fmt.Sprintf("configured attribute_chunk_interval (%ds) does not match the chunk interval the attribute hypertable was created with (%ds)", attributeWant, *appliedAttribute))
	}
	return warns
}

func bootstrapSQL(cfg bootstrapConfig) string {
	// Every slot is a distinct placeholder, so the order they are filled in does not matter. The one
	// ordering that does is CONTRACT_SLOT: the blocks inserted here carry it, so sub() runs last.
	slots := map[string]string{
		"VALUE_CHUNK_SLOT":     intervalSQL(cfg.valueChunk),
		"ATTRIBUTE_CHUNK_SLOT": intervalSQL(cfg.attributeChunk),
		"VALUE_POLICY_SLOT":    policyBlock("value_CONTRACT_SLOT", cfg.compressAfter, cfg.retention, cfg.retentionSet),
		"ATTR_POLICY_SLOT":     policyBlock("attribute_CONTRACT_SLOT", cfg.compressAfter, cfg.retention, cfg.retentionSet),
		"MIGRATIONS_SLOT":      migrationsBlock(),
	}
	s := bootstrapTemplate
	for placeholder, replacement := range slots {
		s = strings.Replace(s, placeholder, replacement, 1)
	}
	return sub(s, cfg.contract)
}

func valueQueryFor(contract string) string     { return sub(valueInsert, contract) }
func attributeQueryFor(contract string) string { return sub(attributeInsert, contract) }

func valueBatchQueryFor(contract string) string     { return sub(valueInsertBatch, contract) }
func attributeBatchQueryFor(contract string) string { return sub(attributeInsertBatch, contract) }
