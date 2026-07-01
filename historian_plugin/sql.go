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

// Port of the ManagementConsole timescaledb-historian template, with the contract name
// substituted into the table names and the policies taking their interval from config.
// All objects live in the umh schema; ltree stays in public (its shared home) so the
// unqualified LTREE type and ::ltree casts resolve via the default search_path. The write
// path canonicalizes location through umh.to_ltree_path() so write and read agree.
// CONTRACT_SLOT is replaced by the validated contract name (^[a-z0-9_]+$, injection-safe).

const bootstrapTemplate = `BEGIN;
SELECT pg_advisory_xact_lock(hashtext('uns_to_timescale_bootstrap'));
CREATE EXTENSION IF NOT EXISTS ltree WITH SCHEMA public;
CREATE SCHEMA IF NOT EXISTS umh;
-- Migration ledger: records which numbered schema changes this database has applied.
-- Everything created below is the immutable BASELINE -- never edit a CREATE here to change
-- an existing table/column/constraint, because a database that already ran it will not
-- re-run it. Append schema CHANGES as numbered, forward-only steps in the MIGRATIONS
-- section just before COMMIT (generated from schemaMigrations in sql.go). Query the current
-- version with: SELECT max(version) FROM umh.schema_migrations;
CREATE TABLE IF NOT EXISTS umh.schema_migrations (
  version    INT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
DO $$
BEGIN
  CREATE TYPE umh.value_type AS ENUM ('numeric', 'text');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
-- Dimension tables use fillfactor 90: the per-message ON CONFLICT DO UPDATE upserts that
-- resolve ids rewrite the same indexed values, so the spare page room keeps those updates
-- HOT (no index churn, dead tuples self-pruned). The value/attribute hypertables are
-- insert-mostly and keep the default fillfactor.
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
SELECT create_hypertable('umh.value_CONTRACT_SLOT', 'ts', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
VALUE_POLICY_SLOT
CREATE TABLE IF NOT EXISTS umh.attribute_CONTRACT_SLOT (
  topic_id  BIGINT      NOT NULL,
  ts        TIMESTAMPTZ NOT NULL,
  attribute JSONB       NOT NULL,
  PRIMARY KEY (topic_id, ts)
);
SELECT create_hypertable('umh.attribute_CONTRACT_SLOT', 'ts', if_not_exists => TRUE);
ATTR_POLICY_SLOT
CREATE OR REPLACE FUNCTION umh.to_ltree_path(p_location_path text)
RETURNS ltree
LANGUAGE sql IMMUTABLE
AS $ltree$
  SELECT string_agg(q.lbl, '.' ORDER BY q.ord)::ltree
    FROM (SELECT left(regexp_replace(s.seg, '[^A-Za-z0-9_]', '_', 'g'), 255) AS lbl, s.ord
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
-- Forward-only, run-once schema changes applied AFTER the baseline above. Generated from
-- schemaMigrations in sql.go so the version list lives in one place; each step is gated on
-- umh.schema_migrations and runs inside this transaction's advisory lock, so the upgrade is
-- atomic and an older bridge sharing the database can never revert a newer one.
MIGRATIONS_SLOT
-- ======================================================
COMMIT;`

const dimensionCTE = `WITH loc AS (
  INSERT INTO umh.location (path) VALUES (umh.to_ltree_path($1))
  ON CONFLICT (path) DO UPDATE SET path = EXCLUDED.path
  RETURNING location_id
),
tg AS (
  INSERT INTO umh.tag (name, virtual_path, data_contract_name, value_type)
  VALUES ($4, $3, $2, $5::umh.value_type)
  ON CONFLICT (virtual_path, name, data_contract_name) DO UPDATE SET value_type = EXCLUDED.value_type
  RETURNING tag_id
),
tp AS (
  INSERT INTO umh.topic (location_id, tag_id)
  SELECT loc.location_id, tg.tag_id FROM loc CROSS JOIN tg
  ON CONFLICT (location_id, tag_id) DO UPDATE SET location_id = EXCLUDED.location_id
  RETURNING topic_id
)
`

// topicResolveSQL upserts the location/tag/topic dimension rows and returns the topic_id,
// WITHOUT touching the per-contract value/attribute tables. It is run once per distinct topic
// in its own short-lived statement so the shared dimension-row write locks (the location row
// especially -- every row of a contract touches it) release immediately, instead of being
// held for the whole value-write batch. Holding them batch-long serializes concurrent batches
// (max_in_flight) on the shared location row, which stops batching from scaling.
//
// Two consequences of splitting dimension resolution from the value write, versus the
// template's single atomic dimension-CTE + INSERT per row:
//   - ~2N+M round-trips per batch when every message is a distinct topic (vs N+M) -- the cost
//     of releasing the shared locks early.
//   - a Phase-2 value RAISE (e.g. raise_pk_conflict) leaves these dimension upserts already
//     committed, orphaning a topic row with no value. It is idempotent on retry (the next
//     attempt reuses the row), so it is wasted rows, not a correctness bug: the template's
//     dimension+value atomicity is intentionally traded for write concurrency.
//
// The trade only pays off at max_in_flight > 1: at max_in_flight = 1 there is no concurrent
// contention on the shared location row, so the split forgoes atomicity for no benefit.
const topicResolveSQL = dimensionCTE + `SELECT topic_id FROM tp;`

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

// migration is one forward-only schema step. version must be unique and ascending across
// schemaMigrations. sql is the DDL applied when the step has not yet run on a database, and
// must itself be idempotent (CREATE/ALTER ... IF NOT EXISTS): a fresh database whose baseline
// already includes the change still records the version, so the DDL must no-op there. sql may
// be empty -- version 1 is the baseline created above, recorded without extra DDL.
// CONTRACT_SLOT in sql is substituted with the contract name like the rest of the bootstrap.
// The step is wrapped in a DO $mig$ block, so sql must not contain the tag "$mig$".
type migration struct {
	version int
	sql     string
}

// schemaMigrations is the ordered, forward-only migration ledger. Version 1 IS the baseline
// above: the umh schema is greenfield (never deployed, and nothing was versioned before it),
// so its initial form is simply version 1 of a fresh sequence -- the standard "initial schema
// is a migration" convention (cf. Flyway baseline, Django 0001_initial). The plugin can record
// a baseline version where the ManagementConsole template leaves the ledger empty because the
// plugin has a single bootstrap statement, not two writers sharing one ledger.
//
// To evolve the schema, APPEND the next version with its (idempotent) DDL -- never edit a
// baseline CREATE or an existing entry, because a database that already ran it will not re-run
// it. "Current version" is SELECT max(version) FROM umh.schema_migrations (1 on a fresh DB).
//
// Example next step:
//
//	{version: 2, sql: "ALTER TABLE umh.value_CONTRACT_SLOT ADD COLUMN IF NOT EXISTS quality SMALLINT;"},
var schemaMigrations = []migration{
	{version: 1}, // baseline above; recorded as the initial schema version
}

// highestMigrationVersion returns the highest version in schemaMigrations: the schema version
// a freshly bootstrapped database ends at, and the in-code source of truth for the version.
func highestMigrationVersion() int {
	v := 0
	for _, m := range schemaMigrations {
		if m.version > v {
			v = m.version
		}
	}
	return v
}

// migrationsBlock renders the MIGRATIONS section: each step is gated on umh.schema_migrations
// so it applies at most once per database and records its version on success. It runs inside
// the bootstrap's BEGIN/COMMIT and advisory lock, so the whole upgrade is atomic.
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

// policyBlock generates the compression/retention setup for one hypertable, gated on the
// migration ledger so it runs exactly once -- at first bootstrap, when the table is empty and
// has no compressed chunks. Re-running it on every Connect (the previous behavior) had two
// hazards: ALTER TABLE SET (compress...) errors on TimescaleDB versions that forbid changing
// compression config while compressed chunks exist (which they do once compress_after elapses),
// and re-applying retention un-scheduled an operator's manually-added policy on every restart.
// Running once, before any chunk is compressed, sidesteps both -- so no remove_* calls are
// needed, since nothing exists yet to remove. Consequence: editing compress_after/retention in
// config and restarting does NOT re-apply; changing them on an existing database means adding a
// numbered migration step that re-runs the policy (see schemaMigrations).
func policyBlock(table string, compressAfter time.Duration, retention time.Duration, retentionSet bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, `DO $pol$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM umh.schema_migrations WHERE version = 1) THEN
    ALTER TABLE %[1]s SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'topic_id',
      timescaledb.compress_orderby   = 'ts DESC'
    );
    PERFORM add_compression_policy('%[1]s', INTERVAL '%[2]d seconds');
`, table, int64(compressAfter.Seconds()))
	if retentionSet {
		fmt.Fprintf(&b, "    PERFORM add_retention_policy('%s', INTERVAL '%d seconds');\n", table, int64(retention.Seconds()))
	}
	b.WriteString("  END IF;\nEND $pol$;")
	return b.String()
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

// policyDriftWarnings compares configured compression/retention intervals against the ones
// actually applied in the database (in seconds; nil = no such policy) and returns a warning per
// divergence. appliedComp == nil means the compression policy could not be read -- either the
// catalog is unavailable on this server or the database was never bootstrapped -- so it returns
// nothing rather than risk a false warning. Compression always has a policy after bootstrap, so
// its presence doubles as the "introspection works" probe for the retention checks.
func policyDriftWarnings(compressWant int64, appliedComp *int64, retentionSet bool, retentionWant int64, appliedRet *int64) []string {
	if appliedComp == nil {
		return nil
	}
	var warns []string
	if *appliedComp != compressWant {
		warns = append(warns, fmt.Sprintf("configured compress_after (%ds) does not match the compression policy applied in the database (%ds)", compressWant, *appliedComp))
	}
	switch {
	case retentionSet && appliedRet == nil:
		warns = append(warns, fmt.Sprintf("configured retention (%ds) is not applied in the database", retentionWant))
	case retentionSet && *appliedRet != retentionWant:
		warns = append(warns, fmt.Sprintf("configured retention (%ds) does not match the retention policy applied in the database (%ds)", retentionWant, *appliedRet))
	case !retentionSet && appliedRet != nil:
		warns = append(warns, fmt.Sprintf("retention is unset in config but a retention policy (%ds) is applied in the database", *appliedRet))
	}
	return warns
}

func bootstrapSQL(contract string, compressAfter time.Duration, retention time.Duration, retentionSet bool) string {
	s := bootstrapTemplate
	s = strings.Replace(s, "VALUE_POLICY_SLOT", policyBlock("umh.value_CONTRACT_SLOT", compressAfter, retention, retentionSet), 1)
	s = strings.Replace(s, "ATTR_POLICY_SLOT", policyBlock("umh.attribute_CONTRACT_SLOT", compressAfter, retention, retentionSet), 1)
	s = strings.Replace(s, "MIGRATIONS_SLOT", migrationsBlock(), 1)
	return sub(s, contract)
}

func valueQueryFor(contract string) string     { return sub(valueInsert, contract) }
func attributeQueryFor(contract string) string { return sub(attributeInsert, contract) }
