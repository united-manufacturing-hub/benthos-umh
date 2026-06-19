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

package timescaledb_historian_plugin

import (
	"fmt"
	"strings"
	"time"
)

// The SQL below is a verbatim port of the merged ManagementConsole template
// (ManagementConsole/frontend/templates/custom/timescaledb/timescaledb-historian.yaml),
// with two parameterizations: the contract name is substituted into table names,
// and the compression/retention policies take their interval from config. The one
// behavioral deviation is that the loc CTE wraps the raw location_path in
// to_ltree_path(), so write and read canonicalize through the same SQL function.
//
// "CONTRACT_SLOT" is a sentinel replaced by the validated contract name (^[a-z0-9_]+$,
// so substitution is injection-safe).

const bootstrapTemplate = `BEGIN;
SELECT pg_advisory_xact_lock(hashtext('uns_to_timescale_bootstrap'));
CREATE EXTENSION IF NOT EXISTS ltree;
CREATE TABLE IF NOT EXISTS schema_migrations (
  version    INT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
DO $$
BEGIN
  CREATE TYPE value_type AS ENUM ('numeric', 'text');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
CREATE TABLE IF NOT EXISTS location (
  location_id BIGSERIAL PRIMARY KEY,
  path        LTREE NOT NULL,
  UNIQUE (path)
);
CREATE INDEX IF NOT EXISTS idx_location_path_gist ON location USING GIST (path);
CREATE TABLE IF NOT EXISTS tag (
  tag_id        BIGSERIAL PRIMARY KEY,
  name          TEXT       NOT NULL,
  virtual_path  TEXT       NOT NULL,
  data_contract_name TEXT  NOT NULL,
  value_type    value_type NOT NULL,
  UNIQUE (virtual_path, name, data_contract_name)
);
CREATE OR REPLACE FUNCTION tag_value_type_guard()
RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
  IF NEW.value_type IS DISTINCT FROM OLD.value_type THEN
    RAISE EXCEPTION 'tag datatype changed for virtual_path=% name=% contract=%: stored % but received % (one tag must always produce the same datatype)',
      OLD.virtual_path, OLD.name, OLD.data_contract_name, OLD.value_type, NEW.value_type;
  END IF;
  RETURN NEW;
END $guard$;
DROP TRIGGER IF EXISTS trg_tag_value_type_guard ON tag;
CREATE TRIGGER trg_tag_value_type_guard
  BEFORE UPDATE ON tag
  FOR EACH ROW EXECUTE FUNCTION tag_value_type_guard();
CREATE OR REPLACE FUNCTION raise_pk_conflict(p_msg text, p_ret anyelement)
RETURNS anyelement LANGUAGE plpgsql AS $rpc$
BEGIN
  RAISE EXCEPTION '%', p_msg;
END $rpc$;
CREATE TABLE IF NOT EXISTS topic (
  topic_id    BIGSERIAL PRIMARY KEY,
  location_id BIGINT NOT NULL REFERENCES location(location_id),
  tag_id      BIGINT NOT NULL REFERENCES tag(tag_id),
  UNIQUE (location_id, tag_id)
);
-- topic_id is deliberately NOT a foreign key on the hypertables below: an FK on a
-- hypertable is re-created per chunk and its ShareRowExclusiveLock on topic deadlocks
-- concurrent writers. topic_id always comes from the tp upsert in the same statement.
CREATE TABLE IF NOT EXISTS value_CONTRACT_SLOT (
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
SELECT create_hypertable('value_CONTRACT_SLOT', 'ts', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
VALUE_POLICY_SLOT
CREATE TABLE IF NOT EXISTS attribute_CONTRACT_SLOT (
  topic_id  BIGINT      NOT NULL,
  ts        TIMESTAMPTZ NOT NULL,
  attribute JSONB       NOT NULL,
  PRIMARY KEY (topic_id, ts)
);
SELECT create_hypertable('attribute_CONTRACT_SLOT', 'ts', if_not_exists => TRUE);
ATTR_POLICY_SLOT
CREATE OR REPLACE FUNCTION to_ltree_path(p_location_path text)
RETURNS ltree
LANGUAGE sql IMMUTABLE
AS $ltree$
  SELECT string_agg(q.lbl, '.' ORDER BY q.ord)::ltree
    FROM (SELECT left(regexp_replace(s.seg, '[^A-Za-z0-9_]', '_', 'g'), 255) AS lbl, s.ord
            FROM unnest(string_to_array(p_location_path, '.')) WITH ORDINALITY AS s(seg, ord)) q
   WHERE length(q.lbl) > 0;
$ltree$;
CREATE OR REPLACE FUNCTION get_topic_id(
  p_location_path text,
  p_virtual_path  text,
  p_data_contract text,
  p_tag_name      text
)
RETURNS bigint
LANGUAGE sql STABLE SECURITY INVOKER SET search_path = public, pg_temp
AS $fn$
  SELECT t.topic_id
    FROM location l
    JOIN tag   g ON g.virtual_path = p_virtual_path
                AND g.name = p_tag_name
                AND g.data_contract_name =
                    '_' || regexp_replace(regexp_replace(p_data_contract, '_v\d+$', ''), '^_', '')
    JOIN topic t ON t.location_id = l.location_id AND t.tag_id = g.tag_id
   WHERE l.path = to_ltree_path(p_location_path);
$fn$;
-- ===================== MIGRATIONS =====================
-- Append schema CHANGES here as numbered, forward-only steps gated on
-- schema_migrations, ascending across the value and attribute tables.
-- ======================================================
COMMIT;`

const dimensionCTE = `WITH loc AS (
  INSERT INTO location (path) VALUES (to_ltree_path($1))
  ON CONFLICT (path) DO UPDATE SET path = EXCLUDED.path
  RETURNING location_id
),
tg AS (
  INSERT INTO tag (name, virtual_path, data_contract_name, value_type)
  VALUES ($4, $3, $2, $5::value_type)
  ON CONFLICT (virtual_path, name, data_contract_name) DO UPDATE SET value_type = EXCLUDED.value_type
  RETURNING tag_id
),
tp AS (
  INSERT INTO topic (location_id, tag_id)
  SELECT loc.location_id, tg.tag_id FROM loc CROSS JOIN tg
  ON CONFLICT (location_id, tag_id) DO UPDATE SET location_id = EXCLUDED.location_id
  RETURNING topic_id
)
`

const valueInsert = `INSERT INTO value_CONTRACT_SLOT AS v (topic_id, ts, value_num, value_text)
SELECT tp.topic_id, $6::timestamptz, $7::double precision, $8 FROM tp
ON CONFLICT (topic_id, ts) DO UPDATE
  SET value_num = CASE
    WHEN v.value_num  IS DISTINCT FROM EXCLUDED.value_num
      OR v.value_text IS DISTINCT FROM EXCLUDED.value_text
    THEN raise_pk_conflict(
           format('value conflict at topic_id=%s ts=%s: stored (num=%s, text=%s) but received (num=%s, text=%s)',
                  v.topic_id, v.ts, v.value_num, v.value_text, EXCLUDED.value_num, EXCLUDED.value_text),
           NULL::double precision)
    ELSE v.value_num
  END;`

const attributeInsert = `INSERT INTO attribute_CONTRACT_SLOT AS a (topic_id, ts, attribute)
SELECT tp.topic_id, $6::timestamptz, $7::jsonb FROM tp
ON CONFLICT (topic_id, ts) DO UPDATE
  SET attribute = CASE
    WHEN a.attribute IS DISTINCT FROM EXCLUDED.attribute
    THEN raise_pk_conflict(
           format('attribute conflict at topic_id=%s ts=%s: stored %s but received %s',
                  a.topic_id, a.ts, a.attribute, EXCLUDED.attribute),
           NULL::jsonb)
    ELSE a.attribute
  END;`

func sub(sql, contract string) string {
	return strings.ReplaceAll(sql, "CONTRACT_SLOT", contract)
}

func policyBlock(table string, compressAfter, retention time.Duration, retentionSet bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, `DO $pol$
BEGIN
  PERFORM remove_compression_policy('%[1]s', if_exists => TRUE);
  ALTER TABLE %[1]s SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'topic_id',
    timescaledb.compress_orderby   = 'ts DESC'
  );
  PERFORM add_compression_policy('%[1]s', INTERVAL '%[2]d seconds');
  PERFORM remove_retention_policy('%[1]s', if_exists => TRUE);
`, table, int64(compressAfter.Seconds()))
	if retentionSet {
		fmt.Fprintf(&b, "  PERFORM add_retention_policy('%s', INTERVAL '%d seconds');\n", table, int64(retention.Seconds()))
	}
	b.WriteString("END $pol$;")
	return b.String()
}

func bootstrapSQL(contract string, compressAfter, retention time.Duration, retentionSet bool) string {
	s := bootstrapTemplate
	s = strings.Replace(s, "VALUE_POLICY_SLOT", policyBlock("value_CONTRACT_SLOT", compressAfter, retention, retentionSet), 1)
	s = strings.Replace(s, "ATTR_POLICY_SLOT", policyBlock("attribute_CONTRACT_SLOT", compressAfter, retention, retentionSet), 1)
	return sub(s, contract)
}

func valueQueryFor(contract string) string     { return sub(dimensionCTE+valueInsert, contract) }
func attributeQueryFor(contract string) string { return sub(dimensionCTE+attributeInsert, contract) }
