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
CREATE EXTENSION IF NOT EXISTS ltree;
CREATE SCHEMA IF NOT EXISTS umh;
CREATE TABLE IF NOT EXISTS umh.schema_migrations (
  version    INT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
DO $$
BEGIN
  CREATE TYPE umh.value_type AS ENUM ('numeric', 'text');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
CREATE TABLE IF NOT EXISTS umh.location (
  location_id BIGSERIAL PRIMARY KEY,
  path        LTREE NOT NULL,
  UNIQUE (path)
);
CREATE INDEX IF NOT EXISTS idx_location_path_gist ON umh.location USING GIST (path);
CREATE TABLE IF NOT EXISTS umh.tag (
  tag_id        BIGSERIAL PRIMARY KEY,
  name          TEXT       NOT NULL,
  virtual_path  TEXT       NOT NULL,
  data_contract_name TEXT  NOT NULL,
  value_type    umh.value_type NOT NULL,
  UNIQUE (virtual_path, name, data_contract_name)
);
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
);
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

const valueInsert = `INSERT INTO umh.value_CONTRACT_SLOT AS v (topic_id, ts, value_num, value_text)
SELECT tp.topic_id, $6::timestamptz, $7::double precision, $8 FROM tp
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
SELECT tp.topic_id, $6::timestamptz, $7::jsonb FROM tp
ON CONFLICT (topic_id, ts) DO UPDATE
  SET attribute = CASE
    WHEN a.attribute IS DISTINCT FROM EXCLUDED.attribute
    THEN umh.raise_pk_conflict(
           format('attribute conflict at topic_id=%s ts=%s: stored %s but received %s',
                  a.topic_id, a.ts, a.attribute, EXCLUDED.attribute),
           NULL::jsonb)
    ELSE a.attribute
  END;`

func sub(sql string, contract string) string {
	return strings.ReplaceAll(sql, "CONTRACT_SLOT", contract)
}

func policyBlock(table string, compressAfter time.Duration, retention time.Duration, retentionSet bool) string {
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

func bootstrapSQL(contract string, compressAfter time.Duration, retention time.Duration, retentionSet bool) string {
	s := bootstrapTemplate
	s = strings.Replace(s, "VALUE_POLICY_SLOT", policyBlock("umh.value_CONTRACT_SLOT", compressAfter, retention, retentionSet), 1)
	s = strings.Replace(s, "ATTR_POLICY_SLOT", policyBlock("umh.attribute_CONTRACT_SLOT", compressAfter, retention, retentionSet), 1)
	return sub(s, contract)
}

func valueQueryFor(contract string) string     { return sub(dimensionCTE+valueInsert, contract) }
func attributeQueryFor(contract string) string { return sub(dimensionCTE+attributeInsert, contract) }
