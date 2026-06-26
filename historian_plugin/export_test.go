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
	"context"
	"fmt"
	"time"

	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// HistorianConfig exposes the config spec for tests.
func HistorianConfig() *service.ConfigSpec { return historianConfig() }

// BootstrapSQLForTest renders the bootstrap DDL for a contract (default policies).
func BootstrapSQLForTest(contract string) string {
	return bootstrapSQL(contract, 168*time.Hour, 0, false)
}

// SchemaVersionForTest exposes the highest schema-migration version the bootstrap applies
// (0 when the ledger ships empty, i.e. the baseline is unversioned).
func SchemaVersionForTest() int { return highestMigrationVersion() }

// perMessageValueQueryFor builds the "no batching" baseline: dimension resolution + value
// insert in ONE combined statement, run once per row (one statement, one commit per row -- the
// way an un-batched / per-message sink writes). Benchmark-only.
func perMessageValueQueryFor(contract string) string {
	return sub(dimensionCTE+`INSERT INTO umh.value_CONTRACT_SLOT AS v (topic_id, ts, value_num, value_text)
SELECT tp.topic_id, $6::timestamptz, $7::double precision, $8 FROM tp
ON CONFLICT (topic_id, ts) DO UPDATE
  SET value_num = CASE
    WHEN v.value_num IS DISTINCT FROM EXCLUDED.value_num
      OR v.value_text IS DISTINCT FROM EXCLUDED.value_text
    THEN umh.raise_pk_conflict(format('value conflict topic_id=%s ts=%s', v.topic_id, v.ts), NULL::double precision)
    ELSE v.value_num
  END;`, contract)
}

// WritePerMessageValue writes one value row with no batching: a single combined statement run in
// autocommit (its own implicit transaction). Concurrency is supplied by the caller. Benchmark-only.
func (h *HistorianTestHandle) WritePerMessageValue(ctx context.Context, rawLoc, contractName, vpath, tag, vtype, ts string, num *float64, text *string) error {
	_, err := h.o.pool.Exec(ctx, perMessageValueQueryFor(h.o.contract), rawLoc, contractName, vpath, tag, vtype, ts, num, text)
	return err
}

// GetTopicID runs the documented umh.get_topic_id() resolver (the Grafana/ad-hoc entry point).
func (h *HistorianTestHandle) GetTopicID(ctx context.Context, loc, vpath, contract, tag string) (int64, bool) {
	ExpectWithOffset(1, h.o.pool).NotTo(BeNil(), "Connect must succeed before GetTopicID")
	var id *int64
	err := h.o.pool.QueryRow(ctx, "SELECT umh.get_topic_id($1, $2, $3, $4)", loc, vpath, contract, tag).Scan(&id)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	if id == nil {
		return 0, false
	}
	return *id, true
}

// ValueWindow runs the documented read pattern: filter value_<contract> by topic_id over a
// [fromMs, toMs) time window, returning the value_num column in ts order.
func (h *HistorianTestHandle) ValueWindow(ctx context.Context, contract string, topicID, fromMs, toMs int64) []float64 {
	ExpectWithOffset(1, h.o.pool).NotTo(BeNil(), "Connect must succeed before ValueWindow")
	q := fmt.Sprintf("SELECT value_num FROM umh.value_%s WHERE topic_id = $1 AND ts >= to_timestamp($2/1000.0) AND ts < to_timestamp($3/1000.0) ORDER BY ts", contract)
	rows, err := h.o.pool.Query(ctx, q, topicID, fromMs, toMs)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer rows.Close()
	var out []float64
	for rows.Next() {
		var v float64
		ExpectWithOffset(1, rows.Scan(&v)).NotTo(HaveOccurred())
		out = append(out, v)
	}
	ExpectWithOffset(1, rows.Err()).NotTo(HaveOccurred())
	return out
}

// SchemaVersion reads max(version) from umh.schema_migrations (integration tests).
func (h *HistorianTestHandle) SchemaVersion(ctx context.Context) int {
	ExpectWithOffset(1, h.o.pool).NotTo(BeNil(), "Connect must succeed before SchemaVersion")
	var v *int
	err := h.o.pool.QueryRow(ctx, "SELECT max(version) FROM umh.schema_migrations").Scan(&v)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	if v == nil {
		return 0
	}
	return *v
}

// RedactDSN builds an output with the given password, returns its DSN (the form that
// leaks in an error) and redact() applied to an error whose text embeds that DSN.
func RedactDSN(password string) (string, string) {
	o := &historianOutput{username: "umh_owner", password: password, host: "db", port: 5432, database: "umh", sslmode: "require"}
	dsn := o.buildDSN()
	return dsn, o.redact(fmt.Errorf("failed to connect: %s", dsn))
}

// HistorianTestHandle wraps the unexported output so external tests can drive it.
type HistorianTestHandle struct{ o *historianOutput }

// NewHistorianForConfig builds an output from a parsed config (config-parse tests).
func NewHistorianForConfig(conf *service.ParsedConfig) (*HistorianTestHandle, error) {
	o, err := newHistorianOutput(conf, service.MockResources())
	if err != nil {
		return nil, err
	}
	return &HistorianTestHandle{o: o}, nil
}

// NewHistorianTestHandle builds an output directly against a DSN (integration tests).
func NewHistorianTestHandle(dsn string, contract string) *HistorianTestHandle {
	mgr := service.MockResources()
	return &HistorianTestHandle{o: &historianOutput{
		dsnOverride:     dsn,
		contract:        contract,
		metadataKeysAll: true,
		compressAfter:   168 * time.Hour,
		logger:          mgr.Logger(),
		dropped:         mgr.Metrics().NewCounter("historian_messages_dropped", "reason"),
		valueRows:       mgr.Metrics().NewCounter("historian_value_rows_written"),
		attrRows:        mgr.Metrics().NewCounter("historian_attribute_rows_written"),
		dedupSize:       mgr.Metrics().NewGauge("historian_dedup_cache_size"),
		dedup:           NewDedupCache(),
	}}
}

// SetMetaExclude configures the metadata blacklist (all-keys mode) for integration tests.
func (h *HistorianTestHandle) SetMetaExclude(patterns []string) {
	h.o.metadataExclude = NewMetaExcluder(patterns)
}

func (h *HistorianTestHandle) BuildDSN() string                  { return h.o.buildDSN() }
func (h *HistorianTestHandle) Connect(ctx context.Context) error { return h.o.Connect(ctx) }
func (h *HistorianTestHandle) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	return h.o.WriteBatch(ctx, b)
}
func (h *HistorianTestHandle) Close(ctx context.Context) error { return h.o.Close(ctx) }

// SetDSN repoints the handle and resets the pool so the next Connect re-opens it.
func (h *HistorianTestHandle) SetDSN(dsn string) {
	h.o.mu.Lock()
	defer h.o.mu.Unlock()
	if h.o.pool != nil {
		h.o.pool.Close()
		h.o.pool = nil
	}
	h.o.bootstrapped = false
	h.o.dsnOverride = dsn
}

// SQLToLtree runs the ported to_ltree_path() and returns (value, isNull).
func (h *HistorianTestHandle) SQLToLtree(ctx context.Context, path string) (string, bool) {
	if h == nil || h.o == nil || h.o.pool == nil {
		return "", false
	}
	var v *string
	if err := h.o.pool.QueryRow(ctx, "SELECT umh.to_ltree_path($1)::text", path).Scan(&v); err != nil {
		return "", false
	}
	if v == nil {
		return "", true
	}
	return *v, false
}

func (h *HistorianTestHandle) CountValueRows(ctx context.Context, contract string) int {
	ExpectWithOffset(1, h.o.pool).NotTo(BeNil(), "Connect must succeed before CountValueRows")
	var n int
	err := h.o.pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM umh.value_%s", contract)).Scan(&n)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return n
}

func (h *HistorianTestHandle) CountAttributeRows(ctx context.Context, contract string) int {
	ExpectWithOffset(1, h.o.pool).NotTo(BeNil(), "Connect must succeed before CountAttributeRows")
	var n int
	err := h.o.pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM umh.attribute_%s", contract)).Scan(&n)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return n
}

// AttributeValue reads the stored JSONB attribute for a key via the read surface
// (attribute->>key), proving the column holds an object, not an array-of-pairs.
func (h *HistorianTestHandle) AttributeValue(ctx context.Context, contract string, key string) (string, bool) {
	ExpectWithOffset(1, h.o.pool).NotTo(BeNil(), "Connect must succeed before AttributeValue")
	var v *string
	q := fmt.Sprintf("SELECT attribute->>$1 FROM umh.attribute_%s LIMIT 1", contract)
	err := h.o.pool.QueryRow(ctx, q, key).Scan(&v)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	if v == nil {
		return "", false
	}
	return *v, true
}
