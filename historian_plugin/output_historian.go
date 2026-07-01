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
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func historianConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary("Writes a UNS data contract into TimescaleDB using the UMH Historian schema.").
		Field(service.NewStringField("host").Description("TimescaleDB/Postgres host.")).
		Field(service.NewStringField("password").Description("Role password (plaintext in config; redacted in logs).").Secret()).
		Field(service.NewStringField("data_contract").Description("Bare lowercase contract name, e.g. \"pump\".")).
		Field(service.NewIntField("port").Description("Port.").Default(5432)).
		Field(service.NewStringField("database").Description("Database name.").Default("umh")).
		Field(service.NewStringField("username").Description("Login role.").Default("umh_owner")).
		Field(service.NewStringField("sslmode").Description("require | disable | verify-full.").Default("require").Examples("require", "disable", "verify-full")).
		Field(service.NewStringField("sslrootcert").Description("CA cert path (inside the umh-core container).").Default("").Advanced()).
		Field(service.NewStringField("sslcert").Description("Client cert path.").Default("").Advanced()).
		Field(service.NewStringField("sslkey").Description("Client key path.").Default("").Advanced()).
		Field(service.NewBoolField("metadata_keys_all").Description("Store all metadata keys except blacklists.").Default(true).Examples(true, false).Advanced()).
		Field(service.NewStringListField("metadata_keys").Description("Allowlist when metadata_keys_all=false.").Default([]any{}).Advanced()).
		Field(service.NewStringListField("metadata_keys_exclude").Description("Blacklist applied only when metadata_keys_all=true: drop these metadata keys on top of the built-in structural/high-churn exclusions. Each entry is an exact key name or a trailing-* prefix (e.g. \"opcua_*\"). Ignored in allowlist mode.").Default([]any{}).Examples([]any{"serialNumber"}, []any{"opcua_*", "spb_*"}).Advanced()).
		Field(service.NewStringField("compress_after").Description("Compress chunks older than this, as a Go duration; use hours (e.g. \"168h\") -- days are not a valid unit. Applied once at first database bootstrap. Per contract.").Default("168h").Advanced()).
		Field(service.NewStringField("retention").Description("Drop chunks older than this, as a Go duration; use hours (e.g. \"720h\") -- days are not a valid unit. Empty = keep forever. Applied once at first database bootstrap.").Default("").Advanced()).
		Field(service.NewBatchPolicyField("batching").Advanced()).
		Field(service.NewIntField("max_in_flight").Description("Max parallel batches in flight.").Default(8).Advanced())
}

type historianOutput struct {
	host, database, username, password    string
	port                                  int
	sslmode, sslrootcert, sslcert, sslkey string
	contract                              string
	metadataKeysAll                       bool
	metadataKeys                          []string
	metadataExclude                       *MetaExcluder
	compressAfter, retention              time.Duration
	retentionSet                          bool
	maxInFlight                           int
	dsnOverride                           string // set by tests; empty => build from fields

	logger    *service.Logger
	dropped   *service.MetricCounter // labeled by drop reason
	valueRows *service.MetricCounter // value rows upserted (after commit)
	attrRows  *service.MetricCounter // attribute rows upserted (after commit)
	dedupSize *service.MetricGauge   // current dedup-cache entry count
	dedup     *DedupCache

	mu           sync.Mutex
	pool         *pgxpool.Pool
	bootstrapped bool

	churnMu     sync.Mutex
	warnedChurn map[string]struct{} // high-churn keys already warned about
}

func newHistorianOutput(conf *service.ParsedConfig, mgr *service.Resources) (*historianOutput, error) {
	o := &historianOutput{
		logger:      mgr.Logger(),
		dropped:     mgr.Metrics().NewCounter("historian_messages_dropped", "reason"),
		valueRows:   mgr.Metrics().NewCounter("historian_value_rows_written"),
		attrRows:    mgr.Metrics().NewCounter("historian_attribute_rows_written"),
		dedupSize:   mgr.Metrics().NewGauge("historian_dedup_cache_size"),
		dedup:       NewDedupCache(),
		warnedChurn: map[string]struct{}{},
	}
	var err error
	str := func(field string, dst *string) bool {
		if err != nil {
			return false
		}
		*dst, err = conf.FieldString(field)
		return err == nil
	}
	str("host", &o.host)
	str("password", &o.password)
	str("data_contract", &o.contract)
	str("database", &o.database)
	str("username", &o.username)
	str("sslmode", &o.sslmode)
	str("sslrootcert", &o.sslrootcert)
	str("sslcert", &o.sslcert)
	str("sslkey", &o.sslkey)
	if err != nil {
		return nil, err
	}
	if o.port, err = conf.FieldInt("port"); err != nil {
		return nil, err
	}
	if err = ValidateContract(o.contract); err != nil {
		return nil, err
	}
	if o.metadataKeysAll, err = conf.FieldBool("metadata_keys_all"); err != nil {
		return nil, err
	}
	if o.metadataKeys, err = conf.FieldStringList("metadata_keys"); err != nil {
		return nil, err
	}
	excludePatterns, err := conf.FieldStringList("metadata_keys_exclude")
	if err != nil {
		return nil, err
	}
	o.metadataExclude = NewMetaExcluder(excludePatterns)
	if !o.metadataKeysAll && len(excludePatterns) > 0 {
		o.logger.Warnf("metadata_keys_exclude is set but ignored: it only applies when metadata_keys_all=true (allowlist mode is already explicit)")
	}
	caStr, err := conf.FieldString("compress_after")
	if err != nil {
		return nil, err
	}
	if o.compressAfter, err = time.ParseDuration(caStr); err != nil {
		return nil, fmt.Errorf("compress_after: %w", err)
	}
	// Sub-second durations render as INTERVAL '0 seconds' (whole-second SQL) and make an
	// invalid policy; reject here for a clear error instead of a bootstrap failure.
	if o.compressAfter < time.Second {
		return nil, fmt.Errorf("compress_after must be at least 1s, got %q", caStr)
	}
	retStr, err := conf.FieldString("retention")
	if err != nil {
		return nil, err
	}
	if retStr != "" {
		if o.retention, err = time.ParseDuration(retStr); err != nil {
			return nil, fmt.Errorf("retention: %w", err)
		}
		if o.retention < time.Second {
			return nil, fmt.Errorf("retention must be at least 1s when set, got %q", retStr)
		}
		o.retentionSet = true
	}
	if o.maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *historianOutput) buildDSN() string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(o.username, o.password),
		Host:   fmt.Sprintf("%s:%d", o.host, o.port),
		Path:   "/" + o.database,
	}
	q := url.Values{}
	q.Set("sslmode", o.sslmode)
	if o.sslrootcert != "" {
		q.Set("sslrootcert", o.sslrootcert)
	}
	if o.sslcert != "" {
		q.Set("sslcert", o.sslcert)
	}
	if o.sslkey != "" {
		q.Set("sslkey", o.sslkey)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// redact masks the password in an error. buildDSN percent-encodes it, so a DSN-bearing
// error carries the encoded form, not the raw one; mask both (the encoded form uses the
// same encoder as buildDSN).
func (o *historianOutput) redact(err error) string {
	msg := err.Error()
	if o.password == "" {
		return msg
	}
	msg = strings.ReplaceAll(msg, o.password, "xxxxx")
	if enc := strings.TrimPrefix(url.UserPassword("", o.password).String(), ":"); enc != "" && enc != o.password {
		msg = strings.ReplaceAll(msg, enc, "xxxxx")
	}
	return msg
}

func (o *historianOutput) dsn() string {
	if o.dsnOverride != "" {
		return o.dsnOverride
	}
	return o.buildDSN()
}

func (o *historianOutput) bootstrapStmt() string {
	return bootstrapSQL(o.contract, o.compressAfter, o.retention, o.retentionSet)
}

func (o *historianOutput) Connect(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.pool == nil {
		cfg, err := pgxpool.ParseConfig(o.dsn())
		if err != nil {
			return fmt.Errorf("invalid connection settings: %s", o.redact(err)) // DSN echoes the password
		}
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec // simple protocol (pgbouncer txn pool)
		// Each in-flight batch holds a pooled connection for its write transaction, so a pool
		// smaller than max_in_flight caps effective concurrency below the configured level.
		// pgxpool defaults to max(4, NumCPU) -- below the default max_in_flight of 8 on small
		// hosts -- so size the pool to serve every in-flight batch, +1 for the Connect-time
		// liveness/bootstrap checks. A larger pool_max_conns set in the DSN is left untouched.
		if want := int32(o.maxInFlight) + 1; cfg.MaxConns < want {
			cfg.MaxConns = want
		}
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return fmt.Errorf("connect failed: %s", o.redact(err))
		}
		o.pool = pool
	}

	// Liveness + version check, bounded so a hung server fails Connect instead of blocking.
	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var version int
	if err := o.pool.QueryRow(checkCtx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		return fmt.Errorf("connect check failed: %w", err)
	}
	if version < 130000 {
		return fmt.Errorf("PostgreSQL 13+ required (ltree must be a trusted extension); got server_version_num=%d", version)
	}

	if o.bootstrapped {
		return nil
	}
	// Bounded: bootstrap takes an advisory lock and runs DDL, which can contend; a deadline
	// lets a hung bootstrap fail-and-retry instead of blocking Connect (and WriteBatch via o.mu).
	bootCtx, cancelBoot := context.WithTimeout(ctx, 2*time.Minute)
	defer cancelBoot()
	conn, err := o.pool.Acquire(bootCtx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(bootCtx, o.bootstrapStmt()); err != nil {
		return fmt.Errorf("schema bootstrap failed: %w", err) // guard stays false -> next Connect retries
	}
	o.warnPolicyDrift(bootCtx)
	o.bootstrapped = true
	return nil
}

// warnPolicyDrift warns when the compression/retention policy applied in the database differs
// from the configured values. Policies are set once at first bootstrap (see policyBlock), so
// editing compress_after/retention and restarting otherwise has no visible effect; this surfaces
// that instead of silently ignoring the change. Best-effort: any introspection error is swallowed
// so a server whose timescaledb_information shape differs never fails Connect. Both hypertables
// get identical policies, so the value table is representative.
func (o *historianOutput) warnPolicyDrift(ctx context.Context) {
	table := "value_" + o.contract
	var appliedComp, appliedRet *int64
	if err := o.pool.QueryRow(ctx, policyIntervalSQL("policy_compression", "compress_after"), table).Scan(&appliedComp); err != nil {
		return
	}
	_ = o.pool.QueryRow(ctx, policyIntervalSQL("policy_retention", "drop_after"), table).Scan(&appliedRet)
	for _, w := range policyDriftWarnings(int64(o.compressAfter.Seconds()), appliedComp, o.retentionSet, int64(o.retention.Seconds()), appliedRet) {
		o.logger.Warnf("TimescaleDB historian: %s. Policies are set once at first bootstrap and are not re-applied on restart; change them on an existing database via a schema migration.", w)
	}
}

func (o *historianOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	o.mu.Lock()
	pool := o.pool
	o.mu.Unlock()
	if pool == nil {
		return service.ErrNotConnected
	}

	view := o.dedup.NewBatch()
	rows := make([]*Row, 0, len(batch))
	churn := map[string]struct{}{}
	for _, msg := range batch {
		meta := map[string]string{}
		_ = msg.MetaWalk(func(k, v string) error { meta[k] = v; return nil })
		structured, err := msg.AsStructured()
		if err != nil {
			o.recordDrop("not_structured", meta["umh_topic"])
			continue
		}
		payload, ok := structured.(map[string]any)
		if !ok {
			o.recordDrop("not_object", meta["umh_topic"])
			continue
		}
		row, reason := Transform(payload, meta, o.contract, o.metadataKeysAll, o.metadataKeys, o.metadataExclude, view)
		if reason != DropNone {
			o.recordDrop(string(reason), meta["umh_topic"])
			continue
		}
		for _, k := range row.churnKeys { // union across the whole batch, not just the first row
			churn[k] = struct{}{}
		}
		rows = append(rows, row)
	}
	o.warnHighChurn(churn)
	if len(rows) == 0 {
		// A fully-dropped non-empty batch writes zero rows while the connection stays up, so
		// umh-core would otherwise see a healthy bridge silently discarding all data. Warn at a
		// level umh-core surfaces as degraded; still return nil (no nack) so a single bad
		// message never stalls the stream.
		if len(batch) > 0 {
			o.logger.Warnf("TimescaleDB historian: dropped all %d message(s) in the batch; nothing written. Check the source data and metadata configuration.", len(batch))
		}
		return nil
	}

	// Phase 1: resolve each DISTINCT topic to its topic_id, each in its own short-lived
	// statement (autocommit). This keeps the shared dimension-row write locks -- above all
	// the single location row every message of a contract upserts -- held only for one tiny
	// resolution, not for the whole value-write batch. Holding them batch-long serializes
	// concurrent batches (max_in_flight) on that row, so batching would not scale; releasing
	// them immediately lets the value writes below run concurrently. See topicResolveSQL.
	type topicKey struct {
		loc, contract, vpath, tag string
		vt                        ValueType
	}
	keyOf := func(r *Row) topicKey {
		return topicKey{r.RawLocation, r.ContractName, r.VirtualPath, r.TagName, r.ValueType}
	}
	topicID := make(map[topicKey]int64)
	for _, r := range rows {
		k := keyOf(r)
		if _, ok := topicID[k]; ok {
			continue
		}
		var id int64
		// QueryRow runs outside an explicit tx, so the dimension upserts commit on their own
		// and release their row locks at once. A datatype flip still RAISEs here (the tag
		// upsert's guard), failing the batch before any value is written.
		if err := pool.QueryRow(ctx, topicResolveSQL, r.RawLocation, r.ContractName, r.VirtualPath, r.TagName, string(r.ValueType)).Scan(&id); err != nil {
			return fmt.Errorf("topic resolve failed: %w", err)
		}
		topicID[k] = id
	}

	// Phase 2: write values + attributes by topic_id in one transaction. These rows have
	// distinct (topic_id, ts), so concurrent batches do not contend; the single commit
	// amortizes across the batch and throughput scales with max_in_flight.
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	vq := valueQueryFor(o.contract)
	aq := attributeQueryFor(o.contract)
	attrCount := 0
	for _, r := range rows {
		id := topicID[keyOf(r)]
		if _, err := tx.Exec(ctx, vq, id, r.TS, r.ValueNum, r.ValueText); err != nil {
			// A value conflict (raise_pk_conflict) or any write error halts the bridge
			// (error -> nack -> retry the same batch -> stall until the source is fixed), the
			// template's append-only conflict policy. Emit an attributable line first so the
			// halt names the offending tag rather than relying on benthos to log a bare error.
			o.logger.Errorf("TimescaleDB historian: value write failed for %s: %v", describeRow(r), err)
			return fmt.Errorf("value write failed: %w", err)
		}
		if r.EmitMeta {
			if _, err := tx.Exec(ctx, aq, id, r.TS, r.MetadataJSON); err != nil {
				o.logger.Errorf("TimescaleDB historian: attribute write failed for %s: %v", describeRow(r), err)
				return fmt.Errorf("attribute write failed: %w", err)
			}
			attrCount++
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	view.Commit() // promote dedup entries only after a successful commit
	o.dedupSize.Set(int64(o.dedup.Len()))
	o.valueRows.Incr(int64(len(rows)))
	o.attrRows.Incr(int64(attrCount))
	return nil
}

// describeRow identifies a row for an attributable write-failure log: which tag, at which ts.
func describeRow(r *Row) string {
	return fmt.Sprintf("contract=%q location=%q virtual_path=%q tag=%q ts=%v",
		r.ContractName, r.RawLocation, r.VirtualPath, r.TagName, r.TS)
}

// recordDrop counts and debug-logs a discarded message, so a misconfigured bridge that
// drops everything is visible (the metric) rather than silently healthy with zero rows.
func (o *historianOutput) recordDrop(reason string, topic string) {
	o.dropped.Incr(1, reason)
	o.logger.Debugf("TimescaleDB historian: dropped message (reason=%s, umh_topic=%q)", reason, topic)
}

// warnHighChurn warns once per distinct high-churn key (re-firing when a new one appears).
func (o *historianOutput) warnHighChurn(keys map[string]struct{}) {
	if len(keys) == 0 {
		return
	}
	o.churnMu.Lock()
	var fresh []string
	for k := range keys {
		if _, seen := o.warnedChurn[k]; !seen {
			o.warnedChurn[k] = struct{}{}
			fresh = append(fresh, k)
		}
	}
	o.churnMu.Unlock()
	if len(fresh) == 0 {
		return
	}
	sort.Strings(fresh)
	o.logger.Warnf("TimescaleDB historian: archiving high-churn metadata key(s) [%s]. These change on nearly every message, so the attribute table grows per-message and de-duplication does not help. Remove them from metadata_keys unless you specifically need them.", strings.Join(fresh, ", "))
}

func (o *historianOutput) Close(_ context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.pool != nil {
		o.pool.Close()
		o.pool = nil
	}
	return nil
}

func init() {
	err := service.RegisterBatchOutput("historian", historianConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			batchPolicy, err := conf.FieldBatchPolicy("batching")
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			out, err := newHistorianOutput(conf, mgr)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			return out, batchPolicy, out.maxInFlight, nil
		})
	if err != nil {
		panic(err)
	}
}
