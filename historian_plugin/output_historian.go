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
		Field(service.NewIntField("port").Description("Port.").Default(5432)).
		Field(service.NewStringField("database").Description("Database name.").Default("umh").Advanced()).
		Field(service.NewStringField("username").Description("Login role.").Default("umh_owner").Advanced()).
		Field(service.NewStringField("password").Description("Role password (plaintext in config; redacted in logs).").Secret()).
		Field(service.NewStringField("sslmode").Description("require | disable | verify-full.").Default("require").Examples("require", "disable", "verify-full").Advanced()).
		Field(service.NewStringField("sslrootcert").Description("CA cert path (inside the umh-core container).").Default("").Advanced()).
		Field(service.NewStringField("sslcert").Description("Client cert path.").Default("").Advanced()).
		Field(service.NewStringField("sslkey").Description("Client key path.").Default("").Advanced()).
		Field(service.NewStringField("data_contract").Description("Bare lowercase contract name, e.g. \"pump\".")).
		Field(service.NewBoolField("metadata_keys_all").Description("Store all metadata keys except blacklists.").Default(true).Examples(true, false).Advanced()).
		Field(service.NewStringListField("metadata_keys").Description("Allowlist when metadata_keys_all=false.").Default([]any{}).Advanced()).
		Field(service.NewStringField("compress_after").Description("Compress chunks older than this (per contract).").Default("168h").Advanced()).
		Field(service.NewStringField("retention").Description("Drop chunks older than this; empty = keep forever.").Default("").Advanced()).
		Field(service.NewBatchPolicyField("batching")).
		Field(service.NewIntField("max_in_flight").Description("Max parallel batches in flight.").Default(8).Advanced())
}

type historianOutput struct {
	host, database, username, password    string
	port                                  int
	sslmode, sslrootcert, sslcert, sslkey string
	contract                              string
	metadataKeysAll                       bool
	metadataKeys                          []string
	compressAfter, retention              time.Duration
	retentionSet                          bool
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
	str("database", &o.database)
	str("username", &o.username)
	str("password", &o.password)
	str("sslmode", &o.sslmode)
	str("sslrootcert", &o.sslrootcert)
	str("sslcert", &o.sslcert)
	str("sslkey", &o.sslkey)
	str("data_contract", &o.contract)
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
	o.bootstrapped = true
	return nil
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
		row, reason := Transform(payload, meta, o.contract, o.metadataKeysAll, o.metadataKeys, view)
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
		return nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	vq := valueQueryFor(o.contract)
	aq := attributeQueryFor(o.contract)
	attrCount := 0
	for _, r := range rows {
		if _, err := tx.Exec(ctx, vq, r.RawLocation, r.ContractName, r.VirtualPath, r.TagName, string(r.ValueType), r.TS, r.ValueNum, r.ValueText); err != nil {
			return fmt.Errorf("value write failed: %w", err) // RAISE -> error -> nack -> degraded+stall
		}
		if r.EmitMeta {
			if _, err := tx.Exec(ctx, aq, r.RawLocation, r.ContractName, r.VirtualPath, r.TagName, string(r.ValueType), r.TS, r.MetadataJSON); err != nil {
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
			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			batchPolicy, err := conf.FieldBatchPolicy("batching")
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			out, err := newHistorianOutput(conf, mgr)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			return out, batchPolicy, maxInFlight, nil
		})
	if err != nil {
		panic(err)
	}
}
