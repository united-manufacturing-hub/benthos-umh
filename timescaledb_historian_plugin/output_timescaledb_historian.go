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
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func timescaledbHistorianConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary("Writes a UNS data contract into TimescaleDB using the UMH Historian schema.").
		Field(service.NewStringField("host").Description("TimescaleDB/Postgres host.")).
		Field(service.NewIntField("port").Description("Port.").Default(5432)).
		Field(service.NewStringField("database").Description("Database name.").Default("umh").Advanced()).
		Field(service.NewStringField("username").Description("Login role.").Default("umh_owner").Advanced()).
		Field(service.NewStringField("password").Description("Role password (plaintext in config; redacted in logs).").Secret()).
		Field(service.NewStringField("sslmode").Description("require | disable | verify-full.").Default("require").Advanced()).
		Field(service.NewStringField("sslrootcert").Description("CA cert path (inside the umh-core container).").Default("").Advanced()).
		Field(service.NewStringField("sslcert").Description("Client cert path.").Default("").Advanced()).
		Field(service.NewStringField("sslkey").Description("Client key path.").Default("").Advanced()).
		Field(service.NewStringField("data_contract").Description("Bare lowercase contract name, e.g. \"pump\".")).
		Field(service.NewBoolField("metadata_keys_all").Description("Store all metadata keys except blacklists.").Default(true).Advanced()).
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

	logger *service.Logger
	dedup  *DedupCache

	mu           sync.Mutex
	pool         *pgxpool.Pool
	bootstrapped bool
	churnOnce    sync.Once
}

func newHistorianOutput(conf *service.ParsedConfig, mgr *service.Resources) (*historianOutput, error) {
	o := &historianOutput{logger: mgr.Logger(), dedup: NewDedupCache()}
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
	retStr, err := conf.FieldString("retention")
	if err != nil {
		return nil, err
	}
	if retStr != "" {
		if o.retention, err = time.ParseDuration(retStr); err != nil {
			return nil, fmt.Errorf("retention: %w", err)
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
			return err
		}
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec // simple protocol (pgbouncer txn pool)
		cfg.ConnConfig.RuntimeParams["search_path"] = "public"
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return err
		}
		o.pool = pool
	}

	// Cheap liveness + version check on every Connect (also the first real round-trip).
	var version int
	if err := o.pool.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		return fmt.Errorf("connect check failed: %w", err)
	}
	if version < 130000 {
		return fmt.Errorf("PostgreSQL 13+ required (ltree must be a trusted extension); got server_version_num=%d", version)
	}

	if o.bootstrapped {
		return nil
	}
	conn, err := o.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, o.bootstrapStmt()); err != nil {
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
	var churn []string
	for _, msg := range batch {
		structured, err := msg.AsStructured()
		if err != nil {
			continue // not a JSON object -> silent drop
		}
		payload, ok := structured.(map[string]any)
		if !ok {
			continue
		}
		meta := map[string]string{}
		_ = msg.MetaWalk(func(k, v string) error { meta[k] = v; return nil })
		row, ok := Transform(payload, meta, o.contract, o.metadataKeysAll, o.metadataKeys, view)
		if !ok {
			continue
		}
		if len(row.churnKeys) > 0 && churn == nil {
			churn = row.churnKeys
		}
		rows = append(rows, row)
	}
	if churn != nil {
		o.warnHighChurnOnce(churn)
	}
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
	for _, r := range rows {
		if _, err := tx.Exec(ctx, vq, r.RawLocation, r.ContractName, r.VirtualPath, r.TagName, r.ValueType, r.TS, r.ValueNum, r.ValueText); err != nil {
			return fmt.Errorf("value write failed: %w", err) // RAISE -> error -> nack -> degraded+stall
		}
		if r.EmitMeta {
			if _, err := tx.Exec(ctx, aq, r.RawLocation, r.ContractName, r.VirtualPath, r.TagName, r.ValueType, r.TS, r.MetadataJSON); err != nil {
				return fmt.Errorf("attribute write failed: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	view.Commit() // promote dedup entries only after a successful commit
	return nil
}

func (o *historianOutput) warnHighChurnOnce(keys []string) {
	o.churnOnce.Do(func() {
		o.logger.Warnf("TimescaleDB historian: archiving high-churn metadata key(s) [%s]. These change on nearly every message, so the attribute table grows per-message and de-duplication does not help. Remove them from metadata_keys unless you specifically need them.", strings.Join(keys, ", "))
	})
}

func (o *historianOutput) Close(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.pool != nil {
		o.pool.Close()
		o.pool = nil
	}
	return nil
}

func init() {
	err := service.RegisterBatchOutput("timescaledb_historian", timescaledbHistorianConfig(),
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
