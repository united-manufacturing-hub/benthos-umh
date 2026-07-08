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
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
		Field(service.NewStringField("data_contract_name").Description("Bare lowercase contract name, e.g. \"pump\"; matches the umh.tag.data_contract_name column.")).
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
	poisoned  *service.MetricCounter // rows dropped as poison (labels: sqlstate, phase)
	truncated *service.MetricCounter // value_text values clipped to maxTextRunes
	dedup     *DedupCache

	lookupHits   atomic.Int64 // topic resolves served by the read-first lookup (no sequence burn)
	lookupMisses atomic.Int64 // topic resolves that fell through to the guarded upsert (new topic or datatype flip)

	mu           sync.Mutex
	pool         *pgxpool.Pool
	bootstrapped bool

	churnMu     sync.Mutex
	warnedChurn map[string]struct{} // high-churn keys already warned about

	warnedTruncate atomic.Bool // warn-once guard for truncation
}

func newHistorianOutput(conf *service.ParsedConfig, mgr *service.Resources) (*historianOutput, error) {
	o := &historianOutput{
		logger:      mgr.Logger(),
		dropped:     mgr.Metrics().NewCounter("historian_messages_dropped", "reason"),
		valueRows:   mgr.Metrics().NewCounter("historian_value_rows_written"),
		attrRows:    mgr.Metrics().NewCounter("historian_attribute_rows_written"),
		dedupSize:   mgr.Metrics().NewGauge("historian_dedup_cache_size"),
		poisoned:    mgr.Metrics().NewCounter("historian_rows_poisoned", "sqlstate", "phase"),
		truncated:   mgr.Metrics().NewCounter("historian_values_truncated"),
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
	str("data_contract_name", &o.contract)
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
		// Each in-flight batch holds a pooled connection for its write tx, so a pool smaller than
		// max_in_flight silently caps concurrency. pgxpool defaults to max(4, NumCPU), below the
		// default 8; size it to max_in_flight+1 (+1 for Connect-time checks). A larger DSN
		// pool_max_conns wins.
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
	if version < 160000 {
		return fmt.Errorf("PostgreSQL 16+ required (ltree labels must accept hyphens); got server_version_num=%d", version)
	}

	// A deadline lets a hung bootstrap (advisory lock + DDL can contend) fail-and-retry instead of
	// blocking Connect and WriteBatch via o.mu.
	bootCtx, cancelBoot := context.WithTimeout(ctx, 2*time.Minute)
	defer cancelBoot()
	if !o.bootstrapped {
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
	}
	// Verify write permission on every Connect (a grant can be revoked between reconnects), so a
	// permission reject fails Connect -> connection_up never registers -> the bridge fails visibly
	// instead of stalling on an endless WriteBatch NACK.
	if err := o.probeWritable(bootCtx); err != nil {
		return err
	}
	return nil
}

// probeWritable verifies the login role can INSERT into this contract's tables. Connect otherwise
// only proves the server is reachable and the schema exists -- and bootstrap runs as the owner, so
// it passes even when a different login role lacks INSERT. Without this a permission reject shows up
// only as an endless WriteBatch NACK and the bridge stalls "starting" instead of failing visibly.
// Read-only catalog lookup, so it is safe on every Connect and under concurrency.
func (o *historianOutput) probeWritable(ctx context.Context) error {
	valueTbl := "umh.value_" + o.contract
	attrTbl := "umh.attribute_" + o.contract
	var ok bool
	if err := o.pool.QueryRow(ctx,
		"SELECT has_table_privilege($1, 'INSERT') AND has_table_privilege($2, 'INSERT')",
		valueTbl, attrTbl).Scan(&ok); err != nil {
		return fmt.Errorf("write-permission check failed for %s / %s: %w", valueTbl, attrTbl, err)
	}
	if !ok {
		return fmt.Errorf("login role %q lacks INSERT on %s or %s; grant it before the bridge can write", o.username, valueTbl, attrTbl)
	}
	return nil
}

// warnPolicyDrift warns when the applied compression/retention policy differs from config. Policies
// are set once at first bootstrap, so editing compress_after/retention and restarting otherwise has
// no visible effect. Best-effort: introspection errors are swallowed so an unexpected catalog shape
// never fails Connect. Both hypertables get identical policies, so the value table is representative.
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

// WriteBatch is the output entry point benthos calls once per message batch. It works in two
// steps: first each message is turned into a Row in-process (parse, validate, build metadata),
// then the surviving rows are written to Postgres in a single transaction (writeBatchFast). A
// row Postgres can never accept ("poison") demotes the write to a row-by-row pass so the good
// rows still land. The return value is the benthos ACK/NACK signal: nil ACKs the batch (including
// a batch where every message was dropped); a returned error NACKs it for retry.
func (o *historianOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	o.mu.Lock()
	pool := o.pool
	o.mu.Unlock()
	if pool == nil {
		return service.ErrNotConnected
	}

	// Step 1: transform every message into a Row. Malformed messages are counted (by reason) and
	// skipped -- never nacked -- so one bad message can't stall the stream.
	view := o.dedup.NewBatch()
	rows := make([]*Row, 0, len(batch))
	churn := map[string]struct{}{} // high-churn metadata keys seen anywhere in this batch (see below)
	for _, msg := range batch {
		meta := map[string]string{}
		_ = msg.MetaWalk(func(k, v string) error { meta[k] = v; return nil })
		structured, err := msg.AsStructured()
		if err != nil {
			o.recordDrop("not_structured", meta["umh_topic"]) // payload is not decodable JSON
			continue
		}
		payload, ok := structured.(map[string]any)
		if !ok {
			o.recordDrop("not_object", meta["umh_topic"]) // payload is JSON but not a {value, timestamp_ms} object
			continue
		}
		// Transform validates the umh_topic/contract and the value+timestamp, and decides whether
		// this row also needs to write a metadata (attribute) row. A non-empty reason means drop.
		row, reason := Transform(payload, meta, o.contract, o.metadataKeysAll, o.metadataKeys, o.metadataExclude, view)
		if reason != DropNone {
			o.recordDrop(string(reason), meta["umh_topic"])
			continue
		}
		for _, k := range row.churnKeys { // union across the whole batch, not just the first row
			churn[k] = struct{}{}
		}
		// row.Truncated means the value was a string longer than maxTextRunes and got clipped to fit
		// value_text. That is silent data loss, so count every occurrence and warn once per process.
		if row.Truncated {
			o.truncated.Incr(1)
			if o.warnedTruncate.CompareAndSwap(false, true) {
				o.logger.Warnf("TimescaleDB historian: a value_text exceeded %d runes and was truncated; longer text is silently clipped. Route oversized payloads to a different tag or shorten upstream.", maxTextRunes)
			}
		}
		rows = append(rows, row)
	}
	// Warn (once per key) about metadata keys that change on nearly every message: they defeat the
	// attribute de-dup cache and make the attribute table grow per-message, so the operator likely
	// wants them out of metadata_keys.
	o.warnHighChurn(churn)
	if len(rows) == 0 {
		// A fully-dropped batch writes nothing while the connection stays up, so umh-core would
		// see a healthy bridge silently discarding data. Warn (umh-core surfaces this as
		// degraded); still return nil so one bad message never stalls the stream.
		if len(batch) > 0 {
			o.logger.Warnf("TimescaleDB historian: dropped all %d message(s) in the batch; nothing written. Check the source data and metadata configuration.", len(batch))
		}
		return nil
	}

	// resolved is the per-batch topic_id cache, shared across both write paths: writeBatchFast
	// seeds it, and on the poison fallback writeRowsIsolated reuses (and extends) it rather than
	// re-resolving topics the fast path already resolved.
	resolved := make(map[topicKey]int64)
	err := o.writeBatchFast(ctx, pool, rows, view, resolved)
	if err == nil {
		return nil
	}
	if errors.Is(err, errIntraBatchConflict) {
		// Two rows in the batch shared (topic_id, ts) with differing values. Re-run row-by-row so
		// the good rows land and the offending row is dropped with a signal.
		o.logger.Warnf("TimescaleDB historian: intra-batch (topic_id, ts) conflict, isolating good rows")
		return o.writeRowsIsolated(ctx, pool, rows, resolved)
	}
	switch classify(err) {
	case dispDropPoison:
		// A poison row failed the fast batch. Re-run row-by-row so the good rows land and only
		// the poison rows are dropped -- otherwise one bad value head-of-line-blocks the stream.
		o.logger.Warnf("TimescaleDB historian: poison row in batch, isolating good rows: %v", o.redact(err))
		return o.writeRowsIsolated(ctx, pool, rows, resolved)
	case dispRetryStanding:
		// Config/resource/unknown: never drop good data. NACK for retry, but loudly -- this will
		// not clear without an operator (e.g. missing table privilege, disk full).
		o.logger.Errorf("TimescaleDB historian: write blocked by a standing fault; holding the batch for retry (no data dropped): %v", o.redact(err))
		return err
	default: // dispRetryTransient: connection blip etc. -- benthos retries; stay quiet to avoid log spam
		return err
	}
}

// resolveTopic returns the topic_id for one row, read-first: an existing topic resolves via a
// lookup (no sequence burn); a genuine miss (new topic, or a datatype flip that misses the
// value_type-qualified lookup) falls to the guarded upsert. Errors wrap the underlying
// pgconn.PgError with %w so classify() can unwrap the SQLSTATE.
func (o *historianOutput) resolveTopic(ctx context.Context, pool *pgxpool.Pool, r *Row) (int64, error) {
	var id int64
	err := pool.QueryRow(ctx, topicLookupSQL, r.RawLocation, r.ContractName, r.VirtualPath, r.TagName, string(r.ValueType)).Scan(&id)
	switch {
	case err == nil:
		o.lookupHits.Add(1)
		return id, nil
	case errors.Is(err, pgx.ErrNoRows):
		o.lookupMisses.Add(1)
		if err = pool.QueryRow(ctx, topicResolveSQL, r.RawLocation, r.ContractName, r.VirtualPath, r.TagName, string(r.ValueType)).Scan(&id); err != nil {
			return 0, fmt.Errorf("topic resolve failed: %w", err)
		}
		return id, nil
	default:
		return 0, fmt.Errorf("topic lookup failed: %w", err)
	}
}

// topicKey identifies a distinct topic within a batch. value_type is part of the key because
// resolution is value_type-qualified: a datatype flip is a different key, so it misses any cached
// entry and falls through to the guarded upsert that rejects it as poison.
type topicKey struct {
	loc, contract, vpath, tag string
	vt                        ValueType
}

func topicKeyOf(r *Row) topicKey {
	return topicKey{r.RawLocation, r.ContractName, r.VirtualPath, r.TagName, r.ValueType}
}

// valueKey collapses exact-duplicate value rows within a batch (a harmless in-batch replay): rows
// sharing it are byte-identical, so only the first is sent to the batched insert. A same-(id, ts)
// row with a DIFFERENT value has a different key, survives, and trips the insert's 21000 -> poison
// path so the isolated fallback drops it.
type valueKey struct {
	id      int64
	ts      string
	num     float64
	hasNum  bool
	text    string
	hasText bool
}

func valueKeyOf(id int64, r *Row) valueKey {
	k := valueKey{id: id, ts: r.TS}
	if r.ValueNum != nil {
		k.num, k.hasNum = *r.ValueNum, true
	}
	if r.ValueText != nil {
		k.text, k.hasText = *r.ValueText, true
	}
	return k
}

// attrKey collapses exact-duplicate attribute rows within a batch, same rationale as valueKey.
type attrKey struct {
	id   int64
	ts   string
	attr string
}

// errIntraBatchConflict marks a batched insert that held the same (topic_id, ts) twice with
// differing values (Postgres 21000, "cannot affect row a second time"). WriteBatch treats it like
// poison and re-runs isolated, which attributes and drops just the offending row. Scoping it to
// this call site keeps a stray 21000 from any future query out of the drop path.
var errIntraBatchConflict = errors.New("intra-batch (topic_id, ts) conflict")

// writeBatchFast is the happy path: resolve every distinct topic, then write all values and
// attributes in one transaction. Returns the first error encountered (the caller classifies it).
// resolved is caller-owned so the isolated fallback can reuse the ids resolved here instead of
// re-resolving them.
func (o *historianOutput) writeBatchFast(ctx context.Context, pool *pgxpool.Pool, rows []*Row, view *BatchView, resolved map[topicKey]int64) error {
	// Phase 1: resolve each distinct topic to its topic_id (autocommit, one statement each).
	for _, r := range rows {
		k := topicKeyOf(r)
		if _, ok := resolved[k]; ok {
			continue
		}
		id, err := o.resolveTopic(ctx, pool, r)
		if err != nil {
			return err // a partial map is fine: the ids already in it are committed and reusable
		}
		resolved[k] = id
	}

	// Phase 2: write the whole batch with one batched value insert (and one attribute insert) in a
	// single transaction. Exact-duplicate (topic_id, ts, value) rows are collapsed first -- a
	// harmless in-batch replay -- so they don't trip the batched insert's 21000; a same-(topic_id,
	// ts) row with a DIFFERENT value survives, trips 21000, and is classified poison so the caller
	// re-runs isolated and drops just the offending row.
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	vIDs := make([]int64, 0, len(rows))
	vTS := make([]string, 0, len(rows))
	vNum := make([]*float64, 0, len(rows))
	vText := make([]*string, 0, len(rows))
	seenVal := make(map[valueKey]struct{}, len(rows))
	var aIDs []int64
	var aTS, aAttr []string
	seenAttr := map[attrKey]struct{}{}
	for _, r := range rows {
		id := resolved[topicKeyOf(r)]
		vk := valueKeyOf(id, r)
		if _, dup := seenVal[vk]; !dup {
			seenVal[vk] = struct{}{}
			vIDs = append(vIDs, id)
			vTS = append(vTS, r.TS)
			vNum = append(vNum, r.ValueNum)
			vText = append(vText, r.ValueText)
		}
		if r.EmitMeta {
			ak := attrKey{id: id, ts: r.TS, attr: r.MetadataJSON}
			if _, dup := seenAttr[ak]; !dup {
				seenAttr[ak] = struct{}{}
				aIDs = append(aIDs, id)
				aTS = append(aTS, r.TS)
				aAttr = append(aAttr, r.MetadataJSON)
			}
		}
	}

	if _, err := tx.Exec(ctx, valueBatchQueryFor(o.contract), vIDs, vTS, vNum, vText); err != nil {
		if pgSQLState(err) == "21000" { // two rows shared (topic_id, ts) with differing values
			return errIntraBatchConflict
		}
		o.logger.Errorf("TimescaleDB historian: value write failed for %d row(s): %v", len(vIDs), o.redact(err))
		return fmt.Errorf("value write failed: %w", err)
	}
	if len(aIDs) > 0 {
		if _, err := tx.Exec(ctx, attributeBatchQueryFor(o.contract), aIDs, aTS, aAttr); err != nil {
			if pgSQLState(err) == "21000" {
				return errIntraBatchConflict
			}
			o.logger.Errorf("TimescaleDB historian: attribute write failed for %d row(s): %v", len(aIDs), o.redact(err))
			return fmt.Errorf("attribute write failed: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	view.Commit() // promote dedup entries only after a successful commit
	o.dedupSize.Set(int64(o.dedup.Len()))
	o.valueRows.Incr(int64(len(vIDs)))
	o.attrRows.Incr(int64(len(aIDs)))
	return nil
}

// writeRowsIsolated is the slow path taken only after writeBatchFast fails with a poison error.
// It writes each row independently (autocommit) so good rows land and poison rows are dropped
// with a loud, attributable signal instead of head-of-line-blocking the batch. A transient or
// standing error returns immediately (NACK the whole batch); rows already written this pass are
// absorbed idempotently on retry. It deliberately does NOT promote the dedup view: after a
// poison batch each surviving tag re-emits its metadata once (a new attribute row at the next
// ts), which is cheap and correct, and avoids marking a row emitted when its value was dropped.
func (o *historianOutput) writeRowsIsolated(ctx context.Context, pool *pgxpool.Pool, rows []*Row, resolved map[topicKey]int64) error {
	vq := valueQueryFor(o.contract)
	aq := attributeQueryFor(o.contract)
	var written, attrs int
	drop := func(err error, phase string, r *Row) {
		o.poisoned.Incr(1, pgSQLState(err), phase)
		o.logger.Errorf("TimescaleDB historian: dropped poison row at %s for %s (sqlstate=%s): %v", phase, describeRow(r), pgSQLState(err), o.redact(err))
	}
	for _, r := range rows {
		// Reuse a topic_id the fast path (or an earlier row here) already resolved; only a genuinely
		// unresolved topic hits the DB. A failed resolve (e.g. a datatype flip) is dropped and never
		// cached.
		k := topicKeyOf(r)
		id, ok := resolved[k]
		if !ok {
			var err error
			id, err = o.resolveTopic(ctx, pool, r)
			if err != nil {
				if classify(err) == dispDropPoison {
					drop(err, "resolve", r)
					continue
				}
				o.valueRows.Incr(int64(written))
				o.attrRows.Incr(int64(attrs))
				return err
			}
			resolved[k] = id
		}
		if _, err := pool.Exec(ctx, vq, id, r.TS, r.ValueNum, r.ValueText); err != nil {
			if classify(err) == dispDropPoison {
				drop(err, "value", r)
				continue
			}
			o.valueRows.Incr(int64(written))
			o.attrRows.Incr(int64(attrs))
			return err
		}
		written++
		if r.EmitMeta {
			if _, err := pool.Exec(ctx, aq, id, r.TS, r.MetadataJSON); err != nil {
				if classify(err) == dispDropPoison {
					drop(err, "attribute", r)
					continue
				}
				o.valueRows.Incr(int64(written))
				o.attrRows.Incr(int64(attrs))
				return err
			}
			attrs++
		}
	}
	o.valueRows.Incr(int64(written))
	o.attrRows.Incr(int64(attrs))
	return nil // ACK: good rows committed, poison rows dropped-with-signal
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
