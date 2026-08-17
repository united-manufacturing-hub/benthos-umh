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

	lru "github.com/hashicorp/golang-lru/v2"
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
		Field(service.NewStringField("sslrootcert").Description("CA cert path, as seen by the benthos process.").Default("").Advanced()).
		Field(service.NewStringField("sslcert").Description("Client cert path.").Default("").Advanced()).
		Field(service.NewStringField("sslkey").Description("Client key path.").Default("").Advanced()).
		Field(service.NewBoolField("allow_datatype_changes").Description("Let a tag change datatype instead of dropping the offending rows. Its stored value_type keeps the first type seen and the tag then holds both numeric and text values, so read it with coalesce(value_num::text, value_text). Applies to every data contract, versioned or not.").Default(false).Examples(true, false).Advanced()).
		Field(service.NewBoolField("metadata_keys_all").Description("Store all metadata keys except blacklists.").Default(true).Examples(true, false).Advanced()).
		Field(service.NewStringListField("metadata_keys").Description("Allowlist when metadata_keys_all=false.").Default([]any{}).Advanced()).
		Field(service.NewStringListField("metadata_keys_exclude").Description("Blacklist applied only when metadata_keys_all=true: drop these metadata keys on top of the built-in structural/high-churn exclusions. Each entry is an exact key name or a trailing-* prefix (e.g. \"opcua_*\"). Ignored in allowlist mode.").Default([]any{}).Examples([]any{"serialNumber"}, []any{"opcua_*", "spb_*"}).Advanced()).
		Field(service.NewStringField("compress_after").Description("Compress chunks older than this, as a Go duration; use hours (e.g. \"168h\") -- days are not a valid unit. Applied once at first database bootstrap. Per contract.").Default("168h").Advanced()).
		Field(service.NewStringField("retention").Description("Drop chunks older than this, as a Go duration; use hours (e.g. \"720h\") -- days are not a valid unit. Empty = keep forever. Applied once at first database bootstrap.").Default("").Advanced()).
		Field(service.NewBatchPolicyField("batching").Advanced()).
		Field(service.NewIntField("max_in_flight").Description("Max parallel batches in flight.").Default(8).Advanced()).
		Field(service.NewStringField("write_timeout").Description("Per-batch write timeout as a Go duration (e.g. \"30s\"). Empty or \"0s\" = no timeout (a write that hangs on a lock or half-open connection blocks until the context is cancelled). When set, a timed-out batch is held for retry (NACK), never dropped. Set it above the largest expected batch commit time.").Default("").Advanced())
}

type historianOutput struct {
	host, database, username, password    string
	port                                  int
	sslmode, sslrootcert, sslcert, sslkey string
	contract                              string
	metadataKeysAll                       bool
	allowDatatypeChanges                  bool
	metadataKeys                          []string
	metadataExclude                       *MetaExcluder
	compressAfter, retention              time.Duration
	retentionSet                          bool
	maxInFlight                           int
	writeTimeout                          time.Duration // 0 => unbounded (per-batch write deadline)
	dsnOverride                           string        // set by tests; empty => build from fields

	logger    *service.Logger
	dropped   *service.MetricCounter // labeled by drop reason
	valueRows *service.MetricCounter // value rows upserted (after commit)
	attrRows  *service.MetricCounter // attribute rows upserted (after commit)
	dedupSize *service.MetricGauge   // current dedup-cache entry count
	poisoned  *service.MetricCounter // rows dropped as poison (labels: sqlstate, phase)
	truncated *service.MetricCounter // value_text values clipped to maxTextRunes
	dedup     *DedupCache

	topicCache     *lru.Cache[topicKey, int64] // process-wide topic_id memo (topic_id is immutable per key)
	topicCacheSize *service.MetricGauge        // current topic-id cache entry count

	lookupHits   atomic.Int64 // topic resolves served by the read-first lookup (no sequence burn)
	lookupMisses atomic.Int64 // topic resolves that fell through to the guarded upsert (new topic or datatype flip)

	mu           sync.Mutex
	pool         *pgxpool.Pool
	bootstrapped bool

	churnMu     sync.Mutex
	warnedChurn map[string]struct{} // high-churn metadata keys already warned about

	warnedTruncate atomic.Bool // warn-once guard for truncation

	everStored atomic.Bool // set once the first value row is stored; lock-free reads gate the hot path

	mismatchLogMu   sync.Mutex // guards lastMismatchLog against concurrent WriteBatch calls (max_in_flight)
	lastMismatchLog time.Time
}

func newHistorianOutput(conf *service.ParsedConfig, mgr *service.Resources) (*historianOutput, error) {
	o := &historianOutput{
		logger:         mgr.Logger(),
		dropped:        mgr.Metrics().NewCounter("messages_dropped", "reason"),
		valueRows:      mgr.Metrics().NewCounter("historian_value_rows_written"),
		attrRows:       mgr.Metrics().NewCounter("historian_attribute_rows_written"),
		dedupSize:      mgr.Metrics().NewGauge("historian_dedup_cache_size"),
		poisoned:       mgr.Metrics().NewCounter("historian_rows_poisoned", "sqlstate", "phase"),
		truncated:      mgr.Metrics().NewCounter("historian_values_truncated"),
		dedup:          NewDedupCache(),
		topicCache:     newTopicCache(),
		topicCacheSize: mgr.Metrics().NewGauge("historian_topic_cache_size"),
		warnedChurn:    map[string]struct{}{},
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
	if o.allowDatatypeChanges, err = conf.FieldBool("allow_datatype_changes"); err != nil {
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
	wtStr, err := conf.FieldString("write_timeout")
	if err != nil {
		return nil, err
	}
	if wtStr != "" {
		if o.writeTimeout, err = time.ParseDuration(wtStr); err != nil {
			return nil, fmt.Errorf("write_timeout: %w", err)
		}
		if o.writeTimeout < 0 {
			return nil, fmt.Errorf("write_timeout must not be negative, got %q", wtStr)
		}
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

// Connect opens the pool (once), verifies the server version, bootstraps the schema idempotently on
// first call, and checks the login role can INSERT. It is the benthos output Connect hook; a
// returned error keeps the output from registering as connected.
//
// The login role is assumed to be able to run the (idempotent) bootstrap DDL -- the owner
// (umh_owner) or an equivalent. Either way a role that cannot write fails Connect visibly, just at
// different steps: on a first connect a role lacking the DDL rights fails at the bootstrap; on a
// reconnect (bootstrapped already true in this process) the DDL is skipped and probeWritable is the
// front-line check -- which is also where a grant revoked between reconnects is caught.
func (o *historianOutput) Connect(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Drop DB-derived caches on every (re)connect. The topic_id cache and the dedup fingerprint
	// cache are only valid for the database currently behind the pool; a reconnect can land on a
	// restored, recreated, or truncated database that has reassigned topic_ids. topic_id is
	// deliberately not an FK on the value/attribute hypertables (see sql.go), so a stale cached id
	// would silently misroute writes with nothing to catch it. Starting cold costs one read-first
	// lookup per topic and removes that hazard entirely.
	o.topicCache.Purge()
	o.dedup.Purge()
	o.topicCacheSize.Set(0)
	o.dedupSize.Set(0)

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
	// permission reject fails Connect -> the output never registers as connected -> it fails
	// visibly instead of stalling on an endless WriteBatch NACK.
	if err := o.probeWritable(bootCtx); err != nil {
		return err
	}
	return nil
}

// probeWritable verifies the login role can INSERT into this contract's tables. Connect otherwise
// only proves the server is reachable and the schema exists -- and bootstrap runs as the owner, so
// it passes even when a different login role lacks INSERT. Without this a permission reject shows up
// only as an endless WriteBatch NACK and the output stalls instead of failing visibly.
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
		return fmt.Errorf("login role %q lacks INSERT on %s or %s; grant it before this output can write", o.username, valueTbl, attrTbl)
	}
	return nil
}

// readAppliedPolicies reads the compression/retention intervals (in seconds) TimescaleDB currently
// has applied for this contract, as nil-able pointers (nil = no such policy scheduled). Both
// hypertables get identical policies, so the value table is representative. It returns an error only
// if the compression lookup itself fails (an unexpected catalog shape); the retention lookup is
// best-effort. This is the I/O read half of warnPolicyDrift, split out so the drift check reads as
// read -> compare (pure policyDriftWarnings) -> log.
func (o *historianOutput) readAppliedPolicies(ctx context.Context) (*int64, *int64, error) {
	table := "value_" + o.contract
	var appliedComp, appliedRet *int64
	if err := o.pool.QueryRow(ctx, policyIntervalSQL("policy_compression", "compress_after"), table).Scan(&appliedComp); err != nil {
		return nil, nil, err
	}
	_ = o.pool.QueryRow(ctx, policyIntervalSQL("policy_retention", "drop_after"), table).Scan(&appliedRet)
	return appliedComp, appliedRet, nil
}

// warnPolicyDrift warns when the applied compression/retention policy differs from config. Policies
// are set once at first bootstrap, so editing compress_after/retention and restarting otherwise has
// no visible effect. Best-effort: introspection errors are swallowed so an unexpected catalog shape
// never fails Connect.
func (o *historianOutput) warnPolicyDrift(ctx context.Context) {
	appliedComp, appliedRet, err := o.readAppliedPolicies(ctx)
	if err != nil {
		return
	}
	var retentionWant *int64
	if o.retentionSet {
		v := int64(o.retention.Seconds())
		retentionWant = &v
	}
	for _, w := range policyDriftWarnings(int64(o.compressAfter.Seconds()), appliedComp, retentionWant, appliedRet) {
		o.logger.Warnf("TimescaleDB historian: %s. Policies are applied once at first bootstrap and not re-applied on restart, so a config change alone does not update them. Change them on the database directly with the TimescaleDB policy functions (remove_/add_compression_policy, remove_/add_retention_policy) on umh.value_%s and umh.attribute_%s, then set the same value in this output's config to silence this warning.", w, o.contract, o.contract)
	}
}

func (o *historianOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	o.mu.Lock()
	pool := o.pool
	o.mu.Unlock()
	if pool == nil {
		return service.ErrNotConnected
	}

	// Bound the write when configured, so a batch that hangs (lock wait, half-open connection)
	// eventually frees its in-flight slot. A deadline surfaces as a no-SQLSTATE error ->
	// dispRetryTransient -> NACK, so the batch is held for retry, never dropped.
	if o.writeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.writeTimeout)
		defer cancel()
	}

	view := o.dedup.NewBatch()
	rows := make([]*Row, 0, len(batch))
	churn := map[string]struct{}{} // high-churn metadata keys seen anywhere in this batch (see below)
	drops := map[DropReason]dropSummary{}
	sawConfiguredContract := false
	for _, msg := range batch {
		meta := map[string]string{}
		_ = msg.MetaWalk(func(k, v string) error { meta[k] = v; return nil })
		structured, err := msg.AsStructured()
		if err != nil {
			o.noteDrop(drops, DropNotStructured, meta["umh_topic"])
			continue
		}
		payload, ok := structured.(map[string]any)
		if !ok {
			o.noteDrop(drops, DropNotObject, meta["umh_topic"])
			continue
		}
		// Transform validates the umh_topic/contract and the value+timestamp, and decides whether
		// this row also needs to write a metadata (attribute) row. A non-empty reason means drop.
		row, drop := Transform(payload, meta, o.contract, o.metadataKeysAll, o.metadataKeys, o.metadataExclude, view)
		// A message that got past the contract check proves the configured contract is published, which
		// is what picks the remedy in noteContractMismatch. Deriving this from the surviving rows
		// instead would lose the proof whenever the payload is also bad, and send the operator to
		// data_contract_name when the real fix is narrowing umh_topics.
		if drop != DropInvalidTopic && drop != DropContractMismatch {
			sawConfiguredContract = true
		}
		if drop != DropNone {
			o.noteDrop(drops, drop, meta["umh_topic"])
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
	o.warnHighChurnMetadata(churn)
	if mismatch := drops[DropContractMismatch]; mismatch.count > 0 {
		o.noteContractMismatch(time.Now(), len(batch), mismatch.count, sawConfiguredContract)
		return errors.New(nackMessage(o.contract, len(batch), mismatch.count))
	}
	o.reportDrops(len(batch), drops)
	if len(rows) == 0 {
		return nil
	}

	// resolved is the per-batch topic_id cache, shared across both write paths: writeBatchFast
	// seeds it, and on the poison fallback writeRowsIsolated reuses (and extends) it rather than
	// re-resolving topics the fast path already resolved.
	resolved := make(map[topicKey]int64)
	err := o.writeBatchFast(ctx, pool, rows, view, resolved)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errIntraBatchConflict):
		o.logger.Warnf("TimescaleDB historian: intra-batch (topic_id, ts) conflict, isolating good rows")
	case classify(err) == dispDropPoison:
		o.logger.Warnf("TimescaleDB historian: poison row in batch, isolating good rows: %v", o.redact(err))
	case classify(err) == dispRetryStanding:
		// Config/resource/unknown: never drop good data. NACK for retry, but loudly -- this will
		// not clear without an operator (e.g. missing table privilege, disk full).
		o.logger.Errorf("TimescaleDB historian: write blocked by a standing fault; this requires operator intervention in the database and will NOT clear on its own. Holding the batch for retry (no data dropped); it resumes automatically once the cause is fixed in the database (e.g. grant the missing table privilege, free disk space): %v", o.redact(err))
		return err
	default: // dispRetryTransient: connection blip etc. -- benthos retries; stay quiet to avoid log spam
		return err
	}
	return o.writeRowsIsolated(ctx, pool, rows, resolved)
}

// resolveTopic returns the topic_id for one row, read-first: an existing topic resolves via a
// lookup (no sequence burn); a genuine miss (new topic, or a datatype flip that misses the
// value_type-qualified lookup) falls to the guarded upsert. Errors wrap the underlying
// pgconn.PgError with %w so classify() can unwrap the SQLSTATE.
func (o *historianOutput) resolveTopic(ctx context.Context, pool *pgxpool.Pool, r *Row) (int64, error) {
	var id int64
	var err error
	resolve := topicResolveSQL
	if o.allowDatatypeChanges {
		resolve = topicResolveKeepTypeSQL
		err = pool.QueryRow(ctx, topicLookupAnyTypeSQL, r.RawLocation, r.ContractName, r.VirtualPath, r.TagName).Scan(&id)
	} else {
		err = pool.QueryRow(ctx, topicLookupSQL, r.RawLocation, r.ContractName, r.VirtualPath, r.TagName, string(r.ValueType)).Scan(&id)
	}
	switch {
	case err == nil:
		o.lookupHits.Add(1)
		return id, nil
	case errors.Is(err, pgx.ErrNoRows):
		o.lookupMisses.Add(1)
		if err = pool.QueryRow(ctx, resolve, r.RawLocation, r.ContractName, r.VirtualPath, r.TagName, string(r.ValueType)).Scan(&id); err != nil {
			return 0, fmt.Errorf("topic resolve failed: %w", err)
		}
		return id, nil
	default:
		return 0, fmt.Errorf("topic lookup failed: %w", err)
	}
}

// topicCacheCap bounds the process-wide topic_id cache. Eviction is safe: a missed entry costs one
// read-first DB lookup (no sequence burn), exactly like a cold start.
const topicCacheCap = 100_000

func newTopicCache() *lru.Cache[topicKey, int64] {
	c, _ := lru.New[topicKey, int64](topicCacheCap) // err only on cap <= 0
	return c
}

// resolveTopicCached returns the topic_id for a row, consulting the process-wide cache before the
// DB. topic_id is immutable for a given topicKey, so a cached id is always valid. Failed resolves
// are never cached. Out-of-band deletion of a topic row is unsupported (the schema is append-only),
// so a stale entry cannot arise in normal operation.
func (o *historianOutput) resolveTopicCached(ctx context.Context, pool *pgxpool.Pool, r *Row) (int64, error) {
	k := topicKeyOf(r)
	if id, ok := o.topicCache.Get(k); ok {
		return id, nil
	}
	id, err := o.resolveTopic(ctx, pool, r)
	if err != nil {
		return 0, err
	}
	o.topicCache.Add(k, id)
	return id, nil
}

// topicKey identifies a distinct topic within a batch. value_type is part of the key because
// resolution is value_type-qualified on a validated contract, so a datatype flip is a different key
// and misses any cached entry.
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
	// Phase 1: resolve each distinct topic to its topic_id. A process-wide cache serves repeat
	// topics with no DB round-trip; only a cache miss issues an autocommit lookup/upsert.
	for _, r := range rows {
		k := topicKeyOf(r)
		if _, ok := resolved[k]; ok {
			continue
		}
		id, err := o.resolveTopicCached(ctx, pool, r)
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
	o.topicCacheSize.Set(int64(o.topicCache.Len()))
	o.valueRows.Incr(int64(len(vIDs)))
	o.attrRows.Incr(int64(len(aIDs)))
	if len(vIDs) > 0 {
		o.noteStored()
	}
	return nil
}

// writeRowsIsolated is the slow path taken only after writeBatchFast fails with a poison error.
// It writes each row independently (autocommit) so good rows land and poison rows are dropped
// with a loud, attributable signal instead of head-of-line-blocking the batch. A transient or
// standing error returns immediately (NACK the whole batch); rows already written this pass are
// absorbed idempotently on retry. Value/attribute row-count metrics are incremented only on the
// ACK path: a NACK'd batch re-runs and is counted by the fast path on the successful retry, so
// counting partial progress here would double-count. It deliberately does NOT promote the dedup view: after a
// poison batch each surviving tag re-emits its metadata once (a new attribute row at the next
// ts), which is cheap and correct, and avoids marking a row emitted when its value was dropped.
func (o *historianOutput) writeRowsIsolated(ctx context.Context, pool *pgxpool.Pool, rows []*Row, resolved map[topicKey]int64) error {
	vq := valueQueryFor(o.contract)
	aq := attributeQueryFor(o.contract)
	var written, attrs int
	dropped := func(err error, phase string, r *Row) bool {
		if classify(err) != dispDropPoison {
			return false
		}
		sqlstate := pgSQLState(err)
		o.poisoned.Incr(1, sqlstate, phase)
		o.logger.Errorf("TimescaleDB historian: dropped poison row at %s for %s (sqlstate=%s): %v%s",
			phase, describeRow(r), sqlstate, o.redact(err), datatypeFlipHint(phase, sqlstate))
		return true
	}
	for _, r := range rows {
		// Reuse a topic_id the fast path (or an earlier row here) already resolved; only a genuinely
		// unresolved topic hits the DB. A failed resolve (e.g. a datatype flip) is dropped and never
		// cached.
		k := topicKeyOf(r)
		id, ok := resolved[k]
		if !ok {
			var err error
			id, err = o.resolveTopicCached(ctx, pool, r)
			if err != nil {
				if dropped(err, phaseResolve, r) {
					continue
				}
				return err // NACK: don't count; the successful retry re-counts via the fast path
			}
			resolved[k] = id
		}
		if _, err := pool.Exec(ctx, vq, id, r.TS, r.ValueNum, r.ValueText); err != nil {
			if dropped(err, phaseValue, r) {
				continue
			}
			return err
		}
		written++
		o.noteStored() // record now: a later non-poison error can return before the tail
		if r.EmitMeta {
			if _, err := pool.Exec(ctx, aq, id, r.TS, r.MetadataJSON); err != nil {
				if dropped(err, phaseAttribute, r) {
					continue
				}
				return err
			}
			attrs++
		}
	}
	o.topicCacheSize.Set(int64(o.topicCache.Len()))
	o.valueRows.Incr(int64(written))
	o.attrRows.Incr(int64(attrs))
	return nil // ACK: good rows committed, poison rows dropped-with-signal
}

func (o *historianOutput) noteStored() {
	if o.everStored.Load() { // hot path: a plain load once the first row is in
		return
	}
	if o.everStored.CompareAndSwap(false, true) {
		o.logger.Infof("TimescaleDB historian: first message stored for data contract _%s.", o.contract)
	}
}

// describeRow identifies a row for an attributable write-failure log: which tag, at which ts.
func describeRow(r *Row) string {
	return fmt.Sprintf("contract=%q location=%q virtual_path=%q tag=%q ts=%v",
		r.ContractName, r.RawLocation, r.VirtualPath, r.TagName, r.TS)
}

type dropSummary struct {
	example string
	count   int
}

func (o *historianOutput) noteDrop(drops map[DropReason]dropSummary, drop DropReason, topic string) {
	o.dropped.Incr(1, string(drop))
	s := drops[drop]
	s.count++
	if s.example == "" {
		s.example = topic
	}
	drops[drop] = s
}

func (o *historianOutput) reportDrops(total int, drops map[DropReason]dropSummary) {
	if len(drops) == 0 {
		return
	}
	reasons := make([]string, 0, len(drops))
	for reason := range drops {
		reasons = append(reasons, string(reason))
	}
	sort.Strings(reasons)
	for _, reason := range reasons {
		d := drops[DropReason(reason)]
		line := fmt.Sprintf("TimescaleDB historian: dropped %d of %d message(s) (reason=%s, example umh_topic=%q)%s",
			d.count, total, reason, d.example, dropHint(DropReason(reason)))
		if DropReason(reason) == DropServerVirtualPath {
			o.logger.Debugf("%s", line)
			continue
		}
		o.logger.Errorf("%s", line)
	}
}

// warnHighChurnMetadata warns once per distinct high-churn metadata key (re-firing when a new one
// appears). A high-churn key is a metadata field whose value changes on nearly every message (a
// timestamp, sequence number, status code); storing it defeats attribute de-duplication and grows
// the attribute table per-message. This is about metadata keys, not topics or write failures.
func (o *historianOutput) warnHighChurnMetadata(keys map[string]struct{}) {
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

// Close releases the connection pool. It is the benthos output Close hook.
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
