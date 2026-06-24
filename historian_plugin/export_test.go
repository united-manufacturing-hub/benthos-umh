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
