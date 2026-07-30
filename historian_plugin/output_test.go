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

package historian_plugin_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	tsh "github.com/united-manufacturing-hub/benthos-umh/historian_plugin"
)

var _ = Describe("config", func() {
	It("parses a minimal config and builds a DSN", func() {
		yaml := `
host: db.example.com
port: 5432
password: secret
data_contract_name: pump
`
		parsed, err := tsh.HistorianConfig().ParseYAML(yaml, service.NewEnvironment())
		Expect(err).NotTo(HaveOccurred())
		h, err := tsh.NewHistorianForConfig(parsed)
		Expect(err).NotTo(HaveOccurred())
		Expect(h.BuildDSN()).To(Equal("postgres://umh_owner:secret@db.example.com:5432/umh?sslmode=require"))
	})

	It("defaults allow_unvalidated_data to false so unversioned data is not stored by accident", func() {
		yaml := "host: h\npassword: p\ndata_contract_name: pump\n"
		parsed, err := tsh.HistorianConfig().ParseYAML(yaml, service.NewEnvironment())
		Expect(err).NotTo(HaveOccurred())
		h, err := tsh.NewHistorianForConfig(parsed)
		Expect(err).NotTo(HaveOccurred())
		Expect(h.AllowUnvalidated()).To(BeFalse())
	})

	It("parses allow_unvalidated_data through to the output", func() {
		yaml := "host: h\npassword: p\ndata_contract_name: pump\nallow_unvalidated_data: true\n"
		parsed, err := tsh.HistorianConfig().ParseYAML(yaml, service.NewEnvironment())
		Expect(err).NotTo(HaveOccurred())
		h, err := tsh.NewHistorianForConfig(parsed)
		Expect(err).NotTo(HaveOccurred())
		Expect(h.AllowUnvalidated()).To(BeTrue())
	})

	It("rejects an invalid data_contract_name at construction", func() {
		yaml := "host: h\npassword: p\ndata_contract_name: Pump\n"
		parsed, err := tsh.HistorianConfig().ParseYAML(yaml, service.NewEnvironment())
		Expect(err).NotTo(HaveOccurred())
		_, err = tsh.NewHistorianForConfig(parsed)
		Expect(err).To(HaveOccurred())
	})

	It("rejects a sub-second compress_after (would render INTERVAL '0 seconds')", func() {
		yaml := "host: h\npassword: p\ndata_contract_name: pump\ncompress_after: 100ms\n"
		parsed, err := tsh.HistorianConfig().ParseYAML(yaml, service.NewEnvironment())
		Expect(err).NotTo(HaveOccurred())
		_, err = tsh.NewHistorianForConfig(parsed)
		Expect(err).To(MatchError(ContainSubstring("compress_after must be at least 1s")))
	})

	It("rejects a sub-second retention when set", func() {
		yaml := "host: h\npassword: p\ndata_contract_name: pump\nretention: 0s\n"
		parsed, err := tsh.HistorianConfig().ParseYAML(yaml, service.NewEnvironment())
		Expect(err).NotTo(HaveOccurred())
		_, err = tsh.NewHistorianForConfig(parsed)
		Expect(err).To(MatchError(ContainSubstring("retention must be at least 1s")))
	})

	It("embeds the contract and the conflict-RAISE invariants in the bootstrap", func() {
		got := tsh.BootstrapSQLForTest("pump")
		Expect(got).To(ContainSubstring("CREATE SCHEMA IF NOT EXISTS umh"))
		Expect(got).To(ContainSubstring("umh.value_pump"))
		Expect(got).To(ContainSubstring("umh.attribute_pump"))
		Expect(got).NotTo(ContainSubstring("CONTRACT_SLOT"))
		Expect(got).To(ContainSubstring("raise_pk_conflict"))
		Expect(got).To(ContainSubstring("tag_value_type_guard"))
		Expect(got).To(ContainSubstring("pg_advisory_xact_lock"))
		Expect(strings.ToUpper(got)).NotTo(ContainSubstring("ON CONFLICT (TOPIC_ID, TS) DO NOTHING"))
	})

	It("wraps the raw location in to_ltree_path on the write path", func() {
		Expect(tsh.BootstrapSQLForTest("pump")).NotTo(ContainSubstring("$1::ltree"))
	})

	It("sets fillfactor on the update-churned dimension tables only, not the hypertables", func() {
		got := tsh.BootstrapSQLForTest("pump")
		// Exactly the three dimension tables (location, tag, topic) get fillfactor for HOT
		// upserts; the two insert-mostly hypertables must not (count would be 5 otherwise).
		Expect(strings.Count(got, "WITH (fillfactor = 90)")).To(Equal(3))
	})

	It("records the baseline as version 1 via a forward-only migration ledger", func() {
		got := tsh.BootstrapSQLForTest("pump")
		Expect(got).To(ContainSubstring("CREATE TABLE IF NOT EXISTS umh.schema_migrations"))
		// The greenfield baseline is the initial schema version, gated so it applies once.
		Expect(got).To(ContainSubstring("IF NOT EXISTS (SELECT 1 FROM umh.schema_migrations WHERE version = 1)"))
		Expect(got).To(ContainSubstring("INSERT INTO umh.schema_migrations (version) VALUES (1)"))
		Expect(tsh.SchemaVersionForTest()).To(Equal(1))
		// The migrations section runs inside the bootstrap transaction, before COMMIT.
		Expect(strings.Index(got, "INSERT INTO umh.schema_migrations (version) VALUES (1)")).
			To(BeNumerically("<", strings.LastIndex(got, "COMMIT;")))
		Expect(got).NotTo(ContainSubstring("MIGRATIONS_SLOT")) // placeholder substituted
	})

	It("gates compression/retention setup on the ledger so it runs once at first bootstrap", func() {
		got := tsh.BootstrapSQLForTest("pump")
		// The policy DO block is wrapped in the version-1 ledger gate, so it never re-runs on
		// restart: the ALTER can't hit the compressed-chunks error and retention is not stripped.
		Expect(got).To(ContainSubstring("ALTER TABLE umh.value_pump SET ("))
		Expect(got).To(ContainSubstring("add_compression_policy('umh.value_pump'"))
		// Runs only on empty tables, so no remove_* churn on restart.
		Expect(got).NotTo(ContainSubstring("remove_retention_policy"))
		Expect(got).NotTo(ContainSubstring("remove_compression_policy"))
	})
})

var _ = Describe("policy drift warnings", func() {
	const (
		sevenDays  = int64(7 * 24 * 60 * 60) // the compress_after default (168h)
		thirtyDays = int64(30 * 24 * 60 * 60)
		oneDay     = int64(24 * 60 * 60)
	)
	sec := func(v int64) *int64 { return &v } // nil = "not set" (config unset / no policy applied)

	DescribeTable("flags drift between the configured and the applied policies",
		func(compressWant int64, appliedComp *int64, retentionWant *int64, appliedRet *int64, wantWarns int) {
			Expect(tsh.PolicyDriftWarningsForTest(compressWant, appliedComp, retentionWant, appliedRet)).To(HaveLen(wantWarns))
		},
		Entry("quiet: compression not readable yet (not bootstrapped)", sevenDays, nil, sec(thirtyDays), nil, 0),
		Entry("quiet: compression matches, no retention configured", sevenDays, sec(sevenDays), nil, nil, 0),
		Entry("quiet: compression and retention both match", sevenDays, sec(sevenDays), sec(thirtyDays), sec(thirtyDays), 0),
		Entry("warn: compress_after changed after bootstrap", oneDay, sec(sevenDays), nil, nil, 1),
		Entry("warn: retention configured but not applied", sevenDays, sec(sevenDays), sec(thirtyDays), nil, 1),
		Entry("warn: applied retention differs from config", sevenDays, sec(sevenDays), sec(thirtyDays), sec(oneDay), 1),
		Entry("warn: retention removed from config but still applied", sevenDays, sec(sevenDays), nil, sec(thirtyDays), 1),
	)
})
