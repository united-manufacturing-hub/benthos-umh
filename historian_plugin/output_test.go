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
data_contract: pump
`
		parsed, err := tsh.HistorianConfig().ParseYAML(yaml, service.NewEnvironment())
		Expect(err).NotTo(HaveOccurred())
		h, err := tsh.NewHistorianForConfig(parsed)
		Expect(err).NotTo(HaveOccurred())
		Expect(h.BuildDSN()).To(Equal("postgres://umh_owner:secret@db.example.com:5432/umh?sslmode=require"))
	})

	It("rejects an invalid data_contract at construction", func() {
		yaml := "host: h\npassword: p\ndata_contract: Pump\n"
		parsed, err := tsh.HistorianConfig().ParseYAML(yaml, service.NewEnvironment())
		Expect(err).NotTo(HaveOccurred())
		_, err = tsh.NewHistorianForConfig(parsed)
		Expect(err).To(HaveOccurred())
	})

	It("rejects a sub-second compress_after (would render INTERVAL '0 seconds')", func() {
		yaml := "host: h\npassword: p\ndata_contract: pump\ncompress_after: 100ms\n"
		parsed, err := tsh.HistorianConfig().ParseYAML(yaml, service.NewEnvironment())
		Expect(err).NotTo(HaveOccurred())
		_, err = tsh.NewHistorianForConfig(parsed)
		Expect(err).To(MatchError(ContainSubstring("compress_after must be at least 1s")))
	})

	It("rejects a sub-second retention when set", func() {
		yaml := "host: h\npassword: p\ndata_contract: pump\nretention: 0s\n"
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
})
