//go:build connect_patched

// Copyright 2025 UMH Systems GmbH
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

// The uns_beta_single translation must reproduce uns_beta's 12 pinned
// redpanda values exactly. This spec drives the YAML construction through a
// pure, broker-free helper (redpandaConfigYAML) so the rendered values can be
// inspected directly against the same Go fetch constants uns_beta's render
// test re-derives, plus the key_pattern join and connect-side schema
// acceptance. A second case renders uns_beta's actual template for the same
// inputs and asserts field-by-field parity with the translation, so a template
// edit that diverges from the translation fails here rather than only in the
// sibling uns_beta render test.

package uns_plugin

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	// The forwarder shim re-exports connect's redpanda ConfigFields so the
	// translated YAML can be parsed against connect's real schema without a
	// live broker. Present only after `make patch-connect`.
	"github.com/redpanda-data/connect/v4/public/components/kafka/redpandaforward"
)

var _ = Describe("uns_beta_single translation parity", Label("uns_beta"), func() {
	It("reproduces all 12 pinned redpanda values, the key_pattern join, and a connect-accepted config from the pure YAML builder", func() {
		seedBrokers := []string{"broker1:9092", "broker2:9092"}
		kafkaTopic := "umh.messages"
		consumerGroup := "parity-group"
		umhTopics := []string{"umh.v1.acme.temp", "umh.v1.acme.pressure"}

		// Pure, broker-free access to the translated redpanda config YAML.
		// buildUnsBetaSingleInner must delegate to this helper so the
		// translation is inspectable without dialing a broker.
		yaml := redpandaConfigYAML(seedBrokers, kafkaTopic, consumerGroup, umhTopics)

		// "connect accepts the parsed config": parse against the forwarder's
		// re-exported redpanda field set (connect's real schema), not a local
		// copy. A YAML that connect's schema rejects fails here.
		spec := service.NewConfigSpec().Fields(redpandaforward.ConfigFields()...)
		conf, err := spec.ParseYAML(yaml, nil)
		Expect(err).NotTo(HaveOccurred(), "translated YAML must parse against connect's redpanda field set:\n%s", yaml)

		// Lean-field pass-throughs.
		gotBrokers, err := conf.FieldStringList("seed_brokers")
		Expect(err).NotTo(HaveOccurred(), "seed_brokers missing")
		Expect(gotBrokers).To(Equal(seedBrokers), "seed_brokers must mirror the lean broker list")
		gotTopics, err := conf.FieldStringList("topics")
		Expect(err).NotTo(HaveOccurred(), "topics missing")
		Expect(gotTopics).To(Equal([]string{kafkaTopic}), "topics must mirror the lean kafka_topic")
		gotGroup, err := conf.FieldString("consumer_group")
		Expect(err).NotTo(HaveOccurred(), "consumer_group missing")
		Expect(gotGroup).To(Equal(consumerGroup), "consumer_group must mirror the lean field")

		// String-valued pins: start_offset is a literal pin; the three
		// fetch byte fields are re-derived from the Go fetch constants in
		// uns_input_config.go (the same constants uns_beta's render test
		// uses), so a constant retune updates production and expectation in
		// lockstep — these pins guard format, not value. They are emitted
		// AS STRINGS (upstream NewStringField contract).
		Expect(conf.FieldString("start_offset")).To(Equal("earliest"), "start_offset must be pinned to earliest")
		Expect(conf.FieldString("fetch_max_bytes")).To(Equal(strconv.FormatFloat(defaultFetchMaxBytes, 'f', -1, 64)), "fetch_max_bytes must mirror defaultFetchMaxBytes")
		Expect(conf.FieldString("fetch_max_partition_bytes")).To(Equal(strconv.FormatFloat(defaultFetchMaxPartitionBytes, 'f', -1, 64)), "fetch_max_partition_bytes must mirror defaultFetchMaxPartitionBytes")
		Expect(conf.FieldString("fetch_min_bytes")).To(Equal(strconv.FormatFloat(defaultFetchMinBytes, 'f', -1, 64)), "fetch_min_bytes must mirror defaultFetchMinBytes")

		// Duration-valued pins: fetch_max_wait and conn_idle_timeout re-derive
		// from the Go fetch/conn constants (guarding format, not value, since
		// both operands derive from the same constant). commit_period is EMITTED
		// by the translation as the literal "5s" — pinned defensively, matching
		// uns_beta's template pin and the auto_replay_nacks treatment, so a
		// future connect default change cannot silently alter the commit cadence.
		Expect(conf.FieldDuration("fetch_max_wait")).To(Equal(defaultFetchMaxWaitTime), "fetch_max_wait must mirror defaultFetchMaxWaitTime")
		Expect(conf.FieldDuration("conn_idle_timeout")).To(Equal(defaultConnIdleTimeout), "conn_idle_timeout must mirror defaultConnIdleTimeout")
		Expect(conf.FieldDuration("commit_period")).To(Equal(5*time.Second), "commit_period must be pinned to 5s by the translation")

		// auto_replay_nacks pinned true (the ENG-5094 NACK-replay guarantee),
		// defensively, so a future connect default change cannot silently
		// disable redelivery.
		Expect(conf.FieldBool("auto_replay_nacks")).To(BeTrue(), "auto_replay_nacks must be pinned true")

		// key_pattern must equal the Go-side betaKeyFilter combined regex for
		// the same umh_topics — (?:p1)|(?:p2)|... — reusing the same wrap, not a
		// raw "|"-join. The wrap prevents a pattern containing '|' from
		// mis-grouping. Cross-check against newBetaKeyFilter's compiled source
		// (filter.re.String()) rather than hand-rebuilding the join, so a drift
		// between this helper's wrap convention and newBetaKeyFilter's fails
		// here — mirroring the sibling uns_beta render test
		// (uns_beta_input_test.go: rp["key_pattern"] == filter.re.String()).
		filter, err := newBetaKeyFilter(umhTopics)
		Expect(err).NotTo(HaveOccurred(), "test umh_topics must compile into a betaKeyFilter")
		Expect(conf.FieldString("key_pattern")).To(Equal(filter.re.String()),
			"key_pattern must equal the Go-side betaKeyFilter combined regex for umh_topics — connect's pre-filter must not drift from the Go keep set")
	})

	// Direct parity with uns_beta: render uns_beta's actual template for the
	// same logical inputs and assert the redpanda block is field-by-field
	// identical to the translation. The constants-based case above proves the
	// translation is self-consistent with the shared Go constants; this case
	// proves it matches uns_beta's rendered template, so a template edit that
	// diverges from the translation (different fetch bytes, a dropped
	// commit_period pin, a different key_pattern join) fails here rather than
	// only in the sibling uns_beta render test. renderUnsBetaReader/redpandaOf
	// are the same helpers uns_beta's own render test uses.
	It("matches uns_beta's rendered redpanda block field-for-field for the same inputs", func() {
		seedBrokers := []string{"broker1:9092", "broker2:9092"}
		kafkaTopic := "umh.messages"
		consumerGroup := "parity-group"
		umhTopics := []string{"umh.v1.acme.temp", "umh.v1.acme.pressure"}

		// uns_beta's lean surface takes broker_address as a CSV string; the
		// template splits/trims/drops empties into seed_brokers. Render it for
		// the same logical brokers/group/topic/topics.
		reader := renderUnsBetaReader(fmt.Sprintf(`
uns_beta:
  broker_address: %q
  consumer_group: %q
  kafka_topic: %q
  umh_topics: [%s]
`, strings.Join(seedBrokers, ", "), consumerGroup, kafkaTopic, `"`+strings.Join(umhTopics, `", "`)+`"`))
		rp := redpandaOf(reader)

		// The translation's parsed config for the same inputs.
		spec := service.NewConfigSpec().Fields(redpandaforward.ConfigFields()...)
		conf, err := spec.ParseYAML(redpandaConfigYAML(seedBrokers, kafkaTopic, consumerGroup, umhTopics), nil)
		Expect(err).NotTo(HaveOccurred(), "translation YAML must parse")

		// List fields: the template renders []any; the parsed conf yields
		// []string. Coerce and compare both sides to the lean inputs.
		Expect(stringSlice(rp["seed_brokers"])).To(Equal(seedBrokers), "seed_brokers must match uns_beta's render")
		gotBrokers, _ := conf.FieldStringList("seed_brokers")
		Expect(gotBrokers).To(Equal(seedBrokers))
		Expect(stringSlice(rp["topics"])).To(Equal([]string{kafkaTopic}), "topics must match uns_beta's render")
		gotTopics, _ := conf.FieldStringList("topics")
		Expect(gotTopics).To(Equal([]string{kafkaTopic}))

		// Scalar string fields.
		Expect(rp["consumer_group"]).To(Equal(consumerGroup))
		Expect(conf.FieldString("consumer_group")).To(Equal(consumerGroup))
		Expect(rp["start_offset"]).To(Equal("earliest"))
		Expect(conf.FieldString("start_offset")).To(Equal("earliest"))
		Expect(rp["fetch_max_bytes"]).To(Equal(strconv.FormatFloat(defaultFetchMaxBytes, 'f', -1, 64)))
		Expect(conf.FieldString("fetch_max_bytes")).To(Equal(rp["fetch_max_bytes"]))
		Expect(rp["fetch_max_partition_bytes"]).To(Equal(strconv.FormatFloat(defaultFetchMaxPartitionBytes, 'f', -1, 64)))
		Expect(conf.FieldString("fetch_max_partition_bytes")).To(Equal(rp["fetch_max_partition_bytes"]))
		Expect(rp["fetch_min_bytes"]).To(Equal(strconv.FormatFloat(defaultFetchMinBytes, 'f', -1, 64)))
		Expect(conf.FieldString("fetch_min_bytes")).To(Equal(rp["fetch_min_bytes"]))

		// Duration fields: the template renders the literal string; the parsed
		// conf yields a time.Duration. Compare via the duration's String() form.
		Expect(rp["fetch_max_wait"]).To(Equal(defaultFetchMaxWaitTime.String()))
		fmw, _ := conf.FieldDuration("fetch_max_wait")
		Expect(fmw.String()).To(Equal(rp["fetch_max_wait"]))
		Expect(rp["conn_idle_timeout"]).To(Equal(defaultConnIdleTimeout.String()))
		cit, _ := conf.FieldDuration("conn_idle_timeout")
		Expect(cit.String()).To(Equal(rp["conn_idle_timeout"]))
		Expect(rp["commit_period"]).To(Equal("5s"))
		Expect(conf.FieldDuration("commit_period")).To(Equal(5 * time.Second))

		// Bool + key_pattern. Cross-check the template's key_pattern against the
		// Go betaKeyFilter compiled source (the canonical join), and the
		// translation against the template.
		filter, err := newBetaKeyFilter(umhTopics)
		Expect(err).NotTo(HaveOccurred(), "test umh_topics must compile into a betaKeyFilter")
		Expect(rp["auto_replay_nacks"]).To(BeTrue())
		Expect(conf.FieldBool("auto_replay_nacks")).To(BeTrue())
		Expect(rp["key_pattern"]).To(Equal(filter.re.String()))
		Expect(conf.FieldString("key_pattern")).To(Equal(rp["key_pattern"]))
	})
})

// stringSlice coerces a rendered YAML list ([]any) to []string for comparison
// against a parsed conf's FieldStringList. Panics on non-string elements, which
// is a test-only path for known string lists.
func stringSlice(v any) []string {
	in, ok := v.([]any)
	Expect(ok).To(BeTrue(), "expected a YAML list, got %T", v)
	out := make([]string, len(in))
	for i, e := range in {
		s, ok := e.(string)
		Expect(ok).To(BeTrue(), "expected string element, got %T", e)
		out[i] = s
	}
	return out
}
