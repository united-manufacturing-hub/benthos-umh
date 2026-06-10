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

package uns_plugin

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gopkg.in/yaml.v3"
)

// parseRedpandaFragment unmarshals a rendered fragment and returns its
// input.redpanda mapping.
func parseRedpandaFragment(frag string) map[string]any {
	GinkgoHelper()
	var doc map[string]any
	Expect(yaml.Unmarshal([]byte(frag), &doc)).To(Succeed(), "fragment is not valid YAML:\n%s", frag)
	input, ok := doc["input"].(map[string]any)
	Expect(ok).To(BeTrue(), "fragment has no input mapping:\n%s", frag)
	rp, ok := input["redpanda"].(map[string]any)
	Expect(ok).To(BeTrue(), "fragment has no input.redpanda mapping:\n%s", frag)
	return rp
}

// buildInnerYAML is the same function newUnsBetaInput calls in production;
// these specs check the rendered fragment, with the OOM-tuned fetch limits
// from uns_input_config.go pinned as literals.
var _ = Describe("uns_beta fragment rendering", Label("uns_beta"), func() {
	It("renders the configured fields into the redpanda fragment", func() {
		conf, err := unsBetaConfigSpec().ParseYAML(`
broker_address: "broker1:9092, broker2:9092,"
consumer_group: "grp"
kafka_topic: "custom.messages"
`, nil)
		Expect(err).NotTo(HaveOccurred())
		frag, err := buildInnerYAML(conf)
		Expect(err).NotTo(HaveOccurred())
		rp := parseRedpandaFragment(frag)

		// csv split + TrimSpace + empty-entry drop: the trailing comma must not
		// produce an empty seed_brokers element.
		Expect(rp["seed_brokers"]).To(Equal([]any{"broker1:9092", "broker2:9092"}))
		Expect(rp["topics"]).To(Equal([]any{"custom.messages"}))
		Expect(rp["consumer_group"]).To(Equal("grp"))
		Expect(rp["start_offset"]).To(Equal("earliest"))

		// Literal pins of the OOM-tuned defaultFetch* constants in
		// uns_input_config.go ("Reduced from 100MB to prevent OOM kills"): an
		// accidental retune fails here; a deliberate one must edit these values.
		for field, want := range map[string]string{
			"fetch_max_bytes":           "10000000",
			"fetch_max_partition_bytes": "10000000",
			"fetch_min_bytes":           "1000000",
			"fetch_max_wait":            "1s",
			"commit_period":             "5s",
			"conn_idle_timeout":         "15m0s",
		} {
			Expect(rp[field]).To(Equal(want), "field %s", field)
		}
		Expect(rp["auto_replay_nacks"]).To(Equal(true))

		// The same parsed config must construct through the production entry point.
		_, err = newUnsBetaInput(conf, nil)
		Expect(err).NotTo(HaveOccurred())
	})

	// Pin, not a regression catch: buildInnerYAML validates the TRIMMED values
	// and must also feed the trimmed values into the fragment. A mutant that
	// validates strings.TrimSpace(v) without reassigning v would pass every
	// validation spec yet render padded values; this spec kills it.
	It("feeds the trimmed consumer_group and kafka_topic into the fragment", func() {
		conf, err := unsBetaConfigSpec().ParseYAML(`
broker_address: "localhost:9092"
consumer_group: " g "
kafka_topic: " custom.messages "
`, nil)
		Expect(err).NotTo(HaveOccurred())
		frag, err := buildInnerYAML(conf)
		Expect(err).NotTo(HaveOccurred())
		rp := parseRedpandaFragment(frag)
		Expect(rp["consumer_group"]).To(Equal("g"))
		Expect(rp["topics"]).To(Equal([]any{"custom.messages"}))
	})

	It("falls back to the default topic when kafka_topic is omitted", func() {
		conf, err := unsBetaConfigSpec().ParseYAML(`
broker_address: "localhost:9092"
consumer_group: "g"
`, nil)
		Expect(err).NotTo(HaveOccurred())
		frag, err := buildInnerYAML(conf)
		Expect(err).NotTo(HaveOccurred())
		rp := parseRedpandaFragment(frag)
		Expect(rp["topics"]).To(Equal([]any{defaultInputKafkaTopic}))
	})
})

var _ = Describe("uns_beta config validation", Label("uns_beta"), func() {
	// Each entry parses through unsBetaConfigSpec and constructs through
	// newUnsBetaInput, the production entry point, so the guard under test is
	// the one production hits. Failures inside the inner OwnedInput land in a
	// noop logger and would otherwise be invisible.
	DescribeTable("newUnsBetaInput rejects the config",
		func(yamlBody, wantErr string) {
			parsed, err := unsBetaConfigSpec().ParseYAML(yamlBody, nil)
			Expect(err).NotTo(HaveOccurred())
			_, err = newUnsBetaInput(parsed, nil)
			Expect(err).To(MatchError(wantErr))
		},
		Entry("empty broker_address",
			"broker_address: \"\"\nconsumer_group: \"g\"",
			"broker_address must contain at least one broker"),
		// Splitting must happen before validation: " , " is non-empty as a raw
		// string but yields zero brokers, which would render seed_brokers: []
		// and fail invisibly inside the noop-logger inner input.
		Entry("separators-only broker_address",
			"broker_address: \" , \"\nconsumer_group: \"g\"",
			"broker_address must contain at least one broker"),
		// An empty group silently disables offset commits (full-topic replay
		// on every restart) — the ENG-5094 failure class.
		Entry("empty consumer_group",
			"broker_address: \"localhost:9092\"\nconsumer_group: \"\"",
			"consumer_group must not be empty"),
		Entry("whitespace-only consumer_group",
			"broker_address: \"localhost:9092\"\nconsumer_group: \"  \"",
			"consumer_group must not be empty"),
		// An explicit empty kafka_topic overrides the Benthos default
		// (defaults apply only when the field is absent) and would render
		// topics: [""], a silently dead input.
		Entry("explicit empty kafka_topic",
			"broker_address: \"localhost:9092\"\nconsumer_group: \"g\"\nkafka_topic: \"\"",
			"kafka_topic must not be empty"),
		Entry("whitespace-only kafka_topic",
			"broker_address: \"localhost:9092\"\nconsumer_group: \"g\"\nkafka_topic: \"  \"",
			"kafka_topic must not be empty"),
		// ':' would switch the inner redpanda input into explicit
		// topic:partition mode, which it rejects in combination with a
		// consumer group — but only via a field lint that the nested
		// ParseYAML path never runs. The legal-name whitelist subsumes it.
		Entry("kafka_topic with partition syntax",
			"broker_address: \"localhost:9092\"\nconsumer_group: \"g\"\nkafka_topic: \"umh.messages:0\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("kafka_topic with comma",
			"broker_address: \"localhost:9092\"\nconsumer_group: \"g\"\nkafka_topic: \"a,b\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		// Kafka itself rejects names outside [a-zA-Z0-9._-], longer than 249
		// chars, or equal to "." / ".." — but only after the input has already
		// connected, inside the noop-logger inner input where the rejection is
		// invisible.
		Entry("kafka_topic with a space",
			"broker_address: \"localhost:9092\"\nconsumer_group: \"g\"\nkafka_topic: \"umh messages\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("kafka_topic with '#'",
			"broker_address: \"localhost:9092\"\nconsumer_group: \"g\"\nkafka_topic: \"umh#messages\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("kafka_topic of 250 chars",
			"broker_address: \"localhost:9092\"\nconsumer_group: \"g\"\nkafka_topic: \""+strings.Repeat("a", 250)+"\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("kafka_topic of exactly '.'",
			"broker_address: \"localhost:9092\"\nconsumer_group: \"g\"\nkafka_topic: \".\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
	)
})
