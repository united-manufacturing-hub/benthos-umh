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
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// uns_beta_single is a Go-registered input that consumes a single Kafka topic
// and filters records by key. This spec exercises its config validation by
// parsing against the plugin's OWN ConfigSpec and constructing through the Go
// constructor newUnsBetaSingle directly — uns_beta_single has no template
// layer, so the bare constructor error is available for exact MatchError (no
// StreamBuilder *componentErr wrapping). This mirrors the sibling
// uns_beta_reader spec's direct-call pattern.
var _ = Describe("uns_beta_single config validation", Label("uns_beta"), func() {
	// parseAndConstruct parses body against the spec and constructs the input,
	// returning the first error encountered: a parse-time required-field
	// rejection or a constructor-time scalar rejection. Both are bare errors
	// (no framework wrapping), so callers can assert with MatchError.
	parseAndConstruct := func(body string) (service.BatchInput, error) {
		parsed, err := unsBetaSingleConfigSpec().ParseYAML(body, nil)
		if err != nil {
			return nil, err
		}
		return newUnsBetaSingle(parsed, nil)
	}

	DescribeTable("newUnsBetaSingle rejects invalid config",
		func(body string, wantErr interface{}) {
			_, err := parseAndConstruct(body)
			Expect(err).To(MatchError(wantErr))
		},
		Entry("empty consumer_group is rejected",
			"consumer_group: \"\"\numh_topics: [\".*\"]\n",
			"consumer_group must not be empty"),
		Entry("kafka_topic '.' is rejected",
			"consumer_group: \"g\"\numh_topics: [\".*\"]\nkafka_topic: \".\"\n",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("kafka_topic '..' is rejected",
			"consumer_group: \"g\"\numh_topics: [\".*\"]\nkafka_topic: \"..\"\n",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("illegal kafka_topic (contains colon) is rejected",
			"consumer_group: \"g\"\numh_topics: [\".*\"]\nkafka_topic: \"a:b\"\n",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("missing umh_topics is rejected",
			"consumer_group: \"g\"\n",
			"umh_topics must not be empty"),
		Entry("empty umh_topics list is rejected",
			"consumer_group: \"g\"\numh_topics: []\n",
			"umh_topics must not be empty"),
		Entry("empty umh_topics element is rejected (an empty pattern wraps to (?:) matching every key)",
			"consumer_group: \"g\"\numh_topics: [\".*\", \"\"]\n",
			ContainSubstring("umh_topics pattern at index 1 must not be empty or whitespace-only")),
		Entry("invalid umh_topics regex is rejected",
			"consumer_group: \"g\"\numh_topics: [\"[\"]\n",
			ContainSubstring("invalid umh_topics pattern at index 0:")),
	)

	It("rejects a missing required consumer_group at parse time", func() {
		// consumer_group is a required field (no Default); the framework
		// rejects its absence before the constructor runs. Framework error
		// strings are not part of this plugin's contract, so match by
		// substring.
		_, err := parseAndConstruct("umh_topics: [\".*\"]\n")
		Expect(err).To(MatchError(ContainSubstring("consumer_group")))
	})

	It("accepts a valid config (constructor passes Go-level validation and returns a non-nil input)", func() {
		in, err := parseAndConstruct("consumer_group: \"g\"\numh_topics: [\".*\"]\n")
		Expect(err).NotTo(HaveOccurred())
		Expect(in).NotTo(BeNil())
	})

	It("returns a not-implemented error from ReadBatch so a valid config is distinguishable from a validation rejection", func() {
		_, _, err := (&unsBetaSingleInput{}).ReadBatch(context.Background())
		Expect(err).To(MatchError(ContainSubstring("uns_beta_single: not implemented")))
	})
})
