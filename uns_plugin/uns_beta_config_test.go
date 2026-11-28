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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("uns_beta config validation", Label("uns_beta"), func() {
	// parseAndConstruct returns the parse error or the constructor error.
	parseAndConstruct := func(body string) (service.BatchInput, error) {
		parsed, err := unsBetaConfigSpec().ParseYAML(body, nil)
		if err != nil {
			return nil, err
		}
		return newUnsBeta(parsed, service.MockResources())
	}

	DescribeTable("newUnsBeta rejects invalid config",
		func(body string, wantErr types.GomegaMatcher) {
			_, err := parseAndConstruct(body)
			Expect(err).To(MatchError(wantErr))
		},
		Entry("empty consumer_group is rejected",
			"consumer_group: \"\"\numh_topics: [\".*\"]\n",
			Equal("consumer_group must not be empty: set a name unique to this consumer, for example analytics_reader")),
		Entry("empty broker_address is rejected",
			"broker_address: \" , \"\nconsumer_group: \"g\"\numh_topics: [\".*\"]\n",
			Equal("broker_address must not be empty: set host:port, for example localhost:9092")),
		Entry("kafka_topic '.' is rejected",
			"consumer_group: \"g\"\numh_topics: [\".*\"]\nkafka_topic: \".\"\n",
			Equal(`kafka_topic "." is not a legal Kafka topic name: use 1-249 letters, digits, '.', '_' or '-'`)),
		Entry("kafka_topic '..' is rejected",
			"consumer_group: \"g\"\numh_topics: [\".*\"]\nkafka_topic: \"..\"\n",
			Equal(`kafka_topic ".." is not a legal Kafka topic name: use 1-249 letters, digits, '.', '_' or '-'`)),
		Entry("illegal kafka_topic (contains colon) is rejected",
			"consumer_group: \"g\"\numh_topics: [\".*\"]\nkafka_topic: \"a:b\"\n",
			Equal(`kafka_topic "a:b" is not a legal Kafka topic name: use 1-249 letters, digits, '.', '_' or '-'`)),
		Entry("missing umh_topics is rejected",
			"consumer_group: \"g\"\n",
			Equal(`umh_topics must not be empty: to read the whole namespace, set umh_topics: [".*"]`)),
		Entry("empty umh_topics list is rejected",
			"consumer_group: \"g\"\numh_topics: []\n",
			Equal(`umh_topics must not be empty: to read the whole namespace, set umh_topics: [".*"]`)),
		Entry("empty umh_topics element is rejected (an empty pattern wraps to (?:) matching every key)",
			"consumer_group: \"g\"\numh_topics: [\".*\", \"\"]\n",
			ContainSubstring("umh_topics pattern at index 1 must not be empty or whitespace-only")),
		Entry("invalid umh_topics regex is rejected",
			"consumer_group: \"g\"\numh_topics: [\"[\"]\n",
			ContainSubstring("invalid umh_topics pattern at index 0:")),
	)

	It("rejects a missing required consumer_group at parse time", func() {
		// The framework's error text is not ours to pin, so match a substring.
		_, err := parseAndConstruct("umh_topics: [\".*\"]\n")
		Expect(err).To(MatchError(ContainSubstring("consumer_group")))
	})

	It("accepts a valid config", func() {
		in, err := parseAndConstruct("consumer_group: \"g\"\numh_topics: [\".*\"]\n")
		Expect(err).NotTo(HaveOccurred())
		Expect(in).NotTo(BeNil())
	})
})
