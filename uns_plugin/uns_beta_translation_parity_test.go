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

// The translated redpanda config must parse against connect's own schema and
// carry every pinned value.

package uns_plugin

import (
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/public/components/kafka/redpandaforward"
)

var _ = Describe("uns_beta translation to the redpanda input", Label("uns_beta"), func() {
	It("produces a config connect accepts, with every pinned value", func() {
		// The quote and newline would break a hand-built YAML document.
		consumerGroup := "group\"\nauto_replay_nacks: false"
		keyPattern := `(?:umh\.v1\.acme\.temp)|(?:umh\.v1\.acme\.pressure)`

		yaml, err := redpandaConfigYAML([]string{"broker1:9092", "broker2:9092"}, "umh.messages", consumerGroup, keyPattern)
		Expect(err).NotTo(HaveOccurred())
		conf, err := service.NewConfigSpec().Fields(redpandaforward.ConfigFields()...).ParseYAML(yaml, nil)
		Expect(err).NotTo(HaveOccurred(), "connect rejected the translated config:\n%s", yaml)

		Expect(conf.FieldStringList("seed_brokers")).To(Equal([]string{"broker1:9092", "broker2:9092"}))
		Expect(conf.FieldStringList("topics")).To(Equal([]string{"umh.messages"}))
		Expect(conf.FieldString("consumer_group")).To(Equal(consumerGroup))
		Expect(conf.FieldString("key_pattern")).To(Equal(keyPattern))

		Expect(conf.FieldString("start_offset")).To(Equal("earliest"))
		Expect(conf.FieldDuration("commit_period")).To(Equal(5 * time.Second))
		Expect(conf.FieldBool("auto_replay_nacks")).To(BeTrue())

		// The fetch byte fields are string fields upstream.
		Expect(conf.FieldString("fetch_max_bytes")).To(Equal(strconv.FormatFloat(defaultFetchMaxBytes, 'f', -1, 64)))
		Expect(conf.FieldString("fetch_max_partition_bytes")).To(Equal(strconv.FormatFloat(defaultFetchMaxPartitionBytes, 'f', -1, 64)))
		Expect(conf.FieldString("fetch_min_bytes")).To(Equal(strconv.FormatFloat(defaultFetchMinBytes, 'f', -1, 64)))
		Expect(conf.FieldDuration("fetch_max_wait")).To(Equal(defaultFetchMaxWaitTime))
		Expect(conf.FieldDuration("conn_idle_timeout")).To(Equal(defaultConnIdleTimeout))
	})
})
