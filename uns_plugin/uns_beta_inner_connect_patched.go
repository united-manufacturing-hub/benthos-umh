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

package uns_plugin

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Added by patches/connect-redpanda-forward.patch (make patch-connect).
	"github.com/redpanda-data/connect/v4/public/components/kafka/redpandaforward"
)

// errUnsBetaUnavailable is nil: this binary carries the connect patches.
var errUnsBetaUnavailable error

// redpandaConfigYAML translates the uns_beta fields into a redpanda input
// config. auto_replay_nacks is pinned true for at-least-once delivery
// (ENG-5094), and commit_period and the fetch settings are pinned so a connect
// default change cannot alter them.
func redpandaConfigYAML(seedBrokers []string, kafkaTopic string, consumerGroup string, umhTopics []string) string {
	// Same join as newBetaKeyFilter, which already validated it.
	wrapped := make([]string, len(umhTopics))
	for i, p := range umhTopics {
		wrapped[i] = fmt.Sprintf("(?:%s)", p)
	}
	keyPattern := strings.Join(wrapped, "|")

	var brokersYAML strings.Builder
	for _, b := range seedBrokers {
		brokersYAML.WriteString(fmt.Sprintf("  - %q\n", b))
	}

	// The fetch byte fields are string fields upstream.
	return fmt.Sprintf(`seed_brokers:
%stopics:
  - %q
consumer_group: %q
start_offset: earliest
commit_period: "5s"
auto_replay_nacks: true
key_pattern: %q
fetch_max_bytes: %q
fetch_max_partition_bytes: %q
fetch_min_bytes: %q
fetch_max_wait: %s
conn_idle_timeout: %s
`,
		brokersYAML.String(),
		kafkaTopic,
		consumerGroup,
		keyPattern,
		strconv.FormatFloat(defaultFetchMaxBytes, 'f', -1, 64),
		strconv.FormatFloat(defaultFetchMaxPartitionBytes, 'f', -1, 64),
		strconv.FormatFloat(defaultFetchMinBytes, 'f', -1, 64),
		defaultFetchMaxWaitTime.String(),
		defaultConnIdleTimeout.String(),
	)
}

// buildUnsBetaInner builds connect's redpanda input from the translated
// config through the forwarder shim.
func buildUnsBetaInner(res *service.Resources, seedBrokers []string, kafkaTopic string, consumerGroup string, umhTopics []string) (service.BatchInput, error) {
	yaml := redpandaConfigYAML(seedBrokers, kafkaTopic, consumerGroup, umhTopics)

	spec := service.NewConfigSpec().Fields(redpandaforward.ConfigFields()...)
	conf, err := spec.ParseYAML(yaml, nil)
	if err != nil {
		return nil, fmt.Errorf("uns_beta: failed to parse translated redpanda config: %w", err)
	}
	return redpandaforward.NewRedpandaInput(conf, res)
}
