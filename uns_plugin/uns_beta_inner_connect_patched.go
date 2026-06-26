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

	// The forwarder shim is created by patches/connect-redpanda-forward.patch
	// and is present only after `make patch-connect` (hence the connect_patched
	// build tag). It re-exports connect's redpanda input constructor so this
	// package can build a redpanda reader without importing connect's
	// internal/impl/kafka directly.
	"github.com/redpanda-data/connect/v4/public/components/kafka/redpandaforward"
)

// redpandaConfigYAML is the pure, broker-free translation of the lean 4-field
// uns_beta surface into a redpanda input config YAML. It pins all 12
// values that uns_beta's render test re-derives so the translation is
// inspectable without dialing a broker: seed_brokers, topics, consumer_group,
// start_offset (earliest), commit_period ("5s"), the three fetch byte fields
// (re-derived from the Go fetch constants in uns_input_config.go as strings,
// matching upstream's NewStringField contract), fetch_max_wait and
// conn_idle_timeout (re-derived from the Go fetch/conn constants),
// auto_replay_nacks (pinned true — the ENG-5094 NACK-replay guarantee), and
// key_pattern (the newBetaKeyFilter join of umh_topics). commit_period is
// pinned to "5s" defensively, matching uns_beta's template pin and the
// auto_replay_nacks treatment, so a future connect default change cannot
// silently alter uns_beta's commit cadence.
func redpandaConfigYAML(seedBrokers []string, kafkaTopic, consumerGroup string, umhTopics []string) string {
	// Build the key_pattern alternation the same way the template and
	// newBetaKeyFilter do: (?:p1)|(?:p2)|... . newBetaKeyFilter already
	// validated each pattern and the combined alternation at construction, so
	// this join cannot fail at RE2 limits here.
	wrapped := make([]string, len(umhTopics))
	for i, p := range umhTopics {
		wrapped[i] = fmt.Sprintf("(?:%s)", p)
	}
	keyPattern := strings.Join(wrapped, "|")

	var brokersYAML strings.Builder
	for _, b := range seedBrokers {
		brokersYAML.WriteString(fmt.Sprintf("  - %q\n", b))
	}

	// The fetch byte fields are NewStringField upstream, so emit them as
	// strings re-derived from the same Go constants uns_beta's render test
	// uses; a constant retune updates production and expectation in lockstep,
	// so these pins guard format, not value. fetch_max_wait and
	// conn_idle_timeout are NewDurationField, re-derived from the Go
	// constants.
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

// buildUnsBetaInner translates the lean 4-field surface into a redpanda
// input config and builds it via the forwarder shim. The translation is
// produced by redpandaConfigYAML (the pure, broker-free helper) so the 12
// pinned values are inspectable without dialing a broker.
func buildUnsBetaInner(res *service.Resources, seedBrokers []string, kafkaTopic, consumerGroup string, umhTopics []string) (service.BatchInput, error) {
	yaml := redpandaConfigYAML(seedBrokers, kafkaTopic, consumerGroup, umhTopics)

	spec := service.NewConfigSpec().Fields(redpandaforward.ConfigFields()...)
	conf, err := spec.ParseYAML(yaml, nil)
	if err != nil {
		return nil, fmt.Errorf("uns_beta: failed to parse translated redpanda config: %w", err)
	}
	return redpandaforward.NewRedpandaInput(conf, res)
}
