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
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"

	// The forwarder shim is created by patches/connect-redpanda-forward.patch
	// and is present only after `make patch-connect` (hence the connect_patched
	// build tag). It re-exports connect's redpanda input constructor so this
	// package can build a redpanda reader without importing connect's
	// internal/impl/kafka directly.
	"github.com/redpanda-data/connect/v4/public/components/kafka/redpandaforward"
)

// buildUnsBetaSingleInner translates the lean 4-field surface into a redpanda
// input config and builds it via the forwarder shim. The translation mirrors
// the uns_beta template's mapping: seed_brokers from broker_address, topics
// from kafka_topic, consumer_group, start_offset pinned earliest so a fresh
// group reads from the beginning, auto_replay_nacks pinned true (the
// ENG-5094 NACK-replay guarantee), and key_pattern set to the umh_topics
// alternation so connect's pre-filter omits non-matching records before
// message construction. The remaining fetch/conn values (R2's 12-value pin)
// are left at redpanda's defaults — R1b only needs delivery to work.
func buildUnsBetaSingleInner(res *service.Resources, seedBrokers []string, kafkaTopic, consumerGroup string, umhTopics []string) (service.BatchInput, error) {
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
	yaml := fmt.Sprintf(`seed_brokers:
%stopics:
  - %q
consumer_group: %q
start_offset: earliest
auto_replay_nacks: true
key_pattern: %q
`, brokersYAML.String(), kafkaTopic, consumerGroup, keyPattern)

	spec := service.NewConfigSpec().Fields(redpandaforward.ConfigFields()...)
	conf, err := spec.ParseYAML(yaml, nil)
	if err != nil {
		return nil, fmt.Errorf("uns_beta_single: failed to parse translated redpanda config: %w", err)
	}
	return redpandaforward.NewRedpandaInput(conf, res)
}
