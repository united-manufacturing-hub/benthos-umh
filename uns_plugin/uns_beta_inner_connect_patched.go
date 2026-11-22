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

	"github.com/redpanda-data/benthos/v4/public/service"
	"gopkg.in/yaml.v3"

	// Added by patches/connect-redpanda-forward.patch (make patch-connect).
	"github.com/redpanda-data/connect/v4/public/components/kafka/redpandaforward"
)

// redpandaConfigYAML translates the uns_beta fields into a redpanda input
// config. auto_replay_nacks is pinned true for at-least-once delivery, and
// commit_period and the fetch settings are pinned so a connect default change
// cannot alter them.
func redpandaConfigYAML(seedBrokers []string, kafkaTopic string, consumerGroup string, keyPattern string) (string, error) {
	out, err := yaml.Marshal(map[string]any{
		"seed_brokers":      seedBrokers,
		"topics":            []string{kafkaTopic},
		"consumer_group":    consumerGroup,
		"start_offset":      "earliest",
		"commit_period":     "5s",
		"auto_replay_nacks": true,
		"key_pattern":       keyPattern,
		// The fetch byte fields are string fields upstream.
		"fetch_max_bytes":           strconv.FormatFloat(defaultFetchMaxBytes, 'f', -1, 64),
		"fetch_max_partition_bytes": strconv.FormatFloat(defaultFetchMaxPartitionBytes, 'f', -1, 64),
		"fetch_min_bytes":           strconv.FormatFloat(defaultFetchMinBytes, 'f', -1, 64),
		"fetch_max_wait":            defaultFetchMaxWaitTime.String(),
		"conn_idle_timeout":         defaultConnIdleTimeout.String(),
	})
	return string(out), err
}

// buildUnsBetaInner builds connect's redpanda input from the translated
// config. It does not dial the broker; that happens on Connect.
func buildUnsBetaInner(res *service.Resources, seedBrokers []string, kafkaTopic string, consumerGroup string, keyPattern string) (service.BatchInput, error) {
	conf, err := redpandaConfigYAML(seedBrokers, kafkaTopic, consumerGroup, keyPattern)
	if err != nil {
		return nil, fmt.Errorf("uns_beta: translating the config for the redpanda input: %w", err)
	}
	parsed, err := service.NewConfigSpec().Fields(redpandaforward.ConfigFields()...).ParseYAML(conf, nil)
	if err != nil {
		return nil, fmt.Errorf("uns_beta: the redpanda input rejected the translated config: %w", err)
	}
	return redpandaforward.NewRedpandaInput(parsed, res)
}
