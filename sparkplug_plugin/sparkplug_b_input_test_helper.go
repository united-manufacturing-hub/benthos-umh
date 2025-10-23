//go:build !integration

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

// Test helper functions for sparkplug_b_input unit tests
// Provides minimal mock setup for testing internal methods without full MQTT infrastructure

package sparkplug_plugin

import (
	"github.com/redpanda-data/benthos/v4/public/service"
	sparkplugb "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

// SparkplugInputTestWrapper wraps the internal sparkplugInput for external testing
type SparkplugInputTestWrapper struct {
	input *sparkplugInput
}

// NewSparkplugInputForTesting creates a minimal sparkplugInput wrapped for unit testing
// This allows testing methods like createSplitMessages without full MQTT setup
func NewSparkplugInputForTesting() *SparkplugInputTestWrapper {
	// Create a no-op logger for tests
	logger := service.MockResources().Logger()

	input := &sparkplugInput{
		config: Config{
			// Minimal config - just enough to construct
			Role: RoleSecondaryPassive,
		},
		logger:           logger,
		nodeStates:       make(map[string]*nodeState),
		legacyAliasCache: make(map[string]map[uint64]string),
		aliasCache:       NewAliasCache(),
		topicParser:      NewTopicParser(),
		typeConverter:    NewTypeConverter(),
		messageProcessor: NewMessageProcessor(logger),
	}

	return &SparkplugInputTestWrapper{input: input}
}

// CreateSplitMessages is an exported wrapper for testing the private createSplitMessages method
func (w *SparkplugInputTestWrapper) CreateSplitMessages(payload *sparkplugb.Payload, msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	return w.input.createSplitMessages(payload, msgType, deviceKey, topicInfo, originalTopic)
}
