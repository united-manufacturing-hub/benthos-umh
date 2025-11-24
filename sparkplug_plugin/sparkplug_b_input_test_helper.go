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
	"sync"
	"time"

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

// NewSparkplugInputForTestingWithRole creates a sparkplugInput with specified role
// Used for testing role-specific behavior like rebirth requests
func NewSparkplugInputForTestingWithRole(role Role) *SparkplugInputTestWrapper {
	logger := service.MockResources().Logger()

	input := &sparkplugInput{
		config: Config{
			Role: role,
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

// GetStateMu returns a pointer to the stateMu lock for lock testing
func (w *SparkplugInputTestWrapper) GetStateMu() *sync.RWMutex {
	return &w.input.stateMu
}

// CreateSplitMessages is an exported wrapper for testing the private createSplitMessages method
func (w *SparkplugInputTestWrapper) CreateSplitMessages(payload *sparkplugb.Payload, msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	return w.input.createSplitMessages(payload, msgType, deviceKey, topicInfo, originalTopic)
}

// ProcessBirthMessage is an exported wrapper for testing the private processBirthMessage method
func (w *SparkplugInputTestWrapper) ProcessBirthMessage(deviceKey, msgType string, payload *sparkplugb.Payload) {
	w.input.processBirthMessage(deviceKey, msgType, payload)
}

// ProcessDataMessage is an exported wrapper for testing the private processDataMessage method
func (w *SparkplugInputTestWrapper) ProcessDataMessage(deviceKey, msgType string, payload *sparkplugb.Payload) {
	w.input.processDataMessage(deviceKey, msgType, payload)
}

// NodeStateInfo represents the public view of a node state for testing
type NodeStateInfo struct {
	LastSeq  uint8
	IsOnline bool
}

// GetNodeState is an exported wrapper for accessing node state in tests
func (w *SparkplugInputTestWrapper) GetNodeState(deviceKey string) *NodeStateInfo {
	w.input.stateMu.RLock()
	defer w.input.stateMu.RUnlock()

	state, exists := w.input.nodeStates[deviceKey]
	if !exists {
		return nil
	}

	return &NodeStateInfo{
		LastSeq:  state.LastSeq,
		IsOnline: state.IsOnline,
	}
}

// ProcessDeathMessage is an exported wrapper for testing the private processDeathMessage method
func (w *SparkplugInputTestWrapper) ProcessDeathMessage(deviceKey, msgType string, payload *sparkplugb.Payload) {
	w.input.processDeathMessage(deviceKey, msgType, payload)
}

// SetNodeBdSeq allows tests to set the bdSeq for a device (simulating NBIRTH)
func (w *SparkplugInputTestWrapper) SetNodeBdSeq(deviceKey string, bdSeq uint64) {
	w.input.stateMu.Lock()
	defer w.input.stateMu.Unlock()

	if state, exists := w.input.nodeStates[deviceKey]; exists {
		state.BdSeq = bdSeq
	} else {
		w.input.nodeStates[deviceKey] = &nodeState{
			BdSeq:    bdSeq,
			IsOnline: true,
			LastSeen: time.Now(),
		}
	}
}

// GetNodeBdSeq returns the bdSeq for a device
func (w *SparkplugInputTestWrapper) GetNodeBdSeq(deviceKey string) (uint64, bool) {
	w.input.stateMu.RLock()
	defer w.input.stateMu.RUnlock()

	state, exists := w.input.nodeStates[deviceKey]
	if !exists {
		return 0, false
	}
	return state.BdSeq, true
}
