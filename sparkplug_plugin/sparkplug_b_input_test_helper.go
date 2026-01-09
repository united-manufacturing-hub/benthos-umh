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
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	sparkplugb "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

// extractNodeKeyForTest extracts group/node from deviceKey for test helper methods.
// This is a test-only utility; production code uses TopicInfo.NodeKey().
func extractNodeKeyForTest(deviceKey string) string {
	if deviceKey == "" {
		return ""
	}
	parts := strings.Split(deviceKey, "/")
	if len(parts) >= 2 {
		return parts[0] + "/" + parts[1]
	}
	return deviceKey
}

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
// ENG-4031: Removed redundant deviceKey parameter - now derived from topicInfo.DeviceKey()
func (w *SparkplugInputTestWrapper) CreateSplitMessages(payload *sparkplugb.Payload, msgType MessageType, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	return w.input.createSplitMessages(payload, msgType, topicInfo, originalTopic)
}

// ProcessBirthMessage is an exported wrapper for testing the private processBirthMessage method
// ENG-4031: Removed redundant deviceKey parameter - now derived from topicInfo.DeviceKey()
func (w *SparkplugInputTestWrapper) ProcessBirthMessage(msgType MessageType, payload *sparkplugb.Payload, topicInfo *TopicInfo) {
	w.input.processBirthMessage(msgType, payload, topicInfo)
}

// ProcessDataMessage is an exported wrapper for testing the private processDataMessage method
// ENG-4031: Removed redundant deviceKey parameter - now derived from topicInfo.DeviceKey()
func (w *SparkplugInputTestWrapper) ProcessDataMessage(msgType MessageType, payload *sparkplugb.Payload, topicInfo *TopicInfo) {
	w.input.processDataMessage(msgType, payload, topicInfo)
}

// NodeStateInfo represents the public view of a node state for testing
type NodeStateInfo struct {
	LastSeq  uint8
	IsOnline bool
}

// GetNodeState is an exported wrapper for accessing node state in tests
// ENG-4031: State is stored at node level, so we extract nodeKey from deviceKey
func (w *SparkplugInputTestWrapper) GetNodeState(deviceKey string) *NodeStateInfo {
	w.input.stateMu.RLock()
	defer w.input.stateMu.RUnlock()

	nodeKey := extractNodeKeyForTest(deviceKey)
	state, exists := w.input.nodeStates[nodeKey]
	if !exists {
		return nil
	}

	return &NodeStateInfo{
		LastSeq:  state.LastSeq,
		IsOnline: state.IsOnline,
	}
}

// ProcessDeathMessage is an exported wrapper for testing the private processDeathMessage method
// ENG-4031: Removed redundant deviceKey parameter - now derived from topicInfo.DeviceKey()
func (w *SparkplugInputTestWrapper) ProcessDeathMessage(msgType MessageType, payload *sparkplugb.Payload, topicInfo *TopicInfo) {
	w.input.processDeathMessage(msgType, payload, topicInfo)
}

// SetNodeBdSeq allows tests to set the bdSeq for a device (simulating NBIRTH)
// ENG-4031: State is stored at node level, so we extract nodeKey from deviceKey
func (w *SparkplugInputTestWrapper) SetNodeBdSeq(deviceKey string, bdSeq uint64) {
	w.input.stateMu.Lock()
	defer w.input.stateMu.Unlock()

	nodeKey := extractNodeKeyForTest(deviceKey)
	if state, exists := w.input.nodeStates[nodeKey]; exists {
		state.BdSeq = bdSeq
	} else {
		w.input.nodeStates[nodeKey] = &nodeState{
			BdSeq:    bdSeq,
			IsOnline: true,
			LastSeen: time.Now(),
		}
	}
}

// GetNodeBdSeq returns the bdSeq for a device
// ENG-4031: State is stored at node level, so we extract nodeKey from deviceKey
func (w *SparkplugInputTestWrapper) GetNodeBdSeq(deviceKey string) (uint64, bool) {
	w.input.stateMu.RLock()
	defer w.input.stateMu.RUnlock()

	nodeKey := extractNodeKeyForTest(deviceKey)
	state, exists := w.input.nodeStates[nodeKey]
	if !exists {
		return 0, false
	}
	return state.BdSeq, true
}
