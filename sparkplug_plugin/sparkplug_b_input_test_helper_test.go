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
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	sparkplugb "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

// testCounter is an in-memory Counter implementation used by the test wrapper to make
// metric increments observable. The production code uses *service.MetricCounter, whose
// mock variant from MockResources is a no-op with no read API; tests would otherwise
// have no way to verify "did this rebirth path actually execute?".
type testCounter struct{ n atomic.Int64 }

func (c *testCounter) Incr(delta int64, _ ...string) { c.n.Add(delta) }
func (c *testCounter) Value() int64                  { return c.n.Load() }

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
	input                *sparkplugInput
	aliasRebirthsCounter *testCounter
}

// NewSparkplugInputForTesting creates a minimal sparkplugInput wrapped for unit testing
// This allows testing methods like createSplitMessages without full MQTT setup
func NewSparkplugInputForTesting() *SparkplugInputTestWrapper {
	return NewSparkplugInputForTestingWithRole(RoleSecondaryPassive)
}

// NewSparkplugInputForTestingWithRole creates a sparkplugInput with specified role
// Used for testing role-specific behavior like rebirth requests.
//
// Production-equivalent fields are wired so code paths reaching Incr() or the
// throttle bookkeeping don't nil-panic. aliasRebirths is wired to an in-memory
// testCounter (instead of the no-op MockResources counter) so tests can assert
// the rebirth path actually executed even though the MQTT publish itself is
// unreachable without a real broker.
func NewSparkplugInputForTestingWithRole(role Role) *SparkplugInputTestWrapper {
	resources := service.MockResources()
	logger := resources.Logger()
	metrics := resources.Metrics()

	aliasRebirths := &testCounter{}
	input := &sparkplugInput{
		config: Config{
			Role: role,
			// RequestBirthOnConnect defaults to false here (Go zero value).
			// Production defaults to true; see the field default near line 125.
			// The wrapper leaves it false so each test isolates one rebirth
			// reason at a time; tests exercising the discovery path call
			// SetRequestBirthOnConnect(true) explicitly.
		},
		logger:                  logger,
		nodeStates:              make(map[string]*nodeState),
		legacyAliasCache:        make(map[string]map[uint64]string),
		birthRequested:          make(map[string]time.Time),
		aliasCache:              NewAliasCache(),
		topicParser:             NewTopicParser(),
		typeConverter:           NewTypeConverter(),
		messageProcessor:        NewMessageProcessor(logger),
		messagesReceived:        metrics.NewCounter("messages_received"),
		messagesProcessed:       metrics.NewCounter("messages_processed"),
		messagesDropped:         metrics.NewCounter("messages_dropped"),
		messagesErrored:         metrics.NewCounter("messages_errored"),
		birthsProcessed:         metrics.NewCounter("births_processed"),
		deathsProcessed:         metrics.NewCounter("deaths_processed"),
		rebirthsRequested:       metrics.NewCounter("rebirths_requested"),
		rebirthsSuppressed:      metrics.NewCounter("rebirths_suppressed"),
		sequenceErrors:          metrics.NewCounter("sequence_errors"),
		aliasResolutions:        metrics.NewCounter("alias_resolutions"),
		aliasResolutionFailures: metrics.NewCounter("alias_resolution_failures"),
		discoveryRebirths:       metrics.NewCounter("discovery_rebirths"),
		sequenceGapRebirths:     metrics.NewCounter("sequence_gap_rebirths"),
		aliasRebirths:           aliasRebirths,
	}

	return &SparkplugInputTestWrapper{input: input, aliasRebirthsCounter: aliasRebirths}
}

// SetBirthRequestThrottle overrides the BirthRequestThrottle config for tests that need
// to exercise throttle boundaries deterministically without sleeping for the default 1s.
func (w *SparkplugInputTestWrapper) SetBirthRequestThrottle(d time.Duration) {
	w.input.config.BirthRequestThrottle = d
}

// SetRequestBirthOnConnect overrides the RequestBirthOnConnect feature flag for tests that
// need to verify the role gate fires regardless of feature flag state. The role gate is
// checked before the feature flag in sendRebirthRequest, so tests asserting passive-role
// suppression of the discovery reason should set this to true to prove the role gate (not
// the feature flag) is what suppressed the rebirth.
func (w *SparkplugInputTestWrapper) SetRequestBirthOnConnect(enabled bool) {
	w.input.config.RequestBirthOnConnect = enabled
}

// SeedAliasCache pre-populates the alias cache for the given deviceKey from the supplied
// (name, alias) pair metrics. Tests use this for a "BIRTH already applied" baseline.
func (w *SparkplugInputTestWrapper) SeedAliasCache(deviceKey string, metrics []*sparkplugb.Payload_Metric) {
	w.input.aliasCache.CacheAliases(deviceKey, metrics)
}

// GetBirthRequestedAt returns the rebirth-throttle timestamp for nodeKey. ok==false
// means no rebirth path ran for this node (e.g. passive role or fully resolved aliases).
func (w *SparkplugInputTestWrapper) GetBirthRequestedAt(nodeKey string) (time.Time, bool) {
	w.input.birthRequestMu.RLock()
	defer w.input.birthRequestMu.RUnlock()
	t, ok := w.input.birthRequested[nodeKey]
	return t, ok
}

// GetAliasRebirthsCount returns the number of times sendRebirthRequest ran with
// rebirthReasonUnresolvedAliases past the role + throttle gates since this wrapper
// was created. Stronger than GetBirthRequestedAt: increments only after the
// alias-recovery branch is reached, and is reason-specific so co-occurring
// reasons (e.g. discovery, sequence-gap) don't pollute the count.
func (w *SparkplugInputTestWrapper) GetAliasRebirthsCount() int64 {
	return w.aliasRebirthsCounter.Value()
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
