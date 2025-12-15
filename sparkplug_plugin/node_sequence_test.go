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

// Unit tests for Sparkplug B NODE-scoped sequence tracking (ENG-4031)
//
// Per Sparkplug B spec, sequence numbers are tracked at NODE scope, not device scope.
// All message types from a node (NBIRTH, NDATA, DBIRTH, DDATA) share one sequence counter.
//
// BUG: Current implementation tracks per-deviceKey, causing false sequence gaps when
// NDATA (deviceKey="group/node") and DDATA (deviceKey="group/node/device") interleave.
//
// This test file uses TDD approach:
// 1. First test demonstrates correct behavior (when using unified nodeKey)
// 2. Second test reproduces the bug (when using separate device keys)
// 3. Third test validates the TopicInfo.NodeKey() method

package sparkplug_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
)

var _ = Describe("Sparkplug B Node-Scoped Sequence Tracking (ENG-4031)", func() {
	// Per Sparkplug B spec, sequence numbers are tracked at NODE scope, not device scope.
	// All message types from a node (NBIRTH, NDATA, DBIRTH, DDATA) share one sequence counter.

	Context("when NDATA and DDATA messages interleave from same node", func() {
		It("should track sequence at NODE level, not device level (customer scenario)", func() {
			// Given: Customer's exact scenario
			// NODE publishes: DDATA seq=15 -> NDATA seq=16 -> NDATA seq=17 -> DDATA seq=18
			// Per spec, this is VALID - sequence is NODE-scoped

			nodeStates := make(map[string]*sparkplugplugin.NodeState)

			// For correct behavior, we need to use NODE key for ALL messages
			nodeKey := "factory_a/line_1" // This is what we SHOULD use

			// Process DDATA seq=15 (device-level message)
			action1 := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 15)
			Expect(action1.NeedsRebirth).To(BeFalse(), "First message should not need rebirth")

			// Process NDATA seq=16 (node-level message)
			action2 := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 16)
			Expect(action2.NeedsRebirth).To(BeFalse(), "Sequential seq=16 should not need rebirth")

			// Process NDATA seq=17 (node-level message)
			action3 := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 17)
			Expect(action3.NeedsRebirth).To(BeFalse(), "Sequential seq=17 should not need rebirth")

			// Process DDATA seq=18 (device-level message)
			// THIS IS THE KEY TEST - should NOT trigger gap
			action4 := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 18)
			Expect(action4.NeedsRebirth).To(BeFalse(),
				"DDATA seq=18 should be valid - NDATA incremented node counter to 17")

			// Verify final state
			Expect(nodeStates[nodeKey].LastSeq).To(Equal(uint8(18)))
		})

		It("should FAIL with current per-device tracking (reproduces the bug)", func() {
			// This test demonstrates the CURRENT BUGGY behavior
			// Using separate keys for NDATA vs DDATA causes false sequence gaps

			nodeStates := make(map[string]*sparkplugplugin.NodeState)

			// CURRENT BUGGY BEHAVIOR: Different keys for different message types
			ndataKey := "factory_a/line_1"            // Node-level messages
			ddataKey := "factory_a/line_1/plc_device" // Device-level messages

			// Process DDATA seq=15
			action1 := sparkplugplugin.UpdateNodeState(nodeStates, ddataKey, 15)
			Expect(action1.IsNewNode).To(BeTrue())

			// Process NDATA seq=16 (different key!)
			action2 := sparkplugplugin.UpdateNodeState(nodeStates, ndataKey, 16)
			Expect(action2.IsNewNode).To(BeTrue()) // Creates NEW entry!

			// Process NDATA seq=17
			action3 := sparkplugplugin.UpdateNodeState(nodeStates, ndataKey, 17)
			Expect(action3.NeedsRebirth).To(BeFalse())

			// Process DDATA seq=18 - BUG: This triggers false gap!
			action4 := sparkplugplugin.UpdateNodeState(nodeStates, ddataKey, 18)

			// BUG MANIFESTATION: This returns NeedsRebirth=true because:
			// - ddataKey state has LastSeq=15
			// - Expected next: 16
			// - Got: 18
			// - Result: FALSE sequence gap detected!
			Expect(action4.NeedsRebirth).To(BeTrue(),
				"BUG: Per-device tracking causes false gap detection")

			// Verify we have TWO separate state entries (the bug)
			Expect(nodeStates).To(HaveLen(2), "BUG: Two separate states instead of one")
		})

		It("should still detect real sequence gaps across message types", func() {
			// Real gap: seq=15 -> seq=16 -> seq=19 (missing 17, 18)
			nodeStates := make(map[string]*sparkplugplugin.NodeState)
			nodeKey := "test_group/edge_node"

			// Process sequential messages
			sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 15)
			sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 16)

			// Process seq=19 - should detect REAL gap (missing 17, 18)
			action := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 19)
			Expect(action.NeedsRebirth).To(BeTrue(), "Should detect real gap: expected 17, got 19")
		})
	})

	Context("TopicInfo.NodeKey() method", func() {
		It("should return node key from TopicInfo with device", func() {
			// Device-level: group + node + device -> "group/node"
			ti := &sparkplugplugin.TopicInfo{
				Group:    "factory_a",
				EdgeNode: "line_1",
				Device:   "plc_device",
			}
			Expect(ti.NodeKey()).To(Equal("factory_a/line_1"))
		})

		It("should return node key from TopicInfo without device", func() {
			// Node-level: group + node -> "group/node"
			ti := &sparkplugplugin.TopicInfo{
				Group:    "factory_a",
				EdgeNode: "line_1",
			}
			Expect(ti.NodeKey()).To(Equal("factory_a/line_1"))
		})

		It("should return empty string for nil TopicInfo", func() {
			var ti *sparkplugplugin.TopicInfo
			Expect(ti.NodeKey()).To(Equal(""))
		})

		It("should return empty string for empty Group", func() {
			ti := &sparkplugplugin.TopicInfo{
				EdgeNode: "line_1",
			}
			Expect(ti.NodeKey()).To(Equal(""))
		})

		It("should return empty string for empty EdgeNode", func() {
			ti := &sparkplugplugin.TopicInfo{
				Group: "factory_a",
			}
			Expect(ti.NodeKey()).To(Equal(""))
		})
	})
})
