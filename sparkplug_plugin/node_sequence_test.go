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
// Per Sparkplug B spec, sequence numbers are tracked at NODE scope (group/edgeNode),
// not device scope (group/edgeNode/device). All message types from a node
// (NBIRTH, NDATA, DBIRTH, DDATA) share one sequence counter.
//
// The fix uses TopicInfo.NodeKey() to ensure all messages from the same node
// share the same sequence state, regardless of message type.

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
			Expect(action1.IsNewNode).To(BeTrue(), "First message creates new node")
			Expect(nodeStates[nodeKey].IsOnline).To(BeTrue(), "New node should be online")

			// Process NDATA seq=16 (node-level message)
			action2 := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 16)
			Expect(action2.NeedsRebirth).To(BeFalse(), "Sequential seq=16 should not need rebirth")
			Expect(nodeStates[nodeKey].IsOnline).To(BeTrue(), "Valid sequence keeps node online")

			// Process NDATA seq=17 (node-level message)
			action3 := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 17)
			Expect(action3.NeedsRebirth).To(BeFalse(), "Sequential seq=17 should not need rebirth")
			Expect(nodeStates[nodeKey].IsOnline).To(BeTrue(), "Valid sequence keeps node online")

			// Process DDATA seq=18 (device-level message)
			// THIS IS THE KEY TEST - should NOT trigger gap
			action4 := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 18)
			Expect(action4.NeedsRebirth).To(BeFalse(),
				"DDATA seq=18 should be valid - NDATA incremented node counter to 17")
			Expect(nodeStates[nodeKey].IsOnline).To(BeTrue(), "Valid sequence keeps node online")

			// Verify final state - all fields
			state := nodeStates[nodeKey]
			Expect(state.LastSeq).To(Equal(uint8(18)))
			Expect(state.IsOnline).To(BeTrue(), "Node should remain online after valid sequence")
			Expect(state.LastSeen).NotTo(BeZero(), "LastSeen should be set")
		})

		It("should still detect real sequence gaps across message types", func() {
			// Real gap: seq=15 -> seq=16 -> seq=19 (missing 17, 18)
			nodeStates := make(map[string]*sparkplugplugin.NodeState)
			nodeKey := "test_group/edge_node"

			// Process sequential messages
			action1 := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 15)
			Expect(action1.IsNewNode).To(BeTrue())
			Expect(nodeStates[nodeKey].IsOnline).To(BeTrue(), "New node should be online")

			action2 := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 16)
			Expect(action2.NeedsRebirth).To(BeFalse())
			Expect(nodeStates[nodeKey].IsOnline).To(BeTrue(), "Valid sequence keeps node online")

			// Process seq=19 - should detect REAL gap (missing 17, 18)
			action := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 19)
			Expect(action.NeedsRebirth).To(BeTrue(), "Should detect real gap: expected 17, got 19")
			Expect(nodeStates[nodeKey].IsOnline).To(BeFalse(), "Sequence gap should mark node offline")
			Expect(nodeStates[nodeKey].LastSeq).To(Equal(uint8(19)), "LastSeq should still update even with gap")
		})

		// Regression test: demonstrates the bug fixed by ENG-4031
		It("should NOT produce false gaps when node key is used consistently (regression ENG-4031)", func() {
			// This test demonstrates the FIX behavior
			// BEFORE FIX: Device-scoped keys caused false sequence gaps
			// AFTER FIX: Node-scoped keys prevent false gaps

			// Scenario from customer: interleaved DDATA and NDATA messages
			// DDATA seq=15 → NDATA seq=16 → NDATA seq=17 → DDATA seq=18
			//
			// BUG (device-scoped): DDATA used "group/node/device", NDATA used "group/node"
			//   - After DDATA seq=15: deviceStates["group/node/device"].LastSeq = 15
			//   - After NDATA seq=16: deviceStates["group/node"].LastSeq = 16 (different key!)
			//   - After DDATA seq=18: compared to 15, not 17 → FALSE GAP
			//
			// FIX (node-scoped): All messages use "group/node" key
			//   - All messages share one counter: 15 → 16 → 17 → 18 = valid

			nodeStates := make(map[string]*sparkplugplugin.NodeState)

			// Using TopicInfo to derive keys ensures consistency
			ddataTopicInfo := &sparkplugplugin.TopicInfo{
				Group:    "factory",
				EdgeNode: "plc_line1",
				Device:   "motor_01", // Device-level message
			}
			ndataTopicInfo := &sparkplugplugin.TopicInfo{
				Group:    "factory",
				EdgeNode: "plc_line1",
				// No device - node-level message
			}

			// THE KEY INSIGHT: Both use NodeKey(), not DeviceKey()
			// This ensures all messages from same node share sequence state
			nodeKey := ddataTopicInfo.NodeKey() // "factory/plc_line1"
			Expect(nodeKey).To(Equal(ndataTopicInfo.NodeKey()), "Both should derive same node key")

			// Simulate customer scenario with node-scoped keys (FIXED behavior)
			sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 15) // DDATA
			sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 16) // NDATA
			sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 17) // NDATA

			// Final DDATA - with node-scoped keys, this is valid (17 → 18)
			action := sparkplugplugin.UpdateNodeState(nodeStates, nodeKey, 18)
			Expect(action.NeedsRebirth).To(BeFalse(),
				"REGRESSION CHECK: node-scoped keys prevent false gap (was bug with device-scoped)")
			Expect(nodeStates[nodeKey].IsOnline).To(BeTrue(),
				"Node should remain online when using correct node-scoped keys")
		})
	})

	// Table-driven tests for TopicInfo key methods
	// Replaces 10 granular tests with one comprehensive table
	DescribeTable("TopicInfo.NodeKey() and DeviceKey() extraction",
		func(ti *sparkplugplugin.TopicInfo, expectedNodeKey, expectedDeviceKey string) {
			Expect(ti.NodeKey()).To(Equal(expectedNodeKey))
			Expect(ti.DeviceKey()).To(Equal(expectedDeviceKey))
		},
		Entry("with device", &sparkplugplugin.TopicInfo{
			Group: "factory_a", EdgeNode: "line_1", Device: "plc_1",
		}, "factory_a/line_1", "factory_a/line_1/plc_1"),

		Entry("without device", &sparkplugplugin.TopicInfo{
			Group: "factory_a", EdgeNode: "line_1",
		}, "factory_a/line_1", "factory_a/line_1"),

		Entry("nil TopicInfo", nil, "", ""),

		Entry("empty Group", &sparkplugplugin.TopicInfo{
			EdgeNode: "line_1", Device: "plc_1",
		}, "", ""),

		Entry("empty EdgeNode", &sparkplugplugin.TopicInfo{
			Group: "factory_a", Device: "plc_1",
		}, "", ""),
	)
})
