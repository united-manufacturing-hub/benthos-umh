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

// Unit tests for Sparkplug B seq=0 implicit behavior
//
// Background:
// - Sparkplug B spec was updated to require explicit seq=0 in BIRTH messages
// - Older devices omit the seq field entirely (implied seq=0 for backwards compatibility)
// - Current code expects payload.Seq != nil which breaks with older devices
//
// Solution:
// - Add getSequenceNumber() helper that treats nil seq as 0
// - Update all 4 call sites to use the helper
//
// Test Strategy (TDD):
// 1. RED: Write failing test for getSequenceNumber with nil seq (returns 0)
// 2. GREEN: Implement minimal getSequenceNumber helper
// 3. RED: Write failing test for getSequenceNumber with explicit seq value
// 4. GREEN: Ensure helper handles explicit values correctly
// 5. Update call sites and add integration tests

package sparkplug_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

var _ = Describe("getSequenceNumber - seq=0 implicit behavior", func() {
	Context("when payload.Seq is nil (older devices)", func() {
		It("should return 0 (implied seq=0 for backwards compatibility)", func() {
			// Given: Payload with nil seq field (older device behavior)
			payload := &sparkplugb.Payload{
				Seq: nil, // Older devices omit this field
			}

			// When: Getting sequence number
			seq := sparkplugplugin.GetSequenceNumber(payload)

			// Then: Should return 0 (implied)
			Expect(seq).To(Equal(uint8(0)), "nil seq should be treated as 0")
		})
	})

	Context("when payload.Seq is explicitly 0", func() {
		It("should return 0 (explicit seq=0)", func() {
			// Given: Payload with explicit seq=0 (updated spec behavior)
			seq0 := uint64(0)
			payload := &sparkplugb.Payload{
				Seq: &seq0,
			}

			// When: Getting sequence number
			seq := sparkplugplugin.GetSequenceNumber(payload)

			// Then: Should return 0
			Expect(seq).To(Equal(uint8(0)), "explicit seq=0 should return 0")
		})
	})

	Context("when payload.Seq has a non-zero value", func() {
		It("should return the sequence number", func() {
			// Given: Payload with non-zero seq
			seq42 := uint64(42)
			payload := &sparkplugb.Payload{
				Seq: &seq42,
			}

			// When: Getting sequence number
			seq := sparkplugplugin.GetSequenceNumber(payload)

			// Then: Should return 42
			Expect(seq).To(Equal(uint8(42)), "should return the actual sequence value")
		})
	})

	Context("when payload.Seq is 255 (wrap-around boundary)", func() {
		It("should return 255", func() {
			// Given: Payload at sequence wrap-around boundary
			seq255 := uint64(255)
			payload := &sparkplugb.Payload{
				Seq: &seq255,
			}

			// When: Getting sequence number
			seq := sparkplugplugin.GetSequenceNumber(payload)

			// Then: Should return 255
			Expect(seq).To(Equal(uint8(255)), "should handle wrap-around boundary")
		})
	})
})

var _ = Describe("Integration Tests - seq=0 implicit behavior in real messages", func() {
	Context("when processing NBIRTH with nil seq", func() {
		It("should treat nil seq as 0 and update node state correctly", func() {
			// Given: Mock sparkplug input
			input := createMockSparkplugInput()
			topicInfo := &sparkplugplugin.TopicInfo{
				Group:    "test",
				EdgeNode: "edge1",
			}

			// NBIRTH payload with nil seq (older device behavior)
			payload := &sparkplugb.Payload{
				Seq:       nil, // Older devices omit seq field
				Timestamp: uint64Ptr(1730986400000),
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("temperature"), Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeDouble), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 23.5}},
				},
			}

			// When: Processing NBIRTH
			input.ProcessBirthMessage(topicInfo.DeviceKey(), "NBIRTH", payload, topicInfo)

			// Then: Node state should be created with seq=0
			state := input.GetNodeState(topicInfo.DeviceKey())
			Expect(state).ToNot(BeNil(), "node state should be created")
			Expect(state.LastSeq).To(Equal(uint8(0)), "nil seq should be stored as 0")
			Expect(state.IsOnline).To(BeTrue(), "node should be online after NBIRTH")
		})
	})

	Context("when processing DBIRTH with nil seq", func() {
		It("should treat nil seq as 0 and update device state correctly", func() {
			// Given: Mock sparkplug input
			input := createMockSparkplugInput()
			topicInfo := &sparkplugplugin.TopicInfo{
				Group:    "test",
				EdgeNode: "edge1",
				Device:   "device1",
			}

			// DBIRTH payload with nil seq (older device behavior)
			payload := &sparkplugb.Payload{
				Seq:       nil, // Older devices omit seq field
				Timestamp: uint64Ptr(1730986400000),
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("pressure"), Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeDouble), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 101.3}},
				},
			}

			// When: Processing DBIRTH
			input.ProcessBirthMessage(topicInfo.DeviceKey(), "DBIRTH", payload, topicInfo)

			// Then: Device state should be created with seq=0
			state := input.GetNodeState(topicInfo.DeviceKey())
			Expect(state).ToNot(BeNil(), "device state should be created")
			Expect(state.LastSeq).To(Equal(uint8(0)), "nil seq should be stored as 0")
			Expect(state.IsOnline).To(BeTrue(), "device should be online after DBIRTH")
		})
	})

	Context("when processing NDATA after BIRTH with nil seq", func() {
		It("should validate sequence correctly when BIRTH had nil seq and NDATA has explicit seq=1", func() {
			// Given: Mock sparkplug input with NBIRTH having nil seq
			input := createMockSparkplugInput()
			topicInfo := &sparkplugplugin.TopicInfo{
				Group:    "test",
				EdgeNode: "edge1",
			}

			// First: NBIRTH with nil seq (stored as 0)
			birthPayload := &sparkplugb.Payload{
				Seq:       nil,
				Timestamp: uint64Ptr(1730986400000),
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("temperature"), Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeDouble), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 23.5}},
				},
			}
			input.ProcessBirthMessage(topicInfo.DeviceKey(), "NBIRTH", birthPayload, topicInfo)

			// When: NDATA with explicit seq=1 (expected next sequence)
			seq1 := uint64(1)
			dataPayload := &sparkplugb.Payload{
				Seq:       &seq1,
				Timestamp: uint64Ptr(1730986401000),
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("temperature"), Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeDouble), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 24.1}},
				},
			}
			input.ProcessDataMessage(topicInfo.DeviceKey(), "NDATA", dataPayload, topicInfo)

			// Then: Sequence validation should pass (0 → 1 is valid)
			state := input.GetNodeState(topicInfo.DeviceKey())
			Expect(state.LastSeq).To(Equal(uint8(1)), "sequence should advance from 0 to 1")
			Expect(state.IsOnline).To(BeTrue(), "node should remain online after valid sequence")
		})

		It("should detect sequence gap when BIRTH had nil seq and NDATA skips seq=1", func() {
			// Given: Mock sparkplug input with NBIRTH having nil seq
			input := createMockSparkplugInput()
			topicInfo := &sparkplugplugin.TopicInfo{
				Group:    "test",
				EdgeNode: "edge1",
			}

			// First: NBIRTH with nil seq (stored as 0)
			birthPayload := &sparkplugb.Payload{
				Seq:       nil,
				Timestamp: uint64Ptr(1730986400000),
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("temperature"), Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeDouble), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 23.5}},
				},
			}
			input.ProcessBirthMessage(topicInfo.DeviceKey(), "NBIRTH", birthPayload, topicInfo)

			// When: NDATA with seq=2 (skips expected seq=1)
			seq2 := uint64(2)
			dataPayload := &sparkplugb.Payload{
				Seq:       &seq2,
				Timestamp: uint64Ptr(1730986401000),
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("temperature"), Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeDouble), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 24.1}},
				},
			}
			input.ProcessDataMessage(topicInfo.DeviceKey(), "NDATA", dataPayload, topicInfo)

			// Then: Sequence validation should fail (0 → 2 is invalid, expected 1)
			state := input.GetNodeState(topicInfo.DeviceKey())
			Expect(state.IsOnline).To(BeFalse(), "node should be marked offline after sequence gap")
		})
	})

	Context("when processing mixed messages (some with seq, some without)", func() {
		It("should handle sequence progression correctly", func() {
			// Given: Mock sparkplug input
			input := createMockSparkplugInput()
			topicInfo := &sparkplugplugin.TopicInfo{
				Group:    "test",
				EdgeNode: "edge1",
				Device:   "device1",
			}

			// Message sequence: DBIRTH (nil) → DDATA (1) → DDATA (2) → DBIRTH (nil) → DDATA (1)

			// 1. DBIRTH with nil seq
			birth1 := &sparkplugb.Payload{
				Seq:       nil,
				Timestamp: uint64Ptr(1730986400000),
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("sensor1"), Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt64), Value: &sparkplugb.Payload_Metric_LongValue{LongValue: 100}},
				},
			}
			input.ProcessBirthMessage(topicInfo.DeviceKey(), "DBIRTH", birth1, topicInfo)

			state := input.GetNodeState(topicInfo.DeviceKey())
			Expect(state.LastSeq).To(Equal(uint8(0)), "first DBIRTH should set seq to 0")

			// 2. DDATA with seq=1
			seq1 := uint64(1)
			data1 := &sparkplugb.Payload{
				Seq:       &seq1,
				Timestamp: uint64Ptr(1730986401000),
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("sensor1"), Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt64), Value: &sparkplugb.Payload_Metric_LongValue{LongValue: 101}},
				},
			}
			input.ProcessDataMessage(topicInfo.DeviceKey(), "DDATA", data1, topicInfo)

			state = input.GetNodeState(topicInfo.DeviceKey())
			Expect(state.LastSeq).To(Equal(uint8(1)), "DDATA should advance to seq=1")
			Expect(state.IsOnline).To(BeTrue(), "valid sequence should keep device online")

			// 3. DDATA with seq=2
			seq2 := uint64(2)
			data2 := &sparkplugb.Payload{
				Seq:       &seq2,
				Timestamp: uint64Ptr(1730986402000),
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("sensor1"), Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt64), Value: &sparkplugb.Payload_Metric_LongValue{LongValue: 102}},
				},
			}
			input.ProcessDataMessage(topicInfo.DeviceKey(), "DDATA", data2, topicInfo)

			state = input.GetNodeState(topicInfo.DeviceKey())
			Expect(state.LastSeq).To(Equal(uint8(2)), "DDATA should advance to seq=2")

			// 4. DBIRTH with nil seq (rebirth scenario)
			birth2 := &sparkplugb.Payload{
				Seq:       nil,
				Timestamp: uint64Ptr(1730986403000),
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("sensor1"), Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt64), Value: &sparkplugb.Payload_Metric_LongValue{LongValue: 100}},
				},
			}
			input.ProcessBirthMessage(topicInfo.DeviceKey(), "DBIRTH", birth2, topicInfo)

			state = input.GetNodeState(topicInfo.DeviceKey())
			Expect(state.LastSeq).To(Equal(uint8(0)), "rebirth should reset seq to 0")
			Expect(state.IsOnline).To(BeTrue(), "rebirth should bring device online")

			// 5. DDATA with seq=1 after rebirth
			seq1AfterRebirth := uint64(1)
			data3 := &sparkplugb.Payload{
				Seq:       &seq1AfterRebirth,
				Timestamp: uint64Ptr(1730986404000),
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("sensor1"), Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt64), Value: &sparkplugb.Payload_Metric_LongValue{LongValue: 103}},
				},
			}
			input.ProcessDataMessage(topicInfo.DeviceKey(), "DDATA", data3, topicInfo)

			state = input.GetNodeState(topicInfo.DeviceKey())
			Expect(state.LastSeq).To(Equal(uint8(1)), "sequence should advance from 0 to 1 after rebirth")
			Expect(state.IsOnline).To(BeTrue(), "valid sequence after rebirth should keep device online")
		})
	})
})

// Note: Helper functions and constants:
// - createMockSparkplugInput() defined in sparkplug_b_input_sequence_test.go
// - uint64Ptr, uint32Ptr, stringPtr defined in integration_test.go (shared across tests)
// - SparkplugDataType constants defined in sparkplug_b_input.go
