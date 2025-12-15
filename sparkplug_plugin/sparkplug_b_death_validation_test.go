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

package sparkplug_plugin_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

// makeTopicInfoDeath constructs a TopicInfo from a deviceKey string (group/node/device format)
func makeTopicInfoDeath(deviceKey string) *sparkplugplugin.TopicInfo {
	parts := strings.Split(deviceKey, "/")
	if len(parts) < 2 {
		return &sparkplugplugin.TopicInfo{}
	}
	ti := &sparkplugplugin.TopicInfo{
		Group:    parts[0],
		EdgeNode: parts[1],
	}
	if len(parts) > 2 {
		ti.Device = parts[2]
	}
	return ti
}

var _ = Describe("NDEATH bdSeq Validation", func() {
	Context("when processing NDEATH with bdSeq", func() {
		It("should accept NDEATH with matching bdSeq", func() {
			wrapper := sparkplugplugin.NewSparkplugInputForTesting()
			deviceKey := "TestGroup/TestNode"

			// Setup: Establish node with bdSeq=42 (simulating NBIRTH)
			wrapper.SetNodeBdSeq(deviceKey, 42)

			// Create NDEATH payload with matching bdSeq
			bdSeqValue := uint64(42)
			payload := &sparkplugb.Payload{
				Metrics: []*sparkplugb.Payload_Metric{
					{
						Name:     stringPtr("bdSeq"),
						Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeUInt64),
						Value:    &sparkplugb.Payload_Metric_LongValue{LongValue: bdSeqValue},
					},
				},
			}

			// Process NDEATH
			wrapper.ProcessDeathMessage(deviceKey, "NDEATH", payload, makeTopicInfoDeath(deviceKey))

			// Verify state is updated (node marked offline)
			state := wrapper.GetNodeState(deviceKey)
			Expect(state).NotTo(BeNil())
			Expect(state.IsOnline).To(BeFalse(), "Node should be offline after valid NDEATH")
		})

		It("should reject stale NDEATH with mismatched bdSeq", func() {
			wrapper := sparkplugplugin.NewSparkplugInputForTesting()
			deviceKey := "TestGroup/TestNode"

			// Setup: Establish node with bdSeq=42
			wrapper.SetNodeBdSeq(deviceKey, 42)

			// Create NDEATH payload with OLD bdSeq (from previous session)
			staleBdSeq := uint64(41)
			payload := &sparkplugb.Payload{
				Metrics: []*sparkplugb.Payload_Metric{
					{
						Name:     stringPtr("bdSeq"),
						Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeUInt64),
						Value:    &sparkplugb.Payload_Metric_LongValue{LongValue: staleBdSeq},
					},
				},
			}

			// Process NDEATH
			wrapper.ProcessDeathMessage(deviceKey, "NDEATH", payload, makeTopicInfoDeath(deviceKey))

			// Verify state is NOT updated (stale NDEATH ignored)
			state := wrapper.GetNodeState(deviceKey)
			Expect(state).NotTo(BeNil())
			Expect(state.IsOnline).To(BeTrue(), "Node should still be online - stale NDEATH ignored")
		})

		It("should handle NDEATH without bdSeq metric gracefully", func() {
			wrapper := sparkplugplugin.NewSparkplugInputForTesting()
			deviceKey := "TestGroup/TestNode"

			// Setup: Establish node
			wrapper.SetNodeBdSeq(deviceKey, 42)

			// Create NDEATH payload WITHOUT bdSeq metric
			payload := &sparkplugb.Payload{
				Metrics: []*sparkplugb.Payload_Metric{}, // Empty metrics
			}

			// Process NDEATH
			wrapper.ProcessDeathMessage(deviceKey, "NDEATH", payload, makeTopicInfoDeath(deviceKey))

			// Verify state is updated (backwards compatibility)
			state := wrapper.GetNodeState(deviceKey)
			Expect(state).NotTo(BeNil())
			Expect(state.IsOnline).To(BeFalse(), "Node should be offline - NDEATH processed despite missing bdSeq")
		})

		It("should process DDEATH without bdSeq validation", func() {
			wrapper := sparkplugplugin.NewSparkplugInputForTesting()
			deviceKey := "TestGroup/TestNode/Device1"

			// Setup: Establish device
			wrapper.SetNodeBdSeq(deviceKey, 42)

			// Create DDEATH payload (DDEATHs don't use bdSeq)
			payload := &sparkplugb.Payload{
				Metrics: []*sparkplugb.Payload_Metric{},
			}

			// Process DDEATH
			wrapper.ProcessDeathMessage(deviceKey, "DDEATH", payload, makeTopicInfoDeath(deviceKey))

			// Verify state is updated
			state := wrapper.GetNodeState(deviceKey)
			Expect(state).NotTo(BeNil())
			Expect(state.IsOnline).To(BeFalse(), "Device should be offline after DDEATH")
		})
	})
})

// Note: Helper functions stringPtr and uint32Ptr are defined in unit_test.go
