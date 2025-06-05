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

package sparkplug_plugin

import (
	"testing"

	"github.com/weekaung/sparkplugb-client/sproto"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSparkplugBProcessor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sparkplug B Processor Suite")
}

var _ = Describe("Sparkplug B Processor", func() {
	var processor *sparkplugProcessor

	BeforeEach(func() {
		// Create a minimal processor for testing
		processor = &sparkplugProcessor{
			dropBirthMessages:     false,
			strictTopicValidation: false,
			cacheTTL:              "",
			aliasCache:            make(map[string]map[uint64]string),
		}
	})

	Describe("Topic Parsing", func() {
		It("should parse valid Sparkplug topics correctly", func() {
			testCases := []struct {
				topic       string
				expectedMsg string
				expectedKey string
			}{
				{"spBv1.0/Group1/NBIRTH/Edge1", "NBIRTH", "Group1/Edge1"},
				{"spBv1.0/Group1/NDATA/Edge1", "NDATA", "Group1/Edge1"},
				{"spBv1.0/Group1/DBIRTH/Edge1/Device1", "DBIRTH", "Group1/Edge1/Device1"},
				{"spBv1.0/Group1/DDATA/Edge1/Device1", "DDATA", "Group1/Edge1/Device1"},
				{"spBv1.0/Factory/NDEATH/PLC123", "NDEATH", "Factory/PLC123"},
			}

			for _, tc := range testCases {
				msgType, deviceKey := processor.parseSparkplugTopic(tc.topic)
				Expect(msgType).To(Equal(tc.expectedMsg), "Topic: %s", tc.topic)
				Expect(deviceKey).To(Equal(tc.expectedKey), "Topic: %s", tc.topic)
			}
		})

		It("should return empty strings for invalid topics", func() {
			invalidTopics := []string{
				"",
				"invalid/topic",
				"spBv1.0/Group1",
				"spBv1.0/Group1/NBIRTH",
				"spBv2.0/Group1/NBIRTH/Edge1",
				"mqtt/Group1/NBIRTH/Edge1",
			}

			for _, topic := range invalidTopics {
				msgType, deviceKey := processor.parseSparkplugTopic(topic)
				Expect(msgType).To(Equal(""), "Topic: %s", topic)
				Expect(deviceKey).To(Equal(""), "Topic: %s", topic)
			}
		})
	})

	Describe("Alias Caching", func() {
		It("should cache aliases from BIRTH messages", func() {
			metrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(1),
				},
				{
					Name:  stringPtr("Pressure"),
					Alias: uint64Ptr(2),
				},
				{
					Name: stringPtr("NoAlias"), // No alias
				},
				{
					Alias: uint64Ptr(3), // No name
				},
			}

			count := processor.cacheAliases("Group1/Edge1", metrics)
			Expect(count).To(Equal(2))

			// Verify cache contents
			processor.mu.RLock()
			aliasMap := processor.aliasCache["Group1/Edge1"]
			processor.mu.RUnlock()

			Expect(aliasMap).To(HaveLen(2))
			Expect(aliasMap[1]).To(Equal("Temperature"))
			Expect(aliasMap[2]).To(Equal("Pressure"))
		})

		It("should handle empty device key", func() {
			metrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(1),
				},
			}

			count := processor.cacheAliases("", metrics)
			Expect(count).To(Equal(0))
		})

		It("should handle nil metrics", func() {
			count := processor.cacheAliases("Group1/Edge1", nil)
			Expect(count).To(Equal(0))
		})
	})

	Describe("Alias Resolution", func() {
		BeforeEach(func() {
			// Pre-populate cache
			processor.mu.Lock()
			processor.aliasCache["Group1/Edge1"] = map[uint64]string{
				1: "Temperature",
				2: "Pressure",
			}
			processor.mu.Unlock()
		})

		It("should resolve aliases in DATA messages", func() {
			metrics := []*sproto.Payload_Metric{
				{
					Alias: uint64Ptr(1), // Should be resolved to "Temperature"
				},
				{
					Alias: uint64Ptr(2), // Should be resolved to "Pressure"
				},
				{
					Alias: uint64Ptr(99), // Unknown alias
				},
				{
					Name:  stringPtr("ExistingName"),
					Alias: uint64Ptr(1), // Already has name, should not be changed
				},
			}

			count := processor.resolveAliases("Group1/Edge1", metrics)
			Expect(count).To(Equal(2))

			// Verify resolutions
			Expect(*metrics[0].Name).To(Equal("Temperature"))
			Expect(*metrics[1].Name).To(Equal("Pressure"))
			Expect(metrics[2].Name).To(BeNil())                // Unknown alias should remain unresolved
			Expect(*metrics[3].Name).To(Equal("ExistingName")) // Should not be overwritten
		})

		It("should handle device with no cached aliases", func() {
			metrics := []*sproto.Payload_Metric{
				{
					Alias: uint64Ptr(1),
				},
			}

			count := processor.resolveAliases("Group1/UnknownEdge", metrics)
			Expect(count).To(Equal(0))
		})
	})

	// Note: Full message processing tests would require a complete processor setup
	// with logger and metrics. For now, we focus on testing the core logic.
})

// Helper functions for creating pointers
func stringPtr(s string) *string {
	return &s
}

func uint64Ptr(u uint64) *uint64 {
	return &u
}
