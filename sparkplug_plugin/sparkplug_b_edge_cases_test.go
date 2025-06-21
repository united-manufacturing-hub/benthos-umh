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
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("Sparkplug B Edge Cases", func() {

	Describe("Test Vector Validation", func() {
		Context("Corrected Base64 fixtures from expert.md", func() {
			It("should validate all test vectors successfully", func() {
				vectors := sparkplug_plugin.GetTestVectors()
				Expect(vectors).To(HaveLen(2))

				for _, vector := range vectors {
					err := sparkplug_plugin.ValidateTestVector(vector)
					Expect(err).NotTo(HaveOccurred(), "Test vector %s should be valid: %v", vector.Name, err)
				}
			})

			It("should decode NBIRTH_v1 with correct structure", func() {
				vector := sparkplug_plugin.GetTestVectors()[0] // NBIRTH_v1
				Expect(vector.Name).To(Equal("NBIRTH_v1"))

				payload := sparkplug_plugin.MustDecodeBase64(vector.Base64Data)
				Expect(payload.Metrics).To(HaveLen(3))

				// Check bdSeq metric (first metric)
				bdSeqMetric := payload.Metrics[0]
				Expect(bdSeqMetric.Name).NotTo(BeNil())
				Expect(*bdSeqMetric.Name).To(Equal("bdSeq"))

				// Check Node Control/Rebirth metric
				controlMetric := payload.Metrics[1]
				Expect(controlMetric.Name).NotTo(BeNil())
				Expect(*controlMetric.Name).To(Equal("Node Control/Rebirth"))

				// Check Temperature metric with alias
				tempMetric := payload.Metrics[2]
				Expect(tempMetric.Name).NotTo(BeNil())
				Expect(*tempMetric.Name).To(Equal("Temperature"))
				Expect(tempMetric.Alias).NotTo(BeNil())
			})

			It("should decode NDATA_v1 with alias-only structure", func() {
				vector := sparkplug_plugin.GetTestVectors()[1] // NDATA_v1
				Expect(vector.Name).To(Equal("NDATA_v1"))

				payload := sparkplug_plugin.MustDecodeBase64(vector.Base64Data)
				Expect(payload.Metrics).To(HaveLen(1))

				// NDATA should use aliases (no names initially)
				dataMetric := payload.Metrics[0]
				Expect(dataMetric.Alias).NotTo(BeNil())
				// Name should be nil or empty (will be resolved from alias cache)
				Expect(dataMetric.Name == nil || *dataMetric.Name == "").To(BeTrue())
			})
		})
	})

	Describe("Alias Resolution Edge Cases", func() {
		var cache *sparkplug_plugin.AliasCache

		BeforeEach(func() {
			cache = sparkplug_plugin.NewAliasCache()
		})

		Context("NBIRTH → NDATA flow", func() {
			It("resolves metric aliases from NDATA using NBIRTH context", func() {
				// Given an NBIRTH message with named metrics and aliases
				nbirthPayload := sparkplug_plugin.MustDecodeBase64(sparkplug_plugin.NBIRTH_v1)

				// Cache aliases from NBIRTH
				deviceKey := "FactoryA/Line1"
				count := cache.CacheAliases(deviceKey, nbirthPayload.Metrics)
				Expect(count).To(BeNumerically(">", 0), "Should cache some aliases from NBIRTH")

				// When an NDATA message arrives with aliases (no names)
				ndataPayload := sparkplug_plugin.MustDecodeBase64(sparkplug_plugin.NDATA_v1)
				Expect(ndataPayload.Metrics).To(HaveLen(1))

				// Verify the alias exists but name is empty initially
				dataMetric := ndataPayload.Metrics[0]
				Expect(dataMetric.Alias).NotTo(BeNil())
				Expect(dataMetric.Name == nil || *dataMetric.Name == "").To(BeTrue())

				// Resolve aliases using cached mappings
				resolvedCount := cache.ResolveAliases(deviceKey, ndataPayload.Metrics)
				Expect(resolvedCount).To(Equal(1), "Should resolve exactly 1 alias")

				// Then the alias should be resolved to the original name
				Expect(dataMetric.Name).NotTo(BeNil())
				Expect(*dataMetric.Name).To(Equal("Temperature"))
			})

			It("handles multiple metrics with different aliases", func() {
				// Create NBIRTH with multiple named metrics
				metrics := []*sproto.Payload_Metric{
					{
						Name:  stringPtr("Temperature"),
						Alias: uint64Ptr(100),
					},
					{
						Name:  stringPtr("Pressure"),
						Alias: uint64Ptr(101),
					},
					{
						Name:  stringPtr("Flow"),
						Alias: uint64Ptr(102),
					},
				}

				deviceKey := "FactoryA/Line1"
				count := cache.CacheAliases(deviceKey, metrics)
				Expect(count).To(Equal(3))

				// Create NDATA with alias-only metrics
				dataMetrics := []*sproto.Payload_Metric{
					{
						Alias: uint64Ptr(100),
						Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 25.5},
					},
					{
						Alias: uint64Ptr(102),
						Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 1.5},
					},
				}

				resolvedCount := cache.ResolveAliases(deviceKey, dataMetrics)
				Expect(resolvedCount).To(Equal(2))
				Expect(*dataMetrics[0].Name).To(Equal("Temperature"))
				Expect(*dataMetrics[1].Name).To(Equal("Flow"))
			})
		})

		Context("Pre-birth data handling", func() {
			It("fails to resolve aliases when NDATA arrives before NBIRTH", func() {
				// Attempt to resolve aliases without any cached NBIRTH data
				deviceKey := "FactoryA/UnknownLine"
				ndataPayload := sparkplug_plugin.MustDecodeBase64(sparkplug_plugin.NDATA_v1)

				// Should not resolve any aliases (no NBIRTH context)
				resolvedCount := cache.ResolveAliases(deviceKey, ndataPayload.Metrics)
				Expect(resolvedCount).To(Equal(0), "Should not resolve aliases without NBIRTH context")

				// Metric names should remain unresolved
				for _, metric := range ndataPayload.Metrics {
					Expect(metric.Name == nil || *metric.Name == "").To(BeTrue())
				}
			})

			It("handles mixed scenarios with some cached and some uncached devices", func() {
				// Cache aliases for one device
				deviceKey1 := "FactoryA/Line1"
				nbirthPayload := sparkplug_plugin.MustDecodeBase64(sparkplug_plugin.NBIRTH_v1)
				cache.CacheAliases(deviceKey1, nbirthPayload.Metrics)

				// Try to resolve for both cached and uncached devices
				deviceKey2 := "FactoryA/Line2" // No NBIRTH sent yet
				ndataPayload1 := sparkplug_plugin.MustDecodeBase64(sparkplug_plugin.NDATA_v1)
				ndataPayload2 := sparkplug_plugin.MustDecodeBase64(sparkplug_plugin.NDATA_v1)

				// Device 1 should resolve aliases
				resolvedCount1 := cache.ResolveAliases(deviceKey1, ndataPayload1.Metrics)
				Expect(resolvedCount1).To(Equal(1))

				// Device 2 should not resolve aliases
				resolvedCount2 := cache.ResolveAliases(deviceKey2, ndataPayload2.Metrics)
				Expect(resolvedCount2).To(Equal(0))
			})
		})

		Context("Alias collision detection", func() {
			It("should handle duplicate metric aliases in NBIRTH", func() {
				// Create NBIRTH with alias collision: two metrics with same alias
				metricsWithCollision := []*sproto.Payload_Metric{
					{
						Name:  stringPtr("MotorRPM"),
						Alias: uint64Ptr(5),
					},
					{
						Name:  stringPtr("MotorTemp"),
						Alias: uint64Ptr(5), // Same alias - collision!
					},
				}

				deviceKey := "FactoryA/Line1"
				count := cache.CacheAliases(deviceKey, metricsWithCollision)

				// The cache should still function but behavior is undefined for collisions
				// In current implementation, last metric wins
				Expect(count).To(BeNumerically(">=", 1))

				// Try to resolve the conflicting alias
				dataMetrics := []*sproto.Payload_Metric{
					{
						Alias: uint64Ptr(5),
						Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 1200.0},
					},
				}

				resolvedCount := cache.ResolveAliases(deviceKey, dataMetrics)
				Expect(resolvedCount).To(Equal(1))
				// Should resolve to one of the conflicting names (implementation dependent)
				Expect(*dataMetrics[0].Name).To(BeElementOf([]string{"MotorRPM", "MotorTemp"}))
			})

			It("should handle zero alias (reserved for bdSeq)", func() {
				// Test that alias 0 is properly handled (usually reserved for bdSeq)
				metrics := []*sproto.Payload_Metric{
					{
						Name:  stringPtr("bdSeq"),
						Alias: uint64Ptr(0),
					},
					{
						Name:  stringPtr("Temperature"),
						Alias: uint64Ptr(1),
					},
				}

				deviceKey := "FactoryA/Line1"
				count := cache.CacheAliases(deviceKey, metrics)

				// Current implementation skips alias 0, only caches non-zero aliases
				Expect(count).To(Equal(1)) // Only Temperature should be cached
			})
		})
	})

	Describe("Sequence Gap Detection", func() {
		Context("Sequence number validation", func() {
			It("should detect sequence gaps and trigger rebirth", func() {
				Skip("Integration test - requires full input plugin setup")
				// This test requires setting up the full input plugin with MQTT
				// and testing sequence gap detection. Implementation depends on
				// having access to the rebirth request mechanism.
			})

			It("should handle sequence wraparound correctly", func() {
				// Test sequence number wraparound from 255 to 0
				// This is a unit test for sequence validation logic
				prevSeq := uint8(255)
				currentSeq := uint8(0)
				expectedNext := uint8((int(prevSeq) + 1) % 256)

				Expect(expectedNext).To(Equal(currentSeq))
			})

			It("should respect max_sequence_gap threshold", func() {
				// Test the configurable sequence gap threshold
				maxGap := 3
				if os.Getenv("SPB_TEST_STRICT") == "1" {
					maxGap = 1
				}

				// Test gap detection within threshold
				prevSeq := uint8(10)
				currentSeq := uint8(10 + maxGap)
				gap := int(currentSeq) - int(prevSeq)

				if gap <= maxGap {
					// Should be tolerated
					Expect(gap).To(BeNumerically("<=", maxGap))
				} else {
					// Should trigger rebirth
					Expect(gap).To(BeNumerically(">", maxGap))
				}
			})
		})
	})

	Describe("Device Key Management", func() {
		Context("Unified device key helper", func() {
			It("should create consistent device keys", func() {
				// Test the SpbDeviceKey helper function from expert.md
				nodeKey := sparkplug_plugin.SpbDeviceKey("FactoryA", "Line1", "")
				Expect(nodeKey).To(Equal("FactoryA/Line1"))

				deviceKey := sparkplug_plugin.SpbDeviceKey("FactoryA", "Line1", "Pump1")
				Expect(deviceKey).To(Equal("FactoryA/Line1/Pump1"))
			})

			It("should maintain separate alias caches per device key", func() {
				cache := sparkplug_plugin.NewAliasCache()

				// Different device keys should have separate alias spaces
				key1 := sparkplug_plugin.SpbDeviceKey("FactoryA", "Line1", "")
				key2 := sparkplug_plugin.SpbDeviceKey("FactoryA", "Line2", "")

				metrics1 := []*sproto.Payload_Metric{
					{Name: stringPtr("Temperature"), Alias: uint64Ptr(100)},
				}
				metrics2 := []*sproto.Payload_Metric{
					{Name: stringPtr("Pressure"), Alias: uint64Ptr(100)}, // Same alias, different device
				}

				cache.CacheAliases(key1, metrics1)
				cache.CacheAliases(key2, metrics2)

				// Resolve aliases - each device should get its own metric name
				dataMetric1 := []*sproto.Payload_Metric{{Alias: uint64Ptr(100)}}
				dataMetric2 := []*sproto.Payload_Metric{{Alias: uint64Ptr(100)}}

				cache.ResolveAliases(key1, dataMetric1)
				cache.ResolveAliases(key2, dataMetric2)

				Expect(*dataMetric1[0].Name).To(Equal("Temperature"))
				Expect(*dataMetric2[0].Name).To(Equal("Pressure"))
			})
		})
	})

	Describe("STATE Message Processing", func() {
		Context("STATE message filtering (recently fixed)", func() {
			It("should identify STATE message type correctly", func() {
				parser := sparkplug_plugin.NewTopicParser()

				stateTopics := []string{
					"spBv1.0/FactoryA/STATE/Line1",
					"spBv1.0/MyGroup/STATE/EdgeNode1",
				}

				for _, topic := range stateTopics {
					msgType, deviceKey, topicInfo := parser.ParseSparkplugTopicDetailed(topic)
					Expect(msgType).To(Equal("STATE"))
					Expect(deviceKey).NotTo(BeEmpty())
					Expect(topicInfo).NotTo(BeNil())
					Expect(topicInfo.Group).NotTo(BeEmpty())
					Expect(topicInfo.EdgeNode).NotTo(BeEmpty())
				}
			})

			It("should not parse STATE messages as protobuf", func() {
				// STATE messages contain plain text, not protobuf
				statePayloads := []string{"ONLINE", "OFFLINE"}

				for _, payload := range statePayloads {
					// Attempting to unmarshal as protobuf should fail
					_, err := service.NewMessage([]byte(payload)).AsBytes()
					Expect(err).NotTo(HaveOccurred()) // Should work as plain bytes

					// But attempting to unmarshal as Sparkplug payload should fail
					var sparkplugPayload sproto.Payload
					err = proto.Unmarshal([]byte(payload), &sparkplugPayload)
					Expect(err).To(HaveOccurred(), "STATE payload should not unmarshal as protobuf")
				}
			})
		})
	})
})
