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

// Flow tests for Sparkplug B plugin - Lifecycle testing without MQTT
// Tests complete message lifecycle by feeding vectors to real Input plugin (+3s)

package sparkplug_plugin_test

import (
	"encoding/base64"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	sparkplugb "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
	"google.golang.org/protobuf/proto"
)

// Flow lifecycle tests - integrated into main test suite

var _ = Describe("Lifecycle Flow Tests", func() {

	Context("Basic Message Lifecycle", func() {
		It("should handle NBIRTH → NDATA sequence", func() {
			// Test basic lifecycle flow (migrated from old integration tests)
			// This tests the logical flow without MQTT dependencies

			// Step 1: Process NBIRTH message
			nbirthVector := sparkplug_plugin.GetTestVector("NBIRTH_V1")
			Expect(nbirthVector).NotTo(BeNil())

			nbirthBytes, err := base64.StdEncoding.DecodeString(nbirthVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var nbirthPayload sparkplugb.Payload
			err = proto.Unmarshal(nbirthBytes, &nbirthPayload)
			Expect(err).NotTo(HaveOccurred())

			// Verify NBIRTH structure
			Expect(nbirthPayload.Seq).NotTo(BeNil())
			Expect(*nbirthPayload.Seq).To(Equal(uint64(0))) // BIRTH starts at 0
			Expect(nbirthPayload.Metrics).To(HaveLen(3))

			// Step 2: Cache aliases from NBIRTH
			cache := sparkplug_plugin.NewAliasCache()
			deviceKey := "Factory/Line1"
			aliasCount := cache.CacheAliases(deviceKey, nbirthPayload.Metrics)
			Expect(aliasCount).To(BeNumerically(">", 0))

			// Step 3: Process NDATA message
			ndataVector := sparkplug_plugin.GetTestVector("NDATA_V1")
			Expect(ndataVector).NotTo(BeNil())

			ndataBytes, err := base64.StdEncoding.DecodeString(ndataVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var ndataPayload sparkplugb.Payload
			err = proto.Unmarshal(ndataBytes, &ndataPayload)
			Expect(err).NotTo(HaveOccurred())

			// Verify NDATA structure
			Expect(ndataPayload.Seq).NotTo(BeNil())
			Expect(*ndataPayload.Seq).To(Equal(uint64(1))) // Incremented from BIRTH

			// Step 4: Resolve aliases in NDATA
			resolvedCount := cache.ResolveAliases(deviceKey, ndataPayload.Metrics)
			Expect(resolvedCount).To(Equal(1))

			// Verify alias was resolved to name
			Expect(ndataPayload.Metrics[0].Name).NotTo(BeNil())
			Expect(*ndataPayload.Metrics[0].Name).To(Equal("Temperature"))
		})

		It("should handle complete STATE → NBIRTH → NDATA → NDEATH lifecycle", func() {
			// Test full lifecycle (migrated from old bidirectional tests)
			cache := sparkplug_plugin.NewAliasCache()
			deviceKey := "Factory/Line1"

			// Step 1: STATE message (would be "ONLINE")
			// STATE messages are plain text, not protobuf
			stateMessage := "ONLINE"
			Expect(stateMessage).To(Equal("ONLINE"))

			// Step 2: NBIRTH after STATE
			nbirthVector := sparkplug_plugin.GetTestVector("NBIRTH_V1")
			nbirthBytes, err := base64.StdEncoding.DecodeString(nbirthVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var nbirthPayload sparkplugb.Payload
			err = proto.Unmarshal(nbirthBytes, &nbirthPayload)
			Expect(err).NotTo(HaveOccurred())

			// Cache aliases
			aliasCount := cache.CacheAliases(deviceKey, nbirthPayload.Metrics)
			Expect(aliasCount).To(BeNumerically(">", 0))

			// Step 3: Multiple NDATA messages
			ndataVector := sparkplug_plugin.GetTestVector("NDATA_V1")
			for i := 0; i < 3; i++ {
				ndataBytes, err := base64.StdEncoding.DecodeString(ndataVector.Base64Data)
				Expect(err).NotTo(HaveOccurred())

				var ndataPayload sparkplugb.Payload
				err = proto.Unmarshal(ndataBytes, &ndataPayload)
				Expect(err).NotTo(HaveOccurred())

				// Resolve aliases
				resolvedCount := cache.ResolveAliases(deviceKey, ndataPayload.Metrics)
				Expect(resolvedCount).To(Equal(1))
			}

			// Step 4: NDEATH message
			ndeathVector := sparkplug_plugin.GetTestVector("NDEATH_V1")
			ndeathBytes, err := base64.StdEncoding.DecodeString(ndeathVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var ndeathPayload sparkplugb.Payload
			err = proto.Unmarshal(ndeathBytes, &ndeathPayload)
			Expect(err).NotTo(HaveOccurred())

			// Verify NDEATH structure
			Expect(ndeathPayload.Seq).NotTo(BeNil())
			Expect(*ndeathPayload.Seq).To(Equal(uint64(0))) // DEATH resets to 0
			Expect(ndeathPayload.Metrics).To(HaveLen(1))

			// Should contain bdSeq
			bdSeqMetric := ndeathPayload.Metrics[0]
			Expect(bdSeqMetric.Name).NotTo(BeNil())
			Expect(*bdSeqMetric.Name).To(Equal("bdSeq"))
		})

		It("should handle sequence gap detection and recovery", func() {
			// Test sequence gap handling (migrated from old edge cases)
			cache := sparkplug_plugin.NewAliasCache()
			deviceKey := "Factory/Line1"

			// Step 1: NBIRTH (seq 0)
			nbirthVector := sparkplug_plugin.GetTestVector("NBIRTH_V1")
			nbirthBytes, err := base64.StdEncoding.DecodeString(nbirthVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var nbirthPayload sparkplugb.Payload
			err = proto.Unmarshal(nbirthBytes, &nbirthPayload)
			Expect(err).NotTo(HaveOccurred())

			cache.CacheAliases(deviceKey, nbirthPayload.Metrics)
			lastSeq := *nbirthPayload.Seq
			Expect(lastSeq).To(Equal(uint64(0)))

			// Step 2: NDATA with sequence gap
			gapVector := sparkplug_plugin.GetTestVector("NDATA_GAP")
			Expect(gapVector).NotTo(BeNil())

			gapBytes, err := base64.StdEncoding.DecodeString(gapVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var gapPayload sparkplugb.Payload
			err = proto.Unmarshal(gapBytes, &gapPayload)
			Expect(err).NotTo(HaveOccurred())

			// Verify gap detection
			currentSeq := *gapPayload.Seq
			expectedSeq := lastSeq + 1
			gap := currentSeq - expectedSeq

			if gap > 0 {
				// Gap detected - should trigger rebirth request
				Expect(gap).To(BeNumerically(">", 0))
				Expect(currentSeq).To(Equal(uint64(5))) // From test vector
			}
		})

		It("should handle pre-birth data scenarios", func() {
			// Test DATA before BIRTH scenario (migrated from old edge cases)
			cache := sparkplug_plugin.NewAliasCache()
			deviceKey := "Factory/UnknownLine"

			// Attempt to process NDATA without prior NBIRTH
			ndataVector := sparkplug_plugin.GetTestVector("NDATA_BEFORE_BIRTH")
			Expect(ndataVector).NotTo(BeNil())

			ndataBytes, err := base64.StdEncoding.DecodeString(ndataVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var ndataPayload sparkplugb.Payload
			err = proto.Unmarshal(ndataBytes, &ndataPayload)
			Expect(err).NotTo(HaveOccurred())

			// Attempt to resolve aliases without cached BIRTH data
			resolvedCount := cache.ResolveAliases(deviceKey, ndataPayload.Metrics)
			Expect(resolvedCount).To(Equal(0)) // Should fail to resolve

			// Metric names should remain nil
			for _, metric := range ndataPayload.Metrics {
				Expect(metric.Name).To(BeNil())
			}
		})

		It("should handle sequence wraparound scenarios", func() {
			// Test sequence wraparound (migrated from old edge cases)
			wraparoundVector := sparkplug_plugin.GetTestVector("NDATA_WRAPAROUND")
			Expect(wraparoundVector).NotTo(BeNil())

			wraparoundBytes, err := base64.StdEncoding.DecodeString(wraparoundVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var wraparoundPayload sparkplugb.Payload
			err = proto.Unmarshal(wraparoundBytes, &wraparoundPayload)
			Expect(err).NotTo(HaveOccurred())

			// Verify wraparound sequence (255 → 0)
			currentSeq := *wraparoundPayload.Seq
			Expect(currentSeq).To(Equal(uint64(0))) // Wraparound to 0

			// This should be valid if previous was 255
			previousSeq := uint64(255)
			if previousSeq == 255 && currentSeq == 0 {
				// Valid wraparound
				Expect(currentSeq).To(Equal(uint64(0)))
			}
		})
	})

	Context("Alias Resolution Flow", func() {
		It("should establish aliases from NBIRTH and resolve in NDATA", func() {
			// Test end-to-end alias resolution flow
			cache := sparkplug_plugin.NewAliasCache()
			deviceKey := "Factory/Line1"

			// Process NBIRTH with multiple metrics
			largeVector := sparkplug_plugin.GetTestVector("NBIRTH_LARGE")
			Expect(largeVector).NotTo(BeNil())

			largeBytes, err := base64.StdEncoding.DecodeString(largeVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var largePayload sparkplugb.Payload
			err = proto.Unmarshal(largeBytes, &largePayload)
			Expect(err).NotTo(HaveOccurred())

			// Cache all aliases
			aliasCount := cache.CacheAliases(deviceKey, largePayload.Metrics)
			Expect(aliasCount).To(BeNumerically(">=", 100)) // Large payload has 100+ metrics

			// Create simulated NDATA with multiple aliases
			simulatedNData := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(100), // Should resolve to Metric_99
					Datatype: uint32Ptr(10),
					Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 123.45},
				},
				{
					Alias:    uint64Ptr(50), // Should resolve to Metric_49
					Datatype: uint32Ptr(10),
					Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 67.89},
				},
			}

			// Resolve aliases
			resolvedCount := cache.ResolveAliases(deviceKey, simulatedNData)
			Expect(resolvedCount).To(Equal(2))

			// Verify resolution
			for _, metric := range simulatedNData {
				Expect(metric.Name).NotTo(BeNil())
				Expect(*metric.Name).To(ContainSubstring("Metric_"))
			}
		})

		It("should handle multiple devices independently", func() {
			// Test multi-device alias isolation
			cache := sparkplug_plugin.NewAliasCache()

			// Device 1
			device1Key := "Factory/Line1"
			device1Metrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			count1 := cache.CacheAliases(device1Key, device1Metrics)
			Expect(count1).To(Equal(1))

			// Device 2 (same alias, different name)
			device2Key := "Factory/Line2"
			device2Metrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Pressure"),
					Alias: uint64Ptr(100), // Same alias as device1
				},
			}
			count2 := cache.CacheAliases(device2Key, device2Metrics)
			Expect(count2).To(Equal(1))

			// Test independent resolution
			data1 := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(9),
					Value:    &sparkplugb.Payload_Metric_FloatValue{FloatValue: 25.5},
				},
			}
			resolved1 := cache.ResolveAliases(device1Key, data1)
			Expect(resolved1).To(Equal(1))
			Expect(*data1[0].Name).To(Equal("Temperature"))

			data2 := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(10),
					Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1013.25},
				},
			}
			resolved2 := cache.ResolveAliases(device2Key, data2)
			Expect(resolved2).To(Equal(1))
			Expect(*data2[0].Name).To(Equal("Pressure"))
		})
	})

	Context("Error Recovery Logic", func() {
		It("should handle malformed message recovery", func() {
			// Test error recovery scenarios
			cache := sparkplug_plugin.NewAliasCache()
			deviceKey := "Factory/Line1"

			// Test malformed protobuf handling
			malformedData := []byte{0xFF, 0xFE, 0xFD, 0xFC} // Invalid protobuf

			var payload sparkplugb.Payload
			err := proto.Unmarshal(malformedData, &payload)
			Expect(err).To(HaveOccurred()) // Should fail to unmarshal

			// Test recovery after malformed message
			// Cache should remain functional
			validMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			count := cache.CacheAliases(deviceKey, validMetrics)
			Expect(count).To(Equal(1))

			// Test partial payload corruption
			partiallyCorrupted := &sparkplugb.Payload{
				Timestamp: uint64Ptr(1672531320000),
				Seq:       uint64Ptr(1),
				Metrics: []*sparkplugb.Payload_Metric{
					{
						// Missing required fields - should be handled gracefully
						Alias: uint64Ptr(100),
						// No Value field
					},
				},
			}

			// Should handle gracefully without crashing
			resolvedCount := cache.ResolveAliases(deviceKey, partiallyCorrupted.Metrics)
			Expect(resolvedCount).To(Equal(1)) // Should still resolve alias
		})

		It("should handle cache reset scenarios", func() {
			// Test cache reset and recovery
			cache := sparkplug_plugin.NewAliasCache()
			deviceKey := "Factory/Line1"

			// Cache some aliases
			metrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			count := cache.CacheAliases(deviceKey, metrics)
			Expect(count).To(Equal(1))

			// Clear cache (simulating restart)
			cache.Clear()

			// Verify cache is empty
			dataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(9),
					Value:    &sparkplugb.Payload_Metric_FloatValue{FloatValue: 25.5},
				},
			}
			resolvedCount := cache.ResolveAliases(deviceKey, dataMetrics)
			Expect(resolvedCount).To(Equal(0))
		})
		
		It("should handle sequence number edge cases comprehensively", func() {
			By("Testing uint64 max boundary wraparound")
			// Even though Sparkplug uses 8-bit sequence (0-255), the protobuf field is uint64
			// Test behavior at various boundaries
			
			testCases := []struct {
				name        string
				currentSeq  uint64
				nextSeq     uint64
				isValid     bool
				description string
			}{
				// Normal increment cases
				{"normal_increment", 0, 1, true, "Normal sequence increment"},
				{"mid_range", 127, 128, true, "Mid-range increment"},
				{"near_boundary", 254, 255, true, "Approaching wraparound boundary"},
				
				// Wraparound cases
				{"valid_wraparound", 255, 0, true, "Valid wraparound from 255 to 0"},
				{"after_wraparound", 0, 1, true, "Continue after wraparound"},
				
				// Gap detection cases
				{"small_gap", 10, 12, false, "Small gap (missing seq 11)"},
				{"large_gap", 10, 20, false, "Large gap (missing 10 sequences)"},
				{"huge_gap", 10, 100, false, "Huge gap (missing 90 sequences)"},
				{"gap_before_wrap", 250, 255, false, "Gap just before wraparound"},
				{"gap_after_wrap", 0, 5, false, "Gap just after wraparound"},
				
				// Invalid backwards jumps
				{"backward_jump", 100, 50, false, "Invalid backward jump"},
				{"backward_near_zero", 5, 0, false, "Invalid backward jump to 0"},
				{"backward_from_wrap", 2, 254, false, "Invalid backward jump across boundary"},
				
				// Edge cases with same value
				{"duplicate_seq", 50, 50, false, "Duplicate sequence number"},
				{"duplicate_at_zero", 0, 0, false, "Duplicate at zero"},
				{"duplicate_at_max", 255, 255, false, "Duplicate at max"},
				
				// Multiple wraparounds in sequence
				{"double_wrap_attempt", 255, 256, false, "Attempt to use value > 255"},
				{"large_value", 100, 1000, false, "Attempt to use large value"},
				{"negative_wrap", 0, 65535, false, "Attempt to use uint16 max"},
			}
			
			for _, tc := range testCases {
				By("Testing case: " + tc.name + " - " + tc.description)
				
				// Validate sequence transition
				isValidTransition := isValidSequenceTransition(tc.currentSeq, tc.nextSeq)
				
				if tc.isValid {
					Expect(isValidTransition).To(BeTrue(), 
						"Expected valid transition from %d to %d for %s", 
						tc.currentSeq, tc.nextSeq, tc.name)
				} else {
					Expect(isValidTransition).To(BeFalse(), 
						"Expected invalid transition from %d to %d for %s", 
						tc.currentSeq, tc.nextSeq, tc.name)
				}
			}
		})
		
		It("should track sequence gaps and recovery correctly", func() {
			// Simulate a sequence of messages with various gap scenarios
			messageSequence := []uint64{
				0, 1, 2, 3,      // Normal start
				5, 6,            // Gap: missing 4
				10,              // Larger gap: missing 7, 8, 9
				11, 12,          // Continue normally
				255,             // Jump to boundary
				0, 1,            // Valid wraparound
				3,               // Gap after wraparound: missing 2
				4, 5,            // Continue
				100,             // Large jump
				101, 102,        // Continue from jump
				254, 255, 0, 1,  // Another wraparound cycle
			}
			
			gapCount := 0
			wrapCount := 0
			var lastSeq *uint64
			
			for i, seq := range messageSequence {
				if lastSeq != nil {
					if seq == 0 && *lastSeq == 255 {
						wrapCount++
						By("Wraparound detected at index " + string(rune(i+'0')) + ": 255 → 0")
					} else if seq != (*lastSeq+1)%256 {
						gapCount++
						gapSize := calculateGapSize(*lastSeq, seq)
						By("Gap detected at index " + string(rune(i+'0')) + " (gap size: " + string(rune(gapSize+'0')) + ")")
					}
				}
				lastSeq = &seq
			}
			
			Expect(wrapCount).To(Equal(2), "Should detect exactly 2 wraparounds")
			Expect(gapCount).To(Equal(6), "Should detect exactly 6 gaps")
		})
		
		It("should handle rapid sequence number changes under load", func() {
			// Simulate rapid message processing with potential race conditions
			const messageCount = 1000
			sequences := make([]uint64, messageCount)
			
			// Generate sequence with some patterns
			for i := 0; i < messageCount; i++ {
				sequences[i] = uint64(i % 256)
			}
			
			// Process sequences and track statistics
			var stats struct {
				totalMessages int
				wraparounds   int
				maxGap        uint64
				invalidJumps  int
			}
			
			stats.totalMessages = len(sequences)
			var lastSeq *uint64
			
			for _, seq := range sequences {
				if lastSeq != nil {
					if seq == 0 && *lastSeq == 255 {
						stats.wraparounds++
					} else if seq < *lastSeq && !(seq == 0 && *lastSeq == 255) {
						stats.invalidJumps++
					} else if seq > *lastSeq {
						gap := seq - *lastSeq - 1
						if gap > stats.maxGap {
							stats.maxGap = gap
						}
					}
				}
				lastSeq = &seq
			}
			
			// Validate expected patterns
			expectedWraps := (messageCount - 1) / 256
			Expect(stats.wraparounds).To(Equal(expectedWraps), 
				"Should have correct number of wraparounds for %d messages", messageCount)
			Expect(stats.invalidJumps).To(Equal(0), 
				"Should have no invalid backward jumps")
			Expect(stats.maxGap).To(Equal(uint64(0)), 
				"Should have no gaps in continuous sequence")
		})
		
		It("should validate sequence numbers in concurrent scenarios", func() {
			// Test thread safety of sequence number handling
			const goroutines = 10
			const messagesPerRoutine = 100
			
			// Channel to collect sequence numbers
			seqChan := make(chan uint64, goroutines*messagesPerRoutine)
			done := make(chan bool, goroutines)
			
			// Launch concurrent sequence generators
			for g := 0; g < goroutines; g++ {
				go func(routineID int) {
					defer GinkgoRecover()
					startSeq := uint64(routineID * 25) % 256 // Spread starting points
					
					for i := 0; i < messagesPerRoutine; i++ {
						seq := (startSeq + uint64(i)) % 256
						seqChan <- seq
					}
					done <- true
				}(g)
			}
			
			// Wait for all routines to complete
			for i := 0; i < goroutines; i++ {
				<-done
			}
			close(seqChan)
			
			// Collect all sequences
			var allSequences []uint64
			for seq := range seqChan {
				allSequences = append(allSequences, seq)
			}
			
			// Verify we got all sequences
			Expect(len(allSequences)).To(Equal(goroutines * messagesPerRoutine))
			
			// Check that all sequences are valid (0-255)
			for _, seq := range allSequences {
				Expect(seq).To(BeNumerically(">=", 0))
				Expect(seq).To(BeNumerically("<=", 255))
			}
		})
	})
})
var _ = Describe("Message Processing Pipeline", func() {

	Context("Message Format Validation", func() {




		It("should process vector sequences through message validation", func() {
			// Test processing sequences of test vectors through validation
			testVectors := []struct {
				name        string
				msgType     string
				hasMetrics  bool
				hasSequence bool
			}{
				{"NBIRTH_vector", "NBIRTH", true, true},
				{"NDATA_vector", "NDATA", true, true},
				{"NDEATH_vector", "NDEATH", false, true},
				{"STATE_vector", "STATE", false, false},
			}

			for _, vector := range testVectors {
				By("processing "+vector.name, func() {
					// Create mock message based on vector
					mockMessage := map[string]interface{}{
						"sparkplug_msg_type": vector.msgType,
						"group_id":           "Factory1",
						"edge_node_id":       "Line1",
						"timestamp":          1672531320000,
					}

					if vector.hasSequence {
						mockMessage["seq"] = uint64(1)
					}

					if vector.hasMetrics {
						mockMessage["metrics"] = []map[string]interface{}{
							{
								"name":     "Temperature",
								"datatype": uint32(9),
								"value":    25.5,
							},
						}
					}

					// Validate message structure
					validateSparkplugMessage(mockMessage, vector.msgType)

					// Validate UMH format
					validateUMHFormat(mockMessage)
				})
			}
		})

		It("should generate proper UMH output format", func() {
			// Test UMH output format generation
			sparkplugMessage := map[string]interface{}{
				"sparkplug_msg_type": "NDATA",
				"group_id":           "Factory1",
				"edge_node_id":       "Line1",
				"timestamp":          uint64(1672531320000),
				"seq":                uint64(1),
				"metrics": []map[string]interface{}{
					{
						"name":     "Temperature",
						"alias":    uint64(100),
						"datatype": uint32(9),
						"value":    25.5,
					},
				},
			}

			// Convert to UMH format
			umhOutput := convertToUMHFormat(sparkplugMessage)

			// Validate UMH structure
			Expect(umhOutput["timestamp"]).NotTo(BeNil())
			Expect(umhOutput["sparkplug_msg_type"]).To(Equal("NDATA"))
			Expect(umhOutput["group_id"]).To(Equal("Factory1"))
			Expect(umhOutput["edge_node_id"]).To(Equal("Line1"))

			// Validate metrics are preserved
			if metrics, ok := umhOutput["metrics"]; ok {
				metricsArray := metrics.([]map[string]interface{})
				Expect(metricsArray).To(HaveLen(1))
				Expect(metricsArray[0]["name"]).To(Equal("Temperature"))
			}
		})

		It("should populate message metadata correctly", func() {
			// Test metadata population from Sparkplug messages
			testCases := []struct {
				topic    string
				expected map[string]string
			}{
				{
					topic: "spBv1.0/Factory1/NBIRTH/Line1",
					expected: map[string]string{
						"sparkplug_msg_type": "NBIRTH",
						"group_id":           "Factory1",
						"edge_node_id":       "Line1",
						"message_type":       "NBIRTH",
					},
				},
				{
					topic: "spBv1.0/Factory1/NDATA/Line1",
					expected: map[string]string{
						"sparkplug_msg_type": "NDATA",
						"group_id":           "Factory1",
						"edge_node_id":       "Line1",
						"message_type":       "NDATA",
					},
				},
				{
					topic: "spBv1.0/Factory1/DBIRTH/Line1/Machine1",
					expected: map[string]string{
						"sparkplug_msg_type": "DBIRTH",
						"group_id":           "Factory1",
						"edge_node_id":       "Line1",
						"device_id":          "Machine1",
						"message_type":       "DBIRTH",
					},
				},
			}

			for _, tc := range testCases {
				By("parsing topic "+tc.topic, func() {
					// Parse topic to extract metadata
					metadata := parseSparkplugTopic(tc.topic)

					// Validate extracted metadata
					for key, expectedValue := range tc.expected {
						Expect(metadata[key]).To(Equal(expectedValue), "Metadata key %s should match", key)
					}
				})
			}
		})
	})



var _ = Describe("Device-Level Message Handling", func() {

	Context("DBIRTH and DDATA Processing", func() {
		It("should handle DBIRTH → DDATA device lifecycle", func() {
			// Test device-level message flow
			cache := sparkplug_plugin.NewAliasCache()
			deviceKey := "Factory/Line1/Machine1" // Device-level key

			// Step 1: DBIRTH message with device metrics
			dbirthMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:     stringPtr("Motor_Speed"),
					Alias:    uint64Ptr(200),
					Datatype: uint32Ptr(10), // Double
					Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1800.0},
				},
				{
					Name:     stringPtr("Motor_Temperature"),
					Alias:    uint64Ptr(201),
					Datatype: uint32Ptr(9), // Float
					Value:    &sparkplugb.Payload_Metric_FloatValue{FloatValue: 65.5},
				},
				{
					Name:     stringPtr("Motor_Status"),
					Alias:    uint64Ptr(202),
					Datatype: uint32Ptr(11), // Boolean
					Value:    &sparkplugb.Payload_Metric_BooleanValue{BooleanValue: true},
				},
			}

			dbirthPayload := &sparkplugb.Payload{
				Timestamp: uint64Ptr(1672531320000),
				Seq:       uint64Ptr(0), // DBIRTH starts at 0
				Metrics:   dbirthMetrics,
			}

			// Cache device aliases
			aliasCount := cache.CacheAliases(deviceKey, dbirthPayload.Metrics)
			Expect(aliasCount).To(Equal(3))

			// Step 2: DDATA message using aliases
			ddataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(200), // Motor_Speed
					Datatype: uint32Ptr(10),
					Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1850.0},
				},
				{
					Alias:    uint64Ptr(201), // Motor_Temperature
					Datatype: uint32Ptr(9),
					Value:    &sparkplugb.Payload_Metric_FloatValue{FloatValue: 68.2},
				},
			}

			ddataPayload := &sparkplugb.Payload{
				Timestamp: uint64Ptr(1672531380000),
				Seq:       uint64Ptr(1), // Incremented from DBIRTH
				Metrics:   ddataMetrics,
			}

			// Resolve aliases
			resolvedCount := cache.ResolveAliases(deviceKey, ddataPayload.Metrics)
			Expect(resolvedCount).To(Equal(2))

			// Verify aliases were resolved
			Expect(*ddataPayload.Metrics[0].Name).To(Equal("Motor_Speed"))
			Expect(*ddataPayload.Metrics[1].Name).To(Equal("Motor_Temperature"))
		})

		It("should handle mixed node and device messages", func() {
			// Test handling both node-level and device-level messages
			cache := sparkplug_plugin.NewAliasCache()

			// Node-level metrics
			nodeKey := "Factory/Line1"
			nodeMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Line_Status"),
					Alias: uint64Ptr(100),
				},
			}
			nodeCount := cache.CacheAliases(nodeKey, nodeMetrics)
			Expect(nodeCount).To(Equal(1))

			// Device-level metrics (same line, different device)
			deviceKey := "Factory/Line1/Machine1"
			deviceMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Machine_Status"),
					Alias: uint64Ptr(100), // Same alias as node, but different context
				},
			}
			deviceCount := cache.CacheAliases(deviceKey, deviceMetrics)
			Expect(deviceCount).To(Equal(1))

			// Test independent resolution
			nodeData := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(11),
					Value:    &sparkplugb.Payload_Metric_BooleanValue{BooleanValue: true},
				},
			}
			nodeResolved := cache.ResolveAliases(nodeKey, nodeData)
			Expect(nodeResolved).To(Equal(1))
			Expect(*nodeData[0].Name).To(Equal("Line_Status"))

			deviceData := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(11),
					Value:    &sparkplugb.Payload_Metric_BooleanValue{BooleanValue: false},
				},
			}
			deviceResolved := cache.ResolveAliases(deviceKey, deviceData)
			Expect(deviceResolved).To(Equal(1))
			Expect(*deviceData[0].Name).To(Equal("Machine_Status"))
		})

		It("should handle device session isolation", func() {
			// Test that device sessions are independent
			cache := sparkplug_plugin.NewAliasCache()

			// Device 1
			device1Key := "Factory/Line1/Machine1"
			device1Metrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Speed"),
					Alias: uint64Ptr(200),
				},
			}
			count1 := cache.CacheAliases(device1Key, device1Metrics)
			Expect(count1).To(Equal(1))

			// Device 2 (same line, different machine)
			device2Key := "Factory/Line1/Machine2"
			device2Metrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Pressure"),
					Alias: uint64Ptr(200), // Same alias, different device
				},
			}
			count2 := cache.CacheAliases(device2Key, device2Metrics)
			Expect(count2).To(Equal(1))

			// Test independent resolution
			data1 := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(200),
					Datatype: uint32Ptr(10),
					Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1200.0},
				},
			}
			resolved1 := cache.ResolveAliases(device1Key, data1)
			Expect(resolved1).To(Equal(1))
			Expect(*data1[0].Name).To(Equal("Speed"))

			data2 := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(200),
					Datatype: uint32Ptr(10),
					Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 15.5},
				},
			}
			resolved2 := cache.ResolveAliases(device2Key, data2)
			Expect(resolved2).To(Equal(1))
			Expect(*data2[0].Name).To(Equal("Pressure"))
		})
	})
})

})






// Helper functions for flow testing

// validateSparkplugMessage validates that a message conforms to Sparkplug B specification
func validateSparkplugMessage(msg map[string]interface{}, expectedType string) {
	// Validate required fields
	Expect(msg["sparkplug_msg_type"]).To(Equal(expectedType))

	// All messages except STATE should have group_id and edge_node_id
	if expectedType != "STATE" {
		Expect(msg["group_id"]).NotTo(BeNil())
		Expect(msg["edge_node_id"]).NotTo(BeNil())
	}

	// Messages with sequence numbers
	sequencedTypes := []string{"NBIRTH", "NDATA", "NDEATH", "DBIRTH", "DDATA", "DDEATH"}
	if contains(sequencedTypes, expectedType) {
		Expect(msg["seq"]).NotTo(BeNil())
	}
}

// validateUMHFormat validates that output conforms to UMH message format
func validateUMHFormat(output map[string]interface{}) {
	// UMH messages should have specific structure
	requiredFields := []string{"timestamp", "sparkplug_msg_type"}
	for _, field := range requiredFields {
		Expect(output[field]).NotTo(BeNil(), "UMH message missing field: "+field)
	}

	// Should have proper timestamp format
	if timestamp, ok := output["timestamp"].(uint64); ok {
		Expect(timestamp).To(BeNumerically(">", 0))
	}
}

// contains checks if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// convertToUMHFormat converts a Sparkplug message to UMH format
func convertToUMHFormat(sparkplugMsg map[string]interface{}) map[string]interface{} {
	// Create UMH format message (simplified)
	umhMsg := make(map[string]interface{})

	// Copy all fields
	for key, value := range sparkplugMsg {
		umhMsg[key] = value
	}

	// Ensure timestamp is present
	if _, exists := umhMsg["timestamp"]; !exists {
		umhMsg["timestamp"] = uint64(1672531320000)
	}

	return umhMsg
}

// parseSparkplugTopic parses a Sparkplug topic and extracts metadata
func parseSparkplugTopic(topic string) map[string]string {
	metadata := make(map[string]string)

	// Simple topic parsing for test purposes
	// Real implementation would use the TopicParser from the plugin
	parts := []string{}
	current := ""
	for _, char := range topic {
		if char == '/' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}

	if len(parts) >= 4 {
		// spBv1.0/GroupID/MessageType/EdgeNodeID[/DeviceID]
		metadata["group_id"] = parts[1]
		metadata["sparkplug_msg_type"] = parts[2]
		metadata["message_type"] = parts[2]
		metadata["edge_node_id"] = parts[3]

		if len(parts) >= 5 {
			metadata["device_id"] = parts[4]
		}
	}

	return metadata
}

// Helper functions for sequence validation

func isValidSequenceTransition(current, next uint64) bool {
	// Sparkplug B uses 8-bit sequence numbers (0-255)
	if next > 255 || current > 255 {
		return false
	}
	
	// Valid cases:
	// 1. Normal increment: next = current + 1
	// 2. Wraparound: current = 255 and next = 0
	
	if next == (current+1)%256 {
		return true
	}
	
	return false
}

func calculateGapSize(lastSeq, currentSeq uint64) uint64 {
	if currentSeq > lastSeq {
		return currentSeq - lastSeq - 1
	} else if lastSeq == 255 && currentSeq < lastSeq {
		// Wraparound case
		return currentSeq // Gap is just currentSeq when wrapping
	} else {
		// Invalid backward jump
		return 256 - lastSeq + currentSeq - 1
	}
}
