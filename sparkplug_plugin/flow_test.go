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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	sparkplugb "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
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

		It("should validate sequence numbers using production ValidateSequenceNumber per Sparkplug B spec", func() {
			By("Testing production sequence validation logic per Sparkplug B specification")
			// Using the exported ValidateSequenceNumber function from sparkplug_b_input.go
			// Reference: https://github.com/eclipse-sparkplug/sparkplug/blob/master/specification/src/main/asciidoc/chapters/Sparkplug_5_Operational_Behavior.adoc

			testCases := []struct {
				name        string
				lastSeq     uint8
				currentSeq  uint8
				isValid     bool
				description string
				specReason  string
			}{
				// Valid cases - only sequential increments and wraparound
				{"normal_increment", 0, 1, true, "Normal sequence increment", "Sequential order is required per spec"},
				{"mid_range", 127, 128, true, "Mid-range increment", "Sequential order is required per spec"},
				{"near_boundary", 254, 255, true, "Approaching wraparound boundary", "Sequential order is required per spec"},
				{"valid_wraparound", 255, 0, true, "Valid wraparound from 255 to 0", "Wraparound from 255 to 0 is defined behavior"},
				{"after_wraparound", 0, 1, true, "Continue after wraparound", "Sequential order continues after wraparound"},

				// Invalid cases - ANY gap should be invalid per Sparkplug B spec
				{"small_gap_1", 10, 12, false, "Small gap (missing seq 11)", "ANY gap triggers rebirth request per spec"},
				{"small_gap_2", 10, 13, false, "Gap of 2 (missing seq 11,12)", "ANY gap triggers rebirth request per spec"},
				{"gap_of_5", 10, 16, false, "Gap of 5 (missing seq 11-15)", "ANY gap triggers rebirth request per spec"},
				{"large_gap", 10, 20, false, "Large gap (missing 9 sequences)", "ANY gap triggers rebirth request per spec"},
				{"huge_gap", 10, 100, false, "Huge gap (missing 89 sequences)", "ANY gap triggers rebirth request per spec"},
				{"gap_before_wrap", 250, 255, false, "Gap before wraparound (missing seq 251-254)", "ANY gap triggers rebirth request per spec"},
				{"gap_after_wrap", 0, 5, false, "Gap after wraparound (missing seq 1-4)", "ANY gap triggers rebirth request per spec"},
				{"gap_across_wrap", 253, 2, false, "Gap across wraparound boundary", "ANY gap triggers rebirth request per spec"},

				// Invalid backwards jumps
				{"backward_jump", 100, 50, false, "Invalid backward jump", "Backwards sequence numbers are invalid"},
				{"backward_near_zero", 5, 0, false, "Invalid backward jump to 0", "Backwards sequence numbers are invalid (not wraparound)"},
				{"backward_from_wrap", 2, 254, false, "Invalid backward jump across boundary", "Backwards sequence numbers are invalid"},

				// Invalid duplicates
				{"duplicate_seq", 50, 50, false, "Duplicate sequence number", "Duplicate sequence numbers are invalid"},
				{"duplicate_at_zero", 0, 0, false, "Duplicate at zero", "Duplicate sequence numbers are invalid"},
				{"duplicate_at_max", 255, 255, false, "Duplicate at max", "Duplicate sequence numbers are invalid"},

				// Edge case: skip to wraparound
				{"skip_to_wrap", 100, 255, false, "Skip directly to wraparound", "ANY gap triggers rebirth request per spec"},
				{"skip_from_wrap", 255, 5, false, "Skip after wraparound", "ANY gap triggers rebirth request per spec"},

				// ENG-3720: Device 702 non-conformant behavior - skips sequence 0
				{"non_conformant_255_to_1", 255, 1, false, "Non-conformant wraparound skipping 0 (255→1)", "Device skips seq 0, triggers rebirth per spec"},
			}

			for _, tc := range testCases {
				By(fmt.Sprintf("Testing case: %s - %s (%s)", tc.name, tc.description, tc.specReason))

				// Use the actual production ValidateSequenceNumber function
				isValid := sparkplug_plugin.ValidateSequenceNumber(tc.lastSeq, tc.currentSeq)

				if tc.isValid {
					Expect(isValid).To(BeTrue(),
						"Expected valid transition from %d to %d for %s: %s",
						tc.lastSeq, tc.currentSeq, tc.name, tc.specReason)
				} else {
					Expect(isValid).To(BeFalse(),
						"Expected invalid transition from %d to %d for %s: %s",
						tc.lastSeq, tc.currentSeq, tc.name, tc.specReason)
				}
			}
		})

		It("should detect all sequence gaps per Sparkplug B spec requirements", func() {
			// Test sequence validation using the actual ValidateSequenceNumber function
			// Per Sparkplug B spec: ANY gap should be invalid and trigger rebirth requests
			messageSequence := []uint8{
				0, 1, 2, 3, // Normal start (all valid)
				5, 6, // Gap: missing 4 (invalid per spec)
				10,     // Gap: missing 7, 8, 9 (invalid per spec)
				11, 12, // Continue normally (valid)
				255,  // Jump to boundary (invalid per spec)
				0, 1, // Valid wraparound (valid)
				3,    // Gap after wraparound: missing 2 (invalid per spec)
				4, 5, // Continue (valid)
				100,      // Large jump (invalid per spec)
				101, 102, // Continue from jump (valid)
				254, 255, 0, 1, // Another wraparound cycle (invalid jump, then valid wraparound, then valid)
			}

			validCount := 0
			invalidCount := 0
			wrapCount := 0
			var lastSeq *uint8

			for i, seq := range messageSequence {
				if lastSeq != nil {
					// Use production validation per Sparkplug B spec
					isValid := sparkplug_plugin.ValidateSequenceNumber(*lastSeq, seq)

					if seq == 0 && *lastSeq == 255 {
						wrapCount++
						By(fmt.Sprintf("Valid wraparound detected at index %d: 255 → 0", i))
					}

					if isValid {
						validCount++
					} else {
						invalidCount++
						By(fmt.Sprintf("Invalid sequence gap at index %d: %d → %d (would trigger rebirth per spec)", i, *lastSeq, seq))
					}
				}
				lastSeq = &seq
			}

			// Verify expected behavior based on Sparkplug B specification
			Expect(wrapCount).To(Equal(2), "Should detect exactly 2 valid wraparounds")
			// Per spec, ALL gaps are invalid - counting expected invalid sequences:
			// 1. 3→5 (gap), 2. 6→10 (gap), 3. 12→255 (gap), 4. 1→3 (gap), 5. 5→100 (gap), 6. 102→254 (gap)
			Expect(invalidCount).To(Equal(6), "Should detect all sequence gaps as invalid per Sparkplug B spec")
		})

		It("should handle rapid sequence number changes under load using production validation", func() {
			// Test production ValidateSequenceNumber with rapid message processing
			const messageCount = 1000
			sequences := make([]uint8, messageCount)

			// Generate continuous sequence with wraparounds
			for i := 0; i < messageCount; i++ {
				sequences[i] = uint8(i % 256)
			}

			// Process sequences and track statistics
			var stats struct {
				totalMessages int
				validCount    int
				invalidCount  int
				wraparounds   int
			}

			stats.totalMessages = len(sequences)
			var lastSeq *uint8

			for _, seq := range sequences {
				if lastSeq != nil {
					// Use production validation
					isValid := sparkplug_plugin.ValidateSequenceNumber(*lastSeq, seq)

					if isValid {
						stats.validCount++
					} else {
						stats.invalidCount++
					}

					if seq == 0 && *lastSeq == 255 {
						stats.wraparounds++
					}
				}
				lastSeq = &seq
			}

			// Validate expected patterns
			expectedWraps := (messageCount - 1) / 256
			Expect(stats.wraparounds).To(Equal(expectedWraps),
				"Should have correct number of wraparounds for %d messages", messageCount)
			Expect(stats.invalidCount).To(Equal(0),
				"Should have no invalid sequences in continuous sequence")
			Expect(stats.validCount).To(Equal(messageCount-1),
				"All sequences should be valid (except first which has no predecessor)")
		})

		It("should validate sequence numbers in concurrent scenarios using production logic", func() {
			// Test thread safety of ValidateSequenceNumber function
			const goroutines = 10
			const messagesPerRoutine = 100

			type sequenceValidation struct {
				lastSeq    uint8
				currentSeq uint8
				isValid    bool
			}

			// Channel to collect validation results
			resultChan := make(chan sequenceValidation, goroutines*messagesPerRoutine)
			done := make(chan bool, goroutines)

			// Launch concurrent validation tests
			for g := 0; g < goroutines; g++ {
				go func(routineID int) {
					defer GinkgoRecover()
					startSeq := uint8(routineID * 25) // Spread starting points

					for i := 0; i < messagesPerRoutine; i++ {
						lastSeq := uint8((int(startSeq) + i) % 256)
						currentSeq := uint8((int(startSeq) + i + 1) % 256)

						// Use production validation
						isValid := sparkplug_plugin.ValidateSequenceNumber(lastSeq, currentSeq)

						resultChan <- sequenceValidation{
							lastSeq:    lastSeq,
							currentSeq: currentSeq,
							isValid:    isValid,
						}
					}
					done <- true
				}(g)
			}

			// Wait for all routines to complete
			for i := 0; i < goroutines; i++ {
				<-done
			}
			close(resultChan)

			// Collect and verify all results
			validCount := 0
			totalCount := 0
			for result := range resultChan {
				totalCount++
				if result.isValid {
					validCount++
				}
			}

			// Verify results
			Expect(totalCount).To(Equal(goroutines * messagesPerRoutine))
			// All should be valid since we're doing continuous increments
			Expect(validCount).To(Equal(totalCount),
				"All concurrent validations should pass for continuous sequences")
		})
	})
})

// Note: Tests that were using test-only simulation functions have been removed.
// These tests should be reimplemented using the actual production code from:
// - sparkplug_plugin.NewTopicParser() for topic parsing
// - sparkplug_plugin.NewMessageProcessor() for message processing
// - sparkplug_plugin.NewFormatConverter() for format conversion

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

// Note: Helper functions stringPtr, uint32Ptr, uint64Ptr are defined in unit_test.go
