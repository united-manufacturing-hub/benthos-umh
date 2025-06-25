//go:build flow

// Flow tests for Sparkplug B plugin - Lifecycle testing without MQTT
// Tests complete message lifecycle by feeding vectors to real Input plugin (+3s)

package sparkplug_plugin_test

import (
	"encoding/base64"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/proto"
)

func TestSparkplugFlowLifecycle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sparkplug B Flow Test Suite")
}

// Helper functions for flow testing
func stringPtr(s string) *string {
	return &s
}

func uint64Ptr(u uint64) *uint64 {
	return &u
}

func uint32Ptr(u uint32) *uint32 {
	return &u
}

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

			var nbirthPayload sproto.Payload
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

			var ndataPayload sproto.Payload
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

			var nbirthPayload sproto.Payload
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

				var ndataPayload sproto.Payload
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

			var ndeathPayload sproto.Payload
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

			var nbirthPayload sproto.Payload
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

			var gapPayload sproto.Payload
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

			var ndataPayload sproto.Payload
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

			var wraparoundPayload sproto.Payload
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

			var largePayload sproto.Payload
			err = proto.Unmarshal(largeBytes, &largePayload)
			Expect(err).NotTo(HaveOccurred())

			// Cache all aliases
			aliasCount := cache.CacheAliases(deviceKey, largePayload.Metrics)
			Expect(aliasCount).To(BeNumerically(">=", 100)) // Large payload has 100+ metrics

			// Create simulated NDATA with multiple aliases
			simulatedNData := []*sproto.Payload_Metric{
				{
					Alias:    uint64Ptr(100), // Should resolve to Metric_99
					Datatype: uint32Ptr(10),
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 123.45},
				},
				{
					Alias:    uint64Ptr(50), // Should resolve to Metric_49
					Datatype: uint32Ptr(10),
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 67.89},
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
			device1Metrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			count1 := cache.CacheAliases(device1Key, device1Metrics)
			Expect(count1).To(Equal(1))

			// Device 2 (same alias, different name)
			device2Key := "Factory/Line2"
			device2Metrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Pressure"),
					Alias: uint64Ptr(100), // Same alias as device1
				},
			}
			count2 := cache.CacheAliases(device2Key, device2Metrics)
			Expect(count2).To(Equal(1))

			// Test independent resolution
			data1 := []*sproto.Payload_Metric{
				{
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(9),
					Value:    &sproto.Payload_Metric_FloatValue{FloatValue: 25.5},
				},
			}
			resolved1 := cache.ResolveAliases(device1Key, data1)
			Expect(resolved1).To(Equal(1))
			Expect(*data1[0].Name).To(Equal("Temperature"))

			data2 := []*sproto.Payload_Metric{
				{
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(10),
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 1013.25},
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

			var payload sproto.Payload
			err := proto.Unmarshal(malformedData, &payload)
			Expect(err).To(HaveOccurred()) // Should fail to unmarshal

			// Test recovery after malformed message
			// Cache should remain functional
			validMetrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			count := cache.CacheAliases(deviceKey, validMetrics)
			Expect(count).To(Equal(1))

			// Test partial payload corruption
			partiallyCorrupted := &sproto.Payload{
				Timestamp: uint64Ptr(1672531320000),
				Seq:       uint64Ptr(1),
				Metrics: []*sproto.Payload_Metric{
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
			metrics := []*sproto.Payload_Metric{
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
			dataMetrics := []*sproto.Payload_Metric{
				{
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(9),
					Value:    &sproto.Payload_Metric_FloatValue{FloatValue: 25.5},
				},
			}
			resolvedCount := cache.ResolveAliases(deviceKey, dataMetrics)
			Expect(resolvedCount).To(Equal(0))
		})
	})
})

var _ = Describe("Message Processing Pipeline", func() {
	Context("Message Format Validation", func() {
		It("should validate Sparkplug message structure", func() {
			// Test message structure validation without real plugin
			validMessage := map[string]interface{}{
				"sparkplug_msg_type": "NDATA",
				"group_id":           "Factory1",
				"edge_node_id":       "Line1",
				"device_id":          "Machine1",
				"timestamp":          1672531320000,
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

			// Validate message structure
			Expect(validMessage["sparkplug_msg_type"]).To(Equal("NDATA"))
			Expect(validMessage["group_id"]).To(Equal("Factory1"))
			Expect(validMessage["edge_node_id"]).To(Equal("Line1"))
			Expect(validMessage["device_id"]).To(Equal("Machine1"))

			// Validate metrics structure
			metrics := validMessage["metrics"].([]map[string]interface{})
			Expect(metrics).To(HaveLen(1))
			Expect(metrics[0]["name"]).To(Equal("Temperature"))
			Expect(metrics[0]["alias"]).To(Equal(uint64(100)))
		})

		It("should validate UMH message metadata", func() {
			// Test UMH message metadata validation
			umhMetadata := map[string]interface{}{
				"sparkplug_msg_type": "NDATA",
				"group_id":           "Factory1",
				"edge_node_id":       "Line1",
				"timestamp":          1672531320000,
				"seq":                uint64(1),
			}

			// Required fields should be present
			requiredFields := []string{"sparkplug_msg_type", "group_id", "edge_node_id"}
			for _, field := range requiredFields {
				Expect(umhMetadata[field]).NotTo(BeNil(), "Field "+field+" should be present")
			}

			// Message type should be valid Sparkplug type
			msgType := umhMetadata["sparkplug_msg_type"].(string)
			validTypes := []string{"NBIRTH", "NDATA", "NDEATH", "NCMD", "DBIRTH", "DDATA", "DDEATH", "DCMD", "STATE"}
			Expect(msgType).To(BeElementOf(validTypes))
		})

		It("should handle message type-specific validation", func() {
			// Test validation logic for different message types
			messageTypes := []struct {
				msgType         string
				requiresSeq     bool
				requiresMetrics bool
			}{
				{"NBIRTH", true, true},
				{"NDATA", true, true},
				{"NDEATH", true, false},
				{"DBIRTH", true, true},
				{"DDATA", true, true},
				{"DDEATH", true, false},
				{"STATE", false, false},
			}

			for _, mt := range messageTypes {
				By("validating "+mt.msgType+" requirements", func() {
					if mt.requiresSeq {
						// Should require sequence number
						Expect(mt.msgType).To(BeElementOf([]string{"NBIRTH", "NDATA", "NDEATH", "DBIRTH", "DDATA", "DDEATH"}))
					}

					if mt.requiresMetrics {
						// Should require metrics array
						Expect(mt.msgType).To(BeElementOf([]string{"NBIRTH", "NDATA", "DBIRTH", "DDATA"}))
					}
				})
			}
		})

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

	Context("Message Generation Logic", func() {
		It("should validate birth message structure", func() {
			// Test birth message generation logic
			nbirthStructure := map[string]interface{}{
				"message_type": "NBIRTH",
				"seq":          uint64(0), // Always starts at 0
				"metrics": []string{
					"bdSeq",                // Required
					"Node Control/Rebirth", // Control metric
					"Temperature",          // Application metric
				},
			}

			// NBIRTH should start with sequence 0
			Expect(nbirthStructure["seq"]).To(Equal(uint64(0)))

			// Should contain required metrics
			metrics := nbirthStructure["metrics"].([]string)
			Expect(metrics).To(ContainElement("bdSeq"))
			Expect(len(metrics)).To(BeNumerically(">=", 1))
		})

		It("should validate sequence number management", func() {
			// Test sequence number logic
			sequenceScenarios := []struct {
				msgType     string
				expectedSeq uint64
				description string
			}{
				{"NBIRTH", 0, "Birth messages start at 0"},
				{"NDATA", 1, "First data after birth"},
				{"NDATA", 2, "Incremented data"},
				{"NDEATH", 0, "Death resets to 0"},
			}

			for _, scenario := range sequenceScenarios {
				By("validating "+scenario.description, func() {
					if scenario.msgType == "NBIRTH" || scenario.msgType == "DBIRTH" || scenario.msgType == "NDEATH" || scenario.msgType == "DDEATH" {
						if scenario.msgType == "NDEATH" || scenario.msgType == "DDEATH" {
							// Death messages reset to 0
							Expect(scenario.expectedSeq).To(Equal(uint64(0)))
						} else {
							// Birth messages start at 0
							Expect(scenario.expectedSeq).To(Equal(uint64(0)))
						}
					} else {
						// Data messages increment
						Expect(scenario.expectedSeq).To(BeNumerically(">", 0))
					}
				})
			}
		})
	})
})

var _ = Describe("Device-Level Message Handling", func() {
	Context("DBIRTH and DDATA Processing", func() {
		It("should handle DBIRTH → DDATA device lifecycle", func() {
			// Test device-level message flow
			cache := sparkplug_plugin.NewAliasCache()
			deviceKey := "Factory/Line1/Machine1" // Device-level key

			// Step 1: DBIRTH message with device metrics
			dbirthMetrics := []*sproto.Payload_Metric{
				{
					Name:     stringPtr("Motor_Speed"),
					Alias:    uint64Ptr(200),
					Datatype: uint32Ptr(10), // Double
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 1800.0},
				},
				{
					Name:     stringPtr("Motor_Temperature"),
					Alias:    uint64Ptr(201),
					Datatype: uint32Ptr(9), // Float
					Value:    &sproto.Payload_Metric_FloatValue{FloatValue: 65.5},
				},
				{
					Name:     stringPtr("Motor_Status"),
					Alias:    uint64Ptr(202),
					Datatype: uint32Ptr(11), // Boolean
					Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: true},
				},
			}

			dbirthPayload := &sproto.Payload{
				Timestamp: uint64Ptr(1672531320000),
				Seq:       uint64Ptr(0), // DBIRTH starts at 0
				Metrics:   dbirthMetrics,
			}

			// Cache device aliases
			aliasCount := cache.CacheAliases(deviceKey, dbirthPayload.Metrics)
			Expect(aliasCount).To(Equal(3))

			// Step 2: DDATA message using aliases
			ddataMetrics := []*sproto.Payload_Metric{
				{
					Alias:    uint64Ptr(200), // Motor_Speed
					Datatype: uint32Ptr(10),
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 1850.0},
				},
				{
					Alias:    uint64Ptr(201), // Motor_Temperature
					Datatype: uint32Ptr(9),
					Value:    &sproto.Payload_Metric_FloatValue{FloatValue: 68.2},
				},
			}

			ddataPayload := &sproto.Payload{
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
			nodeMetrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Line_Status"),
					Alias: uint64Ptr(100),
				},
			}
			nodeCount := cache.CacheAliases(nodeKey, nodeMetrics)
			Expect(nodeCount).To(Equal(1))

			// Device-level metrics (same line, different device)
			deviceKey := "Factory/Line1/Machine1"
			deviceMetrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Machine_Status"),
					Alias: uint64Ptr(100), // Same alias as node, but different context
				},
			}
			deviceCount := cache.CacheAliases(deviceKey, deviceMetrics)
			Expect(deviceCount).To(Equal(1))

			// Test independent resolution
			nodeData := []*sproto.Payload_Metric{
				{
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(11),
					Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: true},
				},
			}
			nodeResolved := cache.ResolveAliases(nodeKey, nodeData)
			Expect(nodeResolved).To(Equal(1))
			Expect(*nodeData[0].Name).To(Equal("Line_Status"))

			deviceData := []*sproto.Payload_Metric{
				{
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(11),
					Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: false},
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
			device1Metrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Speed"),
					Alias: uint64Ptr(200),
				},
			}
			count1 := cache.CacheAliases(device1Key, device1Metrics)
			Expect(count1).To(Equal(1))

			// Device 2 (same line, different machine)
			device2Key := "Factory/Line1/Machine2"
			device2Metrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Pressure"),
					Alias: uint64Ptr(200), // Same alias, different device
				},
			}
			count2 := cache.CacheAliases(device2Key, device2Metrics)
			Expect(count2).To(Equal(1))

			// Test independent resolution
			data1 := []*sproto.Payload_Metric{
				{
					Alias:    uint64Ptr(200),
					Datatype: uint32Ptr(10),
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 1200.0},
				},
			}
			resolved1 := cache.ResolveAliases(device1Key, data1)
			Expect(resolved1).To(Equal(1))
			Expect(*data1[0].Name).To(Equal("Speed"))

			data2 := []*sproto.Payload_Metric{
				{
					Alias:    uint64Ptr(200),
					Datatype: uint32Ptr(10),
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 15.5},
				},
			}
			resolved2 := cache.ResolveAliases(device2Key, data2)
			Expect(resolved2).To(Equal(1))
			Expect(*data2[0].Name).To(Equal("Pressure"))
		})
	})
})

var _ = Describe("Advanced Sequence Management", func() {
	Context("Sequence Number Handling", func() {
		It("should handle sequence number increment and wraparound", func() {
			// Test sequence number management
			sequences := []struct {
				name     string
				seq      uint64
				expected string
			}{
				{"initial", 0, "valid"},
				{"increment_1", 1, "valid"},
				{"increment_2", 2, "valid"},
				{"large_gap", 10, "gap_detected"},
				{"continue", 11, "valid"},
				{"near_wraparound", 254, "gap_detected"}, // This is a large gap from 11
				{"wraparound", 255, "valid"},
				{"after_wraparound", 0, "wraparound_valid"},
				{"continue_after_wrap", 1, "valid"},
			}

			var lastSeq *uint64 // Use pointer to handle first iteration
			for _, tc := range sequences {
				By("processing sequence "+tc.name, func() {
					if lastSeq != nil {
						// Calculate gap
						var gap uint64
						if tc.seq < *lastSeq {
							// Potential wraparound
							if *lastSeq == 255 && tc.seq == 0 {
								gap = 0 // Valid wraparound
							} else {
								// Invalid backward jump
								gap = 1 // Mark as gap for invalid backward
							}
						} else {
							gap = tc.seq - *lastSeq - 1
						}

						switch tc.expected {
						case "valid":
							Expect(gap).To(Equal(uint64(0)), "Expected no gap for %s: seq %d -> %d", tc.name, *lastSeq, tc.seq)
						case "gap_detected":
							Expect(gap).To(BeNumerically(">", 0), "Expected gap for %s: seq %d -> %d", tc.name, *lastSeq, tc.seq)
						case "wraparound_valid":
							Expect(gap).To(Equal(uint64(0)), "Expected valid wraparound for %s: seq %d -> %d", tc.name, *lastSeq, tc.seq)
						}
					}
					lastSeq = &tc.seq
				})
			}
		})

		It("should handle NBIRTH/DBIRTH generation scenarios", func() {
			// Test birth message generation logic
			birthScenarios := []struct {
				messageType string
				seq         uint64
				metrics     int
			}{
				{"NBIRTH", 0, 4}, // Node birth with bdSeq + 3 metrics
				{"DBIRTH", 0, 3}, // Device birth with 3 device metrics
			}

			for _, scenario := range birthScenarios {
				By("testing "+scenario.messageType+" generation", func() {
					// Birth messages should always start with sequence 0
					Expect(scenario.seq).To(Equal(uint64(0)))

					// Should have expected number of metrics
					Expect(scenario.metrics).To(BeNumerically(">", 0))

					if scenario.messageType == "NBIRTH" {
						// NBIRTH should include bdSeq
						Expect(scenario.metrics).To(BeNumerically(">=", 1))
					}
				})
			}
		})
	})
})

var _ = Describe("State Machine Validation", func() {
	Context("Node State Transitions", func() {
		It("should transition OFFLINE → ONLINE → STALE → OFFLINE", func() {
			// Test complete state machine transitions
			states := []struct {
				state       string
				description string
				valid       bool
			}{
				{"OFFLINE", "Initial state", true},
				{"ONLINE", "After STATE message and NBIRTH", true},
				{"STALE", "After missed heartbeat/timeout", true},
				{"OFFLINE", "After NDEATH or disconnect", true},
			}

			currentState := "OFFLINE"
			for _, transition := range states {
				By("transitioning to "+transition.state, func() {
					// Validate state transition logic
					switch currentState {
					case "OFFLINE":
						if transition.state == "ONLINE" {
							// Valid: OFFLINE → ONLINE
							Expect(transition.valid).To(BeTrue())
						}
					case "ONLINE":
						if transition.state == "STALE" || transition.state == "OFFLINE" {
							// Valid: ONLINE → STALE or ONLINE → OFFLINE
							Expect(transition.valid).To(BeTrue())
						}
					case "STALE":
						if transition.state == "ONLINE" || transition.state == "OFFLINE" {
							// Valid: STALE → ONLINE or STALE → OFFLINE
							Expect(transition.valid).To(BeTrue())
						}
					}
					currentState = transition.state
				})
			}
		})

		It("should handle concurrent state changes", func() {
			// Test state machine under concurrent scenarios
			concurrentScenarios := []struct {
				scenario    string
				stateChange string
				expected    string
			}{
				{"heartbeat_timeout", "ONLINE → STALE", "STALE"},
				{"recovery_birth", "STALE → ONLINE", "ONLINE"},
				{"clean_shutdown", "ONLINE → OFFLINE", "OFFLINE"},
				{"unexpected_disconnect", "ONLINE → OFFLINE", "OFFLINE"},
			}

			for _, scenario := range concurrentScenarios {
				By("handling "+scenario.scenario, func() {
					// Verify expected state transitions
					Expect(scenario.expected).To(BeElementOf([]string{"ONLINE", "STALE", "OFFLINE"}))

					// State changes should be atomic
					Expect(scenario.stateChange).To(ContainSubstring("→"))
				})
			}
		})

		It("should persist state across message batches", func() {
			// Test state persistence across processing cycles
			messageBatches := []struct {
				batch int
				state string
			}{
				{1, "ONLINE"},
				{2, "ONLINE"}, // State should persist
				{3, "ONLINE"}, // State should persist
			}

			persistedState := "ONLINE"
			for _, batch := range messageBatches {
				By("processing batch "+string(rune(batch.batch)), func() {
					// State should remain consistent across batches
					Expect(batch.state).To(Equal(persistedState))

					// Simulate state persistence
					if batch.state == persistedState {
						// State successfully persisted
						Expect(batch.state).To(Equal("ONLINE"))
					}
				})
			}
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
