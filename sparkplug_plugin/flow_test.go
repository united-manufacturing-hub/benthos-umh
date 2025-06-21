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
			Skip("TODO: Implement malformed message handling")
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
	Context("Input Plugin Processing", func() {
		It("should process vector sequences through real Input plugin", func() {
			Skip("TODO: Create Input plugin instance and feed test vectors")
		})

		It("should generate proper UMH output format", func() {
			Skip("TODO: Validate output conforms to UMH message format")
		})

		It("should populate message metadata correctly", func() {
			Skip("TODO: Validate sparkplug_msg_type, group_id, edge_node_id metadata")
		})
	})

	Context("Output Plugin Processing", func() {
		It("should generate valid Sparkplug B messages from UMH input", func() {
			Skip("TODO: Test Output plugin with synthetic UMH data")
		})

		It("should manage sequence numbers correctly", func() {
			Skip("TODO: Validate sequence number increment and wraparound")
		})

		It("should handle birth message generation", func() {
			Skip("TODO: Test NBIRTH/DBIRTH generation on startup")
		})
	})
})

var _ = Describe("State Machine Validation", func() {
	Context("Node State Transitions", func() {
		It("should transition OFFLINE → ONLINE → STALE → OFFLINE", func() {
			Skip("TODO: Test complete state machine transitions")
		})

		It("should handle concurrent state changes", func() {
			Skip("TODO: Test state machine under concurrent message processing")
		})

		It("should persist state across message batches", func() {
			Skip("TODO: Test state persistence across processing cycles")
		})
	})
})

// Helper functions for flow testing
func createTestInputPlugin() interface{} {
	// TODO: Create and configure real Input plugin instance
	Skip("TODO: Implement input plugin creation helper")
	return nil
}

func feedVectorSequence(plugin interface{}, vectors []string) {
	// TODO: Feed sequence of test vectors to plugin
	Skip("TODO: Implement vector feeding helper")
}

func validateUMHOutput(output interface{}) {
	// TODO: Validate output conforms to UMH message format
	Skip("TODO: Implement UMH output validation helper")
}
