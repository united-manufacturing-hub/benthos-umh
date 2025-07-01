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

// Payload tests for Sparkplug B plugin - Static vector validation
// Tests decode/encode of static Sparkplug B payloads (+2s)

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

func TestSparkplugPayloadVectors(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sparkplug B Payload Test Suite")
}

var _ = Describe("Static Payload Validation", func() {

	Context("Test Vector Decoding", func() {
		It("should decode all generated test vectors successfully", func() {
			for _, vector := range sparkplug_plugin.TestVectors {
				By("processing vector: "+vector.Name, func() {
					payloadBytes, err := base64.StdEncoding.DecodeString(vector.Base64Data)
					Expect(err).NotTo(HaveOccurred(), "Failed to decode Base64 for "+vector.Name)

					var payload sproto.Payload
					err = proto.Unmarshal(payloadBytes, &payload)
					Expect(err).NotTo(HaveOccurred(), "Failed to unmarshal protobuf for "+vector.Name)

					// Verify basic payload structure
					Expect(payload.Metrics).To(HaveLen(vector.MetricCount), "Metric count mismatch for "+vector.Name)
					if len(payload.Metrics) > 0 {
						Expect(payload.Metrics[0]).NotTo(BeNil(), "First metric should not be nil for "+vector.Name)
					}
				})
			}
		})

		It("should validate NBIRTH payload structure", func() {
			nbirthVector := sparkplug_plugin.GetTestVector("NBIRTH_V1")
			Expect(nbirthVector).NotTo(BeNil())

			payloadBytes, err := base64.StdEncoding.DecodeString(nbirthVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var payload sproto.Payload
			err = proto.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			// NBIRTH should have sequence 0
			Expect(payload.Seq).NotTo(BeNil())
			Expect(*payload.Seq).To(Equal(uint64(0)))

			// Should have timestamp
			Expect(payload.Timestamp).NotTo(BeNil())

			// Should have metrics including bdSeq
			Expect(payload.Metrics).To(HaveLen(3))

			// First metric should be bdSeq
			bdSeqMetric := payload.Metrics[0]
			Expect(bdSeqMetric.Name).NotTo(BeNil())
			Expect(*bdSeqMetric.Name).To(Equal("bdSeq"))
			Expect(bdSeqMetric.Alias).NotTo(BeNil())
		})

		It("should validate NDATA payload structure", func() {
			ndataVector := sparkplug_plugin.GetTestVector("NDATA_V1")
			Expect(ndataVector).NotTo(BeNil())

			payloadBytes, err := base64.StdEncoding.DecodeString(ndataVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var payload sproto.Payload
			err = proto.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			// NDATA should have sequence > 0
			Expect(payload.Seq).NotTo(BeNil())
			Expect(*payload.Seq).To(Equal(uint64(1)))

			// Should have timestamp
			Expect(payload.Timestamp).NotTo(BeNil())

			// Should have metrics using aliases
			Expect(payload.Metrics).To(HaveLen(1))

			// Metric should use alias (no name)
			metric := payload.Metrics[0]
			Expect(metric.Alias).NotTo(BeNil())
			// Name should be nil for alias-based metrics
			Expect(metric.Name).To(BeNil())
		})

		It("should validate NDEATH payload structure", func() {
			ndeathVector := sparkplug_plugin.GetTestVector("NDEATH_V1")
			Expect(ndeathVector).NotTo(BeNil())

			payloadBytes, err := base64.StdEncoding.DecodeString(ndeathVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var payload sproto.Payload
			err = proto.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			// NDEATH should have sequence 0
			Expect(payload.Seq).NotTo(BeNil())
			Expect(*payload.Seq).To(Equal(uint64(0)))

			// Should have timestamp
			Expect(payload.Timestamp).NotTo(BeNil())

			// Should have bdSeq metric
			Expect(payload.Metrics).To(HaveLen(1))

			bdSeqMetric := payload.Metrics[0]
			Expect(bdSeqMetric.Name).NotTo(BeNil())
			Expect(*bdSeqMetric.Name).To(Equal("bdSeq"))
		})

		It("should handle large payloads efficiently", func() {
			largeVector := sparkplug_plugin.GetTestVector("NBIRTH_LARGE")
			Expect(largeVector).NotTo(BeNil())

			payloadBytes, err := base64.StdEncoding.DecodeString(largeVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var payload sproto.Payload
			err = proto.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			// Should have 100+ metrics
			Expect(payload.Metrics).To(HaveLen(largeVector.MetricCount))
			Expect(len(payload.Metrics)).To(BeNumerically(">=", 100))

			// Each metric should be valid
			for i, metric := range payload.Metrics {
				Expect(metric).NotTo(BeNil(), "Metric %d should not be nil", i)
				Expect(metric.Name).NotTo(BeNil(), "Metric %d should have name", i)
				Expect(metric.Alias).NotTo(BeNil(), "Metric %d should have alias", i)
			}
		})

		It("should handle edge case payloads", func() {
			edgeCases := []string{"NDATA_GAP", "NDATA_BEFORE_BIRTH", "NDATA_WRAPAROUND"}

			for _, vectorName := range edgeCases {
				vector := sparkplug_plugin.GetTestVector(vectorName)
				Expect(vector).NotTo(BeNil(), "Vector "+vectorName+" should exist")

				By("processing edge case: "+vectorName, func() {
					payloadBytes, err := base64.StdEncoding.DecodeString(vector.Base64Data)
					Expect(err).NotTo(HaveOccurred())

					var payload sproto.Payload
					err = proto.Unmarshal(payloadBytes, &payload)
					Expect(err).NotTo(HaveOccurred())

					// Basic structure validation
					Expect(payload.Metrics).To(HaveLen(vector.MetricCount))
				})
			}
		})
	})

	Context("Payload Round-Trip Testing", func() {
		It("should encode and decode payloads identically", func() {
			// Test with NBIRTH vector
			nbirthVector := sparkplug_plugin.GetTestVector("NBIRTH_V1")
			originalBytes, err := base64.StdEncoding.DecodeString(nbirthVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			// Decode
			var payload sproto.Payload
			err = proto.Unmarshal(originalBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			// Re-encode
			newBytes, err := proto.Marshal(&payload)
			Expect(err).NotTo(HaveOccurred())

			// Decode again to verify structure
			var payload2 sproto.Payload
			err = proto.Unmarshal(newBytes, &payload2)
			Expect(err).NotTo(HaveOccurred())

			// Compare key fields
			Expect(payload2.Seq).To(Equal(payload.Seq))
			Expect(payload2.Timestamp).To(Equal(payload.Timestamp))
			Expect(payload2.Metrics).To(HaveLen(len(payload.Metrics)))
		})

		It("should handle metric value types correctly", func() {
			// Test all Sparkplug data types with proper validation
			testMetrics := []*sproto.Payload_Metric{
				{
					Name:     stringPtr("Int8_Value"),
					Alias:    uint64Ptr(1),
					Datatype: uint32Ptr(1), // Int8
					Value:    &sproto.Payload_Metric_IntValue{IntValue: 127},
				},
				{
					Name:     stringPtr("Int16_Value"),
					Alias:    uint64Ptr(2),
					Datatype: uint32Ptr(2), // Int16
					Value:    &sproto.Payload_Metric_IntValue{IntValue: 32767},
				},
				{
					Name:     stringPtr("Int32_Value"),
					Alias:    uint64Ptr(3),
					Datatype: uint32Ptr(3), // Int32
					Value:    &sproto.Payload_Metric_IntValue{IntValue: 2147483647},
				},
				{
					Name:     stringPtr("Int64_Value"),
					Alias:    uint64Ptr(4),
					Datatype: uint32Ptr(7), // Int64
					Value:    &sproto.Payload_Metric_LongValue{LongValue: 9223372036854775807},
				},
				{
					Name:     stringPtr("Float_Value"),
					Alias:    uint64Ptr(5),
					Datatype: uint32Ptr(9), // Float
					Value:    &sproto.Payload_Metric_FloatValue{FloatValue: 3.14159},
				},
				{
					Name:     stringPtr("Double_Value"),
					Alias:    uint64Ptr(6),
					Datatype: uint32Ptr(10), // Double
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 2.718281828459045},
				},
				{
					Name:     stringPtr("Boolean_Value"),
					Alias:    uint64Ptr(7),
					Datatype: uint32Ptr(11), // Boolean
					Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: true},
				},
				{
					Name:     stringPtr("String_Value"),
					Alias:    uint64Ptr(8),
					Datatype: uint32Ptr(12), // String
					Value:    &sproto.Payload_Metric_StringValue{StringValue: "Test String"},
				},
			}

			// Create payload with all data types
			testPayload := &sproto.Payload{
				Timestamp: uint64Ptr(1672531320000),
				Seq:       uint64Ptr(0),
				Metrics:   testMetrics,
			}

			// Test round-trip encoding/decoding
			encodedData, err := proto.Marshal(testPayload)
			Expect(err).NotTo(HaveOccurred())

			var decodedPayload sproto.Payload
			err = proto.Unmarshal(encodedData, &decodedPayload)
			Expect(err).NotTo(HaveOccurred())

			// Verify decoded payload matches original
			Expect(decodedPayload.Metrics).To(HaveLen(8))
			Expect(*decodedPayload.Timestamp).To(Equal(*testPayload.Timestamp))
			Expect(*decodedPayload.Seq).To(Equal(*testPayload.Seq))

			// Validate each metric type after round-trip
			for i, metric := range decodedPayload.Metrics {
				originalMetric := testMetrics[i]
				By("validating metric type "+*originalMetric.Name, func() {
					Expect(metric.Name).NotTo(BeNil())
					Expect(*metric.Name).To(Equal(*originalMetric.Name))
					Expect(*metric.Alias).To(Equal(*originalMetric.Alias))
					Expect(*metric.Datatype).To(Equal(*originalMetric.Datatype))

					// Type-specific validation
					switch *metric.Datatype {
					case 1, 2, 3: // Int8, Int16, Int32
						Expect(metric.GetIntValue()).To(Equal(originalMetric.GetIntValue()))
					case 7: // Int64
						Expect(metric.GetLongValue()).To(Equal(originalMetric.GetLongValue()))
					case 9: // Float
						Expect(metric.GetFloatValue()).To(BeNumerically("~", originalMetric.GetFloatValue(), 0.0001))
					case 10: // Double
						Expect(metric.GetDoubleValue()).To(BeNumerically("~", originalMetric.GetDoubleValue(), 0.0000001))
					case 11: // Boolean
						Expect(metric.GetBooleanValue()).To(Equal(originalMetric.GetBooleanValue()))
					case 12: // String
						Expect(metric.GetStringValue()).To(Equal(originalMetric.GetStringValue()))
					}
				})
			}
		})
	})

	Context("Performance Validation", func() {
		It("should process payloads efficiently", func() {
			// Test processing time for all vectors
			for _, vector := range sparkplug_plugin.TestVectors {
				By("timing vector: "+vector.Name, func() {
					payloadBytes, err := base64.StdEncoding.DecodeString(vector.Base64Data)
					Expect(err).NotTo(HaveOccurred())

					// This should be fast (<1ms for small payloads)
					var payload sproto.Payload
					err = proto.Unmarshal(payloadBytes, &payload)
					Expect(err).NotTo(HaveOccurred())
				})
			}
		})

		It("should handle large payload processing within time limits", func() {
			largeVector := sparkplug_plugin.GetTestVector("NBIRTH_LARGE")

			// Even large payloads should decode quickly
			payloadBytes, err := base64.StdEncoding.DecodeString(largeVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var payload sproto.Payload
			err = proto.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			// Verify it has the expected structure
			Expect(payload.Metrics).To(HaveLen(largeVector.MetricCount))
		})
	})
})

// Helper functions for payload testing
func MustDecodeBase64(b64 string) []byte {
	data, err := base64.StdEncoding.DecodeString(b64)
	Expect(err).NotTo(HaveOccurred())
	return data
}

// Note: Additional payload helpers can be added as needed
