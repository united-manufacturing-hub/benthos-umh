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

package topic_browser_plugin

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

var _ = Describe("Protobuf Bundle Operations", func() {
	Context("Bundle Serialization", func() {
		It("serializes and deserializes bundles correctly", func() {
			// Create a test bundle
			bundle := &proto.UnsBundle{
				UnsMap: &proto.TopicMap{
					Entries: map[string]*proto.TopicInfo{
						"test.topic.1": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
						"test.topic.2": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
					},
				},
				Events: &proto.EventTable{
					Entries: []*proto.EventTableEntry{
						{
							UnsTreeId: "test.topic.1",
							Payload: &proto.EventTableEntry_Ts{
								Ts: &proto.TimeSeriesPayload{
									ScalarType:  proto.ScalarType_NUMERIC,
									TimestampMs: 1647753600000,
									Value: &proto.TimeSeriesPayload_NumericValue{
										NumericValue: &wrapperspb.DoubleValue{Value: 42.5},
									},
								},
							},
						},
						{
							UnsTreeId: "test.topic.2",
							Payload: &proto.EventTableEntry_Ts{
								Ts: &proto.TimeSeriesPayload{
									ScalarType:  proto.ScalarType_NUMERIC,
									TimestampMs: 1647753600001,
									Value: &proto.TimeSeriesPayload_NumericValue{
										NumericValue: &wrapperspb.DoubleValue{Value: 24.7},
									},
								},
							},
						},
					},
				},
			}

			// Serialize to protobuf bytes
			protoBytes, err := BundleToProtobufBytes(bundle)
			Expect(err).To(BeNil())
			Expect(protoBytes).NotTo(BeNil())

			// Deserialize back to bundle
			decodedBundle, err := ProtobufBytesToBundle(protoBytes)
			Expect(err).To(BeNil())
			Expect(decodedBundle).NotTo(BeNil())

			// Verify the decoded bundle matches the original
			Expect(decodedBundle.UnsMap.Entries).To(HaveLen(2))
			Expect(decodedBundle.Events.Entries).To(HaveLen(2))
		})
	})

	Context("Backward Compatibility", func() {
		It("ProtobufBytesToBundleWithCompression works with uncompressed data", func() {
			// Create a simple bundle
			bundle := &proto.UnsBundle{
				UnsMap: &proto.TopicMap{
					Entries: map[string]*proto.TopicInfo{
						"test.topic": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
					},
				},
				Events: &proto.EventTable{
					Entries: []*proto.EventTableEntry{
						{
							UnsTreeId: "test.topic",
							Payload: &proto.EventTableEntry_Ts{
								Ts: &proto.TimeSeriesPayload{
									ScalarType:  proto.ScalarType_NUMERIC,
									TimestampMs: 1647753600000,
									Value: &proto.TimeSeriesPayload_NumericValue{
										NumericValue: &wrapperspb.DoubleValue{Value: 42.0},
									},
								},
							},
						},
					},
				},
			}

			// Serialize to uncompressed protobuf bytes
			protoBytes, err := BundleToProtobufBytes(bundle)
			Expect(err).To(BeNil())

			// Use the backward compatibility function
			decodedBundle, err := ProtobufBytesToBundleWithCompression(protoBytes)
			Expect(err).To(BeNil())
			Expect(decodedBundle).NotTo(BeNil())
			Expect(decodedBundle.UnsMap.Entries).To(HaveLen(1))
			Expect(decodedBundle.Events.Entries).To(HaveLen(1))
		})
	})

	Context("Bundle compression integration", func() {
		It("verifies BundleToProtobufBytes returns uncompressed protobuf", func() {
			// Create a bundle with test data
			bundle := &proto.UnsBundle{
				UnsMap: &proto.TopicMap{
					Entries: make(map[string]*proto.TopicInfo),
				},
				Events: &proto.EventTable{
					Entries: make([]*proto.EventTableEntry, 0),
				},
			}

			// Add structured data
			for i := 0; i < 50; i++ {
				topicName := fmt.Sprintf("umh.v1.enterprise.factory.line%d._historian.measurement", i)
				bundle.UnsMap.Entries[topicName] = &proto.TopicInfo{
					Level0:       "enterprise",
					DataContract: "_historian",
				}

				bundle.Events.Entries = append(bundle.Events.Entries, &proto.EventTableEntry{
					UnsTreeId: topicName,
					Payload: &proto.EventTableEntry_Ts{
						Ts: &proto.TimeSeriesPayload{
							ScalarType:  proto.ScalarType_NUMERIC,
							TimestampMs: int64(1647753600000 + i*1000),
							Value: &proto.TimeSeriesPayload_NumericValue{
								NumericValue: &wrapperspb.DoubleValue{Value: 25.0 + float64(i)*0.1},
							},
						},
					},
				})
			}

			// Get uncompressed protobuf size
			uncompressedProto, err := BundleToProtobufBytes(bundle)
			Expect(err).To(BeNil())

			// Get output from BundleToProtobufBytes (now returns uncompressed)
			protoBytes, err := BundleToProtobufBytes(bundle)
			Expect(err).To(BeNil())

			uncompressedSize := len(uncompressedProto)
			outputSize := len(protoBytes)

			By(fmt.Sprintf("Uncompressed: %d bytes, Output: %d bytes",
				uncompressedSize, outputSize))

			// Verify no compression is happening - sizes should be equal
			Expect(outputSize).To(Equal(uncompressedSize),
				"BundleToProtobufBytes should produce same size as uncompressed protobuf")

			// Verify the data can be parsed back to the same bundle
			parsedBundle, err := ProtobufBytesToBundleWithCompression(protoBytes)
			Expect(err).To(BeNil())
			Expect(parsedBundle.UnsMap.Entries).To(HaveLen(len(bundle.UnsMap.Entries)))
			Expect(parsedBundle.Events.Entries).To(HaveLen(len(bundle.Events.Entries)))
		})

		It("verifies round-trip compression preserves data", func() {
			originalBundle := &proto.UnsBundle{
				UnsMap: &proto.TopicMap{
					Entries: map[string]*proto.TopicInfo{
						"test.compression.topic": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
					},
				},
				Events: &proto.EventTable{
					Entries: []*proto.EventTableEntry{
						{
							UnsTreeId: "test.compression.topic",
							Payload: &proto.EventTableEntry_Ts{
								Ts: &proto.TimeSeriesPayload{
									ScalarType:  proto.ScalarType_NUMERIC,
									TimestampMs: 1647753600000,
									Value: &proto.TimeSeriesPayload_NumericValue{
										NumericValue: &wrapperspb.DoubleValue{Value: 123.456},
									},
								},
							},
						},
					},
				},
			}

			// Compress
			compressedBytes, err := BundleToProtobufBytes(originalBundle)
			Expect(err).To(BeNil())

			// Decompress
			decodedBundle, err := ProtobufBytesToBundleWithCompression(compressedBytes)
			Expect(err).To(BeNil())

			// Verify data integrity
			Expect(decodedBundle.UnsMap.Entries).To(HaveLen(1))
			Expect(decodedBundle.UnsMap.Entries["test.compression.topic"].Level0).To(Equal("enterprise"))
			Expect(decodedBundle.UnsMap.Entries["test.compression.topic"].DataContract).To(Equal("_historian"))
			Expect(decodedBundle.Events.Entries).To(HaveLen(1))
			Expect(decodedBundle.Events.Entries[0].UnsTreeId).To(Equal("test.compression.topic"))
			Expect(decodedBundle.Events.Entries[0].Payload.(*proto.EventTableEntry_Ts).Ts.Value.(*proto.TimeSeriesPayload_NumericValue).NumericValue.Value).To(Equal(123.456))
		})

		It("fails properly when given invalid protobuf data", func() {
			By("Testing with guaranteed invalid protobuf data")
			// Use data that is guaranteed to fail protobuf parsing
			// Random bytes will reliably fail protobuf parsing
			invalidProtobufData := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

			_, err := ProtobufBytesToBundleWithCompression(invalidProtobufData)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to parse protobuf data"))

			By("Testing with corrupted protobuf data")
			// Create valid protobuf data, then corrupt it
			validBundle := &proto.UnsBundle{
				UnsMap: &proto.TopicMap{
					Entries: map[string]*proto.TopicInfo{
						"test": {Level0: "enterprise"},
					},
				},
			}

			validProtobuf, err := BundleToProtobufBytes(validBundle)
			Expect(err).To(BeNil())

			// Corrupt the protobuf data by flipping bits
			corruptedData := make([]byte, len(validProtobuf))
			copy(corruptedData, validProtobuf)
			for i := 0; i < len(corruptedData) && i < 8; i++ {
				corruptedData[i] ^= 0xFF // Flip all bits in first 8 bytes
			}

			_, err = ProtobufBytesToBundleWithCompression(corruptedData)
			Expect(err).To(HaveOccurred())
			// Error should be from protobuf parsing
			Expect(err.Error()).To(ContainSubstring("failed to parse protobuf data"))
		})
	})
})
