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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var _ = Describe("Protobuf Bundle Operations", func() {
	Context("Bundle serialization and deserialization", func() {
		It("successfully encodes and decodes a bundle with time series data", func() {
			// Create a test bundle
			originalBundle := &proto.UnsBundle{
				UnsMap: &proto.TopicMap{
					Entries: map[string]*proto.TopicInfo{
						"test-topic": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
					},
				},
				Events: &proto.EventTable{
					Entries: []*proto.EventTableEntry{
						{
							UnsTreeId: "test-topic",
							Payload: &proto.EventTableEntry_Ts{
								Ts: &proto.TimeSeriesPayload{
									ScalarType:  proto.ScalarType_NUMERIC,
									TimestampMs: 1647753600000,
									Value: &proto.TimeSeriesPayload_NumericValue{
										NumericValue: &wrapperspb.DoubleValue{Value: 13.0},
									},
								},
							},
						},
					},
				},
			}

			// Encode the bundle
			protoBytes, err := bundleToProtobuf(originalBundle)
			Expect(err).To(BeNil())
			Expect(protoBytes).NotTo(BeNil())

			// Decode the bundle
			decodedBundle, err := protobufBytesToBundle(protoBytes)
			Expect(err).To(BeNil())
			Expect(decodedBundle).NotTo(BeNil())

			// Verify the decoded bundle matches the original
			Expect(decodedBundle.UnsMap.Entries).To(HaveLen(1))
			Expect(decodedBundle.UnsMap.Entries["test-topic"].Level0).To(Equal(originalBundle.UnsMap.Entries["test-topic"].Level0))
			Expect(decodedBundle.Events.Entries).To(HaveLen(1))
		})

		It("handles empty bundle", func() {
			// Create an empty bundle
			originalBundle := &proto.UnsBundle{
				UnsMap: &proto.TopicMap{
					Entries: make(map[string]*proto.TopicInfo),
				},
				Events: &proto.EventTable{
					Entries: make([]*proto.EventTableEntry, 0),
				},
			}

			// Encode the bundle
			protoBytes, err := bundleToProtobuf(originalBundle)
			Expect(err).To(BeNil())
			Expect(protoBytes).NotTo(BeNil())

			// Decode the bundle
			decodedBundle, err := protobufBytesToBundle(protoBytes)
			Expect(err).To(BeNil())
			Expect(decodedBundle).NotTo(BeNil())

			// Verify the decoded bundle is empty
			Expect(decodedBundle.UnsMap.Entries).To(BeEmpty())
			Expect(decodedBundle.Events.Entries).To(BeEmpty())
		})

		It("handles bundle with multiple entries", func() {
			// Create a bundle with multiple entries
			originalBundle := &proto.UnsBundle{
				UnsMap: &proto.TopicMap{
					Entries: map[string]*proto.TopicInfo{
						"topic1": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
						"topic2": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
					},
				},
				Events: &proto.EventTable{
					Entries: []*proto.EventTableEntry{
						{
							UnsTreeId: "topic1",
							Payload: &proto.EventTableEntry_Ts{
								Ts: &proto.TimeSeriesPayload{
									ScalarType:  proto.ScalarType_NUMERIC,
									TimestampMs: 1647753600000,
									Value: &proto.TimeSeriesPayload_NumericValue{
										NumericValue: &wrapperspb.DoubleValue{Value: 13.0},
									},
								},
							},
						},
						{
							UnsTreeId: "topic2",
							Payload: &proto.EventTableEntry_Ts{
								Ts: &proto.TimeSeriesPayload{
									ScalarType:  proto.ScalarType_NUMERIC,
									TimestampMs: 1647753600001,
									Value: &proto.TimeSeriesPayload_NumericValue{
										NumericValue: &wrapperspb.DoubleValue{Value: 13.0},
									},
								},
							},
						},
					},
				},
			}

			// Encode the bundle
			protoBytes, err := bundleToProtobuf(originalBundle)
			Expect(err).To(BeNil())
			Expect(protoBytes).NotTo(BeNil())

			// Decode the bundle
			decodedBundle, err := protobufBytesToBundle(protoBytes)
			Expect(err).To(BeNil())
			Expect(decodedBundle).NotTo(BeNil())

			// Verify the decoded bundle matches the original
			Expect(decodedBundle.UnsMap.Entries).To(HaveLen(2))
			Expect(decodedBundle.Events.Entries).To(HaveLen(2))
		})
	})

	Context("LZ4 Compression Functions", func() {
		Describe("CompressLZ4", func() {
			It("handles empty data", func() {
				compressed, err := CompressLZ4([]byte{})
				Expect(err).To(BeNil())
				Expect(compressed).To(Equal([]byte{}))
			})

			It("compresses small data", func() {
				originalData := []byte("Hello, World!")
				compressed, err := CompressLZ4(originalData)
				Expect(err).To(BeNil())
				Expect(compressed).NotTo(BeNil())
				Expect(len(compressed)).To(BeNumerically(">", 0))
			})

			It("compresses repetitive data efficiently", func() {
				// Create highly repetitive data that should compress well
				repetitiveData := make([]byte, 1000)
				for i := range repetitiveData {
					repetitiveData[i] = byte(i % 10) // Pattern repeats every 10 bytes
				}

				compressed, err := CompressLZ4(repetitiveData)
				Expect(err).To(BeNil())
				Expect(compressed).NotTo(BeNil())
				Expect(len(compressed)).To(BeNumerically("<", len(repetitiveData)),
					"Compressed size should be smaller than original for repetitive data")

				compressionRatio := float64(len(compressed)) / float64(len(repetitiveData))
				Expect(compressionRatio).To(BeNumerically("<", 0.8),
					"Should achieve at least 20% compression on repetitive data")
			})

			It("handles protobuf data", func() {
				// Create a realistic protobuf-like structure
				testBundle := &UnsBundle{
					UnsMap: &TopicMap{
						Entries: map[string]*TopicInfo{
							"test.topic.with.long.name": {
								Level0:       "enterprise",
								DataContract: "_historian",
							},
						},
					},
					Events: &EventTable{
						Entries: []*EventTableEntry{
							{
								UnsTreeId: "test.topic.with.long.name",
								Payload: &EventTableEntry_Ts{
									Ts: &TimeSeriesPayload{
										ScalarType:  ScalarType_NUMERIC,
										TimestampMs: 1647753600000,
										Value: &TimeSeriesPayload_NumericValue{
											NumericValue: &wrapperspb.DoubleValue{Value: 42.5},
										},
									},
								},
							},
						},
					},
				}

				protoBytes, err := bundleToProtobuf(testBundle)
				Expect(err).To(BeNil())

				compressed, err := CompressLZ4(protoBytes)
				Expect(err).To(BeNil())
				Expect(compressed).NotTo(BeNil())
				Expect(len(compressed)).To(BeNumerically(">", 0))
			})
		})

		Describe("DecompressLZ4", func() {
			It("handles empty data", func() {
				decompressed, err := DecompressLZ4([]byte{})
				Expect(err).To(BeNil())
				Expect(decompressed).To(Equal([]byte{}))
			})

			It("decompresses data correctly", func() {
				originalData := []byte("This is test data for compression and decompression testing!")

				compressed, err := CompressLZ4(originalData)
				Expect(err).To(BeNil())

				decompressed, err := DecompressLZ4(compressed)
				Expect(err).To(BeNil())
				Expect(decompressed).To(Equal(originalData))
			})

			It("handles large data sets", func() {
				// Create larger data set
				largeData := make([]byte, 10000)
				for i := range largeData {
					largeData[i] = byte(i % 256)
				}

				compressed, err := CompressLZ4(largeData)
				Expect(err).To(BeNil())

				decompressed, err := DecompressLZ4(compressed)
				Expect(err).To(BeNil())
				Expect(decompressed).To(Equal(largeData))
			})

			It("returns error for invalid compressed data", func() {
				invalidData := []byte{0x01, 0x02, 0x03, 0x04} // Not valid LZ4 data
				_, err := DecompressLZ4(invalidData)
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("Round-trip compression", func() {
			It("maintains data integrity for various data types", func() {
				testCases := [][]byte{
					[]byte("Simple string"),
					[]byte("{\"json\": \"data\", \"number\": 42}"),
					[]byte(string(make([]byte, 1000))), // Null bytes
					[]byte("Repeated pattern: " + strings.Repeat("ABCD", 100)),
				}

				for i, originalData := range testCases {
					compressed, err := CompressLZ4(originalData)
					Expect(err).To(BeNil(), "Test case %d: compression should succeed", i)

					decompressed, err := DecompressLZ4(compressed)
					Expect(err).To(BeNil(), "Test case %d: decompression should succeed", i)
					Expect(decompressed).To(Equal(originalData), "Test case %d: data should be identical", i)
				}
			})
		})

		Describe("Compression effectiveness", func() {
			It("achieves good compression ratios on structured data", func() {
				// Create a bundle with repetitive structured data
				bundle := &UnsBundle{
					UnsMap: &TopicMap{
						Entries: make(map[string]*TopicInfo),
					},
					Events: &EventTable{
						Entries: make([]*EventTableEntry, 0),
					},
				}

				// Add 100 similar topics and events
				for i := 0; i < 100; i++ {
					topicName := fmt.Sprintf("enterprise.site.area.line.workcell%d._historian.temperature", i)
					bundle.UnsMap.Entries[topicName] = &TopicInfo{
						Level0:       "enterprise",
						DataContract: "_historian",
					}

					bundle.Events.Entries = append(bundle.Events.Entries, &EventTableEntry{
						UnsTreeId: topicName,
						Payload: &EventTableEntry_Ts{
							Ts: &TimeSeriesPayload{
								ScalarType:  ScalarType_NUMERIC,
								TimestampMs: int64(1647753600000 + i),
								Value: &TimeSeriesPayload_NumericValue{
									NumericValue: &wrapperspb.DoubleValue{Value: float64(20 + i%10)},
								},
							},
						},
					})
				}

				protoBytes, err := bundleToProtobuf(bundle)
				Expect(err).To(BeNil())

				compressed, err := CompressLZ4(protoBytes)
				Expect(err).To(BeNil())

				originalSize := len(protoBytes)
				compressedSize := len(compressed)
				compressionRatio := float64(compressedSize) / float64(originalSize)

				By(fmt.Sprintf("Original size: %d bytes, Compressed size: %d bytes, Ratio: %.3f",
					originalSize, compressedSize, compressionRatio))

				// Should achieve good compression on structured data
				Expect(compressionRatio).To(BeNumerically("<", 0.7),
					"Should achieve at least 30% compression on structured protobuf data")
				Expect(compressedSize).To(BeNumerically("<", originalSize),
					"Compressed size should be smaller than original")
			})

			It("reports compression statistics for different data sizes", func() {
				sizes := []int{100, 1000, 10000}

				for _, size := range sizes {
					// Create test data with some structure
					testData := make([]byte, size)
					for i := range testData {
						testData[i] = byte((i / 10) % 256) // Some pattern but not too repetitive
					}

					compressed, err := CompressLZ4(testData)
					Expect(err).To(BeNil())

					originalSize := len(testData)
					compressedSize := len(compressed)
					compressionRatio := float64(compressedSize) / float64(originalSize)
					spaceInvert := originalSize - compressedSize

					By(fmt.Sprintf("Size %d: Original=%d, Compressed=%d, Ratio=%.3f, Saved=%d bytes",
						size, originalSize, compressedSize, compressionRatio, spaceInvert))

					Expect(compressedSize).To(BeNumerically(">", 0))
					Expect(compressionRatio).To(BeNumerically(">", 0))
					Expect(compressionRatio).To(BeNumerically("<=", 1))
				}
			})
		})
	})

	Context("Bundle compression integration", func() {
		It("verifies BundleToProtobufBytes actually compresses", func() {
			// Create a bundle with enough data to show compression
			bundle := &UnsBundle{
				UnsMap: &TopicMap{
					Entries: make(map[string]*TopicInfo),
				},
				Events: &EventTable{
					Entries: make([]*EventTableEntry, 0),
				},
			}

			// Add structured data that should compress well
			for i := 0; i < 50; i++ {
				topicName := fmt.Sprintf("umh.v1.enterprise.factory.line%d._historian.measurement", i)
				bundle.UnsMap.Entries[topicName] = &TopicInfo{
					Level0:       "enterprise",
					DataContract: "_historian",
				}

				bundle.Events.Entries = append(bundle.Events.Entries, &EventTableEntry{
					UnsTreeId: topicName,
					Payload: &EventTableEntry_Ts{
						Ts: &TimeSeriesPayload{
							ScalarType:  ScalarType_NUMERIC,
							TimestampMs: int64(1647753600000 + i*1000),
							Value: &TimeSeriesPayload_NumericValue{
								NumericValue: &wrapperspb.DoubleValue{Value: 25.0 + float64(i)*0.1},
							},
						},
					},
				})
			}

			// Get uncompressed protobuf size
			uncompressedProto, err := bundleToProtobuf(bundle)
			Expect(err).To(BeNil())

			// Get compressed size via BundleToProtobufBytes
			compressedBytes, err := BundleToProtobufBytes(bundle)
			Expect(err).To(BeNil())

			uncompressedSize := len(uncompressedProto)
			compressedSize := len(compressedBytes)
			compressionRatio := float64(compressedSize) / float64(uncompressedSize)

			By(fmt.Sprintf("Uncompressed: %d bytes, Compressed: %d bytes, Ratio: %.3f",
				uncompressedSize, compressedSize, compressionRatio))

			// Verify compression is happening
			Expect(compressedSize).To(BeNumerically("<", uncompressedSize),
				"BundleToProtobufBytes should produce smaller output than uncompressed")
			Expect(compressionRatio).To(BeNumerically("<", 0.9),
				"Should achieve at least 10% compression on structured data")
		})

		It("verifies round-trip compression preserves data", func() {
			originalBundle := &UnsBundle{
				UnsMap: &TopicMap{
					Entries: map[string]*TopicInfo{
						"test.compression.topic": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
					},
				},
				Events: &EventTable{
					Entries: []*EventTableEntry{
						{
							UnsTreeId: "test.compression.topic",
							Payload: &EventTableEntry_Ts{
								Ts: &TimeSeriesPayload{
									ScalarType:  ScalarType_NUMERIC,
									TimestampMs: 1647753600000,
									Value: &TimeSeriesPayload_NumericValue{
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
			Expect(decodedBundle.Events.Entries[0].Payload.(*EventTableEntry_Ts).Ts.Value.(*TimeSeriesPayload_NumericValue).NumericValue.Value).To(Equal(123.456))
		})

		It("fails properly when given invalid compressed data", func() {
			By("Testing with guaranteed invalid LZ4 data")
			// Use data that is guaranteed to fail LZ4 decompression
			// LZ4 format has specific headers - random bytes will reliably fail
			invalidLZ4Data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

			_, err := ProtobufBytesToBundleWithCompression(invalidLZ4Data)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to decompress LZ4 data"))

			By("Testing with corrupted LZ4 header")
			// Create valid compressed data, then corrupt it
			validBundle := &UnsBundle{
				UnsMap: &TopicMap{
					Entries: map[string]*TopicInfo{
						"test": {Level0: "enterprise"},
					},
				},
			}

			validCompressed, err := BundleToProtobufBytes(validBundle)
			Expect(err).To(BeNil())

			// Corrupt the compressed data by flipping bits
			corruptedData := make([]byte, len(validCompressed))
			copy(corruptedData, validCompressed)
			for i := 0; i < len(corruptedData) && i < 8; i++ {
				corruptedData[i] ^= 0xFF // Flip all bits in first 8 bytes
			}

			_, err = ProtobufBytesToBundleWithCompression(corruptedData)
			Expect(err).To(HaveOccurred())
			// Error could be from LZ4 decompression OR protobuf parsing
			// Both are valid failure modes for corrupted data
			Expect(err.Error()).To(Or(
				ContainSubstring("failed to decompress LZ4 data"),
				ContainSubstring("failed to parse protobuf data"),
			))
		})
	})
})
