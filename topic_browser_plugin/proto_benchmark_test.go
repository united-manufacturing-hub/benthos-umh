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
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

func createTestBundle() *UnsBundle {
	return &UnsBundle{
		UnsMap: &TopicMap{
			Entries: map[string]*TopicInfo{
				"test-topic": {
					Level0:       "enterprise",
					DataContract: "_historian",
				},
			},
		},
		Events: &EventTable{
			Entries: []*EventTableEntry{
				{
					UnsTreeId: "test-topic",
					Payload: &EventTableEntry_Ts{
						Ts: &TimeSeriesPayload{
							ScalarType:  ScalarType_NUMERIC,
							TimestampMs: 1647753600000,
							Value: &TimeSeriesPayload_NumericValue{
								NumericValue: &wrapperspb.DoubleValue{Value: 13.0},
							},
						},
					},
				},
			},
		},
	}
}

func createLargeBundle(numTopics int, numEvents int) *UnsBundle {
	bundle := &UnsBundle{
		UnsMap: &TopicMap{
			Entries: make(map[string]*TopicInfo),
		},
		Events: &EventTable{
			Entries: make([]*EventTableEntry, 0, numEvents),
		},
	}

	// Create topics
	for i := 0; i < numTopics; i++ {
		topicID := fmt.Sprintf("topic-%d", i)
		bundle.UnsMap.Entries[topicID] = &TopicInfo{
			Level0:       "enterprise",
			DataContract: "_historian",
		}
	}

	// Create events
	for i := 0; i < numEvents; i++ {
		topicID := fmt.Sprintf("topic-%d", i%numTopics)
		bundle.Events.Entries = append(bundle.Events.Entries, &EventTableEntry{
			UnsTreeId: topicID,
			Payload: &EventTableEntry_Ts{
				Ts: &TimeSeriesPayload{
					ScalarType:  ScalarType_NUMERIC,
					TimestampMs: int64(1647753600000 + i),
					Value: &TimeSeriesPayload_NumericValue{
						NumericValue: &wrapperspb.DoubleValue{Value: rand.Float64() * 100},
					},
				},
			},
		})
	}

	return bundle
}

func createFloatBytes(value float64) []byte {
	bits := make([]byte, 8)
	binary.LittleEndian.PutUint64(bits, binary.LittleEndian.Uint64(bits))
	return bits
}

func BenchmarkBundleToProtobufBytes(b *testing.B) {
	b.Run("small bundle", func(b *testing.B) {
		bundle := createTestBundle()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := bundleToProtobuf(bundle)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("large bundle", func(b *testing.B) {
		bundle := createLargeBundle(100, 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := bundleToProtobuf(bundle)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("very large bundle", func(b *testing.B) {
		bundle := createLargeBundle(1000, 10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := bundleToProtobuf(bundle)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkProtobufBytesToBundle(b *testing.B) {
	b.Run("small bundle", func(b *testing.B) {
		bundle := createTestBundle()
		protoBytes, err := bundleToProtobuf(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := protobufBytesToBundle(protoBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("large bundle", func(b *testing.B) {
		bundle := createLargeBundle(100, 1000)
		protoBytes, err := bundleToProtobuf(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := protobufBytesToBundle(protoBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("very large bundle", func(b *testing.B) {
		bundle := createLargeBundle(1000, 10000)
		protoBytes, err := bundleToProtobuf(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := protobufBytesToBundle(protoBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRoundTrip(b *testing.B) {
	b.Run("small bundle", func(b *testing.B) {
		bundle := createTestBundle()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			protoBytes, err := bundleToProtobuf(bundle)
			if err != nil {
				b.Fatal(err)
			}
			_, err = protobufBytesToBundle(protoBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("large bundle", func(b *testing.B) {
		bundle := createLargeBundle(100, 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			protoBytes, err := bundleToProtobuf(bundle)
			if err != nil {
				b.Fatal(err)
			}
			_, err = protobufBytesToBundle(protoBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("very large bundle", func(b *testing.B) {
		bundle := createLargeBundle(1000, 10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			protoBytes, err := bundleToProtobuf(bundle)
			if err != nil {
				b.Fatal(err)
			}
			_, err = protobufBytesToBundle(protoBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBundleToProtobufBytesWithCompression(b *testing.B) {
	b.Run("small bundle", func(b *testing.B) {
		bundle := createTestBundle()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := BundleToProtobufBytesWithCompression(bundle)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("large bundle", func(b *testing.B) {
		bundle := createLargeBundle(100, 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := BundleToProtobufBytesWithCompression(bundle)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("very large bundle", func(b *testing.B) {
		bundle := createLargeBundle(1000, 10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := BundleToProtobufBytesWithCompression(bundle)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkProtobufBytesToBundleWithCompression(b *testing.B) {
	b.Run("small bundle", func(b *testing.B) {
		bundle := createTestBundle()
		compressedBytes, err := BundleToProtobufBytesWithCompression(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := ProtobufBytesToBundleWithCompression(compressedBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("large bundle", func(b *testing.B) {
		bundle := createLargeBundle(100, 1000)
		compressedBytes, err := BundleToProtobufBytesWithCompression(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := ProtobufBytesToBundleWithCompression(compressedBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("very large bundle", func(b *testing.B) {
		bundle := createLargeBundle(1000, 10000)
		compressedBytes, err := BundleToProtobufBytesWithCompression(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := ProtobufBytesToBundleWithCompression(compressedBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCompressionRoundTrip(b *testing.B) {
	b.Run("small bundle", func(b *testing.B) {
		bundle := createTestBundle()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			compressedBytes, err := BundleToProtobufBytesWithCompression(bundle)
			if err != nil {
				b.Fatal(err)
			}
			_, err = ProtobufBytesToBundleWithCompression(compressedBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("large bundle", func(b *testing.B) {
		bundle := createLargeBundle(100, 1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			compressedBytes, err := BundleToProtobufBytesWithCompression(bundle)
			if err != nil {
				b.Fatal(err)
			}
			_, err = ProtobufBytesToBundleWithCompression(compressedBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("very large bundle", func(b *testing.B) {
		bundle := createLargeBundle(1000, 10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			compressedBytes, err := BundleToProtobufBytesWithCompression(bundle)
			if err != nil {
				b.Fatal(err)
			}
			_, err = ProtobufBytesToBundleWithCompression(compressedBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCompressionRatio(b *testing.B) {
	b.Run("small bundle", func(b *testing.B) {
		bundle := createTestBundle()
		protoBytes, err := bundleToProtobuf(bundle)
		if err != nil {
			b.Fatal(err)
		}
		compressedBytes, err := BundleToProtobufBytesWithCompression(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(compressedBytes))/float64(len(protoBytes)), "compression_ratio")
		b.ReportMetric(float64(len(protoBytes)), "original_size_bytes")
		b.ReportMetric(float64(len(compressedBytes)), "compressed_size_bytes")
		b.ReportMetric(float64(len(protoBytes)-len(compressedBytes)), "size_difference_bytes")
	})

	b.Run("large bundle", func(b *testing.B) {
		bundle := createLargeBundle(100, 1000)
		protoBytes, err := bundleToProtobuf(bundle)
		if err != nil {
			b.Fatal(err)
		}
		compressedBytes, err := BundleToProtobufBytesWithCompression(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(compressedBytes))/float64(len(protoBytes)), "compression_ratio")
		b.ReportMetric(float64(len(protoBytes)), "original_size_bytes")
		b.ReportMetric(float64(len(compressedBytes)), "compressed_size_bytes")
		b.ReportMetric(float64(len(protoBytes)-len(compressedBytes)), "size_difference_bytes")
	})

	b.Run("very large bundle", func(b *testing.B) {
		bundle := createLargeBundle(1000, 10000)
		protoBytes, err := bundleToProtobuf(bundle)
		if err != nil {
			b.Fatal(err)
		}
		compressedBytes, err := BundleToProtobufBytesWithCompression(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(compressedBytes))/float64(len(protoBytes)), "compression_ratio")
		b.ReportMetric(float64(len(protoBytes)), "original_size_bytes")
		b.ReportMetric(float64(len(compressedBytes)), "compressed_size_bytes")
		b.ReportMetric(float64(len(protoBytes)-len(compressedBytes)), "size_difference_bytes")
	})
}
