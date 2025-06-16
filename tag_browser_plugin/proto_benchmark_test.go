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

package tag_browser_plugin

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/types/known/anypb"
)

func createTestBundle() *tagbrowserpluginprotobuf.UnsBundle {
	return &tagbrowserpluginprotobuf.UnsBundle{
		UnsMap: &tagbrowserpluginprotobuf.TopicMap{
			Entries: map[string]*tagbrowserpluginprotobuf.TopicInfo{
				"test-topic": {
					Level0:       "enterprise",
					DataContract: "_historian",
				},
			},
		},
		Events: &tagbrowserpluginprotobuf.EventTable{
			Entries: []*tagbrowserpluginprotobuf.EventTableEntry{
				{
					UnsTreeId: "test-topic",
					Payload: &tagbrowserpluginprotobuf.EventTableEntry_Ts{
						Ts: &tagbrowserpluginprotobuf.TimeSeriesPayload{
							ScalarType:  tagbrowserpluginprotobuf.ScalarType_NUMERIC,
							TimestampMs: 1647753600000,
							Value: &anypb.Any{
								TypeUrl: "golang/float64",
								Value:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40}, // 13.0 in float64
							},
						},
					},
				},
			},
		},
	}
}

func createLargeTestBundle() *tagbrowserpluginprotobuf.UnsBundle {
	bundle := &tagbrowserpluginprotobuf.UnsBundle{
		UnsMap: &tagbrowserpluginprotobuf.TopicMap{
			Entries: make(map[string]*tagbrowserpluginprotobuf.TopicInfo),
		},
		Events: &tagbrowserpluginprotobuf.EventTable{
			Entries: make([]*tagbrowserpluginprotobuf.EventTableEntry, 0, 1000),
		},
	}

	// Add 1000 entries to the bundle
	for i := 0; i < 1000; i++ {
		topic := fmt.Sprintf("topic-%d", i)
		bundle.UnsMap.Entries[topic] = &tagbrowserpluginprotobuf.TopicInfo{
			Level0:       "enterprise",
			DataContract: "_historian",
		}

		bundle.Events.Entries = append(bundle.Events.Entries, &tagbrowserpluginprotobuf.EventTableEntry{
			UnsTreeId: topic,
			Payload: &tagbrowserpluginprotobuf.EventTableEntry_Ts{
				Ts: &tagbrowserpluginprotobuf.TimeSeriesPayload{
					ScalarType:  tagbrowserpluginprotobuf.ScalarType_NUMERIC,
					TimestampMs: 1647753600000 + int64(i),
					Value: &anypb.Any{
						TypeUrl: "golang/float64",
						Value:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40}, // 13.0 in float64
					},
				},
			},
		})
	}

	return bundle
}

func createVeryLargeTestBundle() *tagbrowserpluginprotobuf.UnsBundle {
	bundle := &tagbrowserpluginprotobuf.UnsBundle{
		UnsMap: &tagbrowserpluginprotobuf.TopicMap{
			Entries: make(map[string]*tagbrowserpluginprotobuf.TopicInfo),
		},
		Events: &tagbrowserpluginprotobuf.EventTable{
			Entries: make([]*tagbrowserpluginprotobuf.EventTableEntry, 0, 100000),
		},
	}

	// Create 10 different UNSInfo entries
	for i := 0; i < 10; i++ {
		topic := fmt.Sprintf("topic-%d", i)
		bundle.UnsMap.Entries[topic] = &tagbrowserpluginprotobuf.TopicInfo{
			Level0:       "enterprise",
			DataContract: "_historian",
		}
	}

	// Add 100,000 entries to the bundle, randomly distributed among the 10 UNSInfo entries
	for i := 0; i < 100000; i++ {
		// Generate a random float64 value
		value := rand.Float64()
		valueBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBytes, uint64(value))

		// Randomly select one of the 10 UNSInfo entries
		topicIndex := rand.Intn(10)
		topic := fmt.Sprintf("topic-%d", topicIndex)

		bundle.Events.Entries = append(bundle.Events.Entries, &tagbrowserpluginprotobuf.EventTableEntry{
			UnsTreeId: topic, // Use the topic as the UnsTreeId
			Payload: &tagbrowserpluginprotobuf.EventTableEntry_Ts{
				Ts: &tagbrowserpluginprotobuf.TimeSeriesPayload{
					ScalarType:  tagbrowserpluginprotobuf.ScalarType_NUMERIC,
					TimestampMs: 1647753600000 + int64(i),
					Value: &anypb.Any{
						TypeUrl: "golang/float64",
						Value:   valueBytes,
					},
				},
			},
		})
	}

	return bundle
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
		bundle := createLargeTestBundle()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := bundleToProtobuf(bundle)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("very large bundle", func(b *testing.B) {
		bundle := createVeryLargeTestBundle()
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
		bundle := createLargeTestBundle()
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
		bundle := createVeryLargeTestBundle()
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
		bundle := createLargeTestBundle()
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
		bundle := createVeryLargeTestBundle()
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
		bundle := createLargeTestBundle()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := BundleToProtobufBytesWithCompression(bundle)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("very large bundle", func(b *testing.B) {
		bundle := createVeryLargeTestBundle()
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
		bundle := createLargeTestBundle()
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
		bundle := createVeryLargeTestBundle()
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
		bundle := createLargeTestBundle()
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
		bundle := createVeryLargeTestBundle()
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
		bundle := createLargeTestBundle()
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
		bundle := createVeryLargeTestBundle()
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
