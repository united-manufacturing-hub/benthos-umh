package tag_browser_plugin

import (
	"fmt"
	"testing"

	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func createTestBundle() *tagbrowserpluginprotobuf.UnsBundle {
	return &tagbrowserpluginprotobuf.UnsBundle{
		UnsMap: &tagbrowserpluginprotobuf.UnsMap{
			Entries: map[string]*tagbrowserpluginprotobuf.UnsInfo{
				"test-topic": {
					Enterprise: "enterprise",
					Schema:     "_historian",
					EventTag:   wrapperspb.String("temperature"),
				},
			},
		},
		Events: &tagbrowserpluginprotobuf.EventTable{
			Entries: []*tagbrowserpluginprotobuf.EventTableEntry{
				{
					IsTimeseries: true,
					TimestampMs:  wrapperspb.Int64(1647753600000),
					Value: &anypb.Any{
						TypeUrl: "golang/float64",
						Value:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40}, // 13.0 in float64
					},
				},
			},
		},
	}
}

func createLargeTestBundle() *tagbrowserpluginprotobuf.UnsBundle {
	bundle := &tagbrowserpluginprotobuf.UnsBundle{
		UnsMap: &tagbrowserpluginprotobuf.UnsMap{
			Entries: make(map[string]*tagbrowserpluginprotobuf.UnsInfo),
		},
		Events: &tagbrowserpluginprotobuf.EventTable{
			Entries: make([]*tagbrowserpluginprotobuf.EventTableEntry, 0, 1000),
		},
	}

	// Add 1000 entries to the bundle
	for i := 0; i < 1000; i++ {
		topic := fmt.Sprintf("topic-%d", i)
		bundle.UnsMap.Entries[topic] = &tagbrowserpluginprotobuf.UnsInfo{
			Enterprise: "enterprise",
			Schema:     "_historian",
			EventTag:   wrapperspb.String(fmt.Sprintf("sensor-%d", i)),
		}

		bundle.Events.Entries = append(bundle.Events.Entries, &tagbrowserpluginprotobuf.EventTableEntry{
			IsTimeseries: true,
			TimestampMs:  wrapperspb.Int64(1647753600000 + int64(i)),
			Value: &anypb.Any{
				TypeUrl: "golang/float64",
				Value:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40}, // 13.0 in float64
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
			_, err := BundleToProtobuf(bundle)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("large bundle", func(b *testing.B) {
		bundle := createLargeTestBundle()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := BundleToProtobuf(bundle)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkProtobufBytesToBundle(b *testing.B) {
	b.Run("small bundle", func(b *testing.B) {
		bundle := createTestBundle()
		protoBytes, err := BundleToProtobuf(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := ProtobufBytesToBundle(protoBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("large bundle", func(b *testing.B) {
		bundle := createLargeTestBundle()
		protoBytes, err := BundleToProtobuf(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := ProtobufBytesToBundle(protoBytes)
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
			protoBytes, err := BundleToProtobuf(bundle)
			if err != nil {
				b.Fatal(err)
			}
			_, err = ProtobufBytesToBundle(protoBytes)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("large bundle", func(b *testing.B) {
		bundle := createLargeTestBundle()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			protoBytes, err := BundleToProtobuf(bundle)
			if err != nil {
				b.Fatal(err)
			}
			_, err = ProtobufBytesToBundle(protoBytes)
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
}

func BenchmarkCompressionRatio(b *testing.B) {
	b.Run("small bundle", func(b *testing.B) {
		bundle := createTestBundle()
		protoBytes, err := BundleToProtobuf(bundle)
		if err != nil {
			b.Fatal(err)
		}
		compressedBytes, err := BundleToProtobufBytesWithCompression(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(compressedBytes))/float64(len(protoBytes)), "compression_ratio")
	})

	b.Run("large bundle", func(b *testing.B) {
		bundle := createLargeTestBundle()
		protoBytes, err := BundleToProtobuf(bundle)
		if err != nil {
			b.Fatal(err)
		}
		compressedBytes, err := BundleToProtobufBytesWithCompression(bundle)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(compressedBytes))/float64(len(protoBytes)), "compression_ratio")
	})
}
