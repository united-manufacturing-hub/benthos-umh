package main

import (
	"encoding/hex"
	"fmt"
	"log"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
	"github.com/united-manufacturing-hub/benthos-umh/topic_browser_plugin"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func main() {
	// The original sample data was LZ4 compressed, but now we need uncompressed protobuf
	// Let's decode the old compressed data to understand what it represents
	sampleDataHex := "f643a2d68585fc325214080128c9d58585fc32120909000000000000f03f"

	// First, let's try to decompress the old data to understand what it was
	compressedData, err := hex.DecodeString(sampleDataHex)
	if err != nil {
		log.Fatal("Failed to decode hex:", err)
	}

	fmt.Printf("Original compressed data length: %d bytes\n", len(compressedData))
	fmt.Printf("Original hex: %s\n", sampleDataHex)

	// Since we can't decompress it anymore, let's create a similar bundle that would
	// represent typical test data and generate its uncompressed protobuf
	bundle := &proto.UnsBundle{
		UnsMap: &proto.TopicMap{
			Entries: map[string]*proto.TopicInfo{
				"umh.v1.UMH-Systems-GmbH---Dev-Team._allhands": {
					Level0:       "UMH-Systems-GmbH---Dev-Team",
					DataContract: "_allhands",
				},
			},
		},
		Events: &proto.EventTable{
			Entries: []*proto.EventTableEntry{
				{
					UnsTreeId: "umh.v1.UMH-Systems-GmbH---Dev-Team._allhands",
					Payload: &proto.EventTableEntry_Ts{
						Ts: &proto.TimeSeriesPayload{
							ScalarType:  proto.ScalarType_NUMERIC,
							TimestampMs: 1751283495745,
							Value: &proto.TimeSeriesPayload_NumericValue{
								NumericValue: &wrapperspb.DoubleValue{Value: 1.0},
							},
						},
					},
				},
			},
		},
	}

	// Generate the uncompressed protobuf data
	uncompressedData, err := topic_browser_plugin.BundleToProtobufBytes(bundle)
	if err != nil {
		log.Fatal("Failed to generate protobuf:", err)
	}

	fmt.Printf("New uncompressed data length: %d bytes\n", len(uncompressedData))
	fmt.Printf("New uncompressed hex: %s\n", hex.EncodeToString(uncompressedData))

	// Verify we can parse it back
	parsedBundle, err := topic_browser_plugin.ProtobufBytesToBundleWithCompression(uncompressedData)
	if err != nil {
		log.Fatal("Failed to parse back:", err)
	}

	fmt.Printf("Parsed bundle has %d topics and %d events\n",
		len(parsedBundle.UnsMap.Entries), len(parsedBundle.Events.Entries))

	// Print the exact string to use in the test
	fmt.Printf("\nUse this in the test:\n")
	fmt.Printf("sampleData := \"%s\"\n", hex.EncodeToString(uncompressedData))
}
