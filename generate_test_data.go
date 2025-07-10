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
	// Create test bundle with sample data
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
							ScalarType:  proto.ScalarType_BOOLEAN,
							TimestampMs: 1751283498745,
							Value: &proto.TimeSeriesPayload_BooleanValue{
								BooleanValue: &wrapperspb.BoolValue{Value: false},
							},
						},
					},
				},
			},
		},
	}

	// Generate uncompressed protobuf data
	protoBytes, err := topic_browser_plugin.BundleToProtobufBytes(bundle)
	if err != nil {
		log.Fatalf("Failed to generate protobuf bytes: %v", err)
	}

	// Output as hex string
	fmt.Printf("Uncompressed protobuf data: %s\n", hex.EncodeToString(protoBytes))

	// Test round-trip
	decodedBundle, err := topic_browser_plugin.ProtobufBytesToBundleWithCompression(protoBytes)
	if err != nil {
		log.Fatalf("Failed to decode: %v", err)
	}

	fmt.Printf("Successfully decoded bundle with %d map entries and %d event entries\n",
		len(decodedBundle.UnsMap.Entries), len(decodedBundle.Events.Entries))
}
