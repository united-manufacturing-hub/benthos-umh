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

package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/united-manufacturing-hub/benthos-umh/topic_browser_plugin"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run decode_bundle.go <hex_encoded_data>")
		fmt.Println("  or: go run decode_bundle.go <file_containing_hex_data>")
		os.Exit(1)
	}

	input := os.Args[1]
	var hexData string

	// Check if input is a file or direct hex data
	if strings.HasPrefix(input, "STARTSTARTSTART") {
		// Extract hex data from the wrapped format
		lines := strings.Split(input, "\n")
		if len(lines) >= 2 {
			hexData = lines[1]
		}
	} else if _, err := os.Stat(input); err == nil {
		// It's a file
		data, err := os.ReadFile(input)
		if err != nil {
			log.Fatalf("Error reading file: %v", err)
		}
		content := string(data)
		if strings.HasPrefix(content, "STARTSTARTSTART") {
			lines := strings.Split(content, "\n")
			if len(lines) >= 2 {
				hexData = lines[1]
			}
		} else {
			hexData = strings.TrimSpace(content)
		}
	} else {
		// Assume it's direct hex data
		hexData = input
	}

	// Decode hex
	protoBytes, err := hex.DecodeString(hexData)
	if err != nil {
		log.Fatalf("Error decoding hex: %v", err)
	}

	// Decode protobuf (with optimized LZ4 block decompression)
	bundle, err := topic_browser_plugin.ProtobufBytesToBundleWithCompression(protoBytes)
	if err != nil {
		log.Fatalf("Error decoding protobuf: %v", err)
	}

	// Pretty print the bundle
	fmt.Println("=== UNS Bundle Contents ===")

	// Print topic map
	if bundle.UnsMap != nil && len(bundle.UnsMap.Entries) > 0 {
		fmt.Printf("\n📍 Topics (%d):\n", len(bundle.UnsMap.Entries))
		for treeId, topic := range bundle.UnsMap.Entries {
			fmt.Printf("  Tree ID: %s\n", treeId)
			fmt.Printf("    Level0: %s\n", topic.Level0)
			fmt.Printf("    Location Sublevels: %v\n", topic.LocationSublevels)
			fmt.Printf("    Virtual Path: %s\n", topic.GetVirtualPath())
			fmt.Printf("    Name: %s\n", topic.Name)
			fmt.Printf("    Data Contract: %s\n", topic.DataContract)
			if topic.Metadata != nil && len(topic.Metadata) > 0 {
				fmt.Printf("    Metadata: %v\n", topic.Metadata)
			}
			fmt.Println()
		}
	}

	// Print events
	if bundle.Events != nil && len(bundle.Events.Entries) > 0 {
		fmt.Printf("📊 Events (%d):\n", len(bundle.Events.Entries))
		for i, event := range bundle.Events.Entries {
			fmt.Printf("  Event #%d:\n", i+1)
			fmt.Printf("    UNS Tree ID: %s\n", event.UnsTreeId)

			// Handle payload based on type
			if event.GetTs() != nil {
				ts := event.GetTs()
				fmt.Printf("    Type: Time Series\n")
				fmt.Printf("    Timestamp: %d ms\n", ts.TimestampMs)
				fmt.Printf("    Scalar Type: %s\n", ts.ScalarType.String())

				switch ts.ScalarType.String() {
				case "NUMERIC":
					if ts.GetNumericValue() != nil {
						fmt.Printf("    Value: %f\n", ts.GetNumericValue().Value)
					}
				case "STRING":
					if ts.GetStringValue() != nil {
						fmt.Printf("    Value: %s\n", ts.GetStringValue().Value)
					}
				case "BOOLEAN":
					if ts.GetBooleanValue() != nil {
						fmt.Printf("    Value: %t\n", ts.GetBooleanValue().Value)
					}
				}
			} else if event.GetRel() != nil {
				rel := event.GetRel()
				fmt.Printf("    Type: Relational\n")

				// Try to pretty print JSON
				var jsonData interface{}
				if err := json.Unmarshal(rel.Json, &jsonData); err == nil {
					prettyJSON, _ := json.MarshalIndent(jsonData, "    ", "  ")
					fmt.Printf("    JSON Data:\n    %s\n", string(prettyJSON))
				} else {
					fmt.Printf("    Raw JSON: %s\n", string(rel.Json))
				}
			}

			if event.RawKafkaMsg != nil {
				fmt.Printf("    Kafka Message Payload: %s\n", string(event.RawKafkaMsg.Payload))
			}

			if len(event.BridgedBy) > 0 {
				fmt.Printf("    Bridged By: %v\n", event.BridgedBy)
			}

			fmt.Printf("    Produced At: %d ms\n", event.ProducedAtMs)
			fmt.Println()
		}
	}

	// Summary
	topicCount := 0
	if bundle.UnsMap != nil {
		topicCount = len(bundle.UnsMap.Entries)
	}
	eventCount := 0
	if bundle.Events != nil {
		eventCount = len(bundle.Events.Entries)
	}

	fmt.Printf("📋 Summary: %d topics, %d events\n", topicCount, eventCount)
}
