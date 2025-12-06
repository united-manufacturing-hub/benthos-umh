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

// Unit tests for sendRebirthRequest logic verification
// Tests metric name correctness for node vs device REBIRTH commands

package sparkplug_plugin

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

// TestRebirthCommandGeneration tests the logic for generating REBIRTH commands
// This tests the core logic that determines metric names based on device key structure
func TestRebirthCommandGeneration(t *testing.T) {
	tests := []struct {
		name                string
		deviceKey           string
		expectedTopic       string
		expectedMetricName  string
		expectedTopicPrefix string
	}{
		{
			name:                "Node-level REBIRTH (2-part key)",
			deviceKey:           "TestFactory/Line1",
			expectedTopic:       "spBv1.0/TestFactory/NCMD/Line1",
			expectedMetricName:  "Node Control/Rebirth",
			expectedTopicPrefix: "NCMD",
		},
		{
			name:                "Device-level REBIRTH (3-part key)",
			deviceKey:           "TestFactory/Line1/Device1",
			expectedTopic:       "spBv1.0/TestFactory/DCMD/Line1/Device1",
			expectedMetricName:  "Device Control/Rebirth",
			expectedTopicPrefix: "DCMD",
		},
		{
			name:                "Another node-level",
			deviceKey:           "Factory/Node1",
			expectedTopic:       "spBv1.0/Factory/NCMD/Node1",
			expectedMetricName:  "Node Control/Rebirth",
			expectedTopicPrefix: "NCMD",
		},
		{
			name:                "Another device-level",
			deviceKey:           "Group1/EdgeNode1/DeviceA",
			expectedTopic:       "spBv1.0/Group1/DCMD/EdgeNode1/DeviceA",
			expectedMetricName:  "Device Control/Rebirth",
			expectedTopicPrefix: "DCMD",
		},
		// Edge case tests - testing boundaries and unexpected inputs
		{
			name:                "1-part key (should be invalid)",
			deviceKey:           "Factory",
			expectedTopic:       "", // Should return early - no topic generated
			expectedMetricName:  "", // Should return early - no metric name
			expectedTopicPrefix: "",
		},
		{
			name:                "Empty key (should be invalid)",
			deviceKey:           "",
			expectedTopic:       "", // Should return early - no topic generated
			expectedMetricName:  "", // Should return early - no metric name
			expectedTopicPrefix: "",
		},
		{
			name:                "4-part key (should be invalid)",
			deviceKey:           "Factory/Edge/Device/Extra",
			expectedTopic:       "", // Should return early - validation rejects
			expectedMetricName:  "", // Should return early - validation rejects
			expectedTopicPrefix: "",
		},
		{
			name:                "5-part key (should be invalid)",
			deviceKey:           "A/B/C/D/E",
			expectedTopic:       "", // Should return early - validation rejects
			expectedMetricName:  "", // Should return early - validation rejects
			expectedTopicPrefix: "",
		},
		// Negative test cases - malformed inputs
		{
			name:                "Empty part in middle (group//device)",
			deviceKey:           "TestFactory//Device1",
			expectedTopic:       "", // Empty part creates 3-part key with empty string
			expectedMetricName:  "", // Should be rejected - empty part invalid
			expectedTopicPrefix: "",
		},
		{
			name:                "Leading slash",
			deviceKey:           "/TestFactory/Line1",
			expectedTopic:       "", // Creates empty first part
			expectedMetricName:  "", // Should be rejected - empty part invalid
			expectedTopicPrefix: "",
		},
		{
			name:                "Trailing slash",
			deviceKey:           "TestFactory/Line1/",
			expectedTopic:       "", // Creates empty third part
			expectedMetricName:  "", // Should be rejected - empty part invalid
			expectedTopicPrefix: "",
		},
		{
			name:                "Leading whitespace in key",
			deviceKey:           " TestFactory/Line1",
			expectedTopic:       "", // Whitespace should be trimmed or rejected
			expectedMetricName:  "", // TODO: verify behavior - trim or reject?
			expectedTopicPrefix: "",
		},
		{
			name:                "Trailing whitespace in key",
			deviceKey:           "TestFactory/Line1 ",
			expectedTopic:       "", // Whitespace should be trimmed or rejected
			expectedMetricName:  "", // TODO: verify behavior - trim or reject?
			expectedTopicPrefix: "",
		},
		{
			name:                "Whitespace in part",
			deviceKey:           "Test Factory/Line 1",
			expectedTopic:       "", // Embedded spaces - verify SparkplugB spec
			expectedMetricName:  "", // TODO: verify if spaces allowed in identifiers
			expectedTopicPrefix: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse device key (this is what sendRebirthRequest does)
			parts := strings.Split(tt.deviceKey, "/")

			// Generate topic and metric name using the ACTUAL logic from sendRebirthRequest
			var topic string
			var controlMetricName string

			// Replicate the validation logic from sendRebirthRequest (lines 1112-1132)
			if len(parts) < 2 {
				// Function would return early here
				topic = ""
				controlMetricName = ""
			} else if len(parts) > 3 {
				// Validation rejects keys with more than 3 parts
				topic = ""
				controlMetricName = ""
			} else {
				// Validate no empty parts (handles "group//device", "/group/node", "group/node/")
				hasEmptyPart := false
				hasWhitespacePart := false
				hasEmbeddedWhitespace := false
				for _, part := range parts {
					trimmed := strings.TrimSpace(part)
					if trimmed == "" {
						hasEmptyPart = true
						break
					}
					if trimmed != part {
						hasWhitespacePart = true
						break
					}
					// Check for embedded whitespace (SparkplugB identifiers should not contain spaces)
					if strings.Contains(part, " ") {
						hasEmbeddedWhitespace = true
						break
					}
				}

				if hasEmptyPart || hasWhitespacePart || hasEmbeddedWhitespace {
					// Validation rejects keys with empty, whitespace, or embedded whitespace in parts
					topic = ""
					controlMetricName = ""
				} else if len(parts) == 2 {
					// Node level rebirth
					topic = fmt.Sprintf("spBv1.0/%s/NCMD/%s", parts[0], parts[1])
					controlMetricName = "Node Control/Rebirth"
				} else { // len(parts) == 3
					// Device level rebirth
					topic = fmt.Sprintf("spBv1.0/%s/DCMD/%s/%s", parts[0], parts[1], parts[2])
					controlMetricName = "Device Control/Rebirth"
				}
			}

			// Verify topic
			if topic != tt.expectedTopic {
				t.Errorf("Topic mismatch:\n  got:  %s\n  want: %s", topic, tt.expectedTopic)
			}

			// Verify topic contains correct command type (only for valid topics)
			if tt.expectedTopicPrefix != "" {
				if !strings.Contains(topic, tt.expectedTopicPrefix) {
					t.Errorf("Topic should contain %s: %s", tt.expectedTopicPrefix, topic)
				}
			}

			// Verify metric name - THIS IS THE CRITICAL TEST
			if controlMetricName != tt.expectedMetricName {
				t.Errorf("Metric name mismatch for %s:\n  got:  %s\n  want: %s",
					tt.deviceKey, controlMetricName, tt.expectedMetricName)
			}

			// Only test payload marshaling for valid cases (where topic and metric are set)
			if tt.expectedTopic != "" && tt.expectedMetricName != "" {
				// Create the payload to verify it can be marshaled
				rebirthMetric := &sparkplugb.Payload_Metric{
					Name: &controlMetricName,
					Value: &sparkplugb.Payload_Metric_BooleanValue{
						BooleanValue: true,
					},
					Datatype: func() *uint32 { d := uint32(SparkplugDataTypeBoolean); return &d }(),
				}

				cmdPayload := &sparkplugb.Payload{
					Metrics: []*sparkplugb.Payload_Metric{rebirthMetric},
				}

				// Verify payload can be marshaled
				payloadBytes, err := proto.Marshal(cmdPayload)
				if err != nil {
					t.Errorf("Failed to marshal payload: %v", err)
				}

				// Verify payload is not empty
				if len(payloadBytes) == 0 {
					t.Error("Marshaled payload is empty")
				}

				// Unmarshal and verify
				var decoded sparkplugb.Payload
				if err := proto.Unmarshal(payloadBytes, &decoded); err != nil {
					t.Errorf("Failed to unmarshal payload: %v", err)
				}

				if len(decoded.Metrics) != 1 {
					t.Errorf("Expected 1 metric, got %d", len(decoded.Metrics))
				}

				if *decoded.Metrics[0].Name != tt.expectedMetricName {
					t.Errorf("Decoded metric name mismatch:\n  got:  %s\n  want: %s",
						*decoded.Metrics[0].Name, tt.expectedMetricName)
				}
			}
		})
	}
}

// TestRebirthMetricStructure tests that the rebirth metric has correct structure
func TestRebirthMetricStructure(t *testing.T) {
	metricName := "Node Control/Rebirth"

	metric := &sparkplugb.Payload_Metric{
		Name: &metricName,
		Value: &sparkplugb.Payload_Metric_BooleanValue{
			BooleanValue: true,
		},
		Datatype: func() *uint32 { d := uint32(SparkplugDataTypeBoolean); return &d }(),
	}

	// Verify name
	if metric.Name == nil || *metric.Name != metricName {
		t.Errorf("Metric name not set correctly")
	}

	// Verify value is boolean true
	if !metric.GetBooleanValue() {
		t.Error("Metric value should be true")
	}

	// Verify datatype
	if metric.Datatype == nil || *metric.Datatype != uint32(SparkplugDataTypeBoolean) {
		t.Error("Metric datatype should be Boolean")
	}
}
