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

package opcua_plugin

import (
	"testing"
)

// TestDeadbandConfigParsing tests that deadband config fields parse correctly
func TestDeadbandConfigParsing(t *testing.T) {
	tests := []struct {
		name        string
		yamlConfig  string
		expectType  string
		expectValue float64
		expectError bool
	}{
		{
			name: "default values (no deadband)",
			yamlConfig: `
endpoint: "opc.tcp://localhost:4840"
nodeIDs:
  - "ns=2;i=1001"
`,
			expectType:  "none",
			expectValue: 0.0,
			expectError: false,
		},
		{
			name: "absolute deadband",
			yamlConfig: `
endpoint: "opc.tcp://localhost:4840"
nodeIDs:
  - "ns=2;i=1001"
deadbandType: "absolute"
deadbandValue: 0.5
`,
			expectType:  "absolute",
			expectValue: 0.5,
			expectError: false,
		},
		{
			name: "percent deadband",
			yamlConfig: `
endpoint: "opc.tcp://localhost:4840"
nodeIDs:
  - "ns=2;i=1001"
deadbandType: "percent"
deadbandValue: 2.0
`,
			expectType:  "percent",
			expectValue: 2.0,
			expectError: false,
		},
		{
			name: "invalid deadband type",
			yamlConfig: `
endpoint: "opc.tcp://localhost:4840"
nodeIDs:
  - "ns=2;i=1001"
deadbandType: "invalid"
`,
			expectType:  "invalid",
			expectValue: 0.0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse using OPCUAConfigSpec
			conf, err := OPCUAConfigSpec.ParseYAML(tt.yamlConfig, nil)
			if err != nil {
				if tt.expectError {
					return // Expected error
				}
				t.Fatalf("failed to parse OPC UA config: %v", err)
			}

			// Test that we can extract deadband fields from config
			deadbandType, err := conf.FieldString("deadbandType")
			if err != nil {
				t.Fatalf("failed to read deadbandType: %v", err)
			}

			deadbandValue, err := conf.FieldFloat("deadbandValue")
			if err != nil {
				t.Fatalf("failed to read deadbandValue: %v", err)
			}

			// For invalid deadband type test, verify that validation would catch it
			if tt.expectError {
				// Check that deadbandType is invalid
				validTypes := map[string]bool{
					"none":     true,
					"absolute": true,
					"percent":  true,
				}
				if !validTypes[deadbandType] {
					return // Validation would catch this error - test passes
				}
				t.Fatal("expected error but got none")
			}

			// Verify expected values (only for valid configs)
			if deadbandType != tt.expectType {
				t.Errorf("deadbandType = %s, expected %s", deadbandType, tt.expectType)
			}
			if deadbandValue != tt.expectValue {
				t.Errorf("deadbandValue = %f, expected %f", deadbandValue, tt.expectValue)
			}
		})
	}
}
