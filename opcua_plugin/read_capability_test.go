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

// TestServerCapabilityDetection verifies graceful fallback when
// server doesn't support requested deadband type
func TestServerCapabilityDetection(t *testing.T) {
	tests := []struct {
		name                  string
		requestedType         string
		serverSupportsPercent bool
		expectedType          string
		expectedWarning       bool
	}{
		{
			name:                  "percent requested, server supports it",
			requestedType:         "percent",
			serverSupportsPercent: true,
			expectedType:          "percent",
			expectedWarning:       false,
		},
		{
			name:                  "percent requested, server doesn't support - fallback to absolute",
			requestedType:         "percent",
			serverSupportsPercent: false,
			expectedType:          "absolute",
			expectedWarning:       true,
		},
		{
			name:                  "absolute requested - always works",
			requestedType:         "absolute",
			serverSupportsPercent: false,
			expectedType:          "absolute",
			expectedWarning:       false,
		},
		{
			name:                  "none requested - no capability check needed",
			requestedType:         "none",
			serverSupportsPercent: false,
			expectedType:          "none",
			expectedWarning:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock server capabilities
			// Most servers support absolute deadband (basic OPC UA feature)
			caps := &ServerCapabilities{
				SupportsPercentDeadband:  tt.serverSupportsPercent,
				SupportsAbsoluteDeadband: true, // Absolute is widely supported
			}

			// Adjust deadband type based on capabilities
			resultType := adjustDeadbandType(tt.requestedType, caps)

			if resultType != tt.expectedType {
				t.Errorf("adjustDeadbandType(%s, %+v) = %s, want %s",
					tt.requestedType, caps, resultType, tt.expectedType)
			}
		})
	}
}
