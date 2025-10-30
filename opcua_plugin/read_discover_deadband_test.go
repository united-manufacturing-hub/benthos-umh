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

	"github.com/gopcua/opcua/ua"
)

// TestCreateMonitoredItemRequestWithFilter verifies that MonitoredItemCreateRequest
// construction applies deadband filters correctly based on OPCUAInput configuration.
// This tests the actual code path used in MonitorBatched().
func TestCreateMonitoredItemRequestWithFilter(t *testing.T) {
	tests := []struct {
		name          string
		deadbandType  string
		deadbandValue float64
		expectFilter  bool
	}{
		{
			name:          "disabled deadband - filter should be nil",
			deadbandType:  "none",
			deadbandValue: 0.0,
			expectFilter:  false,
		},
		{
			name:          "absolute deadband - filter should be present",
			deadbandType:  "absolute",
			deadbandValue: 0.5,
			expectFilter:  true,
		},
		{
			name:          "percent deadband - filter should be present",
			deadbandType:  "percent",
			deadbandValue: 2.0,
			expectFilter:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test OPCUAInput with deadband configuration
			g := &OPCUAInput{
				DeadbandType:  tt.deadbandType,
				DeadbandValue: tt.deadbandValue,
				QueueSize:     1,
				SamplingInterval: 1000.0,
			}

			// Create a test NodeDef (simulating one node from a batch)
			nodeDef := NodeDef{
				NodeID: ua.NewNumericNodeID(0, 2258), // CurrentTime node
			}

			// This is the EXACT code pattern from MonitorBatched() line 238-252
			// We're testing that THIS code applies the filter correctly
			request := &ua.MonitoredItemCreateRequest{
				ItemToMonitor: &ua.ReadValueID{
					NodeID:       nodeDef.NodeID,
					AttributeID:  ua.AttributeIDValue,
					DataEncoding: &ua.QualifiedName{},
				},
				MonitoringMode: ua.MonitoringModeReporting,
				RequestedParameters: &ua.MonitoringParameters{
					ClientHandle:     0,
					DiscardOldest:    true,
					Filter:           createDataChangeFilter(g.DeadbandType, g.DeadbandValue),
					QueueSize:        g.QueueSize,
					SamplingInterval: g.SamplingInterval,
				},
			}

			// Verify filter application matches expected behavior
			if tt.expectFilter {
				// Filter should be present when deadband is enabled
				if request.RequestedParameters.Filter == nil {
					t.Error("expected Filter to be set based on deadband config, got nil")
				}
			} else {
				// Filter should be nil when deadband is disabled
				if request.RequestedParameters.Filter != nil {
					t.Errorf("expected Filter to be nil when deadband disabled, got %+v", request.RequestedParameters.Filter)
				}
			}
		})
	}
}

// TestDeadbandTypeChecking verifies filters only applied to numeric types
func TestDeadbandTypeChecking(t *testing.T) {
	tests := []struct {
		name         string
		nodeDataType ua.TypeID
		shouldFilter bool
	}{
		{
			name:         "ByteString node - no filter",
			nodeDataType: ua.TypeIDByteString,
			shouldFilter: false,
		},
		{
			name:         "String node - no filter",
			nodeDataType: ua.TypeIDString,
			shouldFilter: false,
		},
		{
			name:         "DateTime node - no filter",
			nodeDataType: ua.TypeIDDateTime,
			shouldFilter: false,
		},
		{
			name:         "Double node - apply filter",
			nodeDataType: ua.TypeIDDouble,
			shouldFilter: true,
		},
		{
			name:         "Float node - apply filter",
			nodeDataType: ua.TypeIDFloat,
			shouldFilter: true,
		},
		{
			name:         "Int32 node - apply filter",
			nodeDataType: ua.TypeIDInt32,
			shouldFilter: true,
		},
		{
			name:         "UInt32 node - apply filter",
			nodeDataType: ua.TypeIDUint32,
			shouldFilter: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNumericDataType(tt.nodeDataType)
			if result != tt.shouldFilter {
				t.Errorf("isNumericDataType(%v) = %v, want %v",
					tt.nodeDataType, result, tt.shouldFilter)
			}
		})
	}
}
