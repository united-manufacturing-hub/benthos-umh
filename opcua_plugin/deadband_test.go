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

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

func TestCreateDataChangeFilter(t *testing.T) {
	tests := []struct {
		name          string
		deadbandType  string
		deadbandValue float64
		expectNil     bool
		expectType    uint32  // ua.DeadbandTypeAbsolute or ua.DeadbandTypePercent
		expectValue   float64
	}{
		{
			name:          "disabled deadband - type none",
			deadbandType:  "none",
			deadbandValue: 0.5,
			expectNil:     true,
		},
		{
			name:          "duplicate suppression - value zero",
			deadbandType:  "absolute",
			deadbandValue: 0.0,
			expectNil:     false,
			expectType:    uint32(ua.DeadbandTypeAbsolute),
			expectValue:   0.0,
		},
		{
			name:          "absolute deadband",
			deadbandType:  "absolute",
			deadbandValue: 0.5,
			expectNil:     false,
			expectType:    uint32(ua.DeadbandTypeAbsolute),
			expectValue:   0.5,
		},
		{
			name:          "percent deadband",
			deadbandType:  "percent",
			deadbandValue: 2.0,
			expectNil:     false,
			expectType:    uint32(ua.DeadbandTypePercent),
			expectValue:   2.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := createDataChangeFilter(tt.deadbandType, tt.deadbandValue)

			if tt.expectNil {
				if result != nil {
					t.Errorf("expected nil, got %+v", result)
				}
				return
			}

			// Verify ExtensionObject structure
			if result == nil {
				t.Fatal("expected non-nil ExtensionObject, got nil")
			}

			// Verify TypeID
			expectedNodeID := ua.NewNumericNodeID(0, id.DataChangeFilter_Encoding_DefaultBinary)
			if result.TypeID == nil || result.TypeID.NodeID == nil {
				t.Fatal("expected TypeID to be set")
			}
			// Compare NodeID fields directly
			if result.TypeID.NodeID.Namespace() != expectedNodeID.Namespace() ||
				result.TypeID.NodeID.IntID() != expectedNodeID.IntID() {
				t.Errorf("expected TypeID.NodeID ns=%d;i=%d, got ns=%d;i=%d",
					expectedNodeID.Namespace(), expectedNodeID.IntID(),
					result.TypeID.NodeID.Namespace(), result.TypeID.NodeID.IntID())
			}

			// Verify EncodingMask
			if result.EncodingMask != ua.ExtensionObjectBinary {
				t.Errorf("expected EncodingMask %v, got %v", ua.ExtensionObjectBinary, result.EncodingMask)
			}

			// Unwrap and verify DataChangeFilter fields
			filter, ok := result.Value.(*ua.DataChangeFilter)
			if !ok {
				t.Fatalf("expected *ua.DataChangeFilter, got %T", result.Value)
			}

			// Verify Trigger
			if filter.Trigger != ua.DataChangeTriggerStatusValue {
				t.Errorf("expected Trigger %v, got %v", ua.DataChangeTriggerStatusValue, filter.Trigger)
			}

			// Verify DeadbandType
			if filter.DeadbandType != tt.expectType {
				t.Errorf("expected DeadbandType %v, got %v", tt.expectType, filter.DeadbandType)
			}

			// Verify DeadbandValue
			if filter.DeadbandValue != tt.expectValue {
				t.Errorf("expected DeadbandValue %v, got %v", tt.expectValue, filter.DeadbandValue)
			}
		})
	}
}
