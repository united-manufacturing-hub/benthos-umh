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
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

// createDataChangeFilter creates an OPC UA DataChangeFilter for deadband filtering.
// Returns nil if deadband is disabled (type="none").
//
// Important: deadbandValue=0.0 is VALID and creates a filter that suppresses exact duplicates.
// Per OPC UA spec, server only sends notifications when |value - lastValue| > deadbandValue.
// With deadbandValue=0.0, this becomes |value - lastValue| > 0.0, suppressing exact duplicates.
//
// Parameters:
//
//	deadbandType: "none", "absolute", or "percent"
//	deadbandValue: threshold value (absolute units or percentage 0-100)
//	               Use 0.0 for duplicate suppression only
//
// Returns:
//
//	*ua.ExtensionObject wrapping ua.DataChangeFilter, or nil if type="none"
func createDataChangeFilter(deadbandType string, deadbandValue float64) *ua.ExtensionObject {
	// Return nil only if explicitly disabled
	if deadbandType == "none" {
		return nil
	}

	// Determine deadband type constant
	var dbType uint32
	switch deadbandType {
	case "absolute":
		dbType = uint32(ua.DeadbandTypeAbsolute)
	case "percent":
		dbType = uint32(ua.DeadbandTypePercent)
	default:
		// Invalid type, return nil (same as disabled)
		return nil
	}

	// Create DataChangeFilter
	filter := &ua.DataChangeFilter{
		Trigger:       ua.DataChangeTriggerStatusValue,
		DeadbandType:  dbType,
		DeadbandValue: deadbandValue,
	}

	// Wrap in ExtensionObject
	return &ua.ExtensionObject{
		TypeID: &ua.ExpandedNodeID{
			NodeID: ua.NewNumericNodeID(0, id.DataChangeFilter_Encoding_DefaultBinary),
		},
		EncodingMask: ua.ExtensionObjectBinary,
		Value:        filter,
	}
}
