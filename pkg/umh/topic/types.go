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

// Package topic provides parsing, validation, and construction utilities for UMH (Unified Manufacturing Hub) topics.
//
// UMH topics follow a strict hierarchical structure that represents the physical and logical organization
// of manufacturing systems. This package ensures that all topics conform to the UMH specification and
// can be safely used as Kafka message keys.
//
// # Topic Structure
//
// UMH topics follow this format:
//
//	umh.v1.<location_path>.<data_contract>[.<virtual_path>].<name>
//
// Where:
//   - location_path: 1-N hierarchical levels representing physical organization (enterprise.site.area.line...)
//   - data_contract: Service/schema identifier starting with underscore (_historian, _analytics, etc.)
//   - virtual_path: Optional logical grouping (axis.x.position, diagnostics.*, etc.)
//   - name: Final identifier for the specific data point
//
// # Validation Rules
//
// The package enforces these fixed rules:
//   - Location path levels: MUST NOT start with underscore, at least 1 level required
//   - Data contract: MUST start with underscore, cannot be just "_"
//   - Virtual path: CAN start with underscore, optional
//   - Name: CAN start with underscore, required
//   - Kafka compatibility: Only [a-zA-Z0-9._-] characters allowed
//
// # Examples
//
//	// Valid topics:
//	umh.v1.enterprise._historian.temperature
//	umh.v1.acme.berlin._historian.pressure
//	umh.v1.factory.line1.station2._raw.motor.diagnostics.vibration
//	umh.v1.plant._analytics.efficiency._kpi.oee
//
//	// Invalid topics:
//	umh.v1._enterprise._historian.temp     // location cannot start with _
//	umh.v1.factory.historian.temp          // data contract must start with _
//	umh.v1.factory._._historian.temp       // data contract cannot be just _
//	umh.v1.factory.._historian.temp        // empty location level
package topic

import (
	"strings"
)

// TopicInfo represents the parsed components of a UMH topic.
//
// This structure provides access to the individual hierarchical components
// of a UMH topic, allowing for easy inspection, validation, and manipulation
// of topic elements.
type TopicInfo struct {
	// Level0 is the enterprise/root level (mandatory).
	// Cannot be empty and cannot start with underscore.
	// Examples: "enterprise", "acme", "factory"
	Level0 string

	// LocationSublevels contains additional location hierarchy levels (0-N).
	// Each level cannot be empty and cannot start with underscore.
	// Examples: ["site", "area", "line"], ["berlin", "assembly"], []
	LocationSublevels []string

	// DataContract identifies the service/schema handling this data.
	// Must start with underscore and cannot be just "_".
	// Examples: "_historian", "_analytics", "_raw", "_state"
	DataContract string

	// VirtualPath provides optional logical grouping within the data contract.
	// Can contain multiple dot-separated segments, segments can start with underscore.
	// Examples: "motor.diagnostics", "axis.x.position", "_internal.debug"
	VirtualPath *string

	// Name is the final identifier for the specific data point.
	// Cannot be empty, can start with underscore.
	// Examples: "temperature", "pressure", "_internal_counter"
	Name string
}

// LocationPath returns the complete location path as a single string.
//
// This method combines Level0 with all LocationSublevels into a dot-separated
// string representing the full physical hierarchy. It includes defensive
// programming by trimming whitespace to ensure consistent hash equality.
//
// Returns:
//   - Complete location path (e.g., "enterprise.site.area.line")
//   - Just Level0 if no sublevels exist (e.g., "enterprise")
//   - Empty string if TopicInfo is nil
//
// Examples:
//   - Level0="enterprise", LocationSublevels=[] → "enterprise"
//   - Level0="enterprise", LocationSublevels=["site"] → "enterprise.site"
//   - Level0="factory", LocationSublevels=["area", "line"] → "factory.area.line"
func (t *TopicInfo) LocationPath() string {
	if t == nil {
		return ""
	}

	// Trim whitespace to ensure " enterprise " and "enterprise" hash identically
	base := strings.TrimSpace(t.Level0)
	if len(t.LocationSublevels) == 0 {
		return base
	}

	// Pre-allocate slice with known capacity for efficiency
	cleaned := make([]string, 0, len(t.LocationSublevels)+1)
	cleaned = append(cleaned, base)

	// Trim each sublevel for consistency
	for _, s := range t.LocationSublevels {
		cleaned = append(cleaned, strings.TrimSpace(s))
	}

	return strings.Join(cleaned, ".")
}

// TotalLocationLevels returns the total number of location hierarchy levels.
//
// This count includes Level0 plus all LocationSublevels, representing the
// depth of the physical organizational hierarchy.
//
// Returns:
//   - Minimum value: 1 (just Level0)
//   - Actual value: 1 + len(LocationSublevels)
//
// Examples:
//   - Level0="enterprise", LocationSublevels=[] → 1
//   - Level0="enterprise", LocationSublevels=["site"] → 2
//   - Level0="factory", LocationSublevels=["area", "line", "station"] → 4
func (t *TopicInfo) TotalLocationLevels() int {
	if t == nil {
		return 0
	}
	return 1 + len(t.LocationSublevels)
}

// Constants for UMH topic structure validation.
const (
	// TopicPrefix is the mandatory prefix for all UMH topics.
	TopicPrefix = "umh.v1."

	// MinimumParts represents the minimum number of dot-separated segments
	// required for a valid UMH topic: umh.v1.level0._contract.name
	MinimumParts = 5
)
