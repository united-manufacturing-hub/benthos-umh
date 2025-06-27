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

// Constants for UMH topic structure validation.
const (
	// TopicPrefix is the mandatory prefix for all UMH topics.
	TopicPrefix = "umh.v1."

	// MinimumParts represents the minimum number of dot-separated segments
	// required for a valid UMH topic: umh.v1.level0._contract.name
	MinimumParts = 5
)
