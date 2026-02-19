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

import "time"

// SchemaOutput represents the complete schema in "benthos" format (UI format).
// Used by Management Console UI for rendering plugin configuration forms.
// Contains UMH-specific fields (source, summary) not present in JSON Schema format.
// For JSON Schema Draft-07 format (Monaco editor), use generateJSONSchema().
type SchemaOutput struct {
	Metadata   Metadata              `json:"metadata"`
	Inputs     map[string]PluginSpec `json:"inputs"`
	Processors map[string]PluginSpec `json:"processors"`
	Outputs    map[string]PluginSpec `json:"outputs"`
}

// Metadata contains information about the schema export
type Metadata struct {
	BenthosVersion    string    `json:"benthos_version"`
	GeneratedAt       time.Time `json:"generated_at"`
	BenthosUMHVersion string    `json:"benthos_umh_version"`
}

// PluginSpec represents a single Benthos plugin specification
type PluginSpec struct {
	Name        string               `json:"name"`
	Type        string               `json:"type"`
	Source      string               `json:"source"` // "benthos-umh" | "upstream"
	Summary     string               `json:"summary"`
	Description string               `json:"description,omitempty"`
	Config      map[string]FieldSpec `json:"config"`
}

// FieldSpec represents a single configuration field
type FieldSpec struct {
	Name        string        `json:"name"`
	Type        string        `json:"type"`
	Kind        string        `json:"kind"`
	Description string        `json:"description"`
	Required    bool          `json:"required"`
	Default     interface{}   `json:"default"`
	Examples    []interface{} `json:"examples,omitempty"`
	Options     []string      `json:"options,omitempty"`
	Advanced    bool          `json:"advanced,omitempty"`
	Deprecated  bool          `json:"deprecated,omitempty"`
	Children    []FieldSpec   `json:"children,omitempty"` // For nested objects/arrays
}
