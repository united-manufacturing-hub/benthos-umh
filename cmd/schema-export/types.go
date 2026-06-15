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
	"encoding/json"
	"time"
)

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
	FieldOrder  []string             `json:"field_order"` // preserves Go struct declaration order
}

// MarshalJSON serializes PluginSpec with Config keys in FieldOrder order.
// Without this, json.Marshal would sort map keys alphabetically.
func (p PluginSpec) MarshalJSON() ([]byte, error) {
	type pluginSpecJSON struct {
		Name        string      `json:"name"`
		Type        string      `json:"type"`
		Source      string      `json:"source"`
		Summary     string      `json:"summary"`
		Description string      `json:"description,omitempty"`
		Config      *orderedMap `json:"config"`
		FieldOrder  []string    `json:"field_order"`
	}

	// FieldOrder and Config are populated in lockstep by extractFields,
	// so FieldOrder always covers every Config key.
	config := newOrderedMap()
	for _, name := range p.FieldOrder {
		if field, ok := p.Config[name]; ok {
			config.set(name, field)
		}
	}

	return json.Marshal(pluginSpecJSON{
		Name:        p.Name,
		Type:        p.Type,
		Source:      p.Source,
		Summary:     p.Summary,
		Description: p.Description,
		Config:      config,
		FieldOrder:  p.FieldOrder,
	})
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
