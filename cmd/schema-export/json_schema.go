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
	"fmt"
)

// validateFormat validates the format flag value
func validateFormat(format string) error {
	if format == "" {
		return fmt.Errorf("format cannot be empty")
	}
	if format != "benthos" && format != "json-schema" {
		return fmt.Errorf("invalid format: %s (must be 'benthos' or 'json-schema')", format)
	}
	return nil
}

// mapBenthosBaseType converts a Benthos type string to JSON Schema type string.
func mapBenthosBaseType(t string) string {
	switch t {
	case "string":
		return "string"
	case "int", "float", "number":
		return "number"
	case "bool":
		return "boolean"
	case "object":
		return "object"
	case "array":
		return "array"
	default:
		// Default to string for unknown types
		return "string"
	}
}

// benthosTypeToJSONSchemaType converts a Benthos type to JSON Schema type.
// It handles the distinction between "type" (element type) and "kind" (container type).
// For arrays, Benthos reports kind="array" with type being the element type (e.g., "int", "string", "object").
func benthosTypeToJSONSchemaType(benthosType string, kind string) map[string]interface{} {
	result := make(map[string]interface{})

	// Check if this is an array container
	if kind == "array" {
		result["type"] = "array"
		// Set items type based on the element type
		result["items"] = map[string]interface{}{
			"type": mapBenthosBaseType(benthosType),
		}
		return result
	}

	// Non-array: use the base type directly
	result["type"] = mapBenthosBaseType(benthosType)
	return result
}

// convertFieldToJSONSchema converts a FieldSpec to a JSON Schema property
func convertFieldToJSONSchema(field FieldSpec) map[string]interface{} {
	schema := benthosTypeToJSONSchemaType(field.Type, field.Kind)

	// Add description
	if field.Description != "" {
		schema["description"] = field.Description
	}

	// Add default value
	if field.Default != nil {
		schema["default"] = field.Default
	}

	// Add enum options
	if len(field.Options) > 0 {
		schema["enum"] = field.Options
	}

	// Add advanced flag as custom property
	if field.Advanced {
		schema["x-advanced"] = true
	}

	// Add deprecated flag
	if field.Deprecated {
		schema["x-deprecated"] = true
	}

	// Handle object arrays with children (e.g., modbus addresses: type="object", kind="array")
	// Properties go INSIDE items, not at top level
	if field.Kind == "array" && field.Type == "object" && len(field.Children) > 0 {
		properties := make(map[string]interface{})
		for _, child := range field.Children {
			properties[child.Name] = convertFieldToJSONSchema(child)
		}

		// Get the items map and add properties to it
		items, ok := schema["items"].(map[string]interface{})
		if !ok {
			items = map[string]interface{}{}
			schema["items"] = items
		}
		items["properties"] = properties

		// Build required array for the items object
		required := make([]string, 0)
		for _, child := range field.Children {
			if child.Required {
				required = append(required, child.Name)
			}
		}
		if len(required) > 0 {
			items["required"] = required
		}
		// Prevent unknown properties in array item objects
		items["additionalProperties"] = false
		return schema
	}

	// Handle nested object fields (non-array objects with children)
	if field.Type == "object" && field.Kind != "array" && len(field.Children) > 0 {
		properties := make(map[string]interface{})
		for _, child := range field.Children {
			properties[child.Name] = convertFieldToJSONSchema(child)
		}
		schema["properties"] = properties

		// Build required array for nested object
		required := make([]string, 0)
		for _, child := range field.Children {
			if child.Required {
				required = append(required, child.Name)
			}
		}
		if len(required) > 0 {
			schema["required"] = required
		}
		// Prevent unknown properties in nested objects
		schema["additionalProperties"] = false
	}

	// Handle array fields where type="array" (not kind="array")
	if field.Type == "array" && len(field.Children) > 0 {
		// For arrays, the first child defines the items schema
		schema["items"] = convertFieldToJSONSchema(field.Children[0])
	}

	return schema
}

// buildRequiredArray extracts required field names from a field map
func buildRequiredArray(fields map[string]FieldSpec) []string {
	required := make([]string, 0)
	for name, field := range fields {
		if field.Required {
			required = append(required, name)
		}
	}
	return required
}

// generateJSONSchema converts plugin specs to JSON Schema Draft-07 format.
// This output is designed for Monaco JSON editor validation/autocomplete.
// Unlike "benthos" format which preserves UMH-specific fields (source, summary),
// JSON Schema uses standardized properties ($schema, definitions, properties).
func generateJSONSchema(plugins *SchemaOutput, version string) (map[string]interface{}, error) {
	schema := map[string]interface{}{
		"$schema":     "http://json-schema.org/draft-07/schema#",
		"$id":         fmt.Sprintf("https://github.com/united-manufacturing-hub/benthos-umh/schemas/benthos-v%s.json", version),
		"title":       "Benthos UMH Configuration Schema",
		"description": "JSON Schema for Benthos UMH protocol plugins",
	}

	definitions := make(map[string]interface{})

	// Convert input plugins with "input_" prefix
	for name, plugin := range plugins.Inputs {
		definitions["input_"+name] = convertPluginToJSONSchema(plugin)
	}

	// Convert processor plugins with "processor_" prefix
	for name, plugin := range plugins.Processors {
		definitions["processor_"+name] = convertPluginToJSONSchema(plugin)
	}

	// Convert output plugins with "output_" prefix
	for name, plugin := range plugins.Outputs {
		definitions["output_"+name] = convertPluginToJSONSchema(plugin)
	}

	schema["definitions"] = definitions

	return schema, nil
}

// convertPluginToJSONSchema converts a PluginSpec to a JSON Schema definition
func convertPluginToJSONSchema(plugin PluginSpec) map[string]interface{} {
	schema := map[string]interface{}{
		"type": "object",
	}

	// Add description
	if plugin.Description != "" {
		schema["description"] = plugin.Description
	} else if plugin.Summary != "" {
		schema["description"] = plugin.Summary
	}

	// Convert config fields to properties
	properties := make(map[string]interface{})
	for name, field := range plugin.Config {
		properties[name] = convertFieldToJSONSchema(field)
	}

	if len(properties) > 0 {
		schema["properties"] = properties
	}

	// Build required array
	required := buildRequiredArray(plugin.Config)
	if len(required) > 0 {
		schema["required"] = required
	}

	// Prevent unknown properties at plugin root level
	schema["additionalProperties"] = false

	return schema
}

// generateSchemaWithFormat routes to the appropriate formatter:
// - format="benthos": Returns raw SchemaOutput (UI format) for Management Console
// - format="json-schema": Returns JSON Schema Draft-07 (Monaco format) for editors
func generateSchemaWithFormat(plugins *SchemaOutput, format string, version string) (interface{}, error) {
	if err := validateFormat(format); err != nil {
		return nil, err
	}

	if format == "benthos" {
		// Return the original Benthos format
		return plugins, nil
	}

	// Generate JSON Schema format
	return generateJSONSchema(plugins, version)
}
