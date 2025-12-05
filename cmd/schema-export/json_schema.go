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

// benthosTypeToJSONSchemaType converts a Benthos type to JSON Schema type
func benthosTypeToJSONSchemaType(benthosType, kind string) map[string]interface{} {
	result := make(map[string]interface{})

	switch benthosType {
	case "string":
		result["type"] = "string"
	case "int", "float", "number":
		result["type"] = "number"
	case "bool":
		result["type"] = "boolean"
	case "object":
		result["type"] = "object"
	case "array":
		result["type"] = "array"
	default:
		// Default to string for unknown types
		result["type"] = "string"
	}

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

	// Handle nested object fields
	if field.Type == "object" && len(field.Children) > 0 {
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
	}

	// Handle array fields
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

	// Convert input plugins
	for name, plugin := range plugins.Inputs {
		definitions[name] = convertPluginToJSONSchema(plugin)
	}

	// Convert processor plugins
	for name, plugin := range plugins.Processors {
		definitions[name] = convertPluginToJSONSchema(plugin)
	}

	// Convert output plugins
	for name, plugin := range plugins.Outputs {
		definitions[name] = convertPluginToJSONSchema(plugin)
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

	return schema
}

// generateSchemaWithFormat routes to the appropriate formatter:
// - format="benthos": Returns raw SchemaOutput (UI format) for Management Console
// - format="json-schema": Returns JSON Schema Draft-07 (Monaco format) for editors
func generateSchemaWithFormat(plugins *SchemaOutput, format, version string) (interface{}, error) {
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
