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
	"errors"
	"fmt"
	"os/exec"
	"runtime/debug"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/united-manufacturing-hub/benthos-umh/cmd/benthos/bundle"
)

// generateSchemas extracts schema information from all registered Benthos plugins
func generateSchemas() (*SchemaOutput, error) {
	env := service.GlobalEnvironment()

	output := &SchemaOutput{
		Metadata: Metadata{
			BenthosVersion:    getBenthosVersion(),
			GeneratedAt:       time.Now(),
			BenthosUMHVersion: getUMHVersion(),
		},
		Inputs:     make(map[string]PluginSpec),
		Processors: make(map[string]PluginSpec),
		Outputs:    make(map[string]PluginSpec),
	}

	var errs []error

	// Walk all inputs
	env.WalkInputs(func(name string, config *service.ConfigView) {
		spec, err := extractPluginSpec(name, "input", config)
		if err != nil {
			errs = append(errs, err)
			return
		}
		output.Inputs[name] = spec
	})

	// Walk all processors
	env.WalkProcessors(func(name string, config *service.ConfigView) {
		spec, err := extractPluginSpec(name, "processor", config)
		if err != nil {
			errs = append(errs, err)
			return
		}
		output.Processors[name] = spec
	})

	// Walk all outputs
	env.WalkOutputs(func(name string, config *service.ConfigView) {
		spec, err := extractPluginSpec(name, "output", config)
		if err != nil {
			errs = append(errs, err)
			return
		}
		output.Outputs[name] = spec
	})

	if len(errs) > 0 {
		return output, errors.Join(errs...)
	}
	return output, nil
}

// extractPluginSpec converts a Benthos ConfigView to our PluginSpec format
func extractPluginSpec(name, pluginType string, config *service.ConfigView) (PluginSpec, error) {
	jsonData, err := config.FormatJSON()
	if err != nil {
		return PluginSpec{}, fmt.Errorf("failed to format JSON for plugin %s: %w", name, err)
	}

	var rawSpec map[string]interface{}
	if err := json.Unmarshal(jsonData, &rawSpec); err != nil {
		return PluginSpec{}, fmt.Errorf("failed to unmarshal spec for plugin %s: %w", name, err)
	}

	spec := PluginSpec{
		Name:        name,
		Type:        pluginType,
		Source:      detectSource(name),
		Summary:     config.Summary(),
		Description: config.Description(),
		Config:      make(map[string]FieldSpec),
	}

	// Extract fields recursively from config object
	if configObj, ok := rawSpec["config"].(map[string]interface{}); ok {
		spec.Config = extractFields(configObj)
	}

	return spec, nil
}

// extractFields converts Benthos internal field format to our FieldSpec format
func extractFields(configObj map[string]interface{}) map[string]FieldSpec {
	fields := make(map[string]FieldSpec)

	children, ok := configObj["children"].([]interface{})
	if !ok {
		return fields
	}

	for _, child := range children {
		fieldMap, ok := child.(map[string]interface{})
		if !ok {
			continue
		}

		field := FieldSpec{
			Name:        getString(fieldMap, "name"),
			Type:        getString(fieldMap, "type"),
			Kind:        getString(fieldMap, "kind"),
			Description: getString(fieldMap, "description"),
			Required:    !getBool(fieldMap, "is_optional"),
			Advanced:    getBool(fieldMap, "is_advanced"),
		}

		// Handle default value
		if defaultVal, ok := fieldMap["default"]; ok {
			field.Default = defaultVal
		}

		// Handle examples
		if examples, ok := fieldMap["examples"].([]interface{}); ok {
			field.Examples = examples
		}

		// Handle options (for enum-like fields)
		if options, ok := fieldMap["options"].([]interface{}); ok {
			field.Options = make([]string, len(options))
			for i, opt := range options {
				if optStr, ok := opt.(string); ok {
					field.Options[i] = optStr
				}
			}
		}

		// Recursively handle children (nested objects/arrays)
		if childFields, ok := fieldMap["children"].([]interface{}); ok && len(childFields) > 0 {
			field.Children = extractFieldsArray(childFields)
		}

		fields[field.Name] = field
	}

	return fields
}

// extractFieldsArray converts an array of field definitions to FieldSpec slice
func extractFieldsArray(childFields []interface{}) []FieldSpec {
	fields := make([]FieldSpec, 0, len(childFields))

	for _, child := range childFields {
		fieldMap, ok := child.(map[string]interface{})
		if !ok {
			continue
		}

		field := FieldSpec{
			Name:        getString(fieldMap, "name"),
			Type:        getString(fieldMap, "type"),
			Kind:        getString(fieldMap, "kind"),
			Description: getString(fieldMap, "description"),
			Required:    !getBool(fieldMap, "is_optional"),
			Advanced:    getBool(fieldMap, "is_advanced"),
		}

		// Handle default value
		if defaultVal, ok := fieldMap["default"]; ok {
			field.Default = defaultVal
		}

		// Handle examples
		if examples, ok := fieldMap["examples"].([]interface{}); ok {
			field.Examples = examples
		}

		// Handle options
		if options, ok := fieldMap["options"].([]interface{}); ok {
			field.Options = make([]string, len(options))
			for i, opt := range options {
				if optStr, ok := opt.(string); ok {
					field.Options[i] = optStr
				}
			}
		}

		// Recursively handle nested children
		if childFieldsNested, ok := fieldMap["children"].([]interface{}); ok && len(childFieldsNested) > 0 {
			field.Children = extractFieldsArray(childFieldsNested)
		}

		fields = append(fields, field)
	}

	return fields
}

// detectSource determines if a plugin is from benthos-umh or upstream Benthos
func detectSource(pluginName string) string {
	umhPlugins := []string{
		"opcua", "modbus", "s7comm", "sparkplug_b", "eip", "sensorconnect",
		"tag_processor", "stream_processor", "downsampler", "topic_browser",
		"classic_to_core", "nodered_js", "uns",
	}

	for _, umh := range umhPlugins {
		if pluginName == umh {
			return "benthos-umh"
		}
	}
	return "upstream"
}

// getBenthosVersion retrieves the Benthos version from build metadata
func getBenthosVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}

	for _, dep := range info.Deps {
		if strings.Contains(dep.Path, "github.com/redpanda-data/benthos/v4") {
			return dep.Version
		}
	}

	return "unknown"
}

// getUMHVersion retrieves the benthos-umh version from git or VERSION file
func getUMHVersion() string {
	// Try to get version from git tag
	cmd := exec.Command("git", "describe", "--tags", "--always", "--dirty")
	output, err := cmd.Output()
	if err == nil {
		version := strings.TrimSpace(string(output))
		if version != "" {
			return version
		}
	}

	// Fallback to a default version
	return "dev"
}

// Helper functions for safe type assertions
func getString(m map[string]interface{}, key string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return ""
}

func getBool(m map[string]interface{}, key string) bool {
	if val, ok := m[key].(bool); ok {
		return val
	}
	return false
}
