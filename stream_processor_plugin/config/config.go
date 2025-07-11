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

package config

import (
	"fmt"
)

// ValidateConfig validates the stream processor configuration
func ValidateConfig(config StreamProcessorConfig) error {
	if config.Mode == "" {
		return fmt.Errorf("mode is required")
	}

	if config.Mode != "timeseries" {
		return fmt.Errorf("unsupported mode: %s (only 'timeseries' is supported)", config.Mode)
	}

	if config.OutputTopic == "" {
		return fmt.Errorf("output_topic is required")
	}

	if config.Model.Name == "" {
		return fmt.Errorf("model name is required")
	}

	if config.Model.Version == "" {
		return fmt.Errorf("model version is required")
	}

	if len(config.Sources) == 0 {
		return fmt.Errorf("at least one source mapping is required")
	}

	return nil
}

// StreamProcessorConfig holds the configuration for the stream processor
type StreamProcessorConfig struct {
	Mode        string                 `json:"mode" yaml:"mode"`
	Model       ModelConfig            `json:"model" yaml:"model"`
	OutputTopic string                 `json:"output_topic" yaml:"output_topic"`
	Sources     map[string]string      `json:"sources" yaml:"sources"`
	Mapping     map[string]interface{} `json:"mapping" yaml:"mapping"`

	// Pre-analyzed mappings for runtime efficiency
	StaticMappings  map[string]MappingInfo `json:"-" yaml:"-"`
	DynamicMappings map[string]MappingInfo `json:"-" yaml:"-"`
}

// ModelConfig defines the model name and version
type ModelConfig struct {
	Name    string `json:"name" yaml:"name"`
	Version string `json:"version" yaml:"version"`
}

// MappingType defines the type of mapping (static or dynamic)
type MappingType int

const (
	StaticMapping  MappingType = iota // No source variable dependencies
	DynamicMapping                    // References one or more source variables
)

// MappingInfo contains information about a mapping
type MappingInfo struct {
	VirtualPath  string
	Expression   string
	Type         MappingType
	Dependencies []string // Source variables this mapping depends on
}
