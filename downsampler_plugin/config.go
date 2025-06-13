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

package downsampler_plugin

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// DeadbandConfig holds deadband algorithm parameters
type DeadbandConfig struct {
	Threshold float64       `json:"threshold,omitempty" yaml:"threshold,omitempty"`
	MaxTime   time.Duration `json:"max_time,omitempty" yaml:"max_time,omitempty"`
}

// SwingingDoorConfig holds swinging door algorithm parameters
type SwingingDoorConfig struct {
	Threshold float64       `json:"threshold,omitempty" yaml:"threshold,omitempty"`
	MaxTime   time.Duration `json:"max_time,omitempty" yaml:"max_time,omitempty"`
	MinTime   time.Duration `json:"min_time,omitempty" yaml:"min_time,omitempty"`
}

// LatePolicyConfig holds late arrival handling parameters
type LatePolicyConfig struct {
	LatePolicy string `json:"late_policy,omitempty" yaml:"late_policy,omitempty"`
}

// DefaultConfig holds the default algorithm parameters
type DefaultConfig struct {
	Deadband     DeadbandConfig     `json:"deadband,omitempty" yaml:"deadband,omitempty"`
	SwingingDoor SwingingDoorConfig `json:"swinging_door,omitempty" yaml:"swinging_door,omitempty"`
	LatePolicy   LatePolicyConfig   `json:"late_policy,omitempty" yaml:"late_policy,omitempty"`
}

// OverrideConfig defines algorithm parameter overrides for topic patterns.
//
// The pattern field supports both exact topic matching and wildcard patterns:
// - Exact match: "umh.v1.acme._historian.temperature.sensor1"
// - Wildcard patterns: "*.temperature.*", "*sensor*", "umh.v1.acme.*"
//
// This unified approach simplifies configuration by eliminating the need for
// separate "topic" and "pattern" fields while maintaining full functionality.
type OverrideConfig struct {
	Pattern      string              `json:"pattern" yaml:"pattern"`                                 // Topic pattern (exact match or wildcards with * and ?)
	Deadband     *DeadbandConfig     `json:"deadband,omitempty" yaml:"deadband,omitempty"`           // Deadband algorithm overrides
	SwingingDoor *SwingingDoorConfig `json:"swinging_door,omitempty" yaml:"swinging_door,omitempty"` // Swinging door algorithm overrides
	LatePolicy   *LatePolicyConfig   `json:"late_policy,omitempty" yaml:"late_policy,omitempty"`     // Late arrival policy overrides
}

// DownsamplerConfig holds the configuration for the downsampler processor
type DownsamplerConfig struct {
	Default   DefaultConfig    `json:"default" yaml:"default"`
	Overrides []OverrideConfig `json:"overrides,omitempty" yaml:"overrides,omitempty"`
	AllowMeta bool             `json:"allow_meta_overrides" yaml:"allow_meta_overrides"`
}

// GetConfigForTopic returns the effective configuration for a given topic by applying pattern-based overrides.
//
// This function implements a simplified pattern matching system that supports both exact topic matches
// and flexible wildcard patterns using a single configuration field.
//
// ## Pattern Matching Strategy
//
// The function uses Go's filepath.Match() which supports:
// - **Exact matches**: "umh.v1.acme._historian.temperature.sensor1" matches only that specific topic
// - **Wildcard patterns**: "*.temperature.*" matches any topic containing "temperature"
// - **Shell-style patterns**: "*sensor*", "temp_*", "*.pressure" for flexible matching
//
// ## Fallback Matching
//
// For additional flexibility, the function also attempts to match patterns against the
// final segment of the topic (after the last dot). This allows patterns like "temperature"
// to match topics ending in temperature without requiring full path specification.
//
// ## Override Priority
//
// Overrides are processed in configuration order with first-match-wins semantics.
// More specific patterns should be placed before more general ones in the configuration.
//
// Parameters:
//   - topic: The UMH topic to find configuration for (e.g., "umh.v1.acme._historian.temperature.sensor1")
//
// Returns:
//   - string: The selected algorithm name ("deadband" or "swinging_door")
//   - map[string]interface{}: The effective configuration parameters for the algorithm
//   - error: An error if the configuration is invalid
func (c *DownsamplerConfig) GetConfigForTopic(topic string) (string, map[string]interface{}, error) {
	// Determine default algorithm based on which default config has values
	// Prioritize deadband as the simpler algorithm
	algorithm := "deadband" // Default fallback

	// Only use swinging_door if explicitly configured and deadband is not
	hasDeadbandConfig := c.Default.Deadband.Threshold != 0 || c.Default.Deadband.MaxTime != 0
	hasSwingingDoorConfig := c.Default.SwingingDoor.Threshold != 0 || c.Default.SwingingDoor.MaxTime != 0 || c.Default.SwingingDoor.MinTime != 0

	if hasDeadbandConfig {
		algorithm = "deadband"
	} else if hasSwingingDoorConfig {
		algorithm = "swinging_door"
	}

	// Start with defaults based on the determined algorithm
	config := map[string]interface{}{}

	if algorithm == "swinging_door" {
		config["threshold"] = c.Default.SwingingDoor.Threshold
		if c.Default.SwingingDoor.MaxTime > 0 {
			config["max_time"] = c.Default.SwingingDoor.MaxTime.String()
		}
		if c.Default.SwingingDoor.MinTime > 0 {
			config["min_time"] = c.Default.SwingingDoor.MinTime.String()
		}
	} else {
		config["threshold"] = c.Default.Deadband.Threshold
		if c.Default.Deadband.MaxTime > 0 {
			config["max_time"] = c.Default.Deadband.MaxTime.String()
		}
	}

	// Add late policy defaults
	latePolicy := "passthrough" // Default policy
	if c.Default.LatePolicy.LatePolicy != "" {
		latePolicy = c.Default.LatePolicy.LatePolicy
	}
	config["late_policy"] = latePolicy

	// Apply overrides in order (first match wins)
	for _, override := range c.Overrides {
		matched := false

		// Use unified pattern matching for both exact matches and wildcards
		if len(override.Pattern) > 0 {
			// Primary match: full topic against pattern
			if m, err := filepath.Match(override.Pattern, topic); err == nil && m {
				matched = true
			} else {
				// Fallback match: pattern against final topic segment (field name)
				parts := strings.Split(topic, ".")
				if len(parts) > 0 {
					fieldName := parts[len(parts)-1]
					if m, err := filepath.Match(override.Pattern, fieldName); err == nil && m {
						matched = true
					}
				}
			}
		}

		if matched {
			// Validate that only one algorithm is specified per override
			// This should have been caught during parsing, but we include a runtime check as well
			hasDeadband := override.Deadband != nil && (override.Deadband.Threshold != 0 || override.Deadband.MaxTime != 0)
			hasSwingingDoor := override.SwingingDoor != nil && (override.SwingingDoor.Threshold != 0 || override.SwingingDoor.MaxTime != 0 || override.SwingingDoor.MinTime != 0)

			if hasDeadband && hasSwingingDoor {
				return "", nil, fmt.Errorf("override pattern '%s' specifies both deadband and swinging_door algorithms - only one algorithm may be specified per override", override.Pattern)
			}

			// Apply deadband overrides (only if actually configured with meaningful values)
			if hasDeadband {
				algorithm = "deadband"
				if override.Deadband.Threshold != 0 {
					config["threshold"] = override.Deadband.Threshold
				}
				if override.Deadband.MaxTime != 0 {
					config["max_time"] = override.Deadband.MaxTime.String()
				}
			}

			// Apply swinging door overrides (only if actually configured with meaningful values)
			if hasSwingingDoor {
				algorithm = "swinging_door"
				if override.SwingingDoor.Threshold != 0 {
					config["threshold"] = override.SwingingDoor.Threshold
				}
				if override.SwingingDoor.MaxTime != 0 {
					config["max_time"] = override.SwingingDoor.MaxTime.String()
				}
				if override.SwingingDoor.MinTime != 0 {
					config["min_time"] = override.SwingingDoor.MinTime.String()
				}
			}

			// Apply late policy overrides
			if override.LatePolicy != nil {
				if override.LatePolicy.LatePolicy != "" {
					config["late_policy"] = override.LatePolicy.LatePolicy
				}
			}
			break
		}
	}

	return algorithm, config, nil
}
