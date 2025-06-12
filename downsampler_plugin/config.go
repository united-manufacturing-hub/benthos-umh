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
	MinTime   time.Duration `json:"min_time,omitempty" yaml:"min_time,omitempty"`
	MaxTime   time.Duration `json:"max_time,omitempty" yaml:"max_time,omitempty"`
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

// OverrideConfig defines algorithm parameter overrides for specific topics or patterns
type OverrideConfig struct {
	Pattern      string              `json:"pattern,omitempty" yaml:"pattern,omitempty"`
	Topic        string              `json:"topic,omitempty" yaml:"topic,omitempty"`
	Deadband     *DeadbandConfig     `json:"deadband,omitempty" yaml:"deadband,omitempty"`
	SwingingDoor *SwingingDoorConfig `json:"swinging_door,omitempty" yaml:"swinging_door,omitempty"`
	LatePolicy   *LatePolicyConfig   `json:"late_policy,omitempty" yaml:"late_policy,omitempty"`
}

// DownsamplerConfig holds the configuration for the downsampler processor
type DownsamplerConfig struct {
	Default   DefaultConfig    `json:"default" yaml:"default"`
	Overrides []OverrideConfig `json:"overrides,omitempty" yaml:"overrides,omitempty"`
}

// GetConfigForTopic returns the effective configuration for a given topic by applying overrides
func (c *DownsamplerConfig) GetConfigForTopic(topic string) (string, map[string]interface{}) {
	// Determine default algorithm based on which default config has values
	// Prioritize deadband as the simpler algorithm
	algorithm := "deadband" // Default fallback

	// Only use swinging_door if explicitly configured and deadband is not
	hasDeadbandConfig := c.Default.Deadband.Threshold != 0 || c.Default.Deadband.MaxTime != 0
	hasSwingingDoorConfig := c.Default.SwingingDoor.Threshold != 0 || c.Default.SwingingDoor.MinTime != 0 || c.Default.SwingingDoor.MaxTime != 0

	if hasDeadbandConfig {
		algorithm = "deadband"
	} else if hasSwingingDoorConfig {
		algorithm = "swinging_door"
	}

	// Start with defaults based on the determined algorithm
	config := map[string]interface{}{}

	if algorithm == "swinging_door" {
		config["threshold"] = c.Default.SwingingDoor.Threshold
		config["min_time"] = c.Default.SwingingDoor.MinTime
		config["max_time"] = c.Default.SwingingDoor.MaxTime
	} else {
		config["threshold"] = c.Default.Deadband.Threshold
		config["max_time"] = c.Default.Deadband.MaxTime
		config["min_time"] = c.Default.SwingingDoor.MinTime // Deadband doesn't use min_time but include for consistency
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

		if override.Topic != "" {
			matched = (override.Topic == topic)
		} else if len(override.Pattern) > 0 {
			// Use filepath.Match for wildcard patterns (supports * and ?)
			if m, err := filepath.Match(override.Pattern, topic); err == nil && m {
				matched = true
			} else {
				// Also check against just the field name (last part after last dot)
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
			fmt.Printf("      🎯 OVERRIDE MATCHED for topic='%s', pattern='%s'\n", topic, override.Pattern)

			// Apply deadband overrides (only if actually configured with meaningful values)
			if override.Deadband != nil && (override.Deadband.Threshold != 0 || override.Deadband.MaxTime != 0) {
				fmt.Printf("      🎯 APPLYING DEADBAND override: threshold=%v\n", override.Deadband.Threshold)
				algorithm = "deadband"
				if override.Deadband.Threshold != 0 {
					config["threshold"] = override.Deadband.Threshold
				}
				if override.Deadband.MaxTime != 0 {
					config["max_time"] = override.Deadband.MaxTime
				}
			}

			// Apply swinging door overrides (only if actually configured with meaningful values)
			if override.SwingingDoor != nil && (override.SwingingDoor.Threshold != 0 || override.SwingingDoor.MinTime != 0 || override.SwingingDoor.MaxTime != 0) {
				fmt.Printf("      🎯 APPLYING SWINGING_DOOR override: threshold=%v\n", override.SwingingDoor.Threshold)
				algorithm = "swinging_door"
				if override.SwingingDoor.Threshold != 0 {
					config["threshold"] = override.SwingingDoor.Threshold
				}
				if override.SwingingDoor.MinTime != 0 {
					config["min_time"] = override.SwingingDoor.MinTime
				}
				if override.SwingingDoor.MaxTime != 0 {
					config["max_time"] = override.SwingingDoor.MaxTime
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

	fmt.Printf("      ⚙️  FINAL CONFIG for topic='%s': algorithm=%s, config=%+v\n", topic, algorithm, config)
	return algorithm, config
}
