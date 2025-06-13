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
	"github.com/redpanda-data/benthos/v4/public/service"
)

// ConfigurationParser handles parsing of Benthos configuration into DownsamplerConfig
type ConfigurationParser struct{}

// NewConfigurationParser creates a new configuration parser
func NewConfigurationParser() *ConfigurationParser {
	return &ConfigurationParser{}
}

// ParseConfiguration parses the Benthos configuration into a DownsamplerConfig
func (cp *ConfigurationParser) ParseConfiguration(conf *service.ParsedConfig) (DownsamplerConfig, error) {
	var config DownsamplerConfig

	// Parse default configuration
	if err := cp.parseDefaultConfig(conf, &config); err != nil {
		return config, err
	}

	// Parse overrides
	if err := cp.parseOverrides(conf, &config); err != nil {
		return config, err
	}

	// Parse allow_meta_overrides (defaults to true if not specified)
	if allowMeta, err := conf.FieldBool("allow_meta_overrides"); err == nil {
		config.AllowMeta = allowMeta
	} else {
		config.AllowMeta = true
	}

	return config, nil
}

// parseDefaultConfig parses the default configuration section
func (cp *ConfigurationParser) parseDefaultConfig(conf *service.ParsedConfig, config *DownsamplerConfig) error {
	// Parse deadband defaults
	if err := cp.parseDeadbandDefaults(conf, &config.Default); err != nil {
		return err
	}

	// Parse swinging door defaults
	if err := cp.parseSwingingDoorDefaults(conf, &config.Default); err != nil {
		return err
	}

	// Parse late policy defaults
	if err := cp.parseLatePolicyDefaults(conf, &config.Default); err != nil {
		return err
	}

	return nil
}

// parseDeadbandDefaults parses the deadband default configuration
func (cp *ConfigurationParser) parseDeadbandDefaults(conf *service.ParsedConfig, defaultConfig *DefaultConfig) error {
	if defaultParsed := conf.Namespace("default", "deadband"); defaultParsed.Contains() {
		if threshold, err := defaultParsed.FieldFloat("threshold"); err == nil {
			defaultConfig.Deadband.Threshold = threshold
		}
		if maxTime, err := defaultParsed.FieldDuration("max_time"); err == nil {
			defaultConfig.Deadband.MaxTime = maxTime
		}
	}
	return nil
}

// parseSwingingDoorDefaults parses the swinging door default configuration
func (cp *ConfigurationParser) parseSwingingDoorDefaults(conf *service.ParsedConfig, defaultConfig *DefaultConfig) error {
	if defaultParsed := conf.Namespace("default", "swinging_door"); defaultParsed.Contains() {
		if threshold, err := defaultParsed.FieldFloat("threshold"); err == nil {
			defaultConfig.SwingingDoor.Threshold = threshold
		}
		if maxTime, err := defaultParsed.FieldDuration("max_time"); err == nil {
			defaultConfig.SwingingDoor.MaxTime = maxTime
		}
		if minTime, err := defaultParsed.FieldDuration("min_time"); err == nil {
			defaultConfig.SwingingDoor.MinTime = minTime
		}
	}
	return nil
}

// parseLatePolicyDefaults parses the late policy default configuration
func (cp *ConfigurationParser) parseLatePolicyDefaults(conf *service.ParsedConfig, defaultConfig *DefaultConfig) error {
	if defaultParsed := conf.Namespace("default", "late_policy"); defaultParsed.Contains() {
		if latePolicy, err := defaultParsed.FieldString("late_policy"); err == nil {
			defaultConfig.LatePolicy.LatePolicy = latePolicy
		}
	}
	return nil
}

// parseOverrides parses the override configuration section
func (cp *ConfigurationParser) parseOverrides(conf *service.ParsedConfig, config *DownsamplerConfig) error {
	if overridesList, err := conf.FieldObjectList("overrides"); err == nil {
		for _, overrideConf := range overridesList {
			override, err := cp.parseOverrideConfig(overrideConf)
			if err != nil {
				return err
			}
			config.Overrides = append(config.Overrides, override)
		}
	}
	return nil
}

// parseOverrideConfig parses a single override configuration
func (cp *ConfigurationParser) parseOverrideConfig(overrideConf *service.ParsedConfig) (OverrideConfig, error) {
	var override OverrideConfig

	// Parse pattern (unified field for both exact matches and wildcards)
	if pattern, err := overrideConf.FieldString("pattern"); err == nil {
		override.Pattern = pattern
	}

	// Parse algorithm overrides
	if err := cp.parseOverrideDeadband(overrideConf, &override); err != nil {
		return override, err
	}
	if err := cp.parseOverrideSwingingDoor(overrideConf, &override); err != nil {
		return override, err
	}
	if err := cp.parseOverrideLatePolicy(overrideConf, &override); err != nil {
		return override, err
	}

	// If both algorithms are specified, swinging door takes precedence
	// No validation error needed - the precedence is documented behavior

	return override, nil
}

// parseOverrideDeadband parses deadband override configuration
func (cp *ConfigurationParser) parseOverrideDeadband(overrideConf *service.ParsedConfig, override *OverrideConfig) error {
	if deadbandParsed := overrideConf.Namespace("deadband"); deadbandParsed.Contains() {
		override.Deadband = &DeadbandConfig{}
		if threshold, err := deadbandParsed.FieldFloat("threshold"); err == nil {
			override.Deadband.Threshold = threshold
		}
		if maxTime, err := deadbandParsed.FieldDuration("max_time"); err == nil {
			override.Deadband.MaxTime = maxTime
		}
	}
	return nil
}

// parseOverrideSwingingDoor parses swinging door override configuration
func (cp *ConfigurationParser) parseOverrideSwingingDoor(overrideConf *service.ParsedConfig, override *OverrideConfig) error {
	if swingingDoorParsed := overrideConf.Namespace("swinging_door"); swingingDoorParsed.Contains() {
		override.SwingingDoor = &SwingingDoorConfig{}
		if threshold, err := swingingDoorParsed.FieldFloat("threshold"); err == nil {
			override.SwingingDoor.Threshold = threshold
		}
		if maxTime, err := swingingDoorParsed.FieldDuration("max_time"); err == nil {
			override.SwingingDoor.MaxTime = maxTime
		}
		if minTime, err := swingingDoorParsed.FieldDuration("min_time"); err == nil {
			override.SwingingDoor.MinTime = minTime
		}
	}
	return nil
}

// parseOverrideLatePolicy parses late policy override configuration
func (cp *ConfigurationParser) parseOverrideLatePolicy(overrideConf *service.ParsedConfig, override *OverrideConfig) error {
	if latePolicyParsed := overrideConf.Namespace("late_policy"); latePolicyParsed.Contains() {
		override.LatePolicy = &LatePolicyConfig{}
		if latePolicy, err := latePolicyParsed.FieldString("late_policy"); err == nil {
			override.LatePolicy.LatePolicy = latePolicy
		}
	}
	return nil
}
