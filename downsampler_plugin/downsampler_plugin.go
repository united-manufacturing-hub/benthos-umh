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
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
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

// SeriesState holds the state for a single time series
type SeriesState struct {
	processor         *algorithms.ProcessorWrapper
	lastProcessedTime time.Time // Track the last processed timestamp for late arrival detection
	mutex             sync.RWMutex
}

func init() {
	spec := service.NewConfigSpec().
		Version("1.0.0").
		Summary("Downsamples time-series data using configurable algorithms").
		Description(`The downsampler reduces data volume by filtering out insignificant changes in time-series data using configurable algorithms.

It processes UMH-core time-series data with data_contract "_historian", 
passing all other messages through unchanged. Each message that passes the downsampling filter is annotated 
with metadata indicating the algorithm used.

Supported format:
- UMH-core: Single "value" field with timestamp (one tag, one message, one topic)

The plugin maintains separate state for each time series (identified by umh_topic) and applies the configured algorithm
to determine whether each data point represents a significant change worth preserving.

Currently supported algorithms:
- deadband: Filters out changes smaller than a configured threshold
- swinging_door: Dynamic compression maintaining trend fidelity using Swinging Door Trending (SDT)`).
		Field(service.NewObjectField("default",
			service.NewObjectField("deadband",
				service.NewFloatField("threshold").
					Description("Default threshold for deadband algorithm.").
					Default(0.0).
					Optional(),
				service.NewDurationField("max_time").
					Description("Default maximum time interval for deadband algorithm.").
					Optional()).
				Description("Default deadband algorithm parameters.").
				Optional(),
			service.NewObjectField("swinging_door",
				service.NewFloatField("threshold").
					Description("Default compression deviation for swinging door algorithm.").
					Optional(),
				service.NewDurationField("min_time").
					Description("Default minimum time interval for swinging door algorithm.").
					Optional(),
				service.NewDurationField("max_time").
					Description("Default maximum time interval for swinging door algorithm.").
					Optional()).
				Description("Default swinging door algorithm parameters.").
				Optional(),
			service.NewObjectField("late_policy",
				service.NewStringEnumField("late_policy", "passthrough", "drop").
					Description("Default policy for handling late-arriving messages (passthrough=forward unchanged, drop=discard with warning).").
					Default("passthrough").
					Optional(),
			).
				Description("Default late arrival handling parameters.").
				Optional()).
			Description("Default algorithm parameters applied to all topics unless overridden.")).
		Field(service.NewObjectListField("overrides",
			service.NewStringField("pattern").
				Description("Regex pattern to match topics (e.g., '.+_counter', '^temp_'). Mutually exclusive with 'topic'.").
				Optional(),
			service.NewStringField("topic").
				Description("Exact topic to match. Mutually exclusive with 'pattern'.").
				Optional(),

			service.NewObjectField("deadband",
				service.NewFloatField("threshold").
					Description("Override threshold for deadband algorithm.").
					Optional(),
				service.NewDurationField("max_time").
					Description("Override maximum time interval for deadband algorithm.").
					Optional()).
				Description("Deadband algorithm parameter overrides.").
				Optional(),
			service.NewObjectField("swinging_door",
				service.NewFloatField("threshold").
					Description("Override compression deviation for swinging door algorithm.").
					Optional(),
				service.NewDurationField("min_time").
					Description("Override minimum time interval for swinging door algorithm.").
					Optional(),
				service.NewDurationField("max_time").
					Description("Override maximum time interval for swinging door algorithm.").
					Optional()).
				Description("Swinging door algorithm parameter overrides.").
				Optional(),
			service.NewObjectField("late_policy",
				service.NewStringEnumField("late_policy", "passthrough", "drop").
					Description("Override policy for handling late-arriving messages (passthrough=forward unchanged, drop=discard with warning).").
					Optional(),
			).
				Description("Late arrival handling parameter overrides.").
				Optional()).
			Description("Topic-specific parameter overrides. Supports regex patterns and exact topic matching.").
			Optional())

	err := service.RegisterBatchProcessor(
		"downsampler",
		spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			// Parse default configuration
			var defaultConfig DefaultConfig

			// Parse deadband defaults
			if defaultParsed := conf.Namespace("default", "deadband"); defaultParsed.Contains() {
				if threshold, err := defaultParsed.FieldFloat("threshold"); err == nil {
					defaultConfig.Deadband.Threshold = threshold
				}
				if maxTime, err := defaultParsed.FieldDuration("max_time"); err == nil {
					defaultConfig.Deadband.MaxTime = maxTime
				}
			}

			// Parse swinging door defaults
			if defaultParsed := conf.Namespace("default", "swinging_door"); defaultParsed.Contains() {
				if threshold, err := defaultParsed.FieldFloat("threshold"); err == nil {
					defaultConfig.SwingingDoor.Threshold = threshold
				}
				if minTime, err := defaultParsed.FieldDuration("min_time"); err == nil {
					defaultConfig.SwingingDoor.MinTime = minTime
				}
				if maxTime, err := defaultParsed.FieldDuration("max_time"); err == nil {
					defaultConfig.SwingingDoor.MaxTime = maxTime
				}
			}

			// Parse late policy defaults
			if defaultParsed := conf.Namespace("default", "late_policy"); defaultParsed.Contains() {
				if latePolicy, err := defaultParsed.FieldString("late_policy"); err == nil {
					defaultConfig.LatePolicy.LatePolicy = latePolicy
				}
			}

			// Parse overrides
			var overrides []OverrideConfig
			if overridesList, err := conf.FieldObjectList("overrides"); err == nil {
				for _, overrideConf := range overridesList {
					var override OverrideConfig

					if pattern, err := overrideConf.FieldString("pattern"); err == nil {
						override.Pattern = pattern
					}
					if topic, err := overrideConf.FieldString("topic"); err == nil {
						override.Topic = topic
					}

					// Parse deadband overrides
					if deadbandParsed := overrideConf.Namespace("deadband"); deadbandParsed.Contains() {
						override.Deadband = &DeadbandConfig{}
						if threshold, err := deadbandParsed.FieldFloat("threshold"); err == nil {
							override.Deadband.Threshold = threshold
						}
						if maxTime, err := deadbandParsed.FieldDuration("max_time"); err == nil {
							override.Deadband.MaxTime = maxTime
						}
					}

					// Parse swinging door overrides
					if swingingDoorParsed := overrideConf.Namespace("swinging_door"); swingingDoorParsed.Contains() {
						override.SwingingDoor = &SwingingDoorConfig{}
						if threshold, err := swingingDoorParsed.FieldFloat("threshold"); err == nil {
							override.SwingingDoor.Threshold = threshold
						}
						if minTime, err := swingingDoorParsed.FieldDuration("min_time"); err == nil {
							override.SwingingDoor.MinTime = minTime
						}
						if maxTime, err := swingingDoorParsed.FieldDuration("max_time"); err == nil {
							override.SwingingDoor.MaxTime = maxTime
						}
					}

					// Parse late policy overrides
					if latePolicyParsed := overrideConf.Namespace("late_policy"); latePolicyParsed.Contains() {
						override.LatePolicy = &LatePolicyConfig{}
						if latePolicy, err := latePolicyParsed.FieldString("late_policy"); err == nil {
							override.LatePolicy.LatePolicy = latePolicy
						}
					}

					overrides = append(overrides, override)
				}
			}

			config := DownsamplerConfig{
				Default:   defaultConfig,
				Overrides: overrides,
			}

			return newDownsamplerProcessor(config, mgr.Logger(), mgr.Metrics())
		})
	if err != nil {
		panic(err)
	}
}

// DownsamplerProcessor implements the downsampling logic
type DownsamplerProcessor struct {
	config            DownsamplerConfig
	logger            *service.Logger
	seriesState       map[string]*SeriesState
	stateMutex        sync.RWMutex
	messagesProcessed *service.MetricCounter
	messagesFiltered  *service.MetricCounter
	messagesErrored   *service.MetricCounter
	messagesPassed    *service.MetricCounter
}

func newDownsamplerProcessor(config DownsamplerConfig, logger *service.Logger, metrics *service.Metrics) (*DownsamplerProcessor, error) {
	return &DownsamplerProcessor{
		config:            config,
		logger:            logger,
		seriesState:       make(map[string]*SeriesState),
		messagesProcessed: metrics.NewCounter("messages_processed"),
		messagesFiltered:  metrics.NewCounter("messages_filtered"),
		messagesErrored:   metrics.NewCounter("messages_errored"),
		messagesPassed:    metrics.NewCounter("messages_passed_through"),
	}, nil
}

// getThresholdForTopic returns the appropriate threshold for a given topic
func (p *DownsamplerProcessor) getThresholdForTopic(topic string) float64 {
	_, config := p.config.GetConfigForTopic(topic)
	if threshold, ok := config["threshold"].(float64); ok {
		return threshold
	}
	return 0.0 // Default fallback
}

// ProcessBatch processes a batch of messages, applying downsampling to time-series data
func (p *DownsamplerProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var outBatch service.MessageBatch

	for _, msg := range batch {
		// Process UMH-core time-series messages
		if !p.isTimeSeriesMessage(msg) {
			outBatch = append(outBatch, msg)
			p.messagesPassed.Incr(1)
			continue
		}

		// Parse structured payload
		data, err := msg.AsStructured()
		if err != nil {
			p.messagesErrored.Incr(1)
			p.logger.Errorf("Failed to parse structured data: %v", err)
			// Fail open - pass message through on error
			outBatch = append(outBatch, msg)
			continue
		}

		dataMap, ok := data.(map[string]interface{})
		if !ok {
			p.messagesErrored.Incr(1)
			p.logger.Errorf("Payload is not a JSON object")
			// Fail open - pass message through on error
			outBatch = append(outBatch, msg)
			continue
		}

		// Extract timestamp
		timestamp, err := p.extractTimestamp(dataMap)
		if err != nil {
			p.messagesErrored.Incr(1)
			p.logger.Errorf("Failed to extract timestamp: %v", err)
			// Fail open - pass message through on error
			outBatch = append(outBatch, msg)
			continue
		}

		// Process as UMH-core format (single "value" field)
		processedMsg, err := p.processUMHCoreMessage(msg, dataMap, timestamp)
		if err != nil {
			p.messagesErrored.Incr(1)
			p.logger.Errorf("Failed to process UMH-core message: %v", err)
			// Fail open - pass message through on error
			outBatch = append(outBatch, msg)
			continue
		}
		if processedMsg != nil {
			outBatch = append(outBatch, processedMsg)
		}
	}

	if len(outBatch) == 0 {
		return nil, nil
	}
	return []service.MessageBatch{outBatch}, nil
}

// isTimeSeriesMessage determines if a message should be processed for downsampling
// Returns true for UMH-core time-series data (JSON with timestamp_ms and value fields, plus umh_topic metadata)
func (p *DownsamplerProcessor) isTimeSeriesMessage(msg *service.Message) bool {
	// Check for umh_topic metadata (required for UMH-core)
	_, hasUmhTopic := msg.MetaGet("umh_topic")
	if !hasUmhTopic {
		return false
	}

	// UMH-core format: structured payload with timestamp_ms and value fields
	dataMap, err := msg.AsStructured()
	if err != nil {
		return false
	}

	// Convert to map[string]interface{} for field access
	data, ok := dataMap.(map[string]interface{})
	if !ok {
		return false
	}

	// Check for required UMH-core fields
	_, hasTimestamp := data["timestamp_ms"]
	_, hasValue := data["value"]

	// Require both timestamp_ms and value fields for UMH-core format
	return hasTimestamp && hasValue
}

// extractTimestamp extracts and converts timestamp from message data
func (p *DownsamplerProcessor) extractTimestamp(dataMap map[string]interface{}) (time.Time, error) {
	timestampMs, ok := dataMap["timestamp_ms"]
	if !ok {
		return time.Time{}, errors.New("missing timestamp_ms field")
	}

	var ts int64
	switch v := timestampMs.(type) {
	case float64:
		ts = int64(v)
	case int:
		ts = int64(v)
	case int64:
		ts = v
	default:
		return time.Time{}, fmt.Errorf("invalid timestamp_ms type: %T", timestampMs)
	}

	return time.Unix(0, ts*int64(time.Millisecond)), nil
}

// processUMHCoreMessage processes a UMH-core format message (single "value" field)
func (p *DownsamplerProcessor) processUMHCoreMessage(msg *service.Message, dataMap map[string]interface{}, timestamp time.Time) (*service.Message, error) {
	// Get umh_topic for series identification
	umhTopic, exists := msg.MetaGet("umh_topic")
	if !exists {
		return nil, errors.New("missing umh_topic metadata")
	}

	// Extract value
	value := dataMap["value"]
	if value == nil {
		return nil, errors.New("missing value field")
	}

	// Get or create series state
	state, err := p.getOrCreateSeriesState(umhTopic)
	if err != nil {
		return nil, fmt.Errorf("failed to get series state: %w", err)
	}

	// ProcessorWrapper handles late arrival internally based on PassThrough setting

	// Apply downsampling algorithm
	shouldKeep, err := p.shouldKeepMessage(state, value, timestamp)
	if err != nil {
		return nil, fmt.Errorf("algorithm error for series %s: %w", umhTopic, err)
	}

	if shouldKeep {
		// Update state and return message
		p.updateSeriesState(state, value, timestamp)
		msg.MetaSet("downsampled_by", state.processor.GetMetadata())
		p.messagesProcessed.Incr(1)
		return msg, nil
	} else {
		// Message filtered - update processed time for late arrival detection
		p.updateProcessedTime(state, timestamp)
		p.messagesFiltered.Incr(1)
		return nil, nil
	}
}

// getOrCreateSeriesState retrieves or creates the state for a time series
func (p *DownsamplerProcessor) getOrCreateSeriesState(seriesID string) (*SeriesState, error) {
	p.stateMutex.RLock()
	state, exists := p.seriesState[seriesID]
	p.stateMutex.RUnlock()

	if exists {
		return state, nil
	}

	// Create new state
	p.stateMutex.Lock()
	defer p.stateMutex.Unlock()

	// Check again in case another goroutine created it
	if state, exists := p.seriesState[seriesID]; exists {
		return state, nil
	}

	// Get algorithm configuration for this topic
	algorithmType, algorithmConfig := p.config.GetConfigForTopic(seriesID)

	// Map late policy to PassThrough parameter
	latePolicy, _ := algorithmConfig["late_policy"].(string)
	passThrough := true // Default to passthrough
	if latePolicy == "drop" {
		passThrough = false
	}

	// Remove late_policy from algorithm config as it's handled by ProcessorWrapper
	algConfig := make(map[string]interface{})
	for k, v := range algorithmConfig {
		if k != "late_policy" {
			algConfig[k] = v
		}
	}

	processorConfig := algorithms.ProcessorConfig{
		Algorithm:       algorithmType,
		AlgorithmConfig: algConfig,
		PassThrough:     passThrough,
	}

	processor, err := algorithms.NewProcessorWrapper(processorConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create processor %s: %w", algorithmType, err)
	}

	state = &SeriesState{
		processor: processor,
	}

	p.seriesState[seriesID] = state
	return state, nil
}

// handleLateArrival checks if a message is late and handles it according to the late policy
func (p *DownsamplerProcessor) handleLateArrival(seriesID string, state *SeriesState, timestamp time.Time, msg *service.Message) (bool, error) {
	// Skip late arrival check for first message
	if state.lastProcessedTime.IsZero() {
		return false, nil // Not late, continue processing
	}

	// Get late policy configuration for this topic
	_, config := p.config.GetConfigForTopic(seriesID)
	latePolicy, _ := config["late_policy"].(string)

	// Default values if not configured
	if latePolicy == "" {
		latePolicy = "passthrough"
	}

	// Check if message is late (older than last processed timestamp)
	if timestamp.Before(state.lastProcessedTime) {
		switch latePolicy {
		case "drop":
			p.logger.Warnf("Late point %s %v dropped (older than last processed %v, policy=%s)", seriesID, timestamp, state.lastProcessedTime, latePolicy)
			return true, nil // Message handled (dropped)
		case "passthrough":
			p.logger.Debugf("Late point %s %v passed through unchanged (older than last processed %v, policy=%s)", seriesID, timestamp, state.lastProcessedTime, latePolicy)
			// Add metadata to indicate this is a late out-of-order message
			msg.MetaSet("late_oos", "true")
			return true, nil // Message handled (passthrough)
		}
	}

	return false, nil // Not late, continue normal processing
}

// shouldKeepMessage determines if a message should be kept based on the algorithm
func (p *DownsamplerProcessor) shouldKeepMessage(state *SeriesState, value interface{}, timestamp time.Time) (bool, error) {
	// ProcessorWrapper handles all type conversion, boolean/string logic, and algorithm processing
	shouldKeep, err := state.processor.ProcessPoint(value, timestamp)

	if err == nil && !shouldKeep && p.logger != nil {
		p.logger.Debug(fmt.Sprintf("ProcessorWrapper dropped value: %v", value))
	}

	return shouldKeep, err
}

// areEqual checks if two values are equal (used for non-numeric types)
func (p *DownsamplerProcessor) areEqual(a, b interface{}) bool {
	// Handle different types
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}

	// Use deep equal for complex types (maps, slices, arrays)
	aType := reflect.TypeOf(a)
	if aType.Kind() == reflect.Map || aType.Kind() == reflect.Slice || aType.Kind() == reflect.Array {
		return reflect.DeepEqual(a, b)
	}

	// For simple types, direct comparison should work
	return a == b
}

// logDropReason provides detailed logging about why a message was dropped
func (p *DownsamplerProcessor) logDropReason(state *SeriesState, currentValue, previousValue interface{}, currentTime, previousTime time.Time) {
	algorithmName := state.processor.GetName()

	// Extract threshold for current algorithm (assuming deadband for now)
	threshold := p.extractThresholdFromMetadata(state.processor.GetMetadata())

	switch algorithmName {
	case "deadband":
		p.logDeadbandDropReason(currentValue, previousValue, currentTime, previousTime, threshold)
	case "swinging_door":
		p.logSwingingDoorDropReason(currentValue, previousValue, currentTime, previousTime)
	default:
		p.logger.Debugf("Message dropped by %s algorithm: current=%v, previous=%v",
			algorithmName, currentValue, previousValue)
	}
}

// logDeadbandDropReason provides specific logging for deadband algorithm drops
func (p *DownsamplerProcessor) logDeadbandDropReason(currentValue, previousValue interface{}, currentTime, previousTime time.Time, threshold float64) {
	// First message case
	if previousValue == nil {
		p.logger.Debugf("Message kept: first message in series")
		return
	}

	// Try to convert to numeric values for detailed comparison
	currentFloat, currentErr := p.toFloat64(currentValue)
	previousFloat, previousErr := p.toFloat64(previousValue)

	if currentErr != nil || previousErr != nil {
		// Non-numeric comparison
		if currentErr != nil {
			p.logger.Debugf("Message dropped: could not convert current value to numeric (%v), using equality check", currentErr)
		} else if previousErr != nil {
			p.logger.Debugf("Message dropped: could not convert previous value to numeric (%v), treating as different", previousErr)
		}
		return
	}

	// Calculate difference for numeric values
	diff := currentFloat - previousFloat
	absDiff := diff
	if absDiff < 0 {
		absDiff = -absDiff
	}

	// Time since last output
	timeSinceLastOutput := currentTime.Sub(previousTime)

	// Detailed logging with all relevant information
	p.logger.Debugf("Message dropped by deadband: current=%.6f, previous=%.6f, diff=%.6f, absDiff=%.6f, threshold=%.6f, timeSince=%v",
		currentFloat, previousFloat, diff, absDiff, threshold, timeSinceLastOutput)

	if absDiff < threshold {
		p.logger.Debugf("Drop reason: absolute difference (%.6f) below threshold (%.6f)", absDiff, threshold)
	}
}

// logSwingingDoorDropReason provides specific logging for swinging door algorithm drops
func (p *DownsamplerProcessor) logSwingingDoorDropReason(currentValue, previousValue interface{}, currentTime, previousTime time.Time) {
	// First message case
	if previousValue == nil {
		p.logger.Debugf("Message kept: first message in series")
		return
	}

	// Try to convert to numeric values for detailed comparison
	currentFloat, currentErr := p.toFloat64(currentValue)
	previousFloat, previousErr := p.toFloat64(previousValue)

	if currentErr != nil || previousErr != nil {
		// Non-numeric comparison
		if currentErr != nil {
			p.logger.Debugf("Message dropped: could not convert current value to numeric (%v), using fail-open", currentErr)
		} else if previousErr != nil {
			p.logger.Debugf("Message dropped: could not convert previous value to numeric (%v), treating as different", previousErr)
		}
		return
	}

	// Time since last output
	timeSinceLastOutput := currentTime.Sub(previousTime)

	// Detailed logging with all relevant information
	p.logger.Debugf("Message dropped by swinging_door: current=%.6f, previous=%.6f, timeSince=%v",
		currentFloat, previousFloat, timeSinceLastOutput)

	p.logger.Debugf("Drop reason: point remained within swinging door bounds")
}

// extractThresholdFromMetadata extracts threshold value from algorithm metadata string
func (p *DownsamplerProcessor) extractThresholdFromMetadata(metadata string) float64 {
	// Parse metadata string like "deadband(threshold=0.500,max_time=30s)" or "deadband(threshold=0.500)"
	// This is a simple parser - in production you might want something more robust
	start := strings.Index(metadata, "threshold=")
	if start == -1 {
		return 0.0
	}
	start += len("threshold=")

	end := start
	for end < len(metadata) && (metadata[end] >= '0' && metadata[end] <= '9' || metadata[end] == '.') {
		end++
	}

	if end > start {
		if threshold, err := strconv.ParseFloat(metadata[start:end], 64); err == nil {
			return threshold
		}
	}

	return 0.0
}

// toFloat64 converts various numeric types to float64 (helper method)
func (p *DownsamplerProcessor) toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case bool:
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("cannot convert string to number: %s", v)
	default:
		return 0, fmt.Errorf("cannot convert type %T to float64", val)
	}
}

// updateSeriesState updates the state after a message is kept
func (p *DownsamplerProcessor) updateSeriesState(state *SeriesState, value interface{}, timestamp time.Time) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	// ProcessorWrapper maintains its own internal state
	// We only need to track lastProcessedTime for late arrival detection
	state.lastProcessedTime = timestamp
}

// updateProcessedTime updates only the lastProcessedTime (for messages that were processed but not kept)
func (p *DownsamplerProcessor) updateProcessedTime(state *SeriesState, timestamp time.Time) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	// Only update lastProcessedTime for late arrival detection
	// Don't update lastOutput/lastOutputTime since message wasn't kept
	if timestamp.After(state.lastProcessedTime) {
		state.lastProcessedTime = timestamp
	}
}

// Close cleans up resources and flushes any pending points
func (p *DownsamplerProcessor) Close(ctx context.Context) error {
	// Just clean up - don't try to emit messages during close
	// In a real scenario, the algorithms should be designed to emit pending points
	// when they receive envelope-breaking data, not on close
	p.stateMutex.Lock()
	defer p.stateMutex.Unlock()

	for _, state := range p.seriesState {
		// Reset the processor state
		state.processor.Reset()
	}
	p.seriesState = make(map[string]*SeriesState)

	return nil
}
