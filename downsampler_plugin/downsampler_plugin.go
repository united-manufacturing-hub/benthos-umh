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

// DefaultConfig holds the default algorithm parameters
type DefaultConfig struct {
	Deadband     DeadbandConfig     `json:"deadband,omitempty" yaml:"deadband,omitempty"`
	SwingingDoor SwingingDoorConfig `json:"swinging_door,omitempty" yaml:"swinging_door,omitempty"`
}

// OverrideConfig defines algorithm parameter overrides for specific topics or patterns
type OverrideConfig struct {
	Pattern string `json:"pattern,omitempty" yaml:"pattern,omitempty"` // Regex pattern to match topics
	Topic   string `json:"topic,omitempty" yaml:"topic,omitempty"`     // Exact topic match

	// Algorithm-specific configurations
	Deadband     *DeadbandConfig     `json:"deadband,omitempty" yaml:"deadband,omitempty"`
	SwingingDoor *SwingingDoorConfig `json:"swinging_door,omitempty" yaml:"swinging_door,omitempty"`
}

// DownsamplerConfig holds the configuration for the downsampler processor
type DownsamplerConfig struct {
	Default   DefaultConfig    `json:"default" yaml:"default"`
	Overrides []OverrideConfig `json:"overrides,omitempty" yaml:"overrides,omitempty"`
}

// getConfigForTopic returns the effective configuration for a given topic by applying overrides
func (c *DownsamplerConfig) getConfigForTopic(topic string) (string, map[string]interface{}) {
	// Start with defaults
	config := map[string]interface{}{
		"threshold": c.Default.Deadband.Threshold,
		"max_time":  c.Default.Deadband.MaxTime,
		"min_time":  c.Default.SwingingDoor.MinTime,
	}

	// Use swinging door max_time if deadband max_time is not set
	if c.Default.Deadband.MaxTime == 0 && c.Default.SwingingDoor.MaxTime != 0 {
		config["max_time"] = c.Default.SwingingDoor.MaxTime
	}

	// Determine default algorithm based on which default config has values
	algorithm := "deadband" // Default fallback
	if c.Default.SwingingDoor.Threshold != 0 || c.Default.SwingingDoor.MinTime != 0 || c.Default.SwingingDoor.MaxTime != 0 {
		algorithm = "swinging_door"
	} else if c.Default.Deadband.Threshold != 0 || c.Default.Deadband.MaxTime != 0 {
		algorithm = "deadband"
	}

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
			// Apply deadband overrides
			if override.Deadband != nil {
				algorithm = "deadband"
				if override.Deadband.Threshold != 0 {
					config["threshold"] = override.Deadband.Threshold
				}
				if override.Deadband.MaxTime != 0 {
					config["max_time"] = override.Deadband.MaxTime
				}
			}

			// Apply swinging door overrides
			if override.SwingingDoor != nil {
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
			break
		}
	}

	return algorithm, config
}

// SeriesState holds the state for a single time series
type SeriesState struct {
	algorithm      algorithms.DownsampleAlgorithm
	lastOutput     interface{}
	lastOutputTime time.Time
	mutex          sync.RWMutex
}

func init() {
	spec := service.NewConfigSpec().
		Version("1.0.0").
		Summary("Downsamples time-series data using configurable algorithms").
		Description(`The downsampler reduces data volume by filtering out insignificant changes in time-series data using configurable algorithms.

It processes both UMH-core time-series data and UMH classic _historian messages with data_contract "_historian", 
passing all other messages through unchanged. Each message that passes the downsampling filter is annotated 
with metadata indicating the algorithm used.

Supported formats:
- UMH-core: Single "value" field with timestamp (one tag, one message, one topic)
- UMH classic: Multiple fields in one JSON object with shared timestamp

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
					Default(0.5).
					Optional(),
				service.NewDurationField("min_time").
					Description("Default minimum time interval for swinging door algorithm.").
					Optional(),
				service.NewDurationField("max_time").
					Description("Default maximum time interval for swinging door algorithm.").
					Optional()).
				Description("Default swinging door algorithm parameters.").
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
	_, config := p.config.getConfigForTopic(topic)
	if threshold, ok := config["threshold"].(float64); ok {
		return threshold
	}
	return 0.0 // Default fallback
}

// ProcessBatch processes a batch of messages, applying downsampling to time-series data
func (p *DownsamplerProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var outBatch service.MessageBatch

	for _, msg := range batch {
		// Process both UMH-core time-series and classic _historian messages
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

		// Detect message format and process accordingly
		if p.isUMHCoreFormat(dataMap) {
			// UMH-core format: single "value" field - process as before
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
		} else {
			// UMH classic _historian format: multiple fields - process per key
			processedMsg, err := p.processUMHClassicMessage(msg, dataMap, timestamp)
			if err != nil {
				p.messagesErrored.Incr(1)
				p.logger.Errorf("Failed to process UMH classic message: %v", err)
				// Fail open - pass message through on error
				outBatch = append(outBatch, msg)
				continue
			}
			if processedMsg != nil {
				outBatch = append(outBatch, processedMsg)
			}
		}
	}

	if len(outBatch) == 0 {
		return nil, nil
	}
	return []service.MessageBatch{outBatch}, nil
}

// isTimeSeriesMessage determines if a message should be processed for downsampling
// Returns true for:
// - UMH-core time-series data (data_contract = "_historian" with structured payload)
// - UMH classic _historian data (data_contract = "_historian" with any structured payload)
func (p *DownsamplerProcessor) isTimeSeriesMessage(msg *service.Message) bool {
	contract, exists := msg.MetaGet("data_contract")
	if !exists {
		return false
	}

	// Both UMH-core and classic use _historian data contract for time-series data
	return contract == "_historian"
}

// isUMHCoreFormat checks if the message uses UMH-core format (single "value" field)
func (p *DownsamplerProcessor) isUMHCoreFormat(dataMap map[string]interface{}) bool {
	_, hasValue := dataMap["value"]
	return hasValue
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

	// Apply downsampling algorithm
	shouldKeep, err := p.shouldKeepMessage(state, value, timestamp)
	if err != nil {
		return nil, fmt.Errorf("algorithm error for series %s: %w", umhTopic, err)
	}

	if shouldKeep {
		// Update state and return message
		p.updateSeriesState(state, value, timestamp)
		msg.MetaSet("downsampled_by", state.algorithm.GetMetadata())
		p.messagesProcessed.Incr(1)
		return msg, nil
	} else {
		// Message filtered
		p.messagesFiltered.Incr(1)
		return nil, nil
	}
}

// processUMHClassicMessage processes a UMH classic _historian format message (multiple fields)
func (p *DownsamplerProcessor) processUMHClassicMessage(msg *service.Message, dataMap map[string]interface{}, timestamp time.Time) (*service.Message, error) {
	// Get base topic for series identification
	baseTopic, exists := msg.MetaGet("umh_topic")
	if !exists {
		return nil, errors.New("missing umh_topic metadata")
	}

	// Create output payload with timestamp
	outputData := map[string]interface{}{
		"timestamp_ms": dataMap["timestamp_ms"],
	}

	// Track processing results
	keysProcessed := 0
	keysKept := 0
	keysDropped := 0

	// Process each field individually (excluding timestamp_ms and meta)
	for key, value := range dataMap {
		if p.isExcludedKey(key) {
			continue
		}

		keysProcessed++

		// Create series ID for this specific metric
		seriesID := p.createSeriesID(baseTopic, key)

		// Get or create series state for this metric
		state, err := p.getOrCreateSeriesState(seriesID)
		if err != nil {
			p.logger.Errorf("Failed to get series state for %s: %v", seriesID, err)
			// Fail open - keep the field
			outputData[key] = value
			keysKept++
			continue
		}

		// Apply downsampling algorithm to this field
		shouldKeep, err := p.shouldKeepMessage(state, value, timestamp)
		if err != nil {
			p.logger.Errorf("Algorithm error for series %s: %v", seriesID, err)
			// Fail open - keep the field
			outputData[key] = value
			keysKept++
			continue
		}

		if shouldKeep {
			// Keep this field
			p.updateSeriesState(state, value, timestamp)
			outputData[key] = value
			keysKept++
			p.logger.Debugf("Key kept: %s=%v in message", key, value)
		} else {
			// Drop this field
			keysDropped++
			p.logger.Debugf("Key dropped: %s=%v from message", key, value)
		}
	}

	// Log processing summary
	p.logger.Debugf("Message processed: %d keys total, %d kept, %d dropped", keysProcessed, keysKept, keysDropped)

	// Update metrics
	if keysKept > 0 {
		p.messagesProcessed.Incr(1)
	}
	if keysDropped > 0 {
		p.messagesFiltered.Incr(1)
	}

	// If no measurement fields remain (only timestamp_ms), drop the entire message
	if keysKept == 0 {
		p.logger.Debugf("Entire message dropped: no measurement fields remaining")
		return nil, nil
	}

	// Create new message with filtered data
	newMsg := msg.Copy()
	newMsg.SetStructured(outputData)

	// Add metadata annotation
	newMsg.MetaSet("downsampled_by", fmt.Sprintf("downsampler(filtered_%d_of_%d_keys)", keysKept, keysProcessed))

	return newMsg, nil
}

// isExcludedKey checks if a key should be excluded from processing
func (p *DownsamplerProcessor) isExcludedKey(key string) bool {
	excludedKeys := map[string]bool{
		"timestamp_ms": true,
		"meta":         true,
	}
	return excludedKeys[key]
}

// createSeriesID creates a unique series identifier for a specific metric within a message
func (p *DownsamplerProcessor) createSeriesID(baseTopic, metricKey string) string {
	// For classic _historian format, append the metric key to create unique series
	// Example: "umh.v1.plant1.line1._historian" + ".temperature" = "umh.v1.plant1.line1._historian.temperature"
	return baseTopic + "." + metricKey
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
	algorithmType, algorithmConfig := p.config.getConfigForTopic(seriesID)

	algorithm, err := algorithms.Create(algorithmType, algorithmConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create algorithm %s: %w", algorithmType, err)
	}

	state = &SeriesState{
		algorithm: algorithm,
	}

	p.seriesState[seriesID] = state
	return state, nil
}

// shouldKeepMessage determines if a message should be kept based on the algorithm
func (p *DownsamplerProcessor) shouldKeepMessage(state *SeriesState, value interface{}, timestamp time.Time) (bool, error) {
	// Handle "none" algorithm - no filtering
	if state.algorithm == nil {
		if state.lastOutput != nil {
			isEqual := p.areEqual(value, state.lastOutput)
			shouldKeep := !isEqual // Keep if values are different

			// Log dropped message if configured
			if !shouldKeep && p.logger != nil {
				p.logger.Debug(fmt.Sprintf("Dropped identical value: %v", value))
			}

			return shouldKeep, nil
		}
		return true, nil // First value always kept
	}

	// Handle boolean values with simple equality logic (never send to numeric algorithms)
	if _, isBool := value.(bool); isBool {
		if state.lastOutput != nil {
			isEqual := p.areEqual(value, state.lastOutput)
			shouldKeep := !isEqual // Keep if values are different

			// Log boolean handling
			if p.logger != nil {
				if shouldKeep {
					p.logger.Debug(fmt.Sprintf("Boolean value changed: %v -> %v (kept)", state.lastOutput, value))
				} else {
					p.logger.Debug(fmt.Sprintf("Boolean value unchanged: %v (dropped)", value))
				}
			}

			return shouldKeep, nil
		} else {
			// First boolean value is always kept
			if p.logger != nil {
				p.logger.Debug(fmt.Sprintf("First boolean value: %v (kept)", value))
			}
			return true, nil
		}
	}

	// Handle string values with simple equality logic (never send to numeric algorithms)
	if _, isString := value.(string); isString {
		if state.lastOutput != nil {
			isEqual := p.areEqual(value, state.lastOutput)
			shouldKeep := !isEqual // Keep if values are different

			// Log string handling
			if p.logger != nil {
				if shouldKeep {
					p.logger.Debug(fmt.Sprintf("String value changed: %v -> %v (kept)", state.lastOutput, value))
				} else {
					p.logger.Debug(fmt.Sprintf("String value unchanged: %v (dropped)", value))
				}
			}

			return shouldKeep, nil
		} else {
			// First string value is always kept
			if p.logger != nil {
				p.logger.Debug(fmt.Sprintf("First string value: %v (kept)", value))
			}
			return true, nil
		}
	}

	// For numeric values, use the configured algorithm
	shouldKeep, err := state.algorithm.ProcessPoint(value, timestamp)

	if err == nil && !shouldKeep && p.logger != nil {
		p.logger.Debug(fmt.Sprintf("Algorithm dropped numeric value: %v", value))
	}

	return shouldKeep, err
}

// isNonNumericType checks if a value is a non-numeric type (string, boolean, etc.)
func (p *DownsamplerProcessor) isNonNumericType(value interface{}) bool {
	switch value.(type) {
	case string, bool:
		return true
	default:
		return false
	}
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
	algorithmName := state.algorithm.GetName()

	// Extract threshold for current algorithm (assuming deadband for now)
	threshold := p.extractThresholdFromMetadata(state.algorithm.GetMetadata())

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

	state.lastOutput = value
	state.lastOutputTime = timestamp
}

// Close cleans up resources
func (p *DownsamplerProcessor) Close(ctx context.Context) error {
	// Clear all series state
	p.stateMutex.Lock()
	defer p.stateMutex.Unlock()

	for _, state := range p.seriesState {
		state.algorithm.Reset()
	}
	p.seriesState = make(map[string]*SeriesState)

	return nil
}
