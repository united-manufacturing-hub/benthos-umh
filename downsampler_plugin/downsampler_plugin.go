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
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

// SeriesState holds the state for a single time series
type SeriesState struct {
	processor         *algorithms.ProcessorWrapper
	lastProcessedTime time.Time // Track the last processed timestamp for late arrival detection
	mutex             sync.RWMutex
}

// MessageProcessingResult holds the result of processing a single message
type MessageProcessingResult struct {
	OriginalMessage   *service.Message
	ProcessedMessages []*service.Message // May be empty if filtered, may be multiple if algorithm emits multiple points
	WasFiltered       bool
	Error             error
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

// ProcessBatch processes a batch of messages, applying downsampling to time-series data
// Implements at-least-once delivery semantics: only ACK after all processing is complete
func (p *DownsamplerProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var outBatch service.MessageBatch
	var processingErrors []error

	// Process all messages in the batch, collecting any errors
	for i, msg := range batch {
		result := p.processMessage(ctx, msg, i)

		if result.Error != nil {
			processingErrors = append(processingErrors, fmt.Errorf("message %d: %w", i, result.Error))
			p.messagesErrored.Incr(1)
			// Fail open - pass message through on error to avoid data loss
			outBatch = append(outBatch, result.OriginalMessage)
			continue
		}

		// Add all processed messages to output batch
		if len(result.ProcessedMessages) > 0 {
			outBatch = append(outBatch, result.ProcessedMessages...)
		}
		// Note: if len(result.ProcessedMessages) == 0, the message was filtered (not an error)
	}

	// If there were any critical errors, you might want to return an error
	// This would prevent ACKing the batch, ensuring at-least-once delivery
	if len(processingErrors) > 0 {
		p.logger.Errorf("Batch processing had %d errors, but continuing with fail-open policy", len(processingErrors))
		// In a strict at-least-once setup, you might return the first error here:
		// return nil, processingErrors[0]
	}

	// Only return success (allowing ACK) if we've successfully processed the entire batch
	if len(outBatch) == 0 {
		return nil, nil
	}
	return []service.MessageBatch{outBatch}, nil
}

// processMessage processes a single message and returns the result
func (p *DownsamplerProcessor) processMessage(ctx context.Context, msg *service.Message, index int) MessageProcessingResult {
	result := MessageProcessingResult{
		OriginalMessage: msg,
	}

	// Process UMH-core time-series messages
	if !p.isTimeSeriesMessage(msg) {
		result.ProcessedMessages = []*service.Message{msg}
		p.messagesPassed.Incr(1)
		return result
	}

	// Parse structured payload
	data, err := msg.AsStructured()
	if err != nil {
		result.Error = fmt.Errorf("failed to parse structured data: %w", err)
		return result
	}

	dataMap, ok := data.(map[string]interface{})
	if !ok {
		result.Error = fmt.Errorf("payload is not a JSON object, got %T", data)
		return result
	}

	// Extract timestamp
	timestamp, err := p.extractTimestamp(dataMap)
	if err != nil {
		result.Error = fmt.Errorf("failed to extract timestamp: %w", err)
		return result
	}

	// Process as UMH-core format (single "value" field)
	processedMsg, err := p.processUMHCoreMessage(msg, dataMap, timestamp)
	if err != nil {
		result.Error = fmt.Errorf("failed to process UMH-core message: %w", err)
		return result
	}

	if processedMsg != nil {
		result.ProcessedMessages = []*service.Message{processedMsg}
	} else {
		result.WasFiltered = true
		// Empty ProcessedMessages indicates the message was filtered (not an error)
	}

	return result
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

// processUMHCoreMessage processes a single UMH-core format message
func (p *DownsamplerProcessor) processUMHCoreMessage(msg *service.Message, dataMap map[string]interface{}, timestamp time.Time) (*service.Message, error) {
	// Extract series ID from umh_topic
	topicInterface, exists := dataMap["umh_topic"]
	if !exists {
		return nil, fmt.Errorf("missing required field: umh_topic")
	}

	seriesID, ok := topicInterface.(string)
	if !ok {
		return nil, fmt.Errorf("umh_topic must be a string, got %T", topicInterface)
	}

	// Extract value
	value, exists := dataMap["value"]
	if !exists {
		return nil, fmt.Errorf("missing required field: value")
	}

	// Get or create series state
	state, err := p.getOrCreateSeriesState(seriesID)
	if err != nil {
		return nil, fmt.Errorf("failed to get series state: %w", err)
	}

	state.mutex.Lock()
	defer state.mutex.Unlock()

	// Handle late arrivals using ProcessorWrapper's built-in logic
	// ProcessorWrapper handles late arrival internally based on PassThrough setting
	// If PassThrough=false: out-of-order data is dropped automatically
	// If PassThrough=true: out-of-order data is passed through unchanged

	// Use ProcessorWrapper to process the value and get emitted points
	emittedPoints, err := state.processor.Ingest(value, timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to process message: %w", err)
	}

	// If no points were emitted, the message was filtered out
	if len(emittedPoints) == 0 {
		p.messagesFiltered.Incr(1)
		p.logger.Debug(fmt.Sprintf("Message filtered out for series %s: value=%v", seriesID, value))
		return nil, nil // Return nil to indicate message should be dropped
	}

	// Points were emitted - we need to handle them
	// For now, we'll emit the first point (most common case)
	// TODO: Handle multiple points if algorithms start emitting multiple points
	if len(emittedPoints) > 1 {
		p.logger.Warnf("Algorithm emitted %d points, only using first one. This may indicate SDT final emission.", len(emittedPoints))
	}

	emittedPoint := emittedPoints[0]

	p.messagesProcessed.Incr(1)

	// Update internal tracking state
	p.updateProcessedTime(state, emittedPoint.Timestamp)

	// Create output message with emitted point data
	outputData := make(map[string]interface{})
	for key, val := range dataMap {
		outputData[key] = val
	}

	// Update the value and timestamp with the emitted point
	outputData["value"] = emittedPoint.Value
	outputData["timestamp"] = emittedPoint.Timestamp.UnixNano()

	// Add algorithm metadata
	if outputData["metadata"] == nil {
		outputData["metadata"] = make(map[string]interface{})
	}

	metadata, ok := outputData["metadata"].(map[string]interface{})
	if !ok {
		metadata = make(map[string]interface{})
		outputData["metadata"] = metadata
	}

	metadata["downsampling_algorithm"] = state.processor.Config()

	// Create new message with processed data
	outputMsg := msg.Copy()
	outputMsg.SetStructured(outputData)

	return outputMsg, nil
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

// shouldKeepMessage is deprecated - use ProcessorWrapper.Ingest directly in processUMHCoreMessage
// This maintains backward compatibility for any remaining references
func (p *DownsamplerProcessor) shouldKeepMessage(state *SeriesState, value interface{}, timestamp time.Time) (bool, error) {
	emittedPoints, err := state.processor.Ingest(value, timestamp)
	return len(emittedPoints) > 0, err
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
	p.stateMutex.Lock()
	defer p.stateMutex.Unlock()

	// Flush any pending points from algorithms (important for SDT)
	for seriesID, state := range p.seriesState {
		state.mutex.Lock()
		if pendingPoints, err := state.processor.Flush(); err != nil {
			p.logger.Errorf("Failed to flush pending points for series %s: %v", seriesID, err)
		} else if len(pendingPoints) > 0 {
			p.logger.Infof("Flushed %d pending points for series %s on close", len(pendingPoints), seriesID)
			// Note: In a real deployment, you'd want to emit these points to a dead letter queue
			// or persist them for recovery, rather than just logging them
		}
		state.processor.Reset()
		state.mutex.Unlock()
	}

	p.seriesState = make(map[string]*SeriesState)
	return nil
}
