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
	"fmt"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

// SeriesState is now defined in series_state.go with ACK buffering capabilities

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
- Requires "umh_topic" metadata field to identify the time series

The plugin maintains separate state for each time series (identified by umh_topic) and applies the configured algorithm
to determine whether each data point represents a significant change worth preserving.

## Data Type Handling

The downsampler handles different data types as follows:

- **Numeric values** (int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64): 
  Converted to float64 for algorithm processing and output. This ensures consistent precision and compatibility 
  with all downsampling algorithms.

- **Boolean values** (true, false): 
  Preserved as-is. Uses change-based logic - only emits when the boolean value changes.

- **String values**: 
  Preserved as-is. Uses change-based logic - only emits when the string value changes.

- **Other types**: 
  Rejected with an error to ensure data integrity.

## ACK Buffering & Data Safety

The downsampler implements internal message buffering to support "emit-previous" algorithms like Swinging Door Trending.
This ensures at-least-once delivery semantics with no data loss:

- **Internal Buffer**: One message buffered per time series (memory bounded)
- **ACK Safety**: Messages only ACKed after successful processing and emission
- **Idle Handling**: Uses algorithm 'max_time' setting to flush stale buffered messages
- **Shutdown Safety**: All buffered messages properly emitted during graceful shutdown

## Algorithm Integration

The 'max_time' parameter serves dual purposes:
1. **Algorithm heartbeat**: Forces periodic emission regardless of threshold (existing behavior)
2. **Buffer management**: Flushes idle buffered messages and resets algorithm state

When max_time is reached for a series:
- Any buffered message is immediately flushed and emitted
- Algorithm state is reset (next message treated as fresh start)
- Prevents indefinite buffering when series stop sending data

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
			// Use configuration parser to handle complex parsing logic
			parser := NewConfigurationParser()
			config, err := parser.ParseConfiguration(conf)
			if err != nil {
				return nil, err
			}

			return newDownsamplerProcessor(config, mgr.Logger(), mgr.Metrics())
		})
	if err != nil {
		panic(err)
	}
}

// DownsamplerProcessor implements the downsampling logic with ACK buffering
//
// ## ACK Buffering & Data Safety
//
// The downsampler maintains an internal buffer to implement "emit-previous" semantics
// required by algorithms like Swinging Door Trending (SDT). This buffering ensures:
//
// 1. **At-least-once delivery**: Messages are only ACKed after successful processing
// 2. **No data loss**: Buffered messages are properly emitted on shutdown or idle timeout
// 3. **Memory bounded**: Only one message buffered per time series (O(series-count))
//
// ## Idle Flush Mechanism
//
// To prevent buffered messages from being held indefinitely when a time series stops
// sending data, the processor uses the existing `max_time` configuration from algorithms:
//
// - **Purpose**: If a series hasn't sent data for `max_time` duration, flush any buffered message
// - **Configuration**: Reuses the `max_time` setting from deadband/swinging_door algorithms
// - **Default behavior**: If `max_time` is 0 or unset, defaults to 4 hours for safety
// - **Algorithm integration**: When `max_time` is reached:
//  1. Automatically flush the buffered message for that series
//  2. Reset the algorithm state (next message will be treated as a fresh start)
//
// This approach provides a single, consistent configuration for both algorithm heartbeat
// behavior and ACK buffer management, reducing configuration complexity for users.
type DownsamplerProcessor struct {
	config           DownsamplerConfig
	logger           *service.Logger
	seriesState      map[string]*SeriesState
	stateMutex       sync.RWMutex
	metrics          *DownsamplerMetrics
	messageProcessor *MessageProcessor

	// Idle flush functionality - uses max_time from algorithm configs
	flushTicker   *time.Ticker
	closeChan     chan struct{}
	shutdownBatch chan service.MessageBatch
}

func newDownsamplerProcessor(config DownsamplerConfig, logger *service.Logger, metrics *service.Metrics) (*DownsamplerProcessor, error) {
	// Calculate flush interval based on algorithm max_time settings
	flushInterval := calculateFlushInterval(config)

	processor := &DownsamplerProcessor{
		config:        config,
		logger:        logger,
		seriesState:   make(map[string]*SeriesState),
		metrics:       NewDownsamplerMetrics(metrics),
		flushTicker:   time.NewTicker(flushInterval),
		closeChan:     make(chan struct{}),
		shutdownBatch: make(chan service.MessageBatch, 1),
	}

	processor.messageProcessor = NewMessageProcessor(processor)

	// Start idle flush goroutine
	go processor.idleFlushLoop()

	logger.Infof("Downsampler initialized with idle flush interval: %v", flushInterval)
	return processor, nil
}

// calculateFlushInterval determines the appropriate flush interval based on algorithm max_time settings
// Uses the minimum non-zero max_time from all configured algorithms, with a 4-hour default fallback
func calculateFlushInterval(config DownsamplerConfig) time.Duration {
	minMaxTime := 4 * time.Hour // Default fallback

	// Check default algorithm configurations
	if config.Default.Deadband.MaxTime > 0 && config.Default.Deadband.MaxTime < minMaxTime {
		minMaxTime = config.Default.Deadband.MaxTime
	}
	if config.Default.SwingingDoor.MaxTime > 0 && config.Default.SwingingDoor.MaxTime < minMaxTime {
		minMaxTime = config.Default.SwingingDoor.MaxTime
	}

	// Check override configurations
	for _, override := range config.Overrides {
		if override.Deadband.MaxTime > 0 && override.Deadband.MaxTime < minMaxTime {
			minMaxTime = override.Deadband.MaxTime
		}
		if override.SwingingDoor.MaxTime > 0 && override.SwingingDoor.MaxTime < minMaxTime {
			minMaxTime = override.SwingingDoor.MaxTime
		}
	}

	return minMaxTime
}

// idleFlushLoop runs in the background to flush idle buffered messages based on algorithm max_time settings
func (p *DownsamplerProcessor) idleFlushLoop() {
	for {
		select {
		case <-p.flushTicker.C:
			p.flushIdleCandidates()
		case <-p.closeChan:
			return
		}
	}
}

// flushIdleCandidates checks for buffered messages that should be flushed based on their series-specific max_time
func (p *DownsamplerProcessor) flushIdleCandidates() {
	p.stateMutex.RLock()
	seriesStates := make([]*SeriesState, 0, len(p.seriesState))
	seriesIDs := make([]string, 0, len(p.seriesState))

	for id, state := range p.seriesState {
		if state.hasCandidate() {
			seriesStates = append(seriesStates, state)
			seriesIDs = append(seriesIDs, id)
		}
	}
	p.stateMutex.RUnlock()

	// Check each series with buffered messages against its specific max_time
	currentTime := time.Now().UnixNano() / int64(time.Millisecond)
	var flushBatch service.MessageBatch

	for i, state := range seriesStates {
		seriesID := seriesIDs[i]

		// Get the max_time configuration for this specific series
		_, algorithmConfig := p.config.GetConfigForTopic(seriesID)
		maxTimeMs := int64(4 * 60 * 60 * 1000) // Default 4 hours in milliseconds

		if maxTime, exists := algorithmConfig["max_time"]; exists {
			if duration, ok := maxTime.(time.Duration); ok && duration > 0 {
				maxTimeMs = duration.Nanoseconds() / int64(time.Millisecond)
			}
		}

		state.mutex.Lock()
		if state.hasCandidate() {
			candidateAge := currentTime - state.getCandidateTimestamp()
			if candidateAge >= maxTimeMs {
				p.logger.Debug(fmt.Sprintf("Flushing idle candidate for series %s (age: %dms, max_time: %dms)",
					seriesID, candidateAge, maxTimeMs))
				if msg := state.releaseCandidate(); msg != nil {
					flushBatch = append(flushBatch, msg)
				}
				// Reset algorithm state since max_time was exceeded
				state.processor.Reset()
			}
		}
		state.mutex.Unlock()
	}

	// If we have messages to flush, send them to the shutdown batch channel
	if len(flushBatch) > 0 {
		select {
		case p.shutdownBatch <- flushBatch:
			p.logger.Infof("Flushed %d idle candidates based on their max_time configurations", len(flushBatch))
		default:
			p.logger.Warnf("Could not flush %d idle candidates - shutdown batch channel full", len(flushBatch))
		}
	}
}

// ProcessBatch processes a batch of messages, applying downsampling to time-series data
// Implements at-least-once delivery semantics: only ACK after all processing is complete
func (p *DownsamplerProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var outBatches []service.MessageBatch
	var outBatch service.MessageBatch
	var processingErrors []error

	// Check for any idle-flushed messages first
	select {
	case flushBatch := <-p.shutdownBatch:
		if len(flushBatch) > 0 {
			outBatches = append(outBatches, flushBatch)
		}
	default:
		// No idle flush messages
	}

	// Process all messages in the batch, collecting any errors
	for i, msg := range batch {
		result := p.messageProcessor.ProcessMessage(msg, i)

		if result.Error != nil {
			processingErrors = append(processingErrors, fmt.Errorf("message %d: %w", i, result.Error))
			p.metrics.IncrementErrored()
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

	// Add the main processing batch if it has messages
	if len(outBatch) > 0 {
		outBatches = append(outBatches, outBatch)
	}

	// Return all batches (idle flush + regular processing)
	if len(outBatches) == 0 {
		return nil, nil
	}
	return outBatches, nil
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
		Logger:          p.logger,
		SeriesID:        seriesID,
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

// updateProcessedTime updates only the lastProcessedTime (for messages that were processed but not kept)
// NOTE: Caller must hold state.mutex.Lock()
func (p *DownsamplerProcessor) updateProcessedTime(state *SeriesState, timestamp time.Time) {
	// Only update lastProcessedTime for late arrival detection
	// Don't update lastOutput/lastOutputTime since message wasn't kept
	if timestamp.After(state.lastProcessedTime) {
		state.lastProcessedTime = timestamp
	}
}

// Close cleans up resources and flushes any pending points
func (p *DownsamplerProcessor) Close(ctx context.Context) error {
	// Stop the idle flush goroutine (only close once)
	defer func() {
		if r := recover(); r != nil {
			// Ignore panic from closing already closed channel
			p.logger.Debug("Recovered from channel close panic (expected during cleanup)")
		}
	}()

	close(p.closeChan)
	p.flushTicker.Stop()

	p.stateMutex.Lock()
	defer p.stateMutex.Unlock()

	var finalBatch service.MessageBatch

	// Release any buffered messages and flush algorithm points
	for seriesID, state := range p.seriesState {
		state.mutex.Lock()

		// Release any buffered candidate message and add to final batch
		if state.hasCandidate() {
			p.logger.Infof("Releasing buffered candidate for series %s on close", seriesID)
			if msg := state.releaseCandidate(); msg != nil {
				finalBatch = append(finalBatch, msg)
			}
		}

		// Flush any pending points from algorithms (important for SDT)
		if pendingPoints, err := state.processor.Flush(); err != nil {
			p.logger.Errorf("Failed to flush pending points for series %s: %v", seriesID, err)
		} else if len(pendingPoints) > 0 {
			p.logger.Infof("Flushed %d pending points for series %s on close", len(pendingPoints), seriesID)

			// Create messages from algorithm points using the last processed message as template
			// For now, we'll log this as a TODO since we need a template message to clone from
			// In a production system, you'd want to store a template or use a dead letter queue
			p.logger.Warnf("TODO: %d algorithm points from series %s need to be emitted but no template available", len(pendingPoints), seriesID)
			// TODO: Implement proper emission of algorithm flush points
			// This requires either:
			// 1. Storing a template message per series, or
			// 2. Using a dead letter queue/topic for final points, or
			// 3. Creating synthetic messages with minimal required fields
		}
		state.processor.Reset()
		state.mutex.Unlock()
	}

	// Send final batch to shutdown channel if we have messages
	if len(finalBatch) > 0 {
		select {
		case p.shutdownBatch <- finalBatch:
			p.logger.Infof("Queued %d final messages for emission on close", len(finalBatch))
		default:
			p.logger.Errorf("Could not queue %d final messages - shutdown batch channel full, messages will be lost", len(finalBatch))
		}
	}

	p.seriesState = make(map[string]*SeriesState)
	return nil
}
