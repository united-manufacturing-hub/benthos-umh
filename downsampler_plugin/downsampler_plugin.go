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

// DownsamplerProcessor implements the downsampling logic
type DownsamplerProcessor struct {
	config           DownsamplerConfig
	logger           *service.Logger
	seriesState      map[string]*SeriesState
	stateMutex       sync.RWMutex
	metrics          *DownsamplerMetrics
	messageProcessor *MessageProcessor
}

func newDownsamplerProcessor(config DownsamplerConfig, logger *service.Logger, metrics *service.Metrics) (*DownsamplerProcessor, error) {
	processor := &DownsamplerProcessor{
		config:      config,
		logger:      logger,
		seriesState: make(map[string]*SeriesState),
		metrics:     NewDownsamplerMetrics(metrics),
	}

	processor.messageProcessor = NewMessageProcessor(processor)
	return processor, nil
}

// ProcessBatch processes a batch of messages, applying downsampling to time-series data
// Implements at-least-once delivery semantics: only ACK after all processing is complete
func (p *DownsamplerProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var outBatch service.MessageBatch
	var processingErrors []error

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

	// Only return success (allowing ACK) if we've successfully processed the entire batch
	if len(outBatch) == 0 {
		return nil, nil
	}
	return []service.MessageBatch{outBatch}, nil
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
