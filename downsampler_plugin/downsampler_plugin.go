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

// -----------------------------------------------------------------------------
// Downsampler plugin – high‑level contract (applies to entire package)
// -----------------------------------------------------------------------------
//
// 1.  Data contract (input ↦ output)
//     • Accept only UMH‑core time‑series messages
//       { "value": <scalar>, "timestamp_ms": <unix‑ms> }      (one tag, one topic)
//     • Require metadata key `umh_topic`                      (series identity)
//     • Emit message unaltered except for     _downsampler.algorithm_ metadata.
//
// 2.  Purpose                                                  *WHY?*
//     • Compress noisy or low‑value data points (Deadband, SDT).
//     • Guarantee *at‑least‑once* delivery even when algorithms
//       delay or reorder emissions (ACK buffering, idle flush).
//
// 3.  Critical safety levers
//     • Dual use of `max_time`
//         – Algorithm heartbeat: forces a periodic output even if no
//           threshold exceeded.
//         – Idle flush timer : guarantees buffered points are freed and
//           ACKed if a series goes quiet.
//     • Late‑arrival policy (`passthrough` | `drop`) keeps historical
//       accuracy configurable per topic.
//
// 4.  Configuration tiers
//     • default   – global baseline for every topic.
//     • overrides – wildcard or exact topic patterns.
//
// 5.  Error policy
//     • *Fail‑open*: on any processing error, forward the raw message
//       unchanged and log – never lose data.
//
// -----------------------------------------------------------------------------

package downsampler_plugin

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

// SeriesState is now defined in series_state.go with ACK buffering capabilities

// -----------------------------------------------------------------------------
// MessageProcessingResult – single‑message outcome
// -----------------------------------------------------------------------------
//
// Mutually exclusive states:
//
//	a) Emitted immediately           ⇒ len(ProcessedMessages) == 1
//	b) Buffered (emit‑previous algo) ⇒ WasFiltered == true, Error == nil
//	c) Failed                        ⇒ Error != nil            (fail‑open)
//
// WHY:
//   - Distinguishing (b) from (c) is essential – only real errors should
//     block ACKs; filtered messages may be ACKed once their buffered
//     predecessor is emitted.
type MessageProcessingResult struct {
	OriginalMessage   *service.Message   // The input message for ACK tracking
	ProcessedMessages []*service.Message // Output messages (empty if filtered, may contain buffered messages)
	WasFiltered       bool               // True if message was intentionally filtered by algorithm
	Error             error              // Non-nil if processing failed (triggers fail-open)
}

// -----------------------------------------------------------------------------
// init – Benthos registration
// -----------------------------------------------------------------------------
//
// MECE breakdown:
//
//   - Registration          – binds "downsampler" to Benthos.
//   - Config schema         – declares default + overrides tier.
//   - Validation upfront    – Benthos rejects invalid configs before runtime.
//   - Panic on failure      – plugin absence is a deployment bug; crash fast.
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
- swinging_door: Dynamic compression maintaining trend fidelity using Swinging Door Trending (SDT)

## Pattern Matching Examples

Override configuration supports flexible pattern matching:

**Exact topic matching:**
overrides:
  - pattern: "umh.v1.factory._historian.line1.temperature.sensor1"
    deadband:
      threshold: 2.0

**Wildcard patterns:**
overrides:
  - pattern: "*.temperature.*"     # All temperature sensors
    deadband:
      threshold: 1.5
  - pattern: "*line1*"             # All sensors on line1
    deadband:
      max_time: "10m"
  - pattern: "*pressure*"          # Any topic containing "pressure"
    swinging_door:
      threshold: 0.8`).
		Field(service.NewObjectField("default",
			service.NewObjectField("deadband",
				service.NewFloatField("threshold").
					Description("Default threshold for deadband algorithm.").
					Default(0.0).
					Optional(),
				service.NewDurationField("max_time").
					Description("Default maximum time interval for deadband algorithm.").
					Optional(),
				service.NewDurationField("min_time").
					Description("Default minimum time between emissions for deadband algorithm.").
					Optional()).
				Description("Default deadband algorithm parameters.").
				Optional(),
			service.NewObjectField("swinging_door",
				service.NewFloatField("threshold").
					Description("Default compression deviation for swinging door algorithm.").
					Optional(),
				service.NewDurationField("max_time").
					Description("Default maximum time interval for swinging door algorithm.").
					Optional(),
				service.NewDurationField("min_time").
					Description("Default minimum time between emissions for swinging door algorithm.").
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
				Description("Topic pattern for matching (supports exact matches and shell-style wildcards with * and ?). Examples: 'umh.v1.acme._historian.temp.sensor1' (exact), '*.temperature.*' (wildcard), '*pressure*' (contains)."),

			service.NewObjectField("deadband",
				service.NewFloatField("threshold").
					Description("Override threshold for deadband algorithm.").
					Optional(),
				service.NewDurationField("max_time").
					Description("Override maximum time interval for deadband algorithm.").
					Optional(),
				service.NewDurationField("min_time").
					Description("Override minimum time between emissions for deadband algorithm.").
					Optional()).
				Description("Deadband algorithm parameter overrides.").
				Optional(),
			service.NewObjectField("swinging_door",
				service.NewFloatField("threshold").
					Description("Override compression deviation for swinging door algorithm.").
					Optional(),
				service.NewDurationField("max_time").
					Description("Override maximum time interval for swinging door algorithm.").
					Optional(),
				service.NewDurationField("min_time").
					Description("Override minimum time between emissions for swinging door algorithm.").
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
			Description("Topic-specific parameter overrides using pattern matching. Supports exact topic names and shell-style wildcards (* matches any sequence, ? matches any character).").
			Optional()).
		Field(service.NewBoolField("allow_meta_overrides").
			Description("Honour per-message ds_* metadata.").
			Default(true))

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

// -----------------------------------------------------------------------------
// DownsamplerProcessor – runtime structure
// -----------------------------------------------------------------------------
//
// A. Stateless members
//   - config, logger, metrics
//
// B. Per‑series state (guarded by RW mutex)
//   - SeriesState{ processor, candidate msg, timestamps, holdsPrev }
//
// C. Background idle‑flush
//   - flushTicker      – period = min(max_time) across all configs.
//   - closeChan        – graceful shutdown.
//   - shutdownBatch    – drained first in ProcessBatch.
//
// WHY idle flush?
//  1. Memory cap: only one buffered point per quiet series.
//  2. Liveness : ensures every message is eventually ACKed.
//  3. Consistency with algorithm heartbeat (same max_time source).
type DownsamplerProcessor struct {
	config           DownsamplerConfig       // Downsampling algorithm configuration
	logger           *service.Logger         // Benthos logger for debugging and monitoring
	seriesState      map[string]*SeriesState // Per-series algorithm state and buffering
	stateMutex       sync.RWMutex            // Protects seriesState map for concurrent access
	metrics          *DownsamplerMetrics     // Prometheus-compatible metrics collection
	messageProcessor *MessageProcessor       // Delegates individual message processing logic

	// Idle flush mechanism - prevents indefinite message buffering
	flushTicker   *time.Ticker              // Periodic timer for checking idle candidates
	closeChan     chan struct{}             // Signals background goroutine shutdown
	shutdownBatch chan service.MessageBatch // Channel for emitting idle-flushed messages

	closeOnce sync.Once // Ensures Close is only executed once
}

// newDownsamplerProcessor creates and initializes a new downsampler processor instance with comprehensive safety mechanisms.
//
// This constructor implements the complete initialization sequence for the downsampler, including
// algorithm configuration parsing, background goroutine setup, and idle flush mechanism activation.
//
// Parameters:
//   - config: Validated downsampler configuration with algorithm settings and overrides
//   - logger: Benthos logger instance for debugging and monitoring
//   - metrics: Benthos metrics registry for Prometheus integration
//
// Returns:
//   - *DownsamplerProcessor: Fully initialized processor ready for message processing
//   - error: Always nil in current implementation (placeholder for future validation)
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

// -----------------------------------------------------------------------------
// calculateFlushInterval – derives idle‑flush period
// -----------------------------------------------------------------------------
//
// Algorithm (exhaustive):
//  1. Scan default + overrides for the smallest non‑zero max_time.
//  2. Fallback to 4h if none set.
//
// WHY minimum?
//   - Guarantees no series waits longer than its own SLA before a flush.
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

// -----------------------------------------------------------------------------
// idleFlushLoop & flushIdleCandidates
// -----------------------------------------------------------------------------
//
// Responsibility matrix:
//
//	idleFlushLoop
//	  • Run flushIdleCandidates every   flushTicker.C
//	  • Stop on closeChan.
//
// WHY separated goroutine?
//
//	– Keeps main path latency constant.
//	– Allows deterministic flush cadence independent of traffic.
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

// flushIdleCandidates
//   - Gather candidates               (read lock)
//   - Per‑series age check            (fine‑grained lock)
//   - Emit & reset algorithm if age ≥ max_time
func (p *DownsamplerProcessor) flushIdleCandidates() {
	p.stateMutex.RLock()
	seriesStates := make([]*SeriesState, 0, len(p.seriesState))
	seriesIDs := make([]string, 0, len(p.seriesState))

	for id, state := range p.seriesState {
		// Only check series that can have candidates (emit-previous algorithms)
		// This optimization skips deadband and other non-buffering algorithms
		if state.holdsPrev && state.hasCandidate() {
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

		// Calculate flush interval with algorithm configuration resolution
		_, algorithmConfig, err := p.config.GetConfigForTopic(seriesID)
		if err != nil {
			p.logger.Error(fmt.Sprintf("Configuration error for topic %s: %v", seriesID, err))
			continue // Skip this series if configuration is invalid
		}

		state.mutex.Lock()
		if state.hasCandidate() {
			candidateAge := currentTime - state.getCandidateTimestamp()

			// Get max_time configuration with safe type assertion and default
			maxTimeMs := int64(4 * 60 * 60 * 1000) // Default 4 hours in milliseconds
			if maxTime, exists := algorithmConfig["max_time"]; exists {
				if maxTimeStr, ok := maxTime.(string); ok {
					if duration, err := time.ParseDuration(maxTimeStr); err == nil && duration > 0 {
						maxTimeMs = duration.Nanoseconds() / int64(time.Millisecond)
					}
				}
			}

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

// -----------------------------------------------------------------------------
// ProcessBatch – main pipeline
// -----------------------------------------------------------------------------
//
// Ordered steps (MECE):
//  1. Drain any idle‑flush batch (older ⇒ must be emitted first).
//  2. For each message
//     – route through per‑series algorithm
//     – branch: emit / buffer / fail‑open
//  3. Aggregate successful outputs.
//  4. Log but do NOT return errors (fail‑open keeps flow).
//
// WHY this structure?
//   - Temporal ordering: previous flush before current batch.
//   - ACK correctness  : Benthos sees zero‑error path; we record metrics.
//   - Performance      : minimal locking, single pass.
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

// getOrCreateSeriesState returns the state for a series, creating it if needed
func (p *DownsamplerProcessor) getOrCreateSeriesState(seriesID string, msg *service.Message) (*SeriesState, error) {
	p.stateMutex.RLock()
	state, exists := p.seriesState[seriesID]
	p.stateMutex.RUnlock()

	if exists {
		// If metadata overrides are enabled, check for changes
		if p.config.AllowMeta {
			if hints, err := extractMetaHints(msg); err != nil {
				p.logger.Warnf("Meta overrides ignored for series %s: %v", seriesID, err)
				p.metrics.IncrementMetaOverrideRejected()
			} else if hints != nil {
				// Get base algorithm configuration
				algorithmType, algorithmConfig, err := p.config.GetConfigForTopic(seriesID)
				if err != nil {
					p.logger.Error(fmt.Sprintf("Configuration error for topic %s: %v", seriesID, err))
					return nil, err
				}

				// Apply metadata overrides
				for k, v := range hints {
					algorithmConfig[k] = v
				}
				if algo, ok := hints["algorithm"]; ok {
					algorithmType = algo.(string)
				}

				// Check if we need to recreate the processor
				if algorithmType != state.processor.Name() {
					p.logger.Infof("Series %s switching algorithm to %s via metadata", seriesID, algorithmType)

					// Create new processor with merged config
					processorConfig := algorithms.ProcessorConfig{
						Algorithm:       algorithmType,
						AlgorithmConfig: algorithmConfig,
						PassThrough:     algorithmConfig["late_policy"] == "passthrough",
						Logger:          p.logger,
						SeriesID:        seriesID,
					}

					processor, err := algorithms.NewProcessorWrapper(processorConfig)
					if err != nil {
						return nil, fmt.Errorf("failed to create processor %s: %w", algorithmType, err)
					}

					// Update state with new processor
					state.processor = processor
					state.processor.Reset()
				}
				p.metrics.IncrementMetaOverrideApplied()
			}
		}
		return state, nil
	}

	// Create new state
	p.stateMutex.Lock()
	defer p.stateMutex.Unlock()

	// Check again in case another goroutine created it
	if state, exists := p.seriesState[seriesID]; exists {
		return state, nil
	}

	// Get algorithm configuration for this specific topic
	algorithmType, algorithmConfig, err := p.config.GetConfigForTopic(seriesID)
	if err != nil {
		p.logger.Error(fmt.Sprintf("Configuration error for topic %s: %v", seriesID, err))
		return nil, err
	}

	// Apply metadata overrides if enabled
	if p.config.AllowMeta {
		if hints, err := extractMetaHints(msg); err != nil {
			p.logger.Warnf("Meta overrides ignored for series %s: %v", seriesID, err)
			p.metrics.IncrementMetaOverrideRejected()
		} else if hints != nil {
			// Apply metadata overrides
			for k, v := range hints {
				algorithmConfig[k] = v
			}
			if algo, ok := hints["algorithm"]; ok {
				algorithmType = algo.(string)
			}
			p.metrics.IncrementMetaOverrideApplied()
		}
	}

	// Create processor with merged config
	processorConfig := algorithms.ProcessorConfig{
		Algorithm:       algorithmType,
		AlgorithmConfig: algorithmConfig,
		PassThrough:     algorithmConfig["late_policy"] == "passthrough",
		Logger:          p.logger,
		SeriesID:        seriesID,
	}

	processor, err := algorithms.NewProcessorWrapper(processorConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create processor %s: %w", algorithmType, err)
	}

	// Create and store new state
	state = &SeriesState{
		processor: processor,
		holdsPrev: processor.NeedsPreviousPoint(),
	}
	p.seriesState[seriesID] = state

	return state, nil
}

// updateProcessedTime updates the last processed timestamp for late arrival detection without affecting output tracking.
//
// This function serves a specific purpose in the late arrival detection system: it tracks the latest
// timestamp that has been processed (regardless of whether it was emitted) to enable proper
// out-of-order message detection in subsequent processing.
//
// ## Usage Context
//
// Called when a message is successfully processed by an algorithm but not necessarily emitted.
// This occurs when:
// - A message is filtered by the algorithm but its timestamp is newer than previous messages
// - The algorithm emits a previous point (not the current one) but we still need to track progression
//
// ## Design Rationale
//
// Separating "processed time" from "output time" tracking allows the processor to:
// 1. **Detect late arrivals accurately**: Compare incoming timestamps against actual processing progression
// 2. **Maintain algorithm state consistency**: Don't confuse algorithm timestamps with late arrival detection
// 3. **Support emit-previous algorithms**: Track processing progression independently of emission timing
//
// Parameters:
//   - state: The series state to update (caller must hold state.mutex.Lock())
//   - timestamp: The timestamp of the message that was processed
func (p *DownsamplerProcessor) updateProcessedTime(state *SeriesState, timestamp time.Time) {
	// Only update lastProcessedTime for late arrival detection
	// Don't update lastOutput/lastOutputTime since message wasn't kept
	if timestamp.After(state.lastProcessedTime) {
		state.lastProcessedTime = timestamp
	}
}

// -----------------------------------------------------------------------------
// Close – graceful shutdown
// -----------------------------------------------------------------------------
//
// Shutdown checklist (collectively exhaustive):
//  1. Stop ticker & goroutine.
//  2. Release any buffered candidate per series.
//  3. Try algorithm.Flush()
//  4. Queue remaining messages on shutdownBatch.
//  5. Clear maps, return nil.
//
// WHY still fail‑open here?
//   - Better to risk duplicate data than to drop final points.
func (p *DownsamplerProcessor) Close(ctx context.Context) error {
	// Use sync.Once to ensure closeChan is only closed once
	p.closeOnce.Do(func() {
		if p.closeChan != nil {
			close(p.closeChan)
			p.closeChan = nil
		}

		if p.flushTicker != nil {
			p.flushTicker.Stop()
			p.flushTicker = nil
		}
	})

	p.stateMutex.Lock()
	defer p.stateMutex.Unlock()

	var finalBatch service.MessageBatch

	// Release any buffered messages and flush algorithm points
	for seriesID, state := range p.seriesState {
		if state == nil {
			continue
		}

		state.mutex.Lock()

		// Release any buffered candidate message and add to final batch
		// Only check series that can have candidates (emit-previous algorithms)
		if state.holdsPrev && state.hasCandidate() {
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

			// Create synthetic UMH-core messages from algorithm flush points
			// These messages contain only the minimal required fields for UMH-core format
			for _, point := range pendingPoints {
				syntheticMsg := p.createSyntheticMessage(seriesID, point, state)
				if syntheticMsg != nil {
					finalBatch = append(finalBatch, syntheticMsg)
					p.logger.Debugf("Created synthetic message for series %s: value=%v, timestamp=%v",
						seriesID, point.Value, point.Timestamp)
				}
			}
		}
		state.processor.Reset()
		state.mutex.Unlock()
	}

	// Send final batch to shutdown channel if we have messages and channel exists
	if len(finalBatch) > 0 && p.shutdownBatch != nil {
		select {
		case p.shutdownBatch <- finalBatch:
			p.logger.Infof("Queued %d final messages for emission on close", len(finalBatch))
		default:
			p.logger.Errorf("Could not queue %d final messages - shutdown batch channel full, messages will be lost", len(finalBatch))
		}
	}

	// Clear the series state
	if p.seriesState != nil {
		p.seriesState = make(map[string]*SeriesState)
	}

	return nil
}

// createSyntheticMessage creates a minimal UMH-core message from an algorithm flush point.
//
// This function enables proper emission of algorithm flush points (like final SDT points)
// during shutdown without requiring a message template. It creates synthetic messages.
//
// ## Use Cases
//
// - **SDT Final Points**: Swinging Door Trending often has pending points during shutdown
// - **Algorithm State Cleanup**: Ensures all buffered algorithm points are emitted
// - **Data Integrity**: Prevents loss of compressed data points during processor termination
//
// Topic derived from seriesID ensures proper routing in UMH message infrastructure.
//
// Parameters:
//   - seriesID: The series identifier (typically the topic name)
//   - point: Algorithm flush point containing value and timestamp
//   - state: Series state for algorithm metadata
//
// Returns:
//   - *service.Message: Synthetic UMH-core message ready for emission
func (p *DownsamplerProcessor) createSyntheticMessage(seriesID string, point algorithms.GenericPoint, state *SeriesState) *service.Message {
	// Create minimal UMH-core data structure
	data := map[string]interface{}{
		"value":        point.Value,
		"timestamp_ms": point.Timestamp.UnixNano() / int64(time.Millisecond),
	}

	// Create new message with synthetic data
	msg := service.NewMessage(nil)
	msg.SetStructured(data)

	// Set topic metadata to enable proper routing
	// The seriesID is typically the topic name from umh_topic metadata
	msg.MetaSet("umh_topic", seriesID)

	// Add algorithm metadata for traceability (consistent with normal processing)
	algorithmName := state.processor.Name()
	msg.MetaSet("downsampled_by", algorithmName)

	algorithmConfig := state.processor.Config()
	msg.MetaSet("downsampling_config", algorithmConfig)

	// Mark as synthetic for debugging/monitoring
	msg.MetaSet("synthetic_flush_point", "true")

	return msg
}

// extractMetaHints parses metadata hints from a message
func extractMetaHints(msg *service.Message) (map[string]interface{}, error) {
	hints := make(map[string]interface{})

	// 1. algorithm
	if algo, ok := msg.MetaGet("ds_algorithm"); ok {
		if algo == "deadband" || algo == "swinging_door" {
			hints["algorithm"] = algo
		} else {
			return nil, fmt.Errorf("invalid ds_algorithm %q", algo)
		}
	}

	// 2. numeric threshold
	if v, ok := msg.MetaGet("ds_threshold"); ok {
		t, err := strconv.ParseFloat(v, 64)
		if err != nil || t < 0 {
			return nil, fmt.Errorf("bad ds_threshold: %v", v)
		}
		hints["threshold"] = t
	}

	// 3-4. durations (min/max)
	for _, key := range []string{"ds_min_time", "ds_max_time"} {
		if s, ok := msg.MetaGet(key); ok {
			d, err := time.ParseDuration(s)
			if err != nil || d < 0 {
				return nil, fmt.Errorf("bad %s: %v", key, s)
			}
			hints[key[3:]] = d // strip "ds_"
		}
	}

	// 5. late policy
	if pol, ok := msg.MetaGet("ds_late_policy"); ok {
		if pol == "passthrough" || pol == "drop" {
			hints["late_policy"] = pol
		} else {
			return nil, fmt.Errorf("invalid ds_late_policy: %v", pol)
		}
	}

	if len(hints) == 0 {
		return nil, nil // nothing set
	}
	return hints, nil
}
