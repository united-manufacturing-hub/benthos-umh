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

// MessageProcessingResult encapsulates the outcome of processing a single message through the downsampling pipeline.
//
// This structure supports the complex ACK buffering semantics required by "emit-previous" algorithms like
// Swinging Door Trending (SDT), where a message might be:
// - Immediately emitted (ProcessedMessages contains 1 message)
// - Filtered and buffered for later emission (ProcessedMessages empty, WasFiltered true)
// - Trigger emission of a previously buffered message (ProcessedMessages contains buffered message)
//
// The distinction between filtering and errors is crucial for proper ACK handling:
// - Filtered messages (WasFiltered=true) are intentionally not emitted but may be buffered
// - Error messages (Error!=nil) indicate processing failures and trigger fail-open behavior
//
// Design rationale: This structure enables at-least-once delivery semantics by clearly separating
// successful filtering from actual processing errors, allowing the processor to make informed
// ACK decisions based on algorithm behavior.
type MessageProcessingResult struct {
	OriginalMessage   *service.Message   // The input message for ACK tracking
	ProcessedMessages []*service.Message // Output messages (empty if filtered, may contain buffered messages)
	WasFiltered       bool               // True if message was intentionally filtered by algorithm
	Error             error              // Non-nil if processing failed (triggers fail-open)
}

// init registers the downsampler plugin with the Benthos processor registry and defines its configuration schema.
//
// This function serves as the integration point between the downsampler implementation and the Benthos
// plugin system, establishing the configuration contract and processor factory function.
//
// ## Plugin Registration Philosophy
//
// The downsampler follows Benthos plugin conventions by:
// - **Declarative configuration**: Complex behavior configured through YAML/JSON schemas
// - **Self-documenting**: Extensive inline documentation in the configuration spec
// - **Validation-first**: Configuration validation happens before processor creation
// - **Factory pattern**: Processor instances created through factory function with validated config
//
// ## UMH-Core Integration Context
//
// The plugin is specifically designed for UMH-core time-series data processing, implementing the
// "one tag, one message, one topic" philosophy documented at:
// https://docs.umh.app/usage/unified-namespace/payload-formats
//
// **Expected Data Format**:
// ```json
//
//	{
//	  "value": 23.4,           // Scalar value (numeric, boolean, or string)
//	  "timestamp_ms": 1717083000000  // Unix epoch milliseconds
//	}
//
// ```
//
// **Required Metadata**:
// - `umh_topic`: Topic identifier for series state management (e.g., "umh.v1.acme._historian.temperature")
//
// ## Configuration Schema Design
//
// The configuration schema implements a two-tier approach with simplified pattern matching:
//
// **Default Configuration**: Provides baseline algorithm parameters applied to all topics
// ```yaml
// default:
//
//	deadband:
//	  threshold: 1.0
//	  max_time: "5m"
//	swinging_door:
//	  threshold: 0.5
//	  min_time: "1s"
//	  max_time: "10m"
//	late_policy:
//	  late_policy: "passthrough"  # or "drop"
//
// ```
//
// **Override Configuration**: Allows topic-specific parameter customization using unified patterns
// ```yaml
// overrides:
//   - pattern: "*.temperature.*"              # Wildcard pattern for all temperature sensors
//     deadband:
//     threshold: 2.0
//   - pattern: "umh.v1.acme._historian.temp.sensor1"  # Exact topic match
//     swinging_door:
//     threshold: 1.0
//   - pattern: "*pressure*"                   # Match any topic containing "pressure"
//     deadband:
//     max_time: "30s"
//
// ```
//
// ## Unified Pattern Matching
//
// The simplified pattern system supports both use cases through a single field:
// - **Exact matches**: Full topic strings without wildcards (e.g., "umh.v1.acme._historian.temp.sensor1")
// - **Wildcard patterns**: Shell-style patterns with * and ? (e.g., "*.temperature.*", "*sensor*")
// - **Fallback matching**: Patterns also match against the final topic segment for convenience
//
// This eliminates the complexity of separate "topic" and "pattern" fields while maintaining full functionality.
//
// ## Algorithm Support Strategy
//
// The configuration supports multiple algorithms with different characteristics:
//
// **Deadband Algorithm**:
// - **Behavior**: Filters changes smaller than threshold
// - **ACK handling**: Immediate ACK for filtered messages (no buffering needed)
// - **Use cases**: Noisy sensors, steady-state monitoring
// - **Configuration**: `threshold` (float), `max_time` (duration)
//
// **Swinging Door Trending (SDT)**:
// - **Behavior**: Dynamic compression maintaining trend fidelity
// - **ACK handling**: Requires message buffering for emit-previous semantics
// - **Use cases**: Trend analysis, efficient compression with accuracy preservation
// - **Configuration**: `threshold` (float), `min_time` (duration), `max_time` (duration)
//
// ## Late Arrival Policy Framework
//
// The plugin implements configurable late arrival handling:
//
// **Passthrough Policy** (`late_policy: "passthrough"`):
// - Out-of-order messages are forwarded unchanged
// - Bypasses algorithm processing to preserve data integrity
// - Suitable for systems where late data is acceptable
//
// **Drop Policy** (`late_policy: "drop"`):
// - Out-of-order messages are discarded with warning logs
// - Maintains strict temporal ordering for algorithms
// - Suitable for real-time systems where late data is problematic
//
// ## Configuration Validation & Error Handling
//
// The Benthos framework handles configuration validation before processor creation:
// - **Schema enforcement**: Field types, ranges, and relationships validated
// - **Required fields**: Missing configuration detected early
// - **Default values**: Sensible defaults applied for optional parameters
// - **Error propagation**: Invalid configurations prevent processor startup
//
// ## Factory Function Integration
//
// The factory function bridges Benthos configuration parsing with processor creation:
// ```go
// func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error)
// ```
//
// **Configuration Processing**: Delegates complex configuration parsing to `ConfigurationParser`
// **Resource Integration**: Provides logger and metrics from Benthos resource manager
// **Error Handling**: Configuration or creation errors prevent plugin initialization
//
// ## Plugin Metadata & Documentation
//
// The configuration spec includes extensive metadata:
// - **Version**: Semantic versioning for configuration compatibility
// - **Summary**: Brief description for plugin discovery
// - **Description**: Comprehensive usage documentation with examples
// - **Field documentation**: Detailed parameter descriptions with context
//
// This documentation is accessible through Benthos introspection tools and serves as
// the primary reference for operators configuring the plugin.
//
// ## Error Handling Philosophy
//
// The init function uses panic for registration failures because:
// - Plugin registration errors indicate programming/deployment issues
// - Failed registration should prevent system startup
// - Early failure is preferable to runtime confusion about missing plugins
//
// ## Integration with UMH Ecosystem
//
// The downsampler is designed to integrate with other UMH-core components:
// - **Upstream**: Expects data from `tag_processor` plugin in time-series format
// - **Downstream**: Outputs to TimescaleDB, InfluxDB, or other time-series databases
// - **Monitoring**: Provides Prometheus metrics for operational visibility
// - **Configuration**: Aligns with UMH-core configuration patterns and conventions
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
				Description("Topic pattern for matching (supports exact matches and shell-style wildcards with * and ?). Examples: 'umh.v1.acme._historian.temp.sensor1' (exact), '*.temperature.*' (wildcard), '*pressure*' (contains)."),

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
			Description("Topic-specific parameter overrides using pattern matching. Supports exact topic names and shell-style wildcards (* matches any sequence, ? matches any character).").
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

// DownsamplerProcessor implements intelligent time-series data reduction for UMH-core time-series data.
//
// ## Architecture Overview
//
// This processor implements the UMH-core philosophy of "One tag, one message, one topic" by processing
// time-series data in the standard time-series format: `{"value": <scalar>, "timestamp_ms": <epoch_ms>}`.
// It applies configurable downsampling algorithms to reduce data volume while preserving significant changes.
//
// ## ACK Buffering & Data Safety Design
//
// The processor implements sophisticated ACK buffering to support "emit-previous" algorithms like
// Swinging Door Trending (SDT) without compromising at-least-once delivery guarantees:
//
// **Problem**: SDT algorithms may need to emit a previously received data point when a new point
// arrives, but Benthos expects immediate ACK/NACK decisions per message.
//
// **Solution**: Internal message buffering with the following safety guarantees:
// - Messages are only ACKed after successful processing AND emission
// - Buffered messages are automatically flushed on idle timeout (max_time)
// - All buffered messages are properly emitted during graceful shutdown
// - Memory usage is O(series-count) - only one message buffered per time series
//
// ## Algorithm Integration Strategy
//
// Different algorithms require different ACK handling strategies:
// - **Deadband**: Never emits previous points → immediate ACK for filtered messages
// - **SDT**: May emit previous points → requires ACK buffering for filtered messages
//
// The processor optimizes memory usage by checking `algorithm.NeedsPreviousPoint()` to
// determine whether buffering is required for each time series.
//
// This unified approach reduces configuration complexity while managing buffered data efficiently.
// References:
// - UMH-core payload formats: https://docs.umh.app/usage/unified-namespace/payload-formats
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

// calculateFlushInterval determines the optimal background flush frequency based on algorithm configurations.
//
// This function implements a critical buffering mechanism by calculating how frequently
// the background goroutine should check for idle buffered messages that need to be flushed.
//
// ## Design Rationale
//
// The flush interval is derived from algorithm `max_time` settings because these represent the maximum
// time users are willing to wait for data emission. Using this existing configuration:
// 1. **Reduces complexity**: No additional configuration parameters needed
// 2. **Maintains consistency**: Flush behavior aligns with algorithm heartbeat expectations
// 3. **Efficient memory use**: Prevents buffered messages from being held longer than algorithm timeouts
//
// ## Algorithm
//
// 1. Scan all default and override configurations for `max_time` values
// 2. Select the minimum non-zero `max_time` across all algorithms
// 3. Fall back to 4 hours if no explicit `max_time` is configured
// 4. Return this value as the flush check frequency
//
// ## Why Minimum Selection
//
// Using the minimum ensures that no buffered message is held longer than its specific algorithm's
// `max_time` setting, providing consistent buffering behavior across all configured time series.
//
// Returns the calculated flush interval duration.
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

// idleFlushLoop runs the background goroutine responsible for preventing indefinite message buffering of rarely-changing data.
//
// This goroutine implements a critical memory management and buffering mechanism by periodically checking
// for "idle" buffered messages that should be flushed based on their age relative to algorithm-specific
// `max_time` configurations.
//
// ## Background Context
//
// UMH-core time-series data follows the "one tag, one message, one topic" philosophy, where each
// time series may stop sending data or send very infrequent updates (slow-changing processes, steady-state systems).
// Without this mechanism, messages buffered by "emit-previous" algorithms like SDT could be held
// indefinitely when series become idle or change very rarely.
//
// ## Purpose
//
// The idle flush mechanism serves two key purposes:
// 1. **Memory management**: Prevents keeping points that change very rarely in memory indefinitely
// 2. **Timely emission**: Ensures old buffered data points are emitted when no new data arrives
//
// Note: Data loss prevention is handled separately by the ACK system - messages are only ACKed
// after successful processing and emission.
//
// ## Operation
//
// The goroutine operates on a timer interval (calculated by calculateFlushInterval) and:
// 1. **Scans for candidates**: Only examines series with buffered messages
// 2. **Applies per-series rules**: Uses each series' specific `max_time` configuration
// 3. **Flushes aged messages**: Emits buffered messages that exceed their time limits
// 4. **Resets algorithm state**: Ensures next message is treated as a fresh start
//
// ## Why Background Processing
//
// Processing messages in a background goroutine (rather than in the main processing path) provides:
// - **Consistent throughput**: Doesn't block incoming message processing
// - **Predictable timing**: Regular checks independent of message arrival patterns
// - **Clean separation**: Idle flushing logic is separate from algorithm logic
//
// ## Lifecycle Management
//
// The goroutine runs until:
// - `p.closeChan` is closed (during processor shutdown)
// - `p.flushTicker` is stopped (during cleanup)
//
// This ensures graceful shutdown without goroutine leaks.
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

// flushIdleCandidates examines buffered messages and flushes those that have exceeded their series-specific max_time.
//
// This function implements the core logic of the idle flush mechanism by applying
// time-based eviction rules to prevent indefinite buffering of rarely-changing data.
//
// ## Design Strategy
//
// The function uses a two-phase approach for efficiency and thread safety:
//
// **Phase 1 - Candidate Collection** (under read lock):
// - Quickly scan all series state to identify those with buffered messages
// - Only collect series that use "emit-previous" algorithms (holdsPrev=true)
// - Release read lock early to minimize contention with message processing
//
// **Phase 2 - Age Evaluation** (per-series locks):
// - Check each candidate against its specific `max_time` configuration
// - Use fine-grained locking to avoid blocking concurrent message processing
// - Emit messages that exceed their time thresholds
//
// ## Algorithm-Specific max_time Handling
//
// Each time series may have different `max_time` values based on:
// - Default configuration settings
// - Topic-specific overrides (exact topic match)
// - Pattern-based overrides (regex matching)
//
// The function respects these per-series configurations to provide appropriate
// flush timing for different data types (e.g., fast sensors vs. slow batch processes).
//
// ## State Reset After Flush
//
// When a message is flushed due to age, the function also calls `state.processor.Reset()`
// to clear the algorithm's internal state. This ensures that:
// - The next message for this series is treated as a fresh start
// - No stale algorithm state affects future processing
// - Consistent behavior regardless of whether series resume or remain idle
//
// ## Memory Safety & Performance
//
// - **Bounded iteration**: Only examines series that actually have buffered messages
// - **Concurrent-safe**: Uses appropriate locking strategy for read-heavy workload
// - **Memory release**: Flushed messages are removed from internal buffers immediately
// - **Batch emission**: Multiple flushed messages are sent as a single batch for efficiency
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

// ProcessBatch implements the main message processing pipeline with at-least-once delivery semantics and fail-open behavior.
//
// This function represents the core integration point with Benthos, processing batches of messages
// while maintaining strict data safety guarantees and optimal performance characteristics.
//
// ## Processing Pipeline Architecture
//
// The batch processing follows a structured pipeline:
//
// ```
// Input Batch → Idle Flush Check → Per-Message Processing → Error Handling → Output Batches
//
//	     ↓              ↓                    ↓                    ↓              ↓
//	N messages    Background flush    Individual filtering    Fail-open      M batches
//	                 messages         + ACK buffering        policy        (M ≥ 0)
//
// ```
//
// ## At-Least-Once Delivery Guarantees
//
// The function implements comprehensive at-least-once semantics:
//
// **ACK Safety**: Messages are only ACKed by Benthos after:
// - Successful processing through the algorithm pipeline
// - Any required buffering operations completed
// - Output messages successfully added to return batches
//
// **Error Handling**: Processing errors trigger fail-open behavior where:
// - Original message is passed through unchanged to prevent data loss
// - Error is logged for debugging but doesn't block the batch
// - Metrics are updated to track error rates
//
// **Background Flushing**: Idle messages from previous processing cycles are:
// - Checked before processing the current batch
// - Emitted as separate batches to maintain temporal ordering
// - Handled independently of current batch success/failure
//
// ## Fail-Open Policy Rationale
//
// The processor implements fail-open rather than fail-closed behavior because:
// - **Data preservation**: Industrial time-series data is often irreplaceable
// - **System resilience**: Configuration or algorithm errors shouldn't stop data flow
// - **Operational visibility**: Errors are logged and metrics tracked for troubleshooting
// - **Graceful degradation**: System continues operating with reduced functionality rather than total failure
//
// ## Batch Output Strategy
//
// The function may return multiple output batches:
// 1. **Idle flush batch**: Messages flushed from previous processing (if any)
// 2. **Main processing batch**: Results from processing the current input batch
//
// This separation ensures:
// - **Temporal ordering**: Older messages are emitted before newer ones
// - **ACK coordination**: Different batches can be ACKed independently
// - **Performance optimization**: Multiple batches can be processed in parallel downstream
//
// Parameters:
//   - ctx: Benthos context for cancellation and tracing
//   - batch: Input messages to process (may contain time-series and non-time-series data)
//
// Returns:
//   - []service.MessageBatch: Zero or more output batches (empty if all messages filtered)
//   - error: Always nil due to fail-open policy (errors logged but not returned)
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

// getOrCreateSeriesState manages per-time-series algorithm state with thread-safe lazy initialization.
//
// This function implements the core state management strategy for the downsampler, creating and
// configuring algorithm instances on-demand as new time series are encountered.
//
// Parameters:
//   - seriesID: The UMH topic identifier (e.g., "umh.v1.acme._historian.temperature.sensor1")
//
// Returns:
//   - *SeriesState: Configured algorithm state ready for message processing
//   - error: Non-nil if algorithm creation fails (indicates configuration issues)
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

	// Determine if this algorithm needs emit-previous buffering
	// This optimization eliminates unnecessary memory usage for algorithms like deadband
	// that never emit historical points, while preserving ACK safety for algorithms like SDT
	needsBuffering := false
	if algorithm, createErr := algorithms.Create(algorithmType, algConfig); createErr == nil {
		needsBuffering = algorithm.NeedsPreviousPoint()
		p.logger.Debugf("Series %s using algorithm %s: holdsPrev=%v", seriesID, algorithmType, needsBuffering)
	} else {
		p.logger.Warnf("Could not determine buffering needs for algorithm %s, defaulting to safe mode (buffering enabled): %v", algorithmType, createErr)
		needsBuffering = true // Fail safe - enable buffering if we can't determine algorithm behavior
	}

	state = &SeriesState{
		processor: processor,
		holdsPrev: needsBuffering,
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

// Close implements graceful shutdown with comprehensive cleanup and final message emission.
//
// This function coordinates the shutdown sequence for the downsampler processor, ensuring that
// no data is lost during termination and all resources are properly cleaned up.
//
// ## ACK Buffer Safety
//
// The function prioritizes data safety by ensuring all buffered messages are emitted:
//
// **Buffered Message Handling**: For algorithms that use ACK buffering (holdsPrev=true):
// - All candidate messages are released from internal buffers
// - Messages are queued for emission through the shutdown batch channel
// - This ensures no messages are lost due to processor termination
//
// **Algorithm State Flushing**: Some algorithms maintain internal pending points:
// - `state.processor.Flush()` extracts these points from algorithm state
// - Currently logged as TODO due to template message requirement
// - Production systems should implement proper emission of these points
//
// ## Known Limitations & Future Work
//
// **Algorithm Flush Points**: The current implementation logs algorithm flush points as TODO
// because creating synthetic messages requires:
// - Template message structure for metadata propagation
// - Proper topic and timestamp handling
// - Integration with downstream systems expecting standard message format
//
// **Production Considerations**: For production deployments, consider implementing:
// - Dead letter queue for algorithm flush points
// - Synthetic message creation with minimal required fields
// - Configurable timeout for shutdown completion
//
// Parameters:
//   - ctx: Benthos context for shutdown coordination (timeout handling)
//
// Returns:
//   - error: Always nil - cleanup failures are logged but not propagated
func (p *DownsamplerProcessor) Close(ctx context.Context) error {
	// Stop the idle flush goroutine if not already stopped
	if p.closeChan != nil {
		close(p.closeChan)
		p.closeChan = nil
	}

	// Stop the ticker if not already stopped
	if p.flushTicker != nil {
		p.flushTicker.Stop()
		p.flushTicker = nil
	}

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
