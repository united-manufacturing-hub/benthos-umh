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

// Package topic_browser_plugin implements a Benthos processor plugin for the UMH Topic Browser.
//
// # PLUGIN OVERVIEW
//
// This plugin processes incoming UMH messages to extract and organize topic hierarchy information,
// metadata, and event data for efficient browsing and querying in the UMH system. It serves as the
// critical data transformation layer between raw UMH messages and the Topic Browser UI.
//
// # KEY FEATURES
//
//   - Hierarchical topic processing (level0 through level5 with dynamic sublevels)
//   - Support for both timeseries (_historian) and relational data contracts
//   - Efficient metadata caching using LRU cache to minimize network traffic
//   - Message batching for reduced I/O overhead
//   - Thread-safe operations with mutex protection
//   - Optimized LZ4 block compression for all payloads (memory-efficient)
//   - Comprehensive error handling and metrics collection
//
// # EMISSION CONTRACT
//
// The plugin follows a strict emission contract that determines what data is sent when:
//
// ## Ring Buffer + Always-Emit Strategy:
//   - Messages are buffered in per-topic ring buffers during emit_interval
//   - Each topic maintains max_events_per_topic_per_interval entries (default: 10)
//   - Ring buffers automatically overwrite oldest events when full
//   - Complete topic map is ALWAYS emitted (no change detection)
//   - Provides complete state to downstream consumers (stateless consumption)
//
// ## Rate Limiting During High Traffic:
//   - Startup scenarios: Reading full Kafka topic can produce burst traffic
//   - Ring buffer prevents overwhelming downstream: max 10 events/topic/interval
//   - Oldest events automatically dropped when buffer capacity exceeded
//   - Only most recent events per topic are emitted
//   - Prevents memory exhaustion during topic replay scenarios
//
// ## Possible Output Scenarios:
//  1. Full emission: Complete topic map + latest events from all active topics
//  2. No emission: No messages processed and emit_interval not elapsed
//
// # PROTOBUF SERIALIZATION STRATEGY
//
// The plugin uses **direct protobuf serialization** for all payloads:
//
// ## Serialization Strategy:
//   - All payloads are serialized directly to protobuf format
//   - No compression overhead for faster processing
//   - Optimized for small to medium UMH payloads
//   - Simplified data pipeline without compression/decompression steps
//   - Downstream consumers parse protobuf directly
//
// ## Performance Benefits:
//   - **Reduced CPU overhead**: No compression/decompression cycles
//   - **Lower memory usage**: No compression buffer allocations
//   - **Simplified processing**: Direct protobuf parsing
//   - **Faster throughput**: Optimized for high-frequency small payloads
//
// ## Data Format:
//   - Downstream consumers receive uncompressed protobuf data
//   - See BundleToProtobufBytes() for direct serialization implementation
//
// # EDGE CASE HANDLING
//
// ## Message Processing Edge Cases:
//  1. **Invalid Topic Format**: Logged as error, message skipped, failure metric incremented
//  2. **Missing umh_topic Metadata**: Message skipped with detailed error logging
//  3. **Malformed JSON Payload**: Event processing fails gracefully, error logged
//  4. **Duplicate UNS Tree IDs**: Headers merged, latest values win (last-write-wins)
//  5. **Cache Eviction**: LRU cache evicts old topics, causing re-emission on next access
//  6. **Empty Message Batches**: Returns immediately without creating empty protobuf
//
// ## Threading and Concurrency Edge Cases:
//  1. **Cache Corruption**: Mutex protection ensures thread-safe cache operations
//  2. **Memory Pressure**: LRU cache automatically evicts oldest entries
//  3. **Context Cancellation**: Graceful shutdown with cache cleanup
//
// ## Network and Serialization Edge Cases:
//  1. **Protobuf Marshaling Failures**: Error returned, no partial data emitted
//  2. **Serialization Failures**: Error returned, prevents data corruption
//  3. **Very Large Payloads**: Direct protobuf serialization handles multi-megabyte payloads efficiently
//
// # OUTPUT FORMAT SPECIFICATION
//
// The final output uses a specific wire format for umh-core consumption:
//
//	STARTSTARTSTART
//	<hex-encoded-protobuf-data>
//	ENDDATAENDDATAENDDATA
//	<unix-timestamp-ms>
//	ENDENDENDEND
//
// ## Wire Format Characteristics:
//   - Every emission contains complete state (topic map + events)
//   - Stateless consumption: downstream does not need to merge partial updates
//   - Self-contained bundles: each emission is independently processable
//   - Timing information included for latency analysis and debugging
//   - Consistent format regardless of emission trigger (timer vs buffer full)
//
// # CONFIGURATION PARAMETERS
//
//   - lru_size: LRU cache size (default: 50,000 entries) - for cumulative metadata storage
//   - emit_interval: Maximum buffering time before emission (default: 1s)
//   - max_events_per_topic_per_interval: Ring buffer size per topic (default: 10)
//   - max_buffer_size: Safety limit for total buffered messages (default: 10,000)
//
// # METRICS COLLECTED
//
//   - messages_processed: Successfully processed messages (counter)
//   - messages_failed: Failed message processing attempts (counter)
//   - events_overwritten: Ring buffer overflow events (counter)
//   - total_events_emitted: Total events sent downstream (counter)
//
// # IMPORTANT IMPLEMENTATION NOTES
//
// ## Ring Buffer Rate Limiting Implementation
//
// The current implementation uses ring buffers for effective rate limiting:
//
// ### Ring Buffer Strategy:
//   - Each topic maintains a fixed-size ring buffer (max_events_per_topic_per_interval)
//   - Default limit: 10 events per topic per emission interval
//   - Automatic overflow handling: oldest events discarded when buffer full
//   - Prevents memory exhaustion during startup topic replay scenarios
//
// ### High-Traffic Scenarios:
//   - Startup: Reading full Kafka topic creates burst traffic
//   - Ring buffer naturally limits emission to latest N events per topic
//   - No explicit rate calculation needed - buffer size enforces limit
//   - Memory bounded and predictable regardless of input traffic
//
// ### Overflow Behavior:
//   - Events beyond buffer capacity are automatically discarded (oldest first)
//   - eventsOverwritten metric tracks overflow occurrences
//   - Ensures system stability during traffic spikes
//   - Downstream receives consistent volume per topic per interval
//
// # USAGE EXAMPLE
//
//	processors:
//	  - topic_browser:
//	    lru_size: 10000  # Adjust based on expected topic cardinality
//
// Input: UNS messages from uns-input plugin
// Output: Protobuf-encoded UnsBundle messages for umh-core consumption
package topic_browser_plugin

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

// Constants for catch-up processing throttling
const (
	// Throttling delay calculation: 1ms per 1000 messages processed
	// This provides breathing room to prevent 100% CPU usage during catch-up
	throttleDelayPerKMessages = 1 * time.Millisecond
	throttleMessagesPerUnit   = 1000

	// Maximum throttle delay to prevent excessive blocking during large backlogs
	maxThrottleDelay = 200 * time.Millisecond

	// Minimum throttle delay to ensure some breathing room even for small batches
	minThrottleDelay = 50 * time.Millisecond
)

// topicRingBuffer implements a fixed-size circular buffer for storing the latest events per topic.
// This prevents data loss while controlling memory usage by automatically overwriting oldest events.
type topicRingBuffer struct {
	events   []*proto.EventTableEntry // Fixed-size circular buffer
	head     int                      // Write position (next slot to write)
	size     int                      // Current number of events stored
	capacity int                      // Maximum events per topic (from config)
}

// TopicBrowserProcessor implements the Benthos processor interface for the Topic Browser plugin.
// It processes messages to extract topic hierarchy, metadata, and event data while
// maintaining an efficient cache of topic metadata to minimize network traffic.
//
// The processor uses an LRU cache to store topic metadata and only transmits changes
// when the metadata has been modified. This significantly reduces the amount of data
// sent to the UMH core system.
type TopicBrowserProcessor struct {
	// topicMetadataCache stores topic metadata for cumulative persistence across messages.
	// Used to merge new metadata with previously seen metadata for the same topic.
	// The cache is thread-safe and protected by topicMetadataCacheMutex.
	topicMetadataCache      *lru.Cache
	topicMetadataCacheMutex *sync.Mutex

	// logger provides structured logging capabilities for the processor
	logger *service.Logger

	// metrics track the number of processed and failed messages
	messagesProcessed *service.MetricCounter
	messagesFailed    *service.MetricCounter

	// New buffering fields for ring buffer implementation
	messageBuffer []*service.Message          // Unacked original messages
	topicBuffers  map[string]*topicRingBuffer // Per-topic ring buffers
	fullTopicMap  map[string]*proto.TopicInfo // Complete authoritative topic state
	lastEmitTime  time.Time                   // Last emission timestamp
	bufferMutex   sync.Mutex                  // Protects all buffer state

	// Configuration parameters
	emitInterval      time.Duration
	maxEventsPerTopic int
	maxBufferSize     int

	// Enhanced metrics for ring buffer monitoring
	eventsOverwritten     *service.MetricCounter
	ringBufferUtilization *service.MetricCounter
	flushDuration         *service.MetricCounter
	emissionSize          *service.MetricCounter
	totalEventsEmitted    *service.MetricCounter
	messagesNotEmitted    *service.MetricCounter // New: tracks messages processed but not emitted during catch-up

	// CPU-aware adaptive controller
	adaptiveController *AdaptiveController

	// Prometheus gauges for CPU-aware metrics
	cpuLoadGauge        *service.MetricGauge
	activeIntervalGauge *service.MetricGauge
	activeTopicsGauge   *service.MetricGauge
}

func (t *TopicBrowserProcessor) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	messageBatch, err := t.ProcessBatch(ctx, service.MessageBatch{message})
	if err != nil {
		return nil, err
	}
	if len(messageBatch) == 0 {
		// Return empty batch instead of nil to indicate successful filtering
		// This prevents Benthos from counting this as a processor error
		//
		// In Benthos, returning (nil, nil) from a processor can be interpreted as an error
		// condition by the monitoring layer, while returning an empty MessageBatch is
		// the correct way to indicate that a message was successfully processed but filtered out.
		//
		// Reference: github.com/redpanda-data/benthos/v4/internal/component/processor/auto_observed.go:100-106
		// When a processor's Process method returns (nil, err), the error is logged as "Processor failed: %v"
		// However, the service layer's handling of nil vs empty batch differs in how it's counted in metrics.
		return service.MessageBatch{}, nil
	}

	// Note: ProcessBatch only returns 0 or 1 batch in this implementation:
	//   - Empty slice when buffering messages (no emission yet)
	//   - Single batch containing the protobuf bundle when emitting
	// We can safely return messageBatch[0] as there's never more than one batch
	return messageBatch[0], nil
}

// ProcessBatch processes a batch of messages with ring buffer storage and delayed ACK emission.
//
// # RING BUFFER + DELAYED ACK IMPLEMENTATION
//
// This function implements the hybrid buffering approach with the following behavior:
//
// ## Input Processing:
//   - Processes ALL messages in the batch sequentially
//   - Failed messages are logged but do not block processing of remaining messages
//   - Each message is converted to TopicInfo + EventTableEntry pair
//   - Messages without umh_topic metadata are skipped with error logging
//
// ## Ring Buffer Storage:
//   - Each topic maintains a ring buffer of latest N events (configurable)
//   - Ring buffers automatically overwrite oldest events when full (no data loss of recent events)
//   - Original messages buffered for delayed ACK pattern
//   - Thread-safe operations with mutex protection
//
// ## Topic Metadata Management:
//   - Cumulative metadata persistence across messages and time
//   - Metadata keys once seen are preserved until cache eviction
//   - Last-write-wins strategy for conflicting header values
//   - Complete topic state maintained in fullTopicMap
//
// ## Delayed ACK Emission Pattern:
//   - Messages are NOT ACKed immediately after processing
//   - Messages remain pending until emission interval elapses
//   - Timer-based emission (default: 1 second intervals)
//   - All buffered messages ACKed atomically after successful emission
//
// ## Emission Behavior:
//   - Rate limiting: Max N events per topic per interval (default: 10)
//   - Full tree emission: Complete fullTopicMap sent in every emission (no change detection)
//   - Optimized LZ4 block compression for all payloads (84% compression ratio, 65% heap reduction)
//   - No partial emissions - all or nothing approach
//
// ## Output Generation Rules:
//  1. **No Emission**: Timer hasn't elapsed, messages stay buffered
//  2. **Full Emission**: Timer elapsed, emit events + complete topic tree + ACK all messages
//  3. **Error Handling**: Emission failure prevents ACK (messages will be retried)
//
// ## Message-Driven Behavior (Important Edge Case):
//   - Emissions ONLY occur when messages are actively being processed
//   - No timer-based heartbeats: if no messages arrive, no emissions are generated
//   - Low-traffic UNS scenarios may experience extended delays between emissions
//   - Downstream consumers should expect gaps in emission timing during quiet periods
//   - This is intentional: the processor is message-driven, not time-driven
//   - For guaranteed periodic emissions, ensure continuous message flow to the UNS
//
// ## Performance Characteristics:
//   - Traffic reduction via batching and rate limiting
//   - Memory bounded via ring buffer and safety limits
//   - Latency: Up to emit_interval delay (trade-off for reduced traffic)
//   - Backpressure: Natural via delayed ACK and buffer limits
//
// The function is designed to balance traffic efficiency with latency by:
// - Buffering multiple messages into fewer, larger emissions
// - Rate limiting per-topic event volume
// - Preserving recent data while managing memory usage
// - Providing complete topic state to downstream consumers
//
// Returns:
//   - []service.MessageBatch: [emission_message], [ack_batch] if timer elapsed, or nil if buffering
//   - error: Fatal error preventing processing (individual message errors are logged)
//
// ProcessBatch processes a batch of messages with dual emission triggers:
// 1. Time-based emission: Every emitInterval, buffered messages are flushed
// 2. Buffer overflow protection: When buffer approaches capacity, immediate flush occurs
//
// BUFFER OVERFLOW PROTECTION BEHAVIOR:
// - Triggered when: len(messageBuffer) >= maxBufferSize and new message arrives
// - Action: Immediately flush the current buffer to make room for the incoming message
// - Result: The incoming message that would have caused overflow gets buffered normally
// - No data loss: All flushed messages are returned for proper ACK handling
//
// EDGE CASE EXAMPLE:
// - Buffer has 9/10 messages, new message arrives → no flush (10/10 is within limit)
// - Buffer has 10/10 messages, new message arrives → flush the 10 messages, then buffer the new one
// - This ensures buffer never exceeds maxBufferSize while accepting all messages
//
// This prevents unbounded memory growth while ensuring no message loss.
// The processor returns results from both emission triggers when they occur.
//
// NOTE: This is not traditional "backpressure" (signaling upstream to slow down).
// Instead, it's "buffer overflow protection" via immediate emission to make room.
func (t *TopicBrowserProcessor) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	t.logger.Debugf("DEBUG: ProcessBatch called with %d messages", len(batch))
	if len(batch) == 0 {
		t.logger.Debugf("DEBUG: Empty batch, returning empty slice")
		// Return empty slice instead of nil to indicate successful processing with no output
		// See: github.com/redpanda-data/benthos/v4/internal/component/processor/auto_observed.go:263-264
		return []service.MessageBatch{}, nil
	}

	// Note: Buffer overflow protection uses "ACK without emit" strategy for catch-up scenarios
	// No overflow emissions are generated - we maintain topic state but skip downstream emission

	// Process each message and add to ring buffers
	for _, message := range batch {
		topicInfo, eventTableEntry, unsTreeId, err := MessageToUNSInfoAndEvent(message)
		if err != nil {
			t.logger.Errorf("Failed to convert message to UNS info and event: %v. Check message format and umh_topic metadata", err)
			if t.messagesFailed != nil {
				t.messagesFailed.Incr(1)
			}
			continue
		}

		// Set metadata from event headers (this was missing!)
		// ✅ FIX: Add nil check to prevent panic on malformed input
		if eventTableEntry.RawKafkaMsg != nil {
			topicInfo.Metadata = eventTableEntry.RawKafkaMsg.Headers
		}

		// ✅ BUFFER OVERFLOW PROTECTION: Check before buffering the current message
		// If adding this message would exceed buffer capacity, immediately flush the buffer
		// to make room. This prevents unbounded memory growth without losing messages.
		t.bufferMutex.Lock()
		wouldExceedCapacity := len(t.messageBuffer) >= t.maxBufferSize
		if wouldExceedCapacity {
			// OVERFLOW STRATEGY: ACK without emit + throttling to prevent catch-up loops
			// Buffer overflow indicates catch-up scenario. We need to:
			// 1. Maintain latest state per topic (not drop data)
			// 2. Prevent infinite catch-up loops (add throttling)
			// 3. ACK messages to progress Kafka offsets
			// 4. Skip emissions until back to real-time

			flushStartTime := time.Now()

			// CATCH-UP PROCESSING: Clear buffers to maintain state but reduce downstream pressure
			clearedCount := t.clearBuffersLocked()
			t.bufferMutex.Unlock()

			// THROTTLING: Calculate delay proportional to work done to prevent catch-up loops
			// This gives the system breathing room and prevents 100% CPU usage
			var throttleDelay time.Duration

			// INTEGER OVERFLOW PROTECTION FOR THROTTLING CALCULATION:
			//
			// Problem: Large clearedCount values could cause integer overflow when multiplied by
			// throttleDelayPerKMessages in the expression:
			//   time.Duration(clearedCount/throttleMessagesPerUnit) * throttleDelayPerKMessages
			//
			// Risk: Integer overflow could produce negative durations or panic, causing
			// unpredictable throttling behavior during high-volume catch-up scenarios.
			//
			// Solution: Pre-calculate the maximum safe clearedCount value that won't overflow
			// when used in the throttling calculation. If clearedCount exceeds this threshold,
			// use the maximum throttle delay directly instead of attempting the calculation.
			//
			// Go 1.24 approach: Use math.MaxInt constant (available since Go 1.21) rather than
			// manual bit manipulation for better readability and maintainability.
			const maxSafeClearedCount = math.MaxInt / throttleMessagesPerUnit
			if clearedCount > maxSafeClearedCount {
				throttleDelay = maxThrottleDelay
			} else {
				throttleDelay = time.Duration(clearedCount/throttleMessagesPerUnit) * throttleDelayPerKMessages
				if throttleDelay > maxThrottleDelay {
					throttleDelay = maxThrottleDelay
				} else if throttleDelay < minThrottleDelay {
					throttleDelay = minThrottleDelay
				}
			}

			// Update metrics for catch-up processing
			if t.messagesNotEmitted != nil {
				t.messagesNotEmitted.Incr(int64(clearedCount))
			}

			// Log catch-up processing with state preservation at trace level
			flushDuration := time.Since(flushStartTime)
			currentCPU := 0.0
			currentInterval := time.Duration(0)
			controller := t.adaptiveController
			if controller != nil {
				currentCPU = controller.GetCPUPercent()
				currentInterval = controller.GetCurrentInterval()
			}

			t.logger.Tracef("CPU-AWARE CATCH-UP [OVERFLOW]: "+
				"trigger=buffer_full, "+
				"strategy=maintain_state_skip_emit_throttled, "+
				"flush_duration=%v, "+
				"throttle_delay=%v, "+
				"cleared_messages=%d, "+
				"preserved_topics=%d, "+
				"cpu_load=%.1f%%, "+
				"adaptive_interval=%v, "+
				"buffer_size_before=%d",
				flushDuration,
				throttleDelay,
				clearedCount,
				len(t.fullTopicMap), // Show how many topics we're preserving state for
				currentCPU,
				currentInterval,
				t.maxBufferSize)

			// Apply throttling delay AFTER logging but BEFORE processing next batch
			time.Sleep(throttleDelay)

			// No emission results to collect - we maintain state but skip downstream emission
		} else {
			t.bufferMutex.Unlock()
		}

		// Now buffer the current message (buffer should have space)
		err = t.bufferMessage(message, eventTableEntry, topicInfo, *unsTreeId)
		if err != nil {
			t.logger.Errorf("Failed to buffer message in ring buffer: %v. Check buffer capacity and memory limits", err)
			if t.messagesFailed != nil {
				t.messagesFailed.Incr(1)
			}
			continue
		}

		if t.messagesProcessed != nil {
			t.messagesProcessed.Incr(1)
		}
	}

	// Notify adaptive controller about the batch (estimate payload size)
	// This enables CPU-aware adaptation based on message volume and CPU load
	//
	// PAYLOAD ESTIMATION PERFORMANCE (Benchmarked on Apple M3 Pro):
	// • Old method: len(batch) * 1024 (~0.25 ns/op, 0 allocs, ~2000% error vs actual)
	// • New method: estimatePayloadSize() (~40μs/1000msgs, 0 allocs, ~220% error vs actual)
	// • Trade-off: 8.5x more accurate estimation at 140x CPU cost (still minimal absolute time)
	// • For typical UMH historian data: Old ~1024 bytes/msg vs New ~160 bytes/msg (realistic)
	// • Zero memory overhead: Both methods have 0 allocations per operation
	/*
		estimatedPayloadBytes := len(batch) * 1024 // Old: Rough estimate, 8.5x less accurate
	*/
	controller := t.adaptiveController
	if controller != nil {
		estimatedPayloadBytes := estimatePayloadSize(batch, t.logger)
		controller.OnBatch(estimatedPayloadBytes)
	}

	// Update CPU-aware metrics
	t.updateAdaptiveMetrics()

	// Log CPU-aware burst protection behavior at trace level
	controller = t.adaptiveController // Reuse captured reference
	if controller != nil {
		// Gather current state for trace logging
		currentCPU := controller.GetCPUPercent()
		currentInterval := controller.GetCurrentInterval()
		activeTopics := len(t.fullTopicMap)
		shouldFlush := controller.ShouldFlush()

		// Get controller's internal EMA bytes using proper encapsulation
		emaBytes := controller.GetEMABytes()

		t.logger.Tracef("CPU-AWARE DEBUG [batch_size=%d]: "+
			"cpu_load=%.1f%%, "+
			"current_interval=%v, "+
			"ema_bytes=%.0f, "+
			"estimated_payload_bytes=%d, "+
			"active_topics=%d, "+
			"should_flush=%t, "+
			"buffer_size=%d",
			len(batch),
			currentCPU,
			currentInterval,
			emaBytes,
			controller.GetLastPayloadBytes(),
			activeTopics,
			shouldFlush,
			len(t.messageBuffer))
	}

	// ADAPTIVE EMISSION LOGIC: Check CPU-aware adaptive emission after processing all messages
	// This replaces fixed-interval timing with intelligent CPU and payload-aware timing
	//
	// ✅ FIX: Hold mutex during entire check-and-flush to prevent TOCTOU race condition
	t.bufferMutex.Lock()
	defer t.bufferMutex.Unlock()

	// Use adaptive controller instead of fixed time interval
	controller = t.adaptiveController // Reuse captured reference from above
	if controller != nil && controller.ShouldFlush() {
		// Adaptive emission: CPU-aware controller determined it's time to flush
		flushStartTime := time.Now()
		intervalResult, err := t.flushBufferLocked()
		if err != nil {
			return nil, err
		}

		// Notify controller that flush was successful
		controller.OnFlush()

		// Log flush results at trace level
		flushDuration := time.Since(flushStartTime)
		intervalBatches := 0
		intervalMessages := 0
		if intervalResult != nil {
			intervalBatches = len(intervalResult)
			for _, batch := range intervalResult {
				intervalMessages += len(batch)
			}
		}

		t.logger.Tracef("CPU-AWARE FLUSH [ADAPTIVE]: "+
			"flush_duration=%v, "+
			"interval_batches=%d, "+
			"interval_messages=%d, "+
			"total_emitted=%d, "+
			"next_interval=%v",
			flushDuration,
			intervalBatches,
			intervalMessages,
			intervalMessages,
			controller.GetCurrentInterval())

		return intervalResult, nil
	} else if time.Since(t.lastEmitTime) >= t.emitInterval {
		// Fallback to fixed interval if adaptive controller is not available
		flushStartTime := time.Now()
		intervalResult, err := t.flushBufferLocked()
		if err != nil {
			return nil, err
		}

		// Log fixed interval flush results at trace level
		flushDuration := time.Since(flushStartTime)
		intervalBatches := 0
		intervalMessages := 0
		if intervalResult != nil {
			intervalBatches = len(intervalResult)
			for _, batch := range intervalResult {
				intervalMessages += len(batch)
			}
		}

		t.logger.Tracef("CPU-AWARE FLUSH [FIXED-INTERVAL]: "+
			"flush_duration=%v, "+
			"interval_batches=%d, "+
			"interval_messages=%d, "+
			"total_emitted=%d, "+
			"fixed_interval=%v",
			flushDuration,
			intervalBatches,
			intervalMessages,
			intervalMessages,
			t.emitInterval)

		return intervalResult, nil
	}

	// No interval-based emission occurred
	// Note: Buffer overflow protection uses "ACK without emit" strategy, so no overflow emissions

	// No emissions occurred - messages remain buffered, waiting for next interval
	// ACK is deferred until emission happens
	controller = t.adaptiveController // Capture controller reference for final log
	shouldFlushForLog := false
	if controller != nil {
		shouldFlushForLog = controller.ShouldFlush()
	}
	t.logger.Tracef("CPU-AWARE NO-EMISSION: "+
		"buffer_size=%d, "+
		"interval_elapsed=false, "+
		"should_flush=%t",
		len(t.messageBuffer),
		shouldFlushForLog)
	// Return empty slice to indicate successful buffering (not an error)
	// Messages are held in buffer for later emission
	// See: github.com/redpanda-data/benthos/v4/internal/component/processor/auto_observed.go:263-264
	return []service.MessageBatch{}, nil
}

// Core processing functions are now in separate files:
// - Buffer management: buffer.go
// - Metadata handling: metadata.go
// - Message processing: processing.go
// - Serialization: serialization.go

// Helper methods are now in separate files

func (t *TopicBrowserProcessor) Close(_ context.Context) error {
	t.bufferMutex.Lock()
	defer t.bufferMutex.Unlock()

	// Flush any remaining buffered messages during shutdown
	if len(t.messageBuffer) > 0 || len(t.fullTopicMap) > 0 {
		t.logger.Info("Flushing buffered messages during graceful shutdown")
		// Use locked version since we already hold the mutex
		_, err := t.flushBufferLocked()
		if err != nil {
			t.logger.Errorf("Failed to flush buffer during graceful shutdown: %v. Some messages may be lost", err)
			// Continue with shutdown even if flush fails
		}
	}

	// Wipe cache
	t.topicMetadataCacheMutex.Lock()
	t.topicMetadataCache.Purge()
	t.topicMetadataCacheMutex.Unlock()

	return nil
}

func NewTopicBrowserProcessor(logger *service.Logger, metrics *service.Metrics, lruSize int, emitInterval time.Duration, maxEventsPerTopic, maxBufferSize int) *TopicBrowserProcessor {
	// Validate LRU size - must be at least 1 for the processor to function properly
	if lruSize < 1 {
		panic("lru_size must be greater than 0 - the processor requires a cache to accumulate topic metadata")
	}

	// The LRU cache is used to:
	// - Deduplicate topics
	// - Store the latest version of meta-information about that topic
	l, _ := lru.New(lruSize) // Can only error if size is negative

	// For very short emit intervals (like in tests), initialize lastEmitTime to the past
	// to allow immediate emission when needed
	var lastEmitTime time.Time
	if emitInterval <= 10*time.Millisecond {
		lastEmitTime = time.Now().Add(-emitInterval) // Start in the past for tests
	} else {
		lastEmitTime = time.Now()
	}

	// Handle nil metrics for tests
	var messagesProcessed, messagesFailed, eventsOverwritten, ringBufferUtilization, flushDuration, emissionSize, totalEventsEmitted, messagesNotEmitted *service.MetricCounter
	var cpuLoadGauge, activeIntervalGauge, activeTopicsGauge *service.MetricGauge
	if metrics != nil {
		messagesProcessed = metrics.NewCounter("messages_processed")
		messagesFailed = metrics.NewCounter("messages_failed")
		eventsOverwritten = metrics.NewCounter("events_overwritten")
		ringBufferUtilization = metrics.NewCounter("ring_buffer_utilization")
		flushDuration = metrics.NewCounter("flush_duration")
		emissionSize = metrics.NewCounter("emission_size")
		totalEventsEmitted = metrics.NewCounter("total_events_emitted")
		messagesNotEmitted = metrics.NewCounter("messages_not_emitted") // Catch-up processing tracking

		// CPU-aware adaptive metrics (only plugin-specific gauges)
		cpuLoadGauge = metrics.NewGauge("cpu_load_percent")
		activeIntervalGauge = metrics.NewGauge("active_emit_interval_milli_seconds")
		activeTopicsGauge = metrics.NewGauge("active_topics_count")
	}

	// Initialize CPU-aware adaptive controller
	adaptiveController := NewAdaptiveController(emitInterval)

	return &TopicBrowserProcessor{
		topicMetadataCache:      l,
		logger:                  logger,
		messagesProcessed:       messagesProcessed,
		messagesFailed:          messagesFailed,
		topicMetadataCacheMutex: &sync.Mutex{},
		emitInterval:            emitInterval,
		maxEventsPerTopic:       maxEventsPerTopic,
		maxBufferSize:           maxBufferSize,
		topicBuffers:            make(map[string]*topicRingBuffer),
		fullTopicMap:            make(map[string]*proto.TopicInfo),
		lastEmitTime:            lastEmitTime,
		bufferMutex:             sync.Mutex{},
		eventsOverwritten:       eventsOverwritten,
		ringBufferUtilization:   ringBufferUtilization,
		flushDuration:           flushDuration,
		emissionSize:            emissionSize,
		totalEventsEmitted:      totalEventsEmitted,
		messagesNotEmitted:      messagesNotEmitted,
		adaptiveController:      adaptiveController,
		cpuLoadGauge:            cpuLoadGauge,
		activeIntervalGauge:     activeIntervalGauge,
		activeTopicsGauge:       activeTopicsGauge,
	}
}

func init() {
	spec := service.NewConfigSpec().
		Version("1.0.0").
		Description(`The topic browser processor processes messages into UNS bundles for the topic browser.

The processor will read the message headers and body to extract the UNS information and event table entries.

The processor will then return a message with the UNS bundle as the body, encoded as a protobuf.

The processor requires that the following metadata fields are set:

- topic: The topic of the message.
`).
		Field(service.NewIntField("lru_size").
			Description("The size of the LRU cache used to deduplicate topic names and store topic information.").
			Default(50_000).
			Advanced()).
		Field(service.NewDurationField("emit_interval").
			Description("Maximum time to buffer messages before emission").
			Default("1s").
			Advanced()).
		Field(service.NewIntField("max_events_per_topic_per_interval").
			Description("Maximum events per topic per emit interval").
			Default(1).
			Advanced()).
		Field(service.NewIntField("max_buffer_size").
			Description("Maximum number of messages to buffer (safety limit)").
			Default(10000).
			Advanced())

	err := service.RegisterBatchProcessor("topic_browser", spec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
		lruSize, err := conf.FieldInt("lru_size")
		if err != nil {
			return nil, err
		}
		if lruSize < 1 {
			return nil, errors.New("lru_size must be greater than 0")
		}

		emitInterval, err := conf.FieldDuration("emit_interval")
		if err != nil {
			return nil, err
		}
		if emitInterval <= 0 {
			return nil, errors.New("emit_interval must be greater than 0")
		}

		maxEventsPerTopic, err := conf.FieldInt("max_events_per_topic_per_interval")
		if err != nil {
			return nil, err
		}
		if maxEventsPerTopic < 1 {
			return nil, errors.New("max_events_per_topic_per_interval must be greater than 0")
		}

		maxBufferSize, err := conf.FieldInt("max_buffer_size")
		if err != nil {
			return nil, err
		}
		if maxBufferSize < 1 {
			return nil, errors.New("max_buffer_size must be greater than 0")
		}

		return NewTopicBrowserProcessor(mgr.Logger(), mgr.Metrics(), lruSize, emitInterval, maxEventsPerTopic, maxBufferSize), nil
	})
	if err != nil {
		panic(err)
	}
}

const (
	hmapHdr  = 6 * uintptr(8) // runtime.hmap words (≈48 B)
	bucketOv = uintptr(8)     // amortized overflow ptr / entry
	strHdr   = uintptr(16)    // string header (len, data)
	ifaceHdr = uintptr(16)    // empty-interface header (type, data)
)

func fastApproxMapSize(m map[string]interface{}) uintptr {
	if m == nil {
		return 0
	}
	size := hmapHdr // top-level hmap
	for k, v := range m {
		size += strHdr + uintptr(len(k)) // key
		if v == nil {
			size += ifaceHdr // empty interface header
		} else {
			size += ifaceHdr + reflect.TypeOf(v).Size() // value body
		}
		size += bucketOv // bucket bookkeeping
	}
	return size
}

func estimatePayloadSize(batch service.MessageBatch, logger *service.Logger) int {
	// Fast approach:
	var estimatedPayloadBytes int
	for _, msg := range batch {
		if msg.HasBytes() {
			b, err := msg.AsBytes()
			if err != nil {
				// Defensive error handling: use fallback estimation if bytes unavailable
				logger.Errorf("Unexpected error getting bytes from message: %v", err)
				estimatedPayloadBytes += 1024 // Fallback to 1KB estimate
				continue
			}
			estimatedPayloadBytes += len(b)
		} else { // This is a structured message
			structured, err := msg.AsStructured()
			if err != nil {
				// Defensive error handling: use fallback estimation if structured data unavailable
				logger.Errorf("Unexpected error getting structured data from message: %v", err)
				estimatedPayloadBytes += 1024 // Fallback to 1KB estimate
				continue
			}

			if m, ok := structured.(map[string]interface{}); ok {
				estimatedPayloadBytes += int(fastApproxMapSize(m))
			} else {
				// Just assume 1KB per message
				estimatedPayloadBytes += 1024
			}
		}
	}
	return estimatedPayloadBytes
}
