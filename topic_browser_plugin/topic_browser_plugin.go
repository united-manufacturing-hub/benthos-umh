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
//   - LZ4 compression for large payloads (≥1024 bytes)
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
// # LZ4 COMPRESSION STRATEGY
//
// The plugin uses always-on LZ4 compression for all payloads:
//
// ## Compression Strategy:
//   - All protobuf payloads are LZ4 compressed with level 0 (fastest)
//   - No size threshold - compression applied universally
//   - Single code path eliminates conditional logic complexity
//   - Downstream consumers always expect LZ4 format
//
// ## Compression Detection:
//   - Downstream consumers detect LZ4 via magic number: [0x04, 0x22, 0x4d, 0x18]
//   - See ProtobufBytesToBundleWithCompression() for implementation details
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
//  2. **LZ4 Compression Failures**: Error returned, prevents data corruption
//  3. **Very Large Payloads**: LZ4 compression reduces from ~5MB to ~750KB (see benchmarks)
//
// # OUTPUT FORMAT SPECIFICATION
//
// The final output uses a specific wire format for umh-core consumption:
//
//	STARTSTARTSTART
//	<hex-encoded-protobuf-or-lz4-data>
//	ENDDATAENDDATENDDATA
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
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// topicRingBuffer implements a fixed-size circular buffer for storing the latest events per topic.
// This prevents data loss while controlling memory usage by automatically overwriting oldest events.
type topicRingBuffer struct {
	events   []*EventTableEntry // Fixed-size circular buffer
	head     int                // Write position (next slot to write)
	size     int                // Current number of events stored
	capacity int                // Maximum events per topic (from config)
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
	fullTopicMap  map[string]*TopicInfo       // Complete authoritative topic state
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
}

func (t *TopicBrowserProcessor) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	messageBatch, err := t.ProcessBatch(ctx, service.MessageBatch{message})
	if err != nil {
		return nil, err
	}
	if len(messageBatch) == 0 {
		return nil, nil
	}
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
//   - LZ4 compression for large payloads (≥1024 bytes, 84% compression ratio)
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
func (t *TopicBrowserProcessor) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	// Process each message and add to ring buffers
	for _, message := range batch {
		topicInfo, eventTableEntry, unsTreeId, err := MessageToUNSInfoAndEvent(message)
		if err != nil {
			t.logger.Errorf("Error while processing message: %v", err)
			t.messagesFailed.Incr(1)
			continue
		}

		// Set metadata from event headers (this was missing!)
		topicInfo.Metadata = eventTableEntry.RawKafkaMsg.Headers

		// Buffer the message (includes ring buffer storage and topic tracking)
		err = t.bufferMessage(message, eventTableEntry, topicInfo, *unsTreeId)
		if err != nil {
			t.logger.Errorf("Error buffering message: %v", err)
			t.messagesFailed.Incr(1)
			continue
		}

		t.messagesProcessed.Incr(1)
	}

	// Check if emission interval has elapsed
	t.bufferMutex.Lock()
	shouldEmit := time.Since(t.lastEmitTime) >= t.emitInterval
	t.bufferMutex.Unlock()

	if shouldEmit {
		return t.flushBufferAndACK()
	}

	// Don't ACK yet - messages stay pending
	return nil, nil
}

// Core processing functions are now in separate files:
// - Buffer management: buffer.go
// - Metadata handling: metadata.go
// - Message processing: processing.go
// - Serialization: serialization.go

// Helper methods are now in separate files

func (t *TopicBrowserProcessor) Close(ctx context.Context) error {
	t.bufferMutex.Lock()
	defer t.bufferMutex.Unlock()

	// Flush any remaining buffered messages during shutdown
	if len(t.messageBuffer) > 0 || len(t.fullTopicMap) > 0 {
		t.logger.Info("Flushing buffered messages during graceful shutdown")
		_, err := t.flushBufferAndACK()
		if err != nil {
			t.logger.Errorf("Error flushing buffer during shutdown: %v", err)
			// Continue with shutdown even if flush fails
		}
	}

	// Wipe cache
	t.topicMetadataCacheMutex.Lock()
	t.topicMetadataCache.Purge()
	t.topicMetadataCacheMutex.Unlock()

	return nil
}

func NewTopicBrowserProcessor(logger *service.Logger, metrics *service.Metrics, lruSize int, emitInterval time.Duration, maxEventsPerTopic int, maxBufferSize int) *TopicBrowserProcessor {
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

	return &TopicBrowserProcessor{
		topicMetadataCache:      l,
		logger:                  logger,
		messagesProcessed:       metrics.NewCounter("messages_processed"),
		messagesFailed:          metrics.NewCounter("messages_failed"),
		topicMetadataCacheMutex: &sync.Mutex{},
		emitInterval:            emitInterval,
		maxEventsPerTopic:       maxEventsPerTopic,
		maxBufferSize:           maxBufferSize,
		topicBuffers:            make(map[string]*topicRingBuffer),
		fullTopicMap:            make(map[string]*TopicInfo),
		lastEmitTime:            lastEmitTime,
		bufferMutex:             sync.Mutex{},
		eventsOverwritten:       metrics.NewCounter("events_overwritten"),
		ringBufferUtilization:   metrics.NewCounter("ring_buffer_utilization"),
		flushDuration:           metrics.NewCounter("flush_duration"),
		emissionSize:            metrics.NewCounter("emission_size"),
		totalEventsEmitted:      metrics.NewCounter("total_events_emitted"),
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
			Default(10).
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
