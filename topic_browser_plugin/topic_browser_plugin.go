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
// ## UNS Map Emission Rules:
//   - uns_map is ONLY emitted when there is ≥1 new/changed topic since the previous frame
//   - Topic metadata changes are detected via xxHash comparison in the LRU cache
//   - When uns_map is emitted, it contains the ENTIRE current topic tree (not just deltas)
//   - This prevents complex merge logic downstream by always providing complete state
//
// ## Events Emission Rules:
//   - events contains ALL successfully processed messages from the current batch
//   - Each event represents one real incoming message that passed validation
//   - No synthetic events are created - processor never fabricates additional data
//   - Failed messages are logged and counted but do not appear in events array
//
// ## Possible Output Scenarios:
//  1. Both uns_map + events: New/changed topics with their corresponding events
//  2. Only events: No topic changes, but new event data for existing topics
//  3. Only uns_map: Topic metadata changed but no new events (rare edge case)
//  4. No output: No processable messages and no topic changes (empty batch returned)
//
// # LZ4 COMPRESSION STRATEGY
//
// The plugin uses conditional LZ4 compression with a 1024-byte threshold:
//
// ## When LZ4 Compression is Applied:
//   - Protobuf payload size ≥ 1024 bytes after marshaling
//   - Compression level 0 (fastest) for minimal latency impact
//   - Achieves ~84% compression ratio on typical UMH data (see benchmarks in proto.go)
//
// ## When LZ4 Compression is Skipped:
//   - Protobuf payload size < 1024 bytes
//   - LZ4 frame overhead would actually increase total size
//   - Small payloads are sent uncompressed to avoid unnecessary CPU overhead
//
// ## Compression Detection:
//   - Downstream consumers detect LZ4 via magic number: [0x04, 0x22, 0x4d, 0x18]
//   - Falls back to uncompressed protobuf decoding if magic number not present
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
// This format allows easy parsing and includes timing information for latency analysis.
//
// # CONFIGURATION PARAMETERS
//
//   - lru_size: LRU cache size (default: 50,000 entries)
//   - Larger values reduce re-emissions but consume more memory
//   - Smaller values save memory but may cause unnecessary traffic
//   - Recommended: 1000-100,000 depending on topic cardinality
//
// # METRICS COLLECTED
//
//   - messages_processed: Successfully processed messages (counter)
//   - messages_failed: Failed message processing attempts (counter)
//
// # PERFORMANCE CHARACTERISTICS
//
// Based on benchmark results in proto.go:
//   - Small bundles (<1KB): ~450ns processing time, no compression overhead
//   - Large bundles (~94KB): ~506µs processing time, 84.8% compression ratio
//   - Very large bundles (~5MB): ~14.8ms processing time, 84.6% compression ratio
//
// The plugin is optimized for high-throughput scenarios while maintaining low latency
// for small message batches typical in industrial IoT deployments.
//
// # IMPORTANT IMPLEMENTATION NOTES
//
// ## Current Implementation vs Specification Discrepancy
//
// The user specification mentions a "10 msg/s per-topic token-bucket" for throttling,
// but the CURRENT IMPLEMENTATION does not include this feature. The current behavior is:
//
// ### What the Current Implementation Actually Does:
//   - Processes ALL successfully parsed messages without throttling
//   - No rate limiting or token bucket implementation
//   - All events that pass validation appear in the events array
//   - No events are dropped due to rate limiting
//
// ### What the Specification Describes (Not Yet Implemented):
//   - Token bucket throttling at 10 messages/second per topic
//   - Excess messages dropped with ring_overflow markers
//   - FSM tracks dropped events via events_dropped_total{topic} metric
//
// ### Migration Path:
//
//	If token bucket throttling is implemented in the future, it would be added in the
//	processMessageBatch function, where messages could be filtered based on their
//	topic rate before being added to the events array. The current architecture
//	supports this addition without breaking changes.
//
// ## Actual Current Behavior:
//   - Every valid message produces an event in the output
//   - No artificial rate limiting or message dropping
//   - Performance is limited by downstream processing capability
//   - Cache-based topic deduplication is the primary traffic optimization
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
	"maps"
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
	// topicMetadataCache stores the most recently used topic metadata to prevent
	// re-sending unchanged topic information. The cache is thread-safe and protected
	// by topicMetadataCacheMutex.
	topicMetadataCache      *lru.Cache
	topicMetadataCacheMutex *sync.Mutex

	// logger provides structured logging capabilities for the processor
	logger *service.Logger

	// metrics track the number of processed and failed messages
	messagesProcessed *service.MetricCounter
	messagesFailed    *service.MetricCounter

	// New buffering fields for ring buffer implementation
	messageBuffer       []*service.Message          // Unacked original messages
	topicBuffers        map[string]*topicRingBuffer // Per-topic ring buffers
	pendingTopicChanges map[string]*TopicInfo       // Topics with metadata changes
	fullTopicMap        map[string]*TopicInfo       // Complete authoritative topic state
	lastEmitTime        time.Time                   // Last emission timestamp
	bufferMutex         sync.Mutex                  // Protects all buffer state

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

// ProcessBatch processes a batch of messages into UNS bundles for the topic browser.
//
// # EMISSION CONTRACT IMPLEMENTATION
//
// This function implements the core emission contract specified in the package documentation:
//
// ## Input Processing:
//   - Processes ALL messages in the batch sequentially
//   - Failed messages are logged but do not block processing of remaining messages
//   - Each message is converted to TopicInfo + EventTableEntry pair
//   - Messages without umh_topic metadata are skipped with error logging
//
// ## Topic Metadata Management:
//   - Groups TopicInfo entries by UNS Tree ID (xxHash of topic hierarchy)
//   - Merges headers from multiple messages for the same topic (last-write-wins)
//   - Compares merged headers against LRU cache to detect changes
//   - Only includes topics in uns_map when metadata has changed or topic is new
//
// ## Output Generation Rules:
//  1. **No Data Case**: Returns nil if no events processed AND no topic changes
//  2. **Events Only**: uns_map empty, events contains new message data
//  3. **Topics Only**: events empty, uns_map contains changed topic metadata
//  4. **Both**: uns_map AND events populated (most common case)
//
// ## Cache Behavior:
//   - Cache hits: Topic metadata compared, no emission if unchanged
//   - Cache misses: Topic treated as new, automatically included in uns_map
//   - Cache eviction: Evicted topics will be re-emitted on next access
//   - Thread safety: Mutex protects all cache operations
//
// ## Error Handling:
//   - Individual message failures do not abort batch processing
//   - Protobuf serialization failures return error (no partial emission)
//   - LZ4 compression failures return error (prevents data corruption)
//
// ## Performance Optimizations:
//   - Pre-allocated data structures to minimize allocations
//   - Batch processing reduces per-message overhead
//   - LZ4 compression for payloads ≥1024 bytes (84% compression ratio)
//   - Early return for empty batches avoids unnecessary work
//
// The function is designed to minimize traffic by:
// - Batching multiple messages into a single protobuf message
// - Using an LRU cache to avoid re-sending unchanged topic metadata
// - Only including topics in the output when their metadata has changed
//
// Returns:
//   - []service.MessageBatch: Single batch containing one protobuf message, or nil if no data
//   - error: Any fatal error that prevented processing (individual message errors are logged)
func (t *TopicBrowserProcessor) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	// Step 1: Initialize the UNS bundle
	unsBundle := t.initializeUnsBundle()

	// Step 2: Process all messages in the batch
	topicInfos, err := t.processMessageBatch(batch, unsBundle)
	if err != nil {
		return nil, err
	}

	// Step 3: Update topic metadata and cache
	t.updateTopicMetadata(topicInfos, unsBundle)

	// Step 4: Check if we have any data to return
	if !t.hasDataToReturn(unsBundle) {
		return nil, nil
	}

	// Step 5: Create and return the final message
	return t.createFinalMessage(unsBundle)
}

// Helper methods to break down the complexity

// initializeUnsBundle creates a new empty UNS bundle structure.
// The bundle is pre-allocated with initial capacity to avoid reallocations during processing.
// This structure will hold both the topic metadata map and the event entries.
func (t *TopicBrowserProcessor) initializeUnsBundle() *UnsBundle {
	return &UnsBundle{
		UnsMap: &TopicMap{
			Entries: make(map[string]*TopicInfo),
		},
		Events: &EventTable{
			Entries: make([]*EventTableEntry, 0, 1),
		},
	}
}

// processMessageBatch handles the conversion of raw messages into structured UNS data.
//
// # MESSAGE PROCESSING CONTRACT
//
// This function implements the individual message processing logic with the following guarantees:
//
// ## Processing Behavior:
//   - Processes each message independently (failure of one doesn't affect others)
//   - Extracts umh_topic metadata as the primary topic identifier
//   - Converts topic string to hierarchical TopicInfo structure
//   - Processes message payload into TimeSeriesPayload or RelationalPayload
//   - Generates UNS Tree ID via xxHash for efficient topic identification
//
// ## Error Handling Strategy:
//   - Individual message errors are logged with context but processing continues
//   - messages_failed metric is incremented for each failed message
//   - Failed messages do not appear in the final events array
//   - Non-fatal errors (e.g., malformed JSON) are handled gracefully
//
// ## Data Grouping Logic:
//   - Messages are grouped by UNS Tree ID for efficient metadata merging
//   - Multiple messages for the same topic have their headers merged
//   - Last-write-wins strategy for conflicting header values
//   - Each successful message produces exactly one EventTableEntry
//
// ## Header Management:
//   - Raw Kafka headers are preserved in EventTableEntry.RawKafkaMsg
//   - Headers are copied to TopicInfo.Metadata for search/filter functionality
//   - Bridge routing information extracted from "processed-by" header
//   - Kafka timestamp extracted for ProducedAtMs field
//
// For each message in the batch:
// - Extracts topic information and event data
// - Groups topic infos by UNS tree ID for efficient metadata merging
// - Updates metrics for monitoring and debugging
//
// Args:
//   - batch: Input message batch to process
//   - unsBundle: Output bundle to populate with events
//
// Returns:
//   - map[string][]*TopicInfo: Topics grouped by UNS Tree ID for cache comparison
//   - error: Fatal error that prevents further processing (nil for individual message failures)
func (t *TopicBrowserProcessor) processMessageBatch(
	batch service.MessageBatch,
	unsBundle *UnsBundle,
) (map[string][]*TopicInfo, error) {
	topicInfos := make(map[string][]*TopicInfo)

	for _, message := range batch {
		topicInfo, eventTableEntry, unsTreeId, err := MessageToUNSInfoAndEvent(message)
		if err != nil {
			t.logger.Errorf("Error while processing message: %v", err)
			t.messagesFailed.Incr(1)
			continue
		}

		// Add event to bundle
		unsBundle.Events.Entries = append(unsBundle.Events.Entries, eventTableEntry)
		topicInfo.Metadata = eventTableEntry.RawKafkaMsg.Headers

		// Group topic infos by UNS tree ID
		if _, ok := topicInfos[*unsTreeId]; !ok {
			topicInfos[*unsTreeId] = make([]*TopicInfo, 0, 1)
		}
		topicInfos[*unsTreeId] = append(topicInfos[*unsTreeId], topicInfo)
		t.messagesProcessed.Incr(1)
	}

	return topicInfos, nil
}

// updateTopicMetadata manages the topic metadata cache and bundle updates.
//
// # CACHE MANAGEMENT IMPLEMENTATION
//
// This function implements the core caching logic that enables the emission contract:
//
// ## Cache Strategy:
//   - LRU cache stores merged headers for each UNS Tree ID
//   - Cache key: UNS Tree ID (xxHash of topic hierarchy)
//   - Cache value: map[string]string of merged headers
//   - Cache comparison uses maps.Equal for deep equality checking
//
// ## Metadata Merging Logic:
//   - Multiple messages for same topic have headers merged
//   - Last header value wins for duplicate keys (last-write-wins)
//   - Merged headers become the canonical metadata for the topic
//   - Original per-message headers preserved in EventTableEntry.RawKafkaMsg
//
// ## Change Detection:
//   - Cache miss: Topic is new, automatically included in uns_map
//   - Cache hit: Deep comparison of merged headers vs cached headers
//   - Header changes: Topic included in uns_map, cache updated
//   - No changes: Topic excluded from uns_map (traffic optimization)
//
// ## Thread Safety:
//   - Mutex protects ALL cache operations (Get, Add, comparison logic)
//   - Lock held for entire function to ensure consistency
//   - Prevents race conditions in multi-threaded Benthos environment
//
// ## Memory Management:
//   - LRU cache automatically evicts oldest entries when full
//   - Evicted topics will be re-emitted on next access (acceptable trade-off)
//   - maps.Clone ensures cached data doesn't share memory with active processing
//
// This function is critical for performance optimization as it:
// - Uses a mutex to ensure thread-safe cache operations
// - Merges headers from multiple messages for the same topic
// - Only updates the bundle when topic metadata has changed
// - Prevents unnecessary traffic by caching unchanged topics
//
// Args:
//   - topicInfos: Topics grouped by UNS Tree ID from message batch
//   - unsBundle: Bundle to update with changed topics
func (t *TopicBrowserProcessor) updateTopicMetadata(
	topicInfos map[string][]*TopicInfo,
	unsBundle *UnsBundle,
) {
	t.topicMetadataCacheMutex.Lock()
	defer t.topicMetadataCacheMutex.Unlock()

	for unsTreeId, topics := range topicInfos {
		mergedHeaders := t.mergeTopicHeaders(unsTreeId, topics)

		if t.shouldReportTopic(unsTreeId, mergedHeaders) {
			t.updateTopicCacheAndBundle(unsTreeId, mergedHeaders, topics[0], unsBundle)
		}
	}
}

// mergeTopicHeaders combines headers from multiple topic infos into a single map.
// This implements cumulative metadata persistence where each metadata key retains
// its last known value even if it disappears from subsequent messages.
//
// # CUMULATIVE METADATA ALGORITHM
//
// This function implements metadata persistence per key:
//
// ## Persistence Strategy:
//   - Start with previously cached metadata (if exists)
//   - Layer on new metadata from current batch
//   - Each key keeps its most recent value
//   - Keys never disappear once seen (Topic Browser requirement)
//
// ## Use Case Examples:
//   - Message 1: {unit: "celsius"} → Store: {unit: "celsius"}
//   - Message 2: {serial: "ABC123"} → Store: {unit: "celsius", serial: "ABC123"}
//   - Message 3: {unit: "fahrenheit"} → Store: {unit: "fahrenheit", serial: "ABC123"}
//   - Message 4: {location: "factory"} → Store: {unit: "fahrenheit", serial: "ABC123", location: "factory"}
//
// This ensures the Topic Browser UI shows all known metadata about a topic,
// not just what was in the most recent message.
//
// Args:
//   - unsTreeId: UNS Tree ID for cache lookup of existing metadata
//   - topics: Topic infos from current batch to merge
//
// Returns:
//   - map[string]string: Cumulative metadata with all keys ever seen
func (t *TopicBrowserProcessor) mergeTopicHeaders(unsTreeId string, topics []*TopicInfo) map[string]string {
	// Start with previously cached metadata (if exists)
	mergedHeaders := make(map[string]string)
	if stored, ok := t.topicMetadataCache.Get(unsTreeId); ok {
		cachedHeaders := stored.(map[string]string)
		// Copy all previously known metadata
		for key, value := range cachedHeaders {
			mergedHeaders[key] = value
		}
	}

	// Layer on new metadata from current batch
	for _, topicInfo := range topics {
		for key, value := range topicInfo.Metadata {
			mergedHeaders[key] = value // Update with latest value
		}
	}

	return mergedHeaders
}

// shouldReportTopic determines if a topic's metadata has changed and needs to be reported.
//
// # CHANGE DETECTION ALGORITHM
//
// This function implements the core logic for the emission contract's topic filtering:
//
// ## Cache Lookup Behavior:
//   - Cache miss (topic not found): Always returns true (new topic)
//   - Cache hit (topic found): Performs deep comparison of headers
//
// ## Comparison Strategy:
//   - Uses maps.Equal for deep equality checking of header maps
//   - Compares current merged headers vs previously cached headers
//   - Returns true if ANY header key/value has changed
//   - Returns false if headers are identical (prevents re-emission)
//
// ## Performance Characteristics:
//   - O(n) comparison where n = number of headers per topic
//   - Typical topics have 5-20 headers, making this very fast
//   - maps.Equal is optimized for common cases (length mismatch, key differences)
//
// ## Edge Cases:
//   - Empty headers (both current and cached): Returns false (no change)
//   - New headers added: Returns true (metadata expansion)
//   - Headers removed: Returns true (metadata contraction)
//   - Header values changed: Returns true (metadata update)
//   - Cache eviction: Returns true on next access (topic treated as new)
//
// This is a key optimization that prevents unnecessary traffic by:
// - Checking if the topic exists in the cache
// - Comparing current headers with cached headers
// - Only returning true when the topic is new or has changed
//
// Args:
//   - unsTreeId: UNS Tree ID (xxHash) for cache lookup
//   - mergedHeaders: Current merged headers to compare against cache
//
// Returns:
//   - bool: true if topic should be included in uns_map, false if can be skipped
func (t *TopicBrowserProcessor) shouldReportTopic(unsTreeId string, mergedHeaders map[string]string) bool {
	stored, ok := t.topicMetadataCache.Get(unsTreeId)
	if !ok {
		return true
	}

	cachedHeaders := stored.(map[string]string)
	return !maps.Equal(cachedHeaders, mergedHeaders)
}

// updateTopicCacheAndBundle updates both the cache and the output bundle with new topic metadata.
// This function ensures that:
// - The cache is updated with the latest metadata
// - The bundle includes the updated topic information
// - The topic's metadata is properly set in the output
func (t *TopicBrowserProcessor) updateTopicCacheAndBundle(
	unsTreeId string,
	mergedHeaders map[string]string,
	topic *TopicInfo,
	unsBundle *UnsBundle,
) {
	t.topicMetadataCache.Add(unsTreeId, mergedHeaders)
	clone := maps.Clone(mergedHeaders)
	topic.Metadata = clone
	unsBundle.UnsMap.Entries[unsTreeId] = topic
}

// hasDataToReturn checks if there is any data to include in the output message.
//
// # OUTPUT DECISION ALGORITHM
//
// This function implements the final decision logic for whether to emit a message:
//
// ## Emission Criteria:
//   - Events present: At least one message was successfully processed
//   - Topics present: At least one topic metadata change was detected
//   - Both present: Most common case (new events + topic changes)
//   - Neither present: No output (returns nil to prevent empty protobuf)
//
// ## Performance Optimization:
//   - Prevents creation of empty protobuf messages
//   - Avoids unnecessary serialization and compression overhead
//   - Reduces network traffic by eliminating no-op messages
//   - Saves downstream processing cycles in umh-core
//
// ## Edge Case Handling:
//   - All messages failed processing: len(Events) == 0, returns false
//   - No topic changes but events exist: Returns true (events-only emission)
//   - Topic changes but no events: Returns true (topics-only emission, rare)
//   - Empty input batch: Returns false early (prevents unnecessary work)
//
// This prevents creating empty messages when:
// - No events were successfully processed
// - No topic metadata has changed
//
// Args:
//   - unsBundle: The bundle to check for data content
//
// Returns:
//   - bool: true if bundle contains data worthy of emission, false if empty
func (t *TopicBrowserProcessor) hasDataToReturn(unsBundle *UnsBundle) bool {
	return len(unsBundle.Events.Entries) > 0 || len(unsBundle.UnsMap.Entries) > 0
}

// createFinalMessage converts the UNS bundle into a protobuf-encoded message.
//
// # SERIALIZATION AND COMPRESSION PIPELINE
//
// This function implements the final serialization pipeline with conditional compression:
//
// ## Protobuf Serialization:
//   - Marshals UnsBundle to binary protobuf format
//   - Protobuf chosen for efficiency and forward/backward compatibility
//   - Schema evolution supported via protobuf field numbering
//
// ## LZ4 Compression Decision:
//   - Applied if protobuf size ≥ 1024 bytes (see BundleToProtobufBytesWithCompression)
//   - Compression level 0 for fastest processing (latency-optimized)
//   - Typical compression ratio: 84% for UMH data (5MB → 750KB)
//   - Skipped for small payloads to avoid LZ4 frame overhead
//
// ## Wire Format Generation:
//   - Hex-encodes the final bytes (protobuf or LZ4-compressed protobuf)
//   - Wraps in delimiter format for umh-core parsing:
//     STARTSTARTSTART\n<hex-data>\nENDDATAENDDATENDDATA\n<timestamp>\nENDENDENDEND
//   - Includes current timestamp for latency analysis
//
// ## Error Handling:
//   - Protobuf marshaling errors: Returns error (prevents data corruption)
//   - LZ4 compression errors: Returns error (prevents data corruption)
//   - No partial emission on failure (atomic success/failure)
//
// ## Performance Characteristics:
//   - Small bundles (<1KB): ~450ns processing, no compression
//   - Large bundles (~94KB): ~506µs processing, 84.8% compression
//   - Very large bundles (~5MB): ~14.8ms processing, 84.6% compression
//
// This function:
// - Compresses the bundle to minimize traffic (when beneficial)
// - Creates a new message with the encoded data
// - Returns the message in the format expected by the Benthos framework
//
// Args:
//   - unsBundle: The bundle to serialize and potentially compress
//
// Returns:
//   - []service.MessageBatch: Single batch with one message containing encoded data
//   - error: Any serialization or compression error
func (t *TopicBrowserProcessor) createFinalMessage(unsBundle *UnsBundle) ([]service.MessageBatch, error) {
	protoBytes, err := BundleToProtobufBytesWithCompression(unsBundle)
	if err != nil {
		return nil, err
	}

	message := service.NewMessage(nil)
	message.SetBytes(bytesToMessageWithStartEndBlocksAndTimestamp(protoBytes))

	return []service.MessageBatch{{message}}, nil
}

// Ring Buffer Management Functions

// addEventToTopicBuffer adds an event to the per-topic ring buffer.
// If the buffer is full, it overwrites the oldest event and increments the overwritten metric.
func (t *TopicBrowserProcessor) addEventToTopicBuffer(topic string, event *EventTableEntry) {
	buffer := t.getOrCreateTopicBuffer(topic)

	// Add to ring buffer (overwrites oldest if full)
	buffer.events[buffer.head] = event
	buffer.head = (buffer.head + 1) % buffer.capacity

	if buffer.size < buffer.capacity {
		buffer.size++
	} else {
		// Buffer is full - we're overwriting the oldest event
		t.eventsOverwritten.Incr(1)
	}
}

// getOrCreateTopicBuffer returns the ring buffer for a topic, creating it if it doesn't exist.
func (t *TopicBrowserProcessor) getOrCreateTopicBuffer(topic string) *topicRingBuffer {
	if buffer, exists := t.topicBuffers[topic]; exists {
		return buffer
	}

	// Create new ring buffer for this topic
	buffer := &topicRingBuffer{
		events:   make([]*EventTableEntry, t.maxEventsPerTopic),
		head:     0,
		size:     0,
		capacity: t.maxEventsPerTopic,
	}
	t.topicBuffers[topic] = buffer
	return buffer
}

// getLatestEventsForTopic extracts all events from a topic's ring buffer in chronological order.
// Returns events from oldest to newest, preserving the correct time sequence.
func (t *TopicBrowserProcessor) getLatestEventsForTopic(topic string) []*EventTableEntry {
	buffer := t.topicBuffers[topic]
	if buffer == nil || buffer.size == 0 {
		return nil
	}

	// Extract events in chronological order (oldest to newest)
	events := make([]*EventTableEntry, buffer.size)
	for i := 0; i < buffer.size; i++ {
		idx := (buffer.head - buffer.size + i + buffer.capacity) % buffer.capacity
		events[i] = buffer.events[idx]
	}
	return events
}

// extractTopicFromMessage extracts the topic string from message metadata.
// Returns the topic string or error if umh_topic metadata is missing.
func (t *TopicBrowserProcessor) extractTopicFromMessage(msg *service.Message) (string, error) {
	topic, exists := msg.MetaGet("umh_topic")
	if !exists {
		return "", errors.New("missing umh_topic metadata")
	}
	return topic, nil
}

// bufferMessage handles the buffering of a processed message and its topic information.
// This function manages both the ring buffer storage and the message buffer for ACK control.
func (t *TopicBrowserProcessor) bufferMessage(msg *service.Message, event *EventTableEntry, topicInfo *TopicInfo, unsTreeId string) error {
	t.bufferMutex.Lock()
	defer t.bufferMutex.Unlock()

	// Safety check: prevent unbounded growth
	if len(t.messageBuffer) >= t.maxBufferSize {
		return errors.New("buffer full - dropping message")
	}

	// Extract topic string for ring buffer
	topic, err := t.extractTopicFromMessage(msg)
	if err != nil {
		return err
	}

	// Buffer original message (for ACK)
	t.messageBuffer = append(t.messageBuffer, msg)

	// Add to per-topic ring buffer
	t.addEventToTopicBuffer(topic, event)

	// Track topic changes using cumulative metadata
	cumulativeMetadata := t.mergeTopicHeaders(unsTreeId, []*TopicInfo{topicInfo})
	if t.shouldReportTopic(unsTreeId, cumulativeMetadata) {
		// Update topic info with cumulative metadata before storing
		topicInfoWithCumulative := *topicInfo // shallow copy
		topicInfoWithCumulative.Metadata = cumulativeMetadata
		t.pendingTopicChanges[unsTreeId] = &topicInfoWithCumulative
		t.updateTopicCache(unsTreeId, cumulativeMetadata)
	}

	// Update full topic map (authoritative state) with cumulative metadata
	topicInfoWithCumulative := *topicInfo // shallow copy
	topicInfoWithCumulative.Metadata = cumulativeMetadata
	t.fullTopicMap[unsTreeId] = &topicInfoWithCumulative

	return nil
}

// updateTopicCache updates the LRU cache with new topic metadata.
// This is separated from updateTopicCacheAndBundle to support the ring buffer workflow.
func (t *TopicBrowserProcessor) updateTopicCache(unsTreeId string, headers map[string]string) {
	t.topicMetadataCacheMutex.Lock()
	defer t.topicMetadataCacheMutex.Unlock()

	t.topicMetadataCache.Add(unsTreeId, maps.Clone(headers))
}

func (t *TopicBrowserProcessor) Close(_ context.Context) error {
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
		pendingTopicChanges:     make(map[string]*TopicInfo),
		fullTopicMap:            make(map[string]*TopicInfo),
		lastEmitTime:            time.Now(),
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
