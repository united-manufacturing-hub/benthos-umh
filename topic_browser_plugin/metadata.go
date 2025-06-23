package topic_browser_plugin

import (
	"maps"
)

// METADATA FLOW DOCUMENTATION
//
// # CUMULATIVE METADATA PERSISTENCE FLOW
//
// The Topic Browser processor implements persistent metadata accumulation across messages and time.
// Metadata keys once seen are preserved indefinitely (until cache eviction), with values updated
// to the latest observed value per key.
//
// ## Complete Processing Flow:
//
// ### 1. Message Processing (ProcessBatch → bufferMessage)
//   - New message arrives with headers in EventTableEntry.RawKafkaMsg.Headers
//   - TopicInfo.Metadata is set to current message headers
//   - bufferMessage() calls mergeTopicHeaders() for cumulative persistence
//
// ### 2. Cumulative Merging (mergeTopicHeaders)
//   - Retrieves previously cached metadata for this UNS Tree ID
//   - Merges with current message headers (last-write-wins per key)
//   - Returns complete cumulative metadata map
//
// ### 3. State Updates (bufferMessage continued)
//   - Updates LRU cache with merged metadata via updateTopicCache()
//   - Updates fullTopicMap with cumulative metadata for emission
//
// ### 4. Emission (flushBufferAndACK)
//   - Emits complete fullTopicMap containing all topics with cumulative metadata
//   - Each TopicInfo.Metadata contains ALL keys ever seen for that topic
//   - Downstream consumers receive complete metadata state (no partial updates)
//
// ## Persistence Guarantees:
//   - **Keys persist**: Once a metadata key is seen, it remains until cache eviction
//   - **Values update**: Each key always holds the most recent observed value
//   - **Cross-session**: Metadata persists across multiple ProcessBatch calls
//   - **Cache-backed**: LRU cache provides efficient memory management
//
// ## Example Timeline:
//   T1: Message with {unit: "celsius"} → Cache: {unit: "celsius"}
//   T2: Message with {serial: "ABC123"} → Cache: {unit: "celsius", serial: "ABC123"}
//   T3: Message with {unit: "fahrenheit"} → Cache: {unit: "fahrenheit", serial: "ABC123"}
//   T4: Message with {location: "factory"} → Cache: {unit: "fahrenheit", serial: "ABC123", location: "factory"}
//   T5: Message with {} → Cache: {unit: "fahrenheit", serial: "ABC123", location: "factory"} (no change)
//
// This ensures the Topic Browser UI always shows the complete metadata profile for each topic,
// regardless of which specific message is being viewed.

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

	// ✅ FIX: Add mutex protection around cache access to prevent race conditions
	t.topicMetadataCacheMutex.Lock()
	if stored, ok := t.topicMetadataCache.Get(unsTreeId); ok {
		cachedHeaders := stored.(map[string]string)
		// Copy all previously known metadata
		for key, value := range cachedHeaders {
			mergedHeaders[key] = value
		}
	}
	t.topicMetadataCacheMutex.Unlock()

	// Layer on new metadata from current batch
	for _, topicInfo := range topics {
		for key, value := range topicInfo.Metadata {
			mergedHeaders[key] = value // Update with latest value
		}
	}

	return mergedHeaders
}

// updateTopicCache updates the LRU cache with new topic metadata.
// This function is used by the ring buffer workflow to maintain cumulative metadata persistence.
func (t *TopicBrowserProcessor) updateTopicCache(unsTreeId string, headers map[string]string) {
	t.topicMetadataCacheMutex.Lock()
	defer t.topicMetadataCacheMutex.Unlock()

	t.topicMetadataCache.Add(unsTreeId, maps.Clone(headers))
}
