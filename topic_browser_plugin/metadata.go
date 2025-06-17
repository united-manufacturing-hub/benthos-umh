package topic_browser_plugin

import (
	"maps"
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

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

// updateTopicCache updates the LRU cache with new topic metadata.
// This is separated from updateTopicCacheAndBundle to support the ring buffer workflow.
func (t *TopicBrowserProcessor) updateTopicCache(unsTreeId string, headers map[string]string) {
	t.topicMetadataCacheMutex.Lock()
	defer t.topicMetadataCacheMutex.Unlock()

	t.topicMetadataCache.Add(unsTreeId, maps.Clone(headers))
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

// initializeTopicMetadataCache creates and initializes the LRU cache for topic metadata.
// Returns the cache and mutex for thread-safe operations.
func initializeTopicMetadataCache(lruSize int) (*lru.Cache, *sync.Mutex) {
	cache, _ := lru.New(lruSize) // Can only error if size is negative
	return cache, &sync.Mutex{}
}
