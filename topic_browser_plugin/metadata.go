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

// updateTopicMetadata manages the topic metadata processing and bundle updates.
//
// # SIMPLIFIED METADATA PROCESSING
//
// This function implements simplified metadata processing with no conditional logic:
//
// ## Processing Strategy:
//   - All topics are always included in the uns_map bundle
//   - No comparison or change detection logic
//   - Simplified flow: merge headers → update cache → add to bundle
//   - Cumulative metadata merging still preserved
//
// ## Metadata Merging Logic:
//   - Multiple messages for same topic have headers merged
//   - Last header value wins for duplicate keys (last-write-wins)
//   - Merged headers become the canonical metadata for the topic
//   - Original per-message headers preserved in EventTableEntry.RawKafkaMsg
//
// ## Thread Safety:
//   - Mutex protects ALL cache operations (Get, Add)
//   - Lock held for entire function to ensure consistency
//   - Prevents race conditions in multi-threaded Benthos environment
//
// ## Memory Management:
//   - LRU cache still maintained for cumulative metadata persistence
//   - maps.Clone ensures cached data doesn't share memory with active processing
//   - Cache used only for metadata accumulation, not change detection
//
// ## Simplification Benefits:
//   - No conditional topic emission logic
//   - Always provides complete topic state to downstream consumers
//   - Eliminates complexity around cache hit/miss scenarios
//   - Predictable output behavior
//
// This function:
// - Uses a mutex to ensure thread-safe cache operations
// - Merges headers from multiple messages for the same topic
// - Always updates the bundle with all topics (no filtering)
// - Maintains cumulative metadata in cache for next processing cycle
//
// Args:
//   - topicInfos: Topics grouped by UNS Tree ID from message batch
//   - unsBundle: Bundle to update with all topics
func (t *TopicBrowserProcessor) updateTopicMetadata(
	topicInfos map[string][]*TopicInfo,
	unsBundle *UnsBundle,
) {
	t.topicMetadataCacheMutex.Lock()
	defer t.topicMetadataCacheMutex.Unlock()

	for unsTreeId, topics := range topicInfos {
		mergedHeaders := t.mergeTopicHeaders(unsTreeId, topics)

		// Always include all topics in uns_map (no conditional emission)
		t.updateTopicCacheAndBundle(unsTreeId, mergedHeaders, topics[0], unsBundle)
	}
}

// initializeTopicMetadataCache creates and initializes the LRU cache for topic metadata.
// Returns the cache and mutex for thread-safe operations.
func initializeTopicMetadataCache(lruSize int) (*lru.Cache, *sync.Mutex) {
	cache, _ := lru.New(lruSize) // Can only error if size is negative
	return cache, &sync.Mutex{}
}
