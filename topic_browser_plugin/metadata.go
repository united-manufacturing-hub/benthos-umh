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

package topic_browser_plugin

import (
	"sync"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
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

// metadataMapPool provides a pool of reusable metadata maps to reduce GC pressure.
// This addresses the high allocation churn seen in pprof where we were allocating
// ~200MB+ of map[string]string objects per processing cycle.
//
// MEMORY OPTIMIZATION STRATEGY:
// - Reuse existing map[string]string instances instead of allocating new ones
// - Dramatically reduce garbage collection pressure (2.8GB alloc_space → much lower)
// - Pool automatically handles cleanup under memory pressure
// - Thread-safe and well-tested Go standard pattern
var metadataMapPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate with reasonable capacity for typical metadata
		// Most UMH topics have 3-10 metadata keys (unit, description, etc.)
		return make(map[string]string, 8)
	},
}

// getMetadataMap retrieves a clean metadata map from the pool.
// The returned map is guaranteed to be empty and ready for use.
func getMetadataMap() map[string]string {
	return metadataMapPool.Get().(map[string]string)
}

// putMetadataMap returns a metadata map to the pool after clearing it.
// IMPORTANT: Do not use the map after calling this function - it may be reused by other goroutines.
//
// Parameters:
//   - m: The map to return to the pool (will be cleared automatically)
func putMetadataMap(m map[string]string) {
	// Clear the map before returning to pool to prevent data leakage
	for k := range m {
		delete(m, k)
	}
	metadataMapPool.Put(m)
}

// cloneMetadataMap creates a defensive copy of a metadata map.
// Use this when you need to store a map reference that outlives the current function scope.
//
// Parameters:
//   - source: The map to clone
//
// Returns:
//   - map[string]string: A new map with copied data (safe to store/reference)
func cloneMetadataMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}

	clone := make(map[string]string, len(source))
	for k, v := range source {
		clone[k] = v
	}
	return clone
}

// mergeTopicHeaders efficiently merges Kafka headers from multiple TopicInfo objects
// with previously cached metadata for a given unsTreeId using pooled maps.
//
// MEMORY OPTIMIZATION STRATEGY:
// - Use sync.Pool to reuse map[string]string objects
// - Minimize allocations during high-throughput processing
// - Return cached reference when no changes detected (zero allocation)
//
// CONCURRENCY SAFETY:
// - Mutex protection around cache access
// - Pooled maps are thread-safe via sync.Pool
//
// Parameters:
//   - unsTreeId: The unique identifier for this topic (xxHash of TopicInfo)
//   - topics: Slice of TopicInfo objects containing new metadata to merge
//
// Returns:
//   - map[string]string: Merged metadata map (may be cached reference or new allocation)
//   - bool: True if this is a pooled map that needs to be returned via putMetadataMap()
//
// USAGE PATTERN:
//
//	mergedHeaders, needsReturn := t.mergeTopicHeaders(unsTreeId, topics)
//	defer func() {
//	    if needsReturn {
//	        putMetadataMap(mergedHeaders)
//	    }
//	}()
func (t *TopicBrowserProcessor) mergeTopicHeaders(unsTreeId string, topics []*proto.TopicInfo) (map[string]string, bool) {
	// Check cache first - if we have cached data and no changes, return cached reference
	t.topicMetadataCacheMutex.Lock()
	var cachedHeaders map[string]string
	var hasCachedHeaders bool

	if stored, ok := t.topicMetadataCache.Get(unsTreeId); ok {
		cachedHeaders = stored.(map[string]string)
		hasCachedHeaders = true
	}

	// Quick scan to detect if incoming metadata would change anything
	hasChanges := false
	if hasCachedHeaders {
		for _, topicInfo := range topics {
			for key, newValue := range topicInfo.Metadata {
				if cachedValue, exists := cachedHeaders[key]; !exists || cachedValue != newValue {
					hasChanges = true
					break
				}
			}
			if hasChanges {
				break
			}
		}
	} else {
		// No cached data, so any incoming metadata represents changes
		for _, topicInfo := range topics {
			if len(topicInfo.Metadata) > 0 {
				hasChanges = true
				break
			}
		}
	}

	// If no changes and we have cached data, return cached reference (zero allocation)
	if !hasChanges && hasCachedHeaders {
		t.topicMetadataCacheMutex.Unlock()
		return cachedHeaders, false // Don't return cached reference to pool
	}

	t.topicMetadataCacheMutex.Unlock()

	// Changes detected - use pooled map for merging
	mergedHeaders := getMetadataMap()

	// Copy existing cached data if we have it
	if hasCachedHeaders {
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

	return mergedHeaders, true // Caller needs to return this pooled map
}

// updateTopicCache stores the merged metadata for a given unsTreeId in the cache.
// This function works with both cached references and pooled maps.
//
// MEMORY OPTIMIZATION STRATEGY:
// - Only update cache if metadata has actually changed
// - Create defensive clone only when necessary for cache storage
// - Work efficiently with pooled maps
//
// Parameters:
//   - unsTreeId: The unique identifier for this topic
//   - headers: The merged metadata map to cache
//   - isPooled: Whether this map came from the pool (affects cleanup logic)
func (t *TopicBrowserProcessor) updateTopicCache(unsTreeId string, headers map[string]string, isPooled bool) {
	t.topicMetadataCacheMutex.Lock()
	defer t.topicMetadataCacheMutex.Unlock()

	// Check if we already have this exact data cached
	if stored, ok := t.topicMetadataCache.Get(unsTreeId); ok {
		cachedHeaders := stored.(map[string]string)

		// Quick comparison: if maps are identical, skip cache update
		if len(cachedHeaders) == len(headers) {
			identical := true
			for key, newValue := range headers {
				if cachedValue, exists := cachedHeaders[key]; !exists || cachedValue != newValue {
					identical = false
					break
				}
			}
			if identical {
				// Data is identical, no need to update cache
				return
			}
		}
	}

	// Data has changed - create defensive clone for cache storage
	// We must clone because:
	// 1. Pooled maps will be reused/modified after this function returns
	// 2. Cache needs stable references that won't change
	clonedHeaders := cloneMetadataMap(headers)
	t.topicMetadataCache.Add(unsTreeId, clonedHeaders)
}
