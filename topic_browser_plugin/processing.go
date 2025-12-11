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
	"maps"

	"github.com/redpanda-data/benthos/v4/public/service"
	protobuf "google.golang.org/protobuf/proto"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

// bufferMessage handles the buffering of a processed message and its topic information.
// This function manages both the ring buffer storage and the message buffer for ACK control.
//
// MEMORY OPTIMIZATION STRATEGY:
// - Use sync.Pool for metadata maps to reduce allocation churn
// - Aggressively reuse existing TopicInfo objects to avoid protobuf cloning
// - Only clone protobuf objects when metadata has actually changed
// - Minimize allocations during high-throughput processing
func (t *TopicBrowserProcessor) bufferMessage(msg *service.Message, event *proto.EventTableEntry, topicInfo *proto.TopicInfo, unsTreeId string) error {
	t.bufferMutex.Lock()
	defer t.bufferMutex.Unlock()

	// Extract topic string for ring buffer
	topic, err := t.extractTopicFromMessage(msg)
	if err != nil {
		return err
	}

	// Buffer original message (for ACK control)
	t.messageBuffer = append(t.messageBuffer, msg)

	// Add to per-topic ring buffer
	t.addEventToTopicBuffer(topic, event)

	// OPTIMIZATION: Check if we already have identical TopicInfo first (before any heavy operations)
	// This avoids both metadata merging AND protobuf cloning for unchanged topics
	if existingTopicInfo, exists := t.fullTopicMap[unsTreeId]; exists {
		// Quick comparison of core TopicInfo fields (non-metadata)
		if existingTopicInfo.Level0 == topicInfo.Level0 &&
			existingTopicInfo.DataContract == topicInfo.DataContract &&
			existingTopicInfo.Name == topicInfo.Name &&
			len(existingTopicInfo.LocationSublevels) == len(topicInfo.LocationSublevels) {
			// Check location sublevels
			locationMatch := true
			for i, level := range topicInfo.LocationSublevels {
				if i >= len(existingTopicInfo.LocationSublevels) || existingTopicInfo.LocationSublevels[i] != level {
					locationMatch = false
					break
				}
			}

			// Check virtual path
			virtualPathMatch := (existingTopicInfo.VirtualPath == nil && topicInfo.VirtualPath == nil) ||
				(existingTopicInfo.VirtualPath != nil && topicInfo.VirtualPath != nil &&
					*existingTopicInfo.VirtualPath == *topicInfo.VirtualPath)

			if locationMatch && virtualPathMatch {
				// Core TopicInfo is identical, now check if incoming metadata would change anything
				incomingMetadata := topicInfo.Metadata
				if incomingMetadata == nil {
					incomingMetadata = make(map[string]string)
				}

				existingMetadata := existingTopicInfo.Metadata
				if existingMetadata == nil {
					existingMetadata = make(map[string]string)
				}

				// Fast metadata comparison using Go 1.21+ maps.Equal
				if maps.Equal(existingMetadata, incomingMetadata) {
					// Perfect match - reuse existing TopicInfo completely (zero allocation!)
					// No need to update cache, merge metadata, or clone anything
					return nil
				}
			}
		}
	}

	// TopicInfo differs or doesn't exist - need to merge metadata and potentially clone
	// Update fullTopicMap with cumulative metadata using pooled maps
	cumulativeMetadata, needsReturn := t.mergeTopicHeaders(unsTreeId, []*proto.TopicInfo{topicInfo})

	// Ensure we return pooled map to avoid memory leaks
	defer func() {
		if needsReturn {
			putMetadataMap(cumulativeMetadata)
		}
	}()

	// OPTIMIZATION: Check if existing TopicInfo can be reused with just metadata update
	if existingTopicInfo, exists := t.fullTopicMap[unsTreeId]; exists {
		// Check if only metadata changed (core fields are identical)
		if existingTopicInfo.Level0 == topicInfo.Level0 &&
			existingTopicInfo.DataContract == topicInfo.DataContract &&
			existingTopicInfo.Name == topicInfo.Name &&
			len(existingTopicInfo.LocationSublevels) == len(topicInfo.LocationSublevels) {
			// Check location sublevels and virtual path again (defensive check)
			canReuseStruct := true
			for i, level := range topicInfo.LocationSublevels {
				if i >= len(existingTopicInfo.LocationSublevels) || existingTopicInfo.LocationSublevels[i] != level {
					canReuseStruct = false
					break
				}
			}

			virtualPathMatch := (existingTopicInfo.VirtualPath == nil && topicInfo.VirtualPath == nil) ||
				(existingTopicInfo.VirtualPath != nil && topicInfo.VirtualPath != nil &&
					*existingTopicInfo.VirtualPath == *topicInfo.VirtualPath)

			if canReuseStruct && virtualPathMatch {
				// Only metadata changed - update metadata in-place instead of cloning entire struct
				// Use optimized cache update that returns storage-ready map
				storageMap := t.updateTopicCacheAndGetStorageMap(unsTreeId, cumulativeMetadata, needsReturn)
				existingTopicInfo.Metadata = storageMap
				// fullTopicMap already points to the updated object
				return nil
			}
		}
	}

	// Core TopicInfo structure changed - need to clone the entire protobuf object
	// This is the expensive path that we try to avoid
	topicInfoWithCumulative := protobuf.Clone(topicInfo).(*proto.TopicInfo)

	// Get storage-ready metadata map from cache update (eliminates double cloning)
	storageMap := t.updateTopicCacheAndGetStorageMap(unsTreeId, cumulativeMetadata, needsReturn)
	topicInfoWithCumulative.Metadata = storageMap

	// Update full topic map (authoritative state) with cumulative metadata
	t.fullTopicMap[unsTreeId] = topicInfoWithCumulative

	return nil
}
