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
// This plugin processes incoming messages to extract and organize topic hierarchy information,
// metadata, and event data for efficient browsing and querying in the UMH system.
//
// Key Features:
// - Hierarchical topic processing (level0 through level5)
// - Support for both timeseries and non-timeseries data
// - Efficient metadata caching using LRU cache
// - Message batching for reduced network traffic
// - Thread-safe operations with mutex protection
//
// Usage:
// 1. Configure as a processor in your Benthos pipeline
// 2. Set up appropriate input (uns-input) and output plugins
// 3. Monitor metrics for processed and failed messages
//
// Example configuration:
//
//	processors:
//	  - topic_browser: {}
package topic_browser_plugin

import (
	"context"
	"errors"
	"maps"
	"sync"

	lru "github.com/hashicorp/golang-lru"

	"github.com/redpanda-data/benthos/v4/public/service"
)

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
// The function follows a multi-step process to efficiently handle message batching and topic deduplication:
// 1. Initializes an empty UNS bundle to collect all processed data
// 2. Processes each message in the batch to extract topic and event information
// 3. Updates topic metadata and manages the LRU cache to prevent duplicate topic transmissions
// 4. Creates a final protobuf message if there is data to return
//
// The function is designed to minimize traffic by:
// - Batching multiple messages into a single protobuf message
// - Using an LRU cache to avoid re-sending unchanged topic metadata
// - Only including topics in the output when their metadata has changed
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
// For each message in the batch:
// - Extracts topic information and event data
// - Groups topic infos by UNS tree ID for efficient metadata merging
// - Updates metrics for monitoring and debugging
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
// This function is critical for performance optimization as it:
// - Uses a mutex to ensure thread-safe cache operations
// - Merges headers from multiple messages for the same topic
// - Only updates the bundle when topic metadata has changed
// - Prevents unnecessary traffic by caching unchanged topics
func (t *TopicBrowserProcessor) updateTopicMetadata(
	topicInfos map[string][]*TopicInfo,
	unsBundle *UnsBundle,
) {
	t.topicMetadataCacheMutex.Lock()
	defer t.topicMetadataCacheMutex.Unlock()

	for unsTreeId, topics := range topicInfos {
		mergedHeaders := t.mergeTopicHeaders(topics)

		if t.shouldReportTopic(unsTreeId, mergedHeaders) {
			t.updateTopicCacheAndBundle(unsTreeId, mergedHeaders, topics[0], unsBundle)
		}
	}
}

// mergeTopicHeaders combines headers from multiple topic infos into a single map.
// This is necessary because a single topic might appear in multiple messages
// with different header values that need to be consolidated.
func (t *TopicBrowserProcessor) mergeTopicHeaders(topics []*TopicInfo) map[string]string {
	mergedHeaders := make(map[string]string)
	for _, topicInfo := range topics {
		for key, value := range topicInfo.Metadata {
			mergedHeaders[key] = value
		}
	}
	return mergedHeaders
}

// shouldReportTopic determines if a topic's metadata has changed and needs to be reported.
// This is a key optimization that prevents unnecessary traffic by:
// - Checking if the topic exists in the cache
// - Comparing current headers with cached headers
// - Only returning true when the topic is new or has changed
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
// This prevents creating empty messages when:
// - No events were successfully processed
// - No topic metadata has changed
func (t *TopicBrowserProcessor) hasDataToReturn(unsBundle *UnsBundle) bool {
	return len(unsBundle.Events.Entries) > 0 || len(unsBundle.UnsMap.Entries) > 0
}

// createFinalMessage converts the UNS bundle into a protobuf-encoded message.
// This function:
// - Compresses the bundle to minimize traffic
// - Creates a new message with the encoded data
// - Returns the message in the format expected by the Benthos framework
func (t *TopicBrowserProcessor) createFinalMessage(unsBundle *UnsBundle) ([]service.MessageBatch, error) {
	protoBytes, err := BundleToProtobufBytesWithCompression(unsBundle)
	if err != nil {
		return nil, err
	}

	message := service.NewMessage(nil)
	message.SetBytes(bytesToMessageWithStartEndBlocksAndTimestamp(protoBytes))

	return []service.MessageBatch{{message}}, nil
}

func (t *TopicBrowserProcessor) Close(_ context.Context) error {
	// Wipe cache
	t.topicMetadataCacheMutex.Lock()
	t.topicMetadataCache.Purge()
	t.topicMetadataCacheMutex.Unlock()
	return nil
}

func NewTopicBrowserProcessor(logger *service.Logger, metrics *service.Metrics, lruSize int) *TopicBrowserProcessor {
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
			Advanced())

	err := service.RegisterBatchProcessor("topic_browser", spec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
		lruSize, err := conf.FieldInt("lru_size")
		if err != nil {
			return nil, err
		}
		if lruSize < 1 {
			return nil, errors.New("lru_size must be greater than 0")
		}
		return NewTopicBrowserProcessor(mgr.Logger(), mgr.Metrics(), lruSize), nil
	})

	if err != nil {
		panic(err)
	}
}
