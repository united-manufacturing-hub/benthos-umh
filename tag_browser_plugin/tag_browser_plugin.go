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

package tag_browser_plugin

import (
	"context"
	"errors"
	lru "github.com/hashicorp/golang-lru"
	"maps"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
)

type TagBrowserProcessor struct {
	// This cache keeps the number of topics
	// transmitted to the umh-core small by trying to not re-send already reported topics.
	// Since it is not thread safe, we need a mutex to protect it.
	topicMetadataCache      *lru.Cache
	topicMetadataCacheMutex *sync.Mutex
	logger                  *service.Logger
	messagesProcessed       *service.MetricCounter
	messagesFailed          *service.MetricCounter
}

func (t *TagBrowserProcessor) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	messageBatch, err := t.ProcessBatch(ctx, service.MessageBatch{message})
	if err != nil {
		return nil, err
	}
	if len(messageBatch) == 0 {
		return nil, nil
	}
	return messageBatch[0], nil
}

// ProcessBatch processes 1-n messages from the uns-input and generates a single protobuf message containing all there data.
// If we receive no data, it will early return.
//
// To do the processing, it first extracts the topicInfo, containing the levels and datacontract.
// It also extracts the eventTableEntry, containing the event itself (e.g. the data inside the UNS payload) and the timestamp.
// Once that is done for every message in the batch, it creates a protobuf encoded message containing both the deduplicated topicInfo and all eventTableEntries.
func (t *TagBrowserProcessor) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	// Construct the empty protobuf message, which we will fill with data.
	unsBundle := &tagbrowserpluginprotobuf.UnsBundle{
		UnsMap: &tagbrowserpluginprotobuf.TopicMap{
			Entries: make(map[string]*tagbrowserpluginprotobuf.TopicInfo),
		},
		Events: &tagbrowserpluginprotobuf.EventTable{
			Entries: make([]*tagbrowserpluginprotobuf.EventTableEntry, 0, 1),
		},
	}

	// Extract data for each message in the batch
	topicInfos := make(map[string][]*tagbrowserpluginprotobuf.TopicInfo)
	for _, message := range batch {
		topicInfo, eventTableEntry, unsTreeId, err := MessageToUNSInfoAndEvent(message)
		if err != nil {
			t.logger.Errorf("Error while processing message: %v", err)
			t.messagesFailed.Incr(1)
			continue
		}

		unsBundle.Events.Entries = append(unsBundle.Events.Entries, eventTableEntry)
		topicInfo.Metadata = eventTableEntry.RawKafkaMsg.Headers

		// Add the topicInfo to the map
		if _, ok := topicInfos[*unsTreeId]; !ok {
			topicInfos[*unsTreeId] = make([]*tagbrowserpluginprotobuf.TopicInfo, 0, 1)
		}
		topicInfos[*unsTreeId] = append(topicInfos[*unsTreeId], topicInfo)
		t.messagesProcessed.Incr(1)
	}

	// For each topic, it will now merge the headers and then check if we have a change compared to the cache
	// If it is either not cached or changed, we will report it
	for unsTreeId, topic := range topicInfos {
		// We need to merge the headers of all messages for this topic
		mergedHeaders := make(map[string]string)
		for _, topicInfo := range topic {
			for key, value := range topicInfo.Metadata {
				mergedHeaders[key] = value
			}
		}

		// We now have the merged headers, we can check if we have a change compared to the cache
		t.topicMetadataCacheMutex.Lock()
		shallReport := true
		stored, ok := t.topicMetadataCache.Get(unsTreeId)
		if ok {
			// A cached entry is present
			cachedHeaders := stored.(map[string]string)
			if maps.Equal(cachedHeaders, mergedHeaders) {
				// Cached headers are equal to current, no need to report
				shallReport = false
			}
		}
		if shallReport {
			// 1. Update cache
			t.topicMetadataCache.Add(unsTreeId, mergedHeaders)
			// 2. Report topic
			t := topic[0]
			t.Metadata = mergedHeaders
			unsBundle.UnsMap.Entries[unsTreeId] = t
		}
		t.topicMetadataCacheMutex.Unlock()
	}

	if len(unsBundle.Events.Entries) == 0 && len(unsBundle.UnsMap.Entries) == 0 {
		return nil, nil
	}

	// Bundle data from all messages into a single one containing the protobuf
	protoBytes, err := BundleToProtobufBytesWithCompression(unsBundle)
	if err != nil {
		return nil, err
	}

	message := service.NewMessage(nil)
	message.SetBytes(bytesToMessage(protoBytes))
	var resultBatch service.MessageBatch
	resultBatch = append(resultBatch, message)

	return []service.MessageBatch{resultBatch}, nil
}

func (t *TagBrowserProcessor) Close(_ context.Context) error {
	// Wipe cache
	t.topicMetadataCacheMutex.Lock()
	t.topicMetadataCache.Purge()
	t.topicMetadataCacheMutex.Unlock()
	return nil
}

func NewTagBrowserProcessor(logger *service.Logger, metrics *service.Metrics, lruSize int) *TagBrowserProcessor {
	// The LRU cache is used to:
	// - Deduplicate topics
	// - Store the latest version of meta-information about that topic
	l, _ := lru.New(lruSize) // Can only error if size is negative
	return &TagBrowserProcessor{
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
		Description(`The tag browser processor processes messages into UNS bundles for the tag browser.

The processor will read the message headers and body to extract the UNS information and event table entries.

The processor will then return a message with the UNS bundle as the body, encoded as a protobuf.

The processor requires that the following metadata fields are set:

- topic: The topic of the message.
`).
		Field(service.NewIntField("lru_size").
			Description("The size of the LRU cache used to  deduplicate topic names and store topic information.").
			Default(50_000).
			Advanced())

	err := service.RegisterBatchProcessor("tag_browser", spec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
		lruSize, err := conf.FieldInt("lru_size")
		if err != nil {
			return nil, err
		}
		if lruSize < 1 {
			return nil, errors.New("lru_size must be greater than 0")
		}
		return NewTagBrowserProcessor(mgr.Logger(), mgr.Metrics(), lruSize), nil
	})

	if err != nil {
		panic(err)
	}
}
