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
	lru "github.com/hashicorp/golang-lru"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
)

type TagBrowserProcessor struct {
	// This cache keeps the number of topics
	// transmitted to the umh-core small by trying to not re-send already reported topics.
	// Since it is not thread safe, we need a mutex to protect it.
	topicDeduplicationCache      *lru.Cache
	topicDeduplicationCacheMutex *sync.Mutex
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
	t.topicDeduplicationCacheMutex.Lock()
	for _, message := range batch {
		topicInfo, eventTableEntry, unsTreeId, err := MessageToUNSInfoAndEvent(message)
		if err != nil {
			// Unlock the mutex when we have an error
			t.topicDeduplicationCacheMutex.Unlock()
			return nil, err
		}

		// This is safe, as unsTreeId will always be set if the error is nil.
		// We only want to report new topics, so we check if the topic is already in the cache.
		if _, ok := t.topicDeduplicationCache.Get(*unsTreeId); !ok {
			unsBundle.UnsMap.Entries[*unsTreeId] = topicInfo
			t.topicDeduplicationCache.Add(*unsTreeId, true)
		}
		unsBundle.Events.Entries = append(unsBundle.Events.Entries, eventTableEntry)
	}
	t.topicDeduplicationCacheMutex.Unlock()

	// Bundle data from all messages into a single one containing the protobuf
	protoBytes, err := BundleToProtobufBytesWithCompression(unsBundle)
	if err != nil {
		return nil, err
	}

	message := service.NewMessage(nil)
	message.SetBytes(protoBytes)
	var resultBatch service.MessageBatch
	resultBatch = append(resultBatch, message)

	return []service.MessageBatch{resultBatch}, nil
}

func (t *TagBrowserProcessor) Close(_ context.Context) error {
	// Wipe cache
	t.topicDeduplicationCacheMutex.Lock()
	t.topicDeduplicationCache.Purge()
	t.topicDeduplicationCacheMutex.Unlock()
	return nil
}

func NewTagBrowserProcessor() *TagBrowserProcessor {
	// Create a new cache for topic names, which we will use to not re-report topics.
	// If we have > 1000 topics, we might re-report some, but this will still be a small number.
	// (Assuming that the topics have different message production rates).
	l, _ := lru.New(1000) // Can only error if size is negative
	return &TagBrowserProcessor{
		topicDeduplicationCache: l,
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
		Field(service.NewObjectField(""))

	err := service.RegisterBatchProcessor("tag_browser", spec, func(_ *service.ParsedConfig, _ *service.Resources) (service.BatchProcessor, error) {
		return NewTagBrowserProcessor(), nil
	})

	if err != nil {
		panic(err)
	}
}
