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

	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
)

type TagBrowserProcessor struct {
	unsMapCache map[string]bool
}

func (t TagBrowserProcessor) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	messageBatch, err := t.ProcessBatch(ctx, service.MessageBatch{message})
	if err != nil {
		return nil, err
	}
	if len(messageBatch) == 0 {
		return nil, nil
	}
	return messageBatch[0], nil
}

func (t TagBrowserProcessor) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	unsBundle := &tagbrowserpluginprotobuf.UnsBundle{
		UnsMap: &tagbrowserpluginprotobuf.UnsMap{
			Entries: make(map[string]*tagbrowserpluginprotobuf.UnsInfo),
		},
		Events: &tagbrowserpluginprotobuf.EventTable{
			Entries: make([]*tagbrowserpluginprotobuf.EventTableEntry, 0, 1),
		},
	}

	var resultBatch service.MessageBatch

	for _, message := range batch {
		unsInfo, eventTableEntry, unsTreeId, err := MessageToUNSInfoAndEvent(message)
		if err != nil {
			return nil, err
		}

		// This is safe, as unsTreeId will always be set if the error is nil
		if _, ok := t.unsMapCache[*unsTreeId]; !ok {
			unsBundle.UnsMap.Entries[*unsTreeId] = unsInfo
			t.unsMapCache[*unsTreeId] = true
		}
		unsBundle.Events.Entries = append(unsBundle.Events.Entries, eventTableEntry)

		protoBytes, err := BundleToProtobufBytes(unsBundle)
		if err != nil {
			return nil, err
		}

		message.SetBytes(protoBytes)
		resultBatch = append(resultBatch, message)
	}

	return []service.MessageBatch{resultBatch}, nil
}

func (t TagBrowserProcessor) Close(_ context.Context) error {
	// Wipe cache
	t.unsMapCache = make(map[string]bool)
	return nil
}

func NewTagBrowserProcessor() *TagBrowserProcessor {
	return &TagBrowserProcessor{
		unsMapCache: make(map[string]bool),
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

	err := service.RegisterProcessor("tag_browser", spec, func(_ *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {
		return NewTagBrowserProcessor(), nil
	})
	if err != nil {
		panic(err)
	}
}
