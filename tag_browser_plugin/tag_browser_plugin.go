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
}

func (t TagBrowserProcessor) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	unsInfo, eventTableEntry, unsTreeId, err := MessageToUNSInfoAndEvent(message)
	if err != nil {
		return nil, err
	}

	unsBundle := &tagbrowserpluginprotobuf.UnsBundle{
		UnsMap: &tagbrowserpluginprotobuf.UnsMap{
			Entries: make(map[string]*tagbrowserpluginprotobuf.UnsInfo),
		},
		Events: &tagbrowserpluginprotobuf.EventTable{
			Entries: make([]*tagbrowserpluginprotobuf.EventTableEntry, 0, 1),
		},
	}
	// This is safe, as unsTreeId will always be set if the error is nil
	unsBundle.UnsMap.Entries[*unsTreeId] = unsInfo
	unsBundle.Events.Entries = append(unsBundle.Events.Entries, eventTableEntry)

	protoBytes, err := BundleToProtobufBytes(unsBundle)
	if err != nil {
		return nil, err
	}

	message.SetBytes(protoBytes)

	var resultBatch service.MessageBatch
	resultBatch = append(resultBatch, message)
	return resultBatch, nil
}

func (t TagBrowserProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	//TODO implement me
	panic("implement me")
}

func (t TagBrowserProcessor) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func NewTagBrowserProcessor() *TagBrowserProcessor {
	return &TagBrowserProcessor{}
}
