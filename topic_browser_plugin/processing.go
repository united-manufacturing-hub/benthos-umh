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
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
	protobuf "google.golang.org/protobuf/proto"
)

// bufferMessage handles the buffering of a processed message and its topic information.
// This function manages both the ring buffer storage and the message buffer for ACK control.
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

	// Update fullTopicMap with cumulative metadata and emit complete state once per interval
	// This maintains the authoritative topic state with merged metadata across all messages
	cumulativeMetadata := t.mergeTopicHeaders(unsTreeId, []*proto.TopicInfo{topicInfo})

	// ✅ FIX: Use protobuf.Clone() to safely copy protobuf struct without copying internal mutex
	topicInfoWithCumulative := protobuf.Clone(topicInfo).(*proto.TopicInfo)
	topicInfoWithCumulative.Metadata = cumulativeMetadata
	t.updateTopicCache(unsTreeId, cumulativeMetadata)

	// Update full topic map (authoritative state) with cumulative metadata
	t.fullTopicMap[unsTreeId] = topicInfoWithCumulative

	return nil
}
