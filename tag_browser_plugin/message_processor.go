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

/*
	This file contains a function that will create the TopicInfo and EventTableEntry from a message.
	It also calculates the uns_tree_id and sets the EventTag if we have time series data.
*/

import (
	"encoding/hex"
	"github.com/cespare/xxhash/v2"
	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// MessageToUNSInfoAndEvent first extracts the topic from the message, then the event data and finally the UNS info.
// It also sets the eventTag if necessary and generates the UnsTreeId, which is required by the frontend.
// Finally, it appends the "raw" kafka message part, including the raw message and its headers
func MessageToUNSInfoAndEvent(message *service.Message) (*tagbrowserpluginprotobuf.TopicInfo, *tagbrowserpluginprotobuf.EventTableEntry, *string, error) {
	topic, err := extractTopicFromMessage(message)
	if err != nil {
		return nil, nil, nil, err
	}
	// Extract Event Data
	event, eventTag, err := messageToEvent(message)
	if err != nil {
		return nil, nil, nil, err
	}

	// Extract UNS Data
	unsInfo, err := topicToUNSInfo(topic)
	if err != nil {
		return nil, nil, nil, err
	}

	// If the message is a time-series entry, we can enrich the unsInfo
	if event.IsTimeseries {
		//unsInfo.EventTag = wrapperspb.String(*eventTag)
	}

	// We now have everything to calculate the uns_tree_id
	unsTreeId := HashUNSTableEntry(unsInfo)
	event.UnsTreeId = unsTreeId

	// Finally, we populate the RawKafkaMsg field in the event
	event.RawKafkaMsg, err = messageToRawKafkaMsg(message)
	if err != nil {
		return nil, nil, nil, err
	}

	return unsInfo, event, &unsTreeId, nil
}

// HashUNSTableEntry generates an xxHash from the Levels and datacontract.
// This is used by the frontend to identify which topic an entry belongs to.
// We use it over full topic names to reduce the amount of data we need to send to the frontend.
func HashUNSTableEntry(info *tagbrowserpluginprotobuf.TopicInfo) string {
	hasher := xxhash.New()
	_, _ = hasher.Write([]byte(info.Level0))
	// GetValue returns either the contained data, or "" if no value is set
	_, _ = hasher.Write([]byte(info.Level1.GetValue()))
	_, _ = hasher.Write([]byte(info.Level2.GetValue()))
	_, _ = hasher.Write([]byte(info.Level3.GetValue()))
	_, _ = hasher.Write([]byte(info.Level4.GetValue()))
	_, _ = hasher.Write([]byte(info.Level5.GetValue()))
	_, _ = hasher.Write([]byte(info.Datacontract))
	_, _ = hasher.Write([]byte(info.EventGroup.GetValue()))
	_, _ = hasher.Write([]byte(info.EventTag.GetValue()))
	return hex.EncodeToString(hasher.Sum(nil))
}
