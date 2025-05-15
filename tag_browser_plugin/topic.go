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
	This file contains functions to extract the topic from a message and to extract the different levels
*/

import (
	"errors"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// extractTopicFromMessage looks up the "umh_topic" meta-field from the benthos message.
func extractTopicFromMessage(message *service.Message) (string, error) {
	// The uns input plugin will set the "umh_topic" meta-field
	topic, found := message.MetaGet("umh_topic")
	if found {
		return topic, nil
	}
	return "", errors.New("unable to extract topic from message. No umh_topic meta-field found")
}

// topicToUNSInfo will extract the levels and datacontract from a topic.
func topicToUNSInfo(topic string) (*tagbrowserpluginprotobuf.TopicInfo, error) {
	// Check (empty topic, not beginning with "umh.v1.")
	if len(topic) == 0 || strings.HasPrefix(topic, "umh.v1.") == false {
		return nil, errors.New("topic does not start with umh.v1")
	}

	// Split by dots to get each part of the topic
	parts := strings.Split(topic, ".")
	// There must be at least 4 parts (umh, v1, level0, datacontract)
	if len(parts) < 4 {
		return nil, errors.New("topic does not have enough parts")
	}

	var unsInfo tagbrowserpluginprotobuf.TopicInfo
	// Part 0 will be umh, and part 1 will be v1, so we can safely ignore them
	unsInfo.Level0 = parts[2]
	if len(unsInfo.Level0) == 0 {
		return nil, errors.New("topic contains empty parts")
	}
	var hasDatacontract bool
	var eventGroup []string
	for i := 3; i < len(parts); i++ {
		if len(parts[i]) == 0 {
			return nil, errors.New("topic contains empty parts")
		}
		// We now need to either assign to the next fields or to datacontract based on the content.
		if parts[i][0] == '_' {
			// We have the datacontract field
			hasDatacontract = true
			unsInfo.Datacontract = parts[i]
			continue
		}

		// If we already have a datacontract, group everything into an "eventGroup"
		if hasDatacontract {
			eventGroup = append(eventGroup, parts[i])
			continue
		}

		// This is neither a datacontract, nor are we behind the datacontract field, so we can just match based on i
		switch i {
		case 3:
			unsInfo.Level1 = wrapperspb.String(parts[i])
		case 4:
			unsInfo.Level2 = wrapperspb.String(parts[i])
		case 5:
			unsInfo.Level3 = wrapperspb.String(parts[i])
		case 6:
			unsInfo.Level4 = wrapperspb.String(parts[i])
		case 7:
			unsInfo.Level5 = wrapperspb.String(parts[i])
		}
	}
	if !hasDatacontract {
		return nil, errors.New("topic does not container datacontract")
	}

	if len(eventGroup) > 0 {

		if len(eventGroup) > 1 {
			// If there are more than 1 even inside the eventGroup, collect all except the last into VirtualPath
			unsInfo.VirtualPath = wrapperspb.String(strings.Join(eventGroup[:len(eventGroup)-1], "."))
		}

		// Set the last part as the eventTag
		unsInfo.EventTag = wrapperspb.String(eventGroup[len(eventGroup)-1])
	}
	return &unsInfo, nil
}
