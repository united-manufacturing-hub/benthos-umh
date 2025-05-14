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
	"errors"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func extractTopicFromMessage(message *service.Message) (string, error) {
	// The uns input plugin will set the "topic" meta-field
	topic, found := message.MetaGet("topic")
	if found {
		return topic, nil
	}
	return "", errors.New("unable to extract topic from message. No topic meta-field found")
}

// topicToUNSInfo will extract the enterprise, site, ... data from an topic
// It will not extract the EventTag, as that one is part of the message itself
func topicToUNSInfo(topic string) (*tagbrowserpluginprotobuf.TopicInfo, error) {
	// Check (empty topic, not beginning with "umh.v1.")
	if len(topic) == 0 || strings.HasPrefix(topic, "umh.v1.") == false {
		return nil, errors.New("topic does not start with umh.v1")
	}

	// Split by dots, to get each part of the topic
	parts := strings.Split(topic, ".")
	// There must be at least 4 parts (Enterprise and schema is required)
	if len(parts) < 4 {
		return nil, errors.New("topic does not have enough parts")
	}

	var unsInfo tagbrowserpluginprotobuf.TopicInfo
	// Part 0 will be umh, and part 1 will be v1, so we can safely ignore them
	unsInfo.Level0 = parts[2]
	var hasSchema bool
	var eventGroup strings.Builder
	for i := 3; i < len(parts); i++ {
		// We now need to either assign to the next fields or to schema based on the content.
		if parts[i][0] == '_' {
			// We have the schema field
			hasSchema = true
			unsInfo.Datacontract = parts[i]
			continue
		}

		// If we already have a schema, group everything into an "eventGroup"
		if hasSchema {
			eventGroup.WriteString(parts[i])
			eventGroup.WriteRune('.')
			continue
		}

		// This is neither a schema, nor are we behind the schema field, so we can just match based on i
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

	// Write the eventGroup (without the last dot)
	eventGroupString := eventGroup.String()
	if len(eventGroupString) > 0 {
		unsInfo.EventGroup = wrapperspb.String(eventGroupString[:len(eventGroupString)-1])
	}
	return &unsInfo, nil
}
