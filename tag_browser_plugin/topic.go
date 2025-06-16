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

// topicToUNSInfo extracts the levels and datacontract from a UNS topic.
// A UNS topic follows the format: umh.v1.{level0}.{level1}.{level2}.{level3}.{level4}.{level5}.{datacontract}.{virtualPath}
// where:
// - level0 is required
// - level1-5 are optional
// - datacontract is required and starts with '_'
// - virtualPath is optional
func topicToUNSInfo(topic string) (*tagbrowserpluginprotobuf.TopicInfo, error) {
	if err := validateTopicFormat(topic); err != nil {
		return nil, err
	}

	parts := strings.Split(topic, ".")
	unsInfo := &tagbrowserpluginprotobuf.TopicInfo{
		Level0: parts[2], // Skip "umh" and "v1"
	}

	// Find datacontract position and process levels
	datacontractIndex, err := findDatacontractIndex(parts)
	if err != nil {
		return nil, err
	}

	// Process levels before datacontract
	if err := processLevels(parts[3:datacontractIndex], unsInfo); err != nil {
		return nil, err
	}

	// Set datacontract
	unsInfo.DataContract = parts[datacontractIndex] // Datacontract renamed to DataContract

	// Process virtual path (everything after datacontract)
	if err := processVirtualPath(parts[datacontractIndex+1:], unsInfo); err != nil {
		return nil, err
	}

	return unsInfo, nil
}

// validateTopicFormat checks if the topic follows the basic UNS format requirements
func validateTopicFormat(topic string) error {
	if len(topic) == 0 {
		return errors.New("topic cannot be empty")
	}
	if !strings.HasPrefix(topic, "umh.v1.") {
		return errors.New("topic must start with umh.v1")
	}
	return nil
}

// findDatacontractIndex locates the datacontract field in the topic parts
func findDatacontractIndex(parts []string) (int, error) {
	for i := 3; i < len(parts); i++ {
		if len(parts[i]) == 0 {
			return 0, errors.New("topic contains empty parts")
		}
		if strings.HasPrefix(parts[i], "_") {
			return i, nil
		}
	}
	return 0, errors.New("topic must contain datacontract")
}

// processLevels assigns level values to the TopicInfo struct
func processLevels(levelParts []string, info *tagbrowserpluginprotobuf.TopicInfo) error {
	for i, part := range levelParts {
		if len(part) == 0 {
			return errors.New("topic contains empty parts")
		}

		switch i {
		case 0:
			info.Level1 = wrapperspb.String(part)
		case 1:
			info.Level2 = wrapperspb.String(part)
		case 2:
			info.Level3 = wrapperspb.String(part)
		case 3:
			info.Level4 = wrapperspb.String(part)
		case 4:
			info.Level5 = wrapperspb.String(part)
		default:
			return errors.New("too many level parts in topic")
		}
	}
	return nil
}

// processVirtualPath handles the virtual path part of the topic (everything after datacontract)
// EventTag is no longer needed and has been removed from the protobuf schema
func processVirtualPath(virtualParts []string, info *tagbrowserpluginprotobuf.TopicInfo) error {
	if len(virtualParts) == 0 {
		return nil
	}

	// All parts after datacontract form the virtual path
	info.VirtualPath = wrapperspb.String(strings.Join(virtualParts, "."))
	return nil
}
