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

/*
	This file contains functions to extract the topic from a message and to extract the different levels
*/

import (
	"errors"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

const (
	metaKey = "umh_topic"
)

// extractTopicFromMessage looks up the "umh_topic" meta-field from the benthos message.
func extractTopicFromMessage(message *service.Message) (string, error) {
	// The uns input plugin will set the "umh_topic" meta-field
	topicStr, found := message.MetaGet(metaKey)
	if found {
		return topicStr, nil
	}
	return "", errors.New("unable to extract topic from message. No umh_topic meta-field found")
}

// topicToUNSInfo converts a topic string to a UNS info struct using the new topic parser library.
// This replaces the previous manual parsing implementation.
//
// Args:
//   - topicStr: The topic string to parse (e.g., "umh.v1.acme.cologne.assembly.machine01._analytics.scada.counter.temperature")
//
// Returns:
//   - *proto.TopicInfo: The parsed topic information compatible with protobuf structure
//   - error: Any error that occurred during parsing
func topicToUNSInfo(topicStr string) (*proto.TopicInfo, error) {
	// Use the new topic parser library
	unsTopic, err := topic.NewUnsTopic(topicStr)
	if err != nil {
		return nil, err
	}

	// Convert from the topic library's TopicInfo to our protobuf TopicInfo
	info := unsTopic.Info()

	// Create protobuf-compatible TopicInfo
	protoTopicInfo := &proto.TopicInfo{
		Level0:            info.Level0,
		LocationSublevels: info.LocationSublevels,
		DataContract:      info.DataContract,
		Name:              info.Name,
	}

	// Handle optional virtual path
	if info.VirtualPath != nil {
		protoTopicInfo.VirtualPath = info.VirtualPath
	}

	return protoTopicInfo, nil
}
