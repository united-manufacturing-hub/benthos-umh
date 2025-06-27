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
