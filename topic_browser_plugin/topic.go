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
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	topicPrefix = "umh.v1."
	metaKey     = "umh_topic"
)

// extractTopicFromMessage looks up the "umh_topic" meta-field from the benthos message.
func extractTopicFromMessage(message *service.Message) (string, error) {
	// The uns input plugin will set the "umh_topic" meta-field
	topic, found := message.MetaGet(metaKey)
	if found {
		return topic, nil
	}
	return "", errors.New("unable to extract topic from message. No umh_topic meta-field found")
}

// topicToUNSInfo converts a topic string to a UNS info struct.
// It parses the topic to extract level information and virtual path details.
//
// Args:
//   - topic: The topic string to parse (e.g., "umh.v1.acme.cologne.assembly.machine01._analytics.scada.counter.temperature")
//
// Returns:
//   - *TopicInfo: The parsed topic information (VirtualPath field is nil when absent)
//   - error: Any error that occurred during parsing
//
// The function expects topics to follow the UMH topic structure:
// umh.v1.<location_path>.<data_contract>[.<virtual_path>].<name>
// where:
// - location_path: level0.level1.level2...levelN (level0 is mandatory, others optional)
// - data_contract: starts with underscore (e.g., "_historian")
// - virtual_path: optional logical grouping (e.g., "motor.diagnostics")
// - name: mandatory final segment (e.g., "temperature", "order_created")
func topicToUNSInfo(topic string) (*TopicInfo, error) {
	if err := validateTopicFormat(topic); err != nil {
		return nil, err
	}

	parts := strings.Split(topic, ".")

	// Minimum valid topic: umh.v1.level0._contract.name (5 parts)
	if len(parts) < 5 {
		return nil, errors.New("topic must have at least: umh.v1.level0._contract.name")
	}

	// Validate level0 is not empty
	if parts[2] == "" {
		return nil, errors.New("level0 (enterprise) cannot be empty")
	}

	// Find datacontract position (must start with underscore and not be the last segment)
	datacontractIndex := -1
	for i := 3; i < len(parts)-1; i++ { // Start from index 3, end before last segment
		if len(parts[i]) == 0 {
			return nil, errors.New("topic contains empty segments")
		}
		if strings.HasPrefix(parts[i], "_") {
			datacontractIndex = i
			break
		}
	}

	if datacontractIndex == -1 {
		return nil, errors.New("topic must contain a data contract (segment starting with '_') and it cannot be the final segment")
	}

	// Validate data contract is not just an underscore
	if len(parts[datacontractIndex]) <= 1 {
		return nil, errors.New("data contract cannot be just an underscore")
	}

	// The last segment is always the name (mandatory)
	nameIndex := len(parts) - 1
	name := parts[nameIndex]
	if name == "" {
		return nil, errors.New("topic name (final segment) cannot be empty")
	}

	// Validate name doesn't start with underscore (to avoid confusion with data contracts)
	if strings.HasPrefix(name, "_") {
		return nil, errors.New("topic name cannot start with underscore")
	}

	// Create TopicInfo struct
	unsInfo := &TopicInfo{
		Level0:       parts[2], // Skip "umh" and "v1"
		DataContract: parts[datacontractIndex],
		Name:         name,
	}

	// Process location sublevels (everything between level0 and datacontract)
	locationStart := 3 // After umh.v1.level0
	locationEnd := datacontractIndex
	if locationEnd > locationStart {
		unsInfo.LocationSublevels = parts[locationStart:locationEnd]
	} else {
		unsInfo.LocationSublevels = []string{} // Ensure non-nil empty slice
	}

	// Process virtual path (everything between datacontract and name)
	virtualStart := datacontractIndex + 1
	virtualEnd := nameIndex
	if virtualEnd > virtualStart {
		virtualParts := parts[virtualStart:virtualEnd]

		// Validate that no virtual path segments are empty
		for _, part := range virtualParts {
			if len(part) == 0 {
				return nil, errors.New("virtual path cannot contain empty segments")
			}
		}

		virtualPath := strings.Join(virtualParts, ".")
		unsInfo.VirtualPath = &virtualPath
	}

	return unsInfo, nil
}

// validateTopicFormat checks if the topic follows the basic UNS format requirements
func validateTopicFormat(topic string) error {
	if len(topic) == 0 {
		return errors.New("topic cannot be empty")
	}
	if !strings.HasPrefix(topic, topicPrefix) {
		return errors.New("topic must start with umh.v1")
	}
	return nil
}
