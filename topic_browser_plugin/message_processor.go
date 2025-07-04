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
	This file contains a function that will create the TopicInfo and EventTableEntry from a message.
	It also calculates the uns_tree_id and sets the EventTag if we have time-series data.
*/

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

// MessageToUNSInfoAndEvent converts a Benthos message to UNS info and event entry.
// This function serves as the main entry point for processing incoming messages
// and extracting both topic hierarchy information and event data.
//
// Args:
//   - message: The Benthos message to process
//
// Returns:
//   - *proto.TopicInfo: Topic hierarchy information
//   - *proto.EventTableEntry: Event data
//   - *string: UNS tree ID (hash of topic info)
//   - error: Any error that occurred during processing
//
// The function performs the following operations:
// 1. Extracts the topic from message metadata
// 2. Converts the topic to UNS hierarchy information
// 3. Converts the message payload to an event entry
// 4. Generates a unique UNS tree ID for the topic
// 5. Links the event to the topic via the tree ID
func MessageToUNSInfoAndEvent(message *service.Message) (*proto.TopicInfo, *proto.EventTableEntry, *string, error) {
	t, err := extractTopicFromMessage(message)
	if err != nil {
		return nil, nil, nil, err
	}

	// Extract UNS Data
	unsTopic, err := topic.NewUnsTopic(t)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse topic '%s': %w", t, err)
	}
	unsInfo := unsTopic.Info()

	// Extract Event Data - EventTag parameter removed since it's no longer needed
	event, err := messageToEvent(message)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to convert message to event: %w", err)
	}

	// We now have everything to calculate the uns_tree_id
	unsTreeId := HashUNSTableEntry(unsInfo)
	event.UnsTreeId = unsTreeId

	// Set the ProducedAtMs field with Kafka timestamp (epoch-ms when record was written to UNS)
	event.ProducedAtMs = extractKafkaTimestamp(message)

	// Finally, we populate the RawKafkaMsg field in the event
	event.RawKafkaMsg, err = messageToRawKafkaMsg(message)
	if err != nil {
		return nil, nil, nil, err
	}

	// If we have a processed-by header, we will extract that and set it in the event
	// The first entry in here will be the producer of the message
	if val, ok := event.RawKafkaMsg.Headers["processed-by"]; ok {
		event.BridgedBy = strings.Split(val, ",")
	}

	return unsInfo, event, &unsTreeId, nil
}

// extractKafkaTimestamp extracts the Kafka timestamp from the message metadata.
// This represents when the record was written to Kafka (produced_at_ms).
// Falls back to current time if no Kafka timestamp is available.
func extractKafkaTimestamp(message *service.Message) uint64 {
	// Primary: Get the Kafka timestamp from UNS input plugin (standardized format)
	if timestampStr, exists := message.MetaGet("kafka_timestamp_ms"); exists {
		if timestampMs, err := strconv.ParseUint(timestampStr, 10, 64); err == nil {
			return timestampMs
		}
	}

	// Fallback: use current time if no Kafka timestamp is available
	// This can happen with non-Kafka inputs or older message formats
	return uint64(time.Now().UnixMilli())
}

// HashUNSTableEntry generates an xxHash from the Levels and datacontract.
// This is used by the frontend to identify which topic an entry belongs to.
// We use it over full topic names to reduce the amount of data we need to send to the frontend.
//
// ✅ FIX: Uses null byte delimiters to prevent hash collisions between different segment combinations.
// For example, ["ab","c"] vs ["a","bc"] would produce different hashes instead of identical ones.
func HashUNSTableEntry(info *proto.TopicInfo) string {
	hasher := xxhash.New()

	// Helper function to write each component followed by NUL delimiter to avoid ambiguity
	write := func(s string) {
		_, _ = hasher.Write(append([]byte(s), 0))
	}

	write(info.Level0)

	// Hash all location sublevels
	for _, level := range info.LocationSublevels {
		write(level)
	}

	write(info.DataContract)

	// Hash virtual path if it exists
	if info.VirtualPath != nil {
		write(*info.VirtualPath)
	}

	// Hash the name (new field)
	write(info.Name)

	return hex.EncodeToString(hasher.Sum(nil))
}
