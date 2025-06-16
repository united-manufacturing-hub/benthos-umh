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
)

// messageToRawKafkaMsg converts a Benthos message to a raw Kafka message structure.
// This function extracts headers and payload from the message for debugging and auditing purposes.
//
// Args:
//   - message: The Benthos message to convert
//
// Returns:
//   - *EventKafka: The raw Kafka message structure
//   - error: Any error that occurred during conversion
//
// The function preserves all message metadata as headers and captures the raw payload bytes.
// This is useful for debugging, auditing, and maintaining traceability of the original message.
func messageToRawKafkaMsg(message *service.Message) (*EventKafka, error) {
	// Extract all metadata as headers
	headers := make(map[string]string)

	// Iterate over all headers
	err := message.MetaWalk(func(key string, value string) error {
		headers[key] = value
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Get raw payload bytes
	payload, err := message.AsBytes()
	if err != nil {
		return nil, err
	}

	return &EventKafka{
		Headers: headers,
		Payload: payload,
	}, nil
}
