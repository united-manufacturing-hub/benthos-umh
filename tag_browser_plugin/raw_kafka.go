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
	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
)

// messageToRawKafkaMsg extracts all headers and the raw payload
func messageToRawKafkaMsg(message *service.Message) (*tagbrowserpluginprotobuf.EventKafka, error) {
	// We need to extract:
	// 1. All headers
	// 2. The raw payload

	headers := make(map[string]string)

	// Iterate over all headers
	err := message.MetaWalk(func(key string, value string) error {
		headers[key] = value
		return nil
	})
	if err != nil {
		return nil, err
	}

	payload, err := message.AsBytes()
	if err != nil {
		return nil, err
	}

	return &tagbrowserpluginprotobuf.EventKafka{
		Headers: headers,
		Payload: payload,
	}, nil
}
