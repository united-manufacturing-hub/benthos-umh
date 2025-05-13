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
		Payload: string(payload),
	}, nil
}
