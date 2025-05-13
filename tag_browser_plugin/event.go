package tag_browser_plugin

import (
	"fmt"
	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func messageToEvent(message *service.Message) (*tagbrowserpluginprotobuf.EventTableEntry, *string, error) {
	structured, err := message.AsStructured()
	if err != nil {
		return processRelationalData(message)
	}
	structuredAsMap, ok := structured.(map[string]interface{})
	if !ok {
		return processRelationalData(message)
	}

	// Exactly timestamp_ms and one key/value pair are required
	if len(structuredAsMap) != 2 {
		return processRelationalData(message)
	}

	if _, ok := structuredAsMap["timestamp_ms"]; !ok {
		return processRelationalData(message)
	}

	return processTimeSeriesData(structuredAsMap)
}

// processTimeSeriesData extracts the timestamp, value name, and value (including its type) from the structured data
func processTimeSeriesData(structured map[string]interface{}) (*tagbrowserpluginprotobuf.EventTableEntry, *string, error) {
	var valueName string
	var valueContent anypb.Any
	var timestampMs int64
	for key, value := range structured {
		switch key {
		case "timestamp_ms":
			timestampMs = value.(int64)
		default:
			valueName = key
			byteValue, valueType, err := ToBytes(value)
			if err != nil {
				return nil, nil, err
			}
			valueContent = anypb.Any{
				TypeUrl: fmt.Sprintf("golang/%s", valueType),
				Value:   byteValue,
			}
		}
	}

	return &tagbrowserpluginprotobuf.EventTableEntry{
		IsTimeseries: true,
		TimestampMs:  wrapperspb.Int64(timestampMs),
		Value:        &valueContent,
	}, &valueName, nil
}

// processRelationalData extracts the messages payload as bytes and returns that
func processRelationalData(message *service.Message) (*tagbrowserpluginprotobuf.EventTableEntry, *string, error) {
	valueBytes, err := message.AsBytes()
	if err != nil {
		// Note: This shall never happen, as AsBytes internally never returns errors
		return nil, nil, err
	}
	return &tagbrowserpluginprotobuf.EventTableEntry{
		IsTimeseries: false,
		TimestampMs:  nil,
		Value:        &anypb.Any{TypeUrl: "golang/bytes", Value: valueBytes},
	}, nil, nil
}
