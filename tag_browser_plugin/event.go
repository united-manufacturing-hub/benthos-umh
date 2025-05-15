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
	The functions in this file allow the program to extract the event data (e.g timestamp, payload key/value) from the benthos message.
*/

import (
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// messageToEvent will convert a benthos message, into an EventTableEntry, and eventTag and an optional error
// It checks if the incoming message is a valid time-series message, otherwise it handles it as relational data
func messageToEvent(message *service.Message, expectedTagNameForTimeseries *wrapperspb.StringValue) (*tagbrowserpluginprotobuf.EventTableEntry, error) {
	// 1. If we don't have structured data (e.g. no JSON), we will handle it as relational data
	structured, err := message.AsStructured()
	if err != nil {
		return processRelationalData(message)
	}
	// 2. If the structured data is not a map, we will handle it as relational data (e.g lists)
	structuredAsMap, ok := structured.(map[string]interface{})
	if !ok {
		return processRelationalData(message)
	}

	// Exactly timestamp_ms and one key/value pair are required, otherwise it is relational data
	if len(structuredAsMap) != 2 {
		return processRelationalData(message)
	}

	// A timestamp_ms field must be present, otherwise it is relational data
	if _, ok := structuredAsMap["timestamp_ms"]; !ok {
		return processRelationalData(message)
	}

	// We passed all checks, we can now process the data as time-series data
	return processTimeSeriesData(structuredAsMap, expectedTagNameForTimeseries)
}

// processTimeSeriesData extracts the timestamp, value name, and value (including its type) from the structured data
func processTimeSeriesData(structured map[string]interface{}, expectedTagNameForTimeseries *wrapperspb.StringValue) (*tagbrowserpluginprotobuf.EventTableEntry, error) {
	var valueContent anypb.Any
	var timestampMs int64

	// We loop over all key/value pairs of the message's payload (we previously checked that there are exactly two)
	for key, value := range structured {
		switch key {
		case "timestamp_ms":
			// Since we do not know the type of the timestamp_ms field, we need to convert it to an int64
			// But we can expect it to be some kind of numeric type, so we check its type and convert accordingly
			switch v := value.(type) {
			case int:
				timestampMs = int64(v)
			case int8:
				timestampMs = int64(v)
			case int16:
				timestampMs = int64(v)
			case int32:
				timestampMs = int64(v)
			case int64:
				timestampMs = v
			case uint:
				timestampMs = int64(v)
			case uint8:
				timestampMs = int64(v)
			case uint16:
				timestampMs = int64(v)
			case uint32:
				timestampMs = int64(v)
			case uint64:
				timestampMs = int64(v)
			case float32:
				timestampMs = int64(v)
			case float64:
				timestampMs = int64(v)
			default:
				return nil, fmt.Errorf("timestamp_ms must be numerical type, but was %T", v)
			}
		default:
			// The non-timestamp key/value pair will be handled here
			expectedValue := expectedTagNameForTimeseries.GetValue()
			if expectedValue == "" {
				return nil, fmt.Errorf("expected tag name for timeseries, but was empty")
			}
			if expectedValue != key {
				return nil, fmt.Errorf("expected tag name for timeseries %s, but was %s", expectedTagNameForTimeseries, key)
			}
			// We need to convert the value to a protobuf Any, which is a wrapper around a byte array
			byteValue, valueType, err := ToBytes(value)
			if err != nil {
				return nil, err
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
	}, nil
}

// processRelationalData extracts the message payload as bytes and returns them
func processRelationalData(message *service.Message) (*tagbrowserpluginprotobuf.EventTableEntry, error) {
	// For relational data, we don't do any parsing and just extract the payload as bytes
	valueBytes, err := message.AsBytes()
	if err != nil {
		// Note: This shall never happen, as AsBytes internally never returns errors
		return nil, err
	}
	return &tagbrowserpluginprotobuf.EventTableEntry{
		IsTimeseries: false,
		TimestampMs:  nil,
		Value:        &anypb.Any{TypeUrl: "golang/[]byte", Value: valueBytes},
	}, nil
}
