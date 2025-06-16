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

	UMH-Core Time-Series Payload Format Specification:
	==================================================

	UMH-Core recognizes a strict time-series payload format that differs from UMH Classic.

	Valid UMH-Core Time-Series Format:
	- Must be valid JSON object
	- Must contain exactly 2 keys: "timestamp_ms" and "value"
	- "timestamp_ms": numeric timestamp in milliseconds since Unix epoch
	- "value": any scalar value (number, boolean, string)

	Example VALID Time-Series:
	{"timestamp_ms": 1717083000000, "value": 23.4}
	{"timestamp_ms": 1717083000000, "value": true}
	{"timestamp_ms": 1717083000000, "value": "running"}

	Example UMH Classic format (processed as RELATIONAL in UMH-Core):
	{"timestamp_ms": 1717083000000, "temperature": 23.4, "humidity": 42.1}
	{"timestamp_ms": 1717083000000, "pressure": 1013.25}

	Important: The _historian data contract in UMH-Core will only process messages as
	time-series when they contain exactly one tag named "value". All other formats,
	including UMH Classic multi-tag messages, will be processed as relational data.

	For migration from UMH Classic to UMH-Core time-series format, use the appropriate
	benthos-umh plugin to convert your data.

	Any payload that is not a valid JSON object will be rejected as invalid.

	See: https://docs.umh.app/usage/unified-namespace/payload-formats
*/

import (
	"fmt"
	"math"

	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/types/known/anypb"
)

// messageToEvent will convert a benthos message, into an EventTableEntry
// It checks if the incoming message is a valid time-series message, otherwise it handles it as relational data
//
// UMH-Core Format Requirements:
// - Time-Series: Must have exactly 2 keys: "timestamp_ms" and "value"
// - Relational: Must be valid JSON object (including UMH Classic format)
// - Invalid: Non-JSON or non-object data will result in an error
//
// UMH-Core Time-Series Format Requirements:
// - Must be valid JSON object
// - Must have exactly 2 keys: "timestamp_ms" and "value"
// - "timestamp_ms": numeric timestamp in milliseconds since Unix epoch
// - "value": any scalar value (number, boolean, string)
//
// Example valid time-series: {"timestamp_ms": 1717083000000, "value": 23.4}
// Example valid relational (UMH Classic): {"timestamp_ms": 1717083000000, "temperature": 23.4}
// Example valid relational: {"order_id": 123, "customer": "ACME Corp", "items": {...}}
// Example invalid: ["array", "data"] or "string" or 123 or non-JSON
//
// Any format that is not a valid JSON object will return an error
func messageToEvent(message *service.Message) (*tagbrowserpluginprotobuf.EventTableEntry, error) {
	// 1. Try to get structured data (valid JSON required for both time-series and relational)
	structured, err := message.AsStructured()
	if err != nil {
		// This is not valid JSON - return error for invalid format
		return nil, fmt.Errorf("invalid JSON format: %v", err)
	}

	// 2. Relational data must be a JSON object (map), not arrays or primitives
	structuredAsMap, ok := structured.(map[string]interface{})
	if !ok {
		// Valid JSON but not an object - this is invalid (arrays, primitives, etc.)
		return nil, fmt.Errorf("relational data must be a JSON object, got %T", structured)
	}

	// 3. Check if it matches UMH-Core time-series format exactly
	if len(structuredAsMap) == 2 {
		_, hasTimestamp := structuredAsMap["timestamp_ms"]
		_, hasValue := structuredAsMap["value"]
		if hasTimestamp && hasValue {
			// Valid UMH-Core time-series format
			return processTimeSeriesData(structuredAsMap)
		}
	}

	// 4. If it doesn't match time-series format, process as relational data
	// (This includes UMH Classic format with arbitrary key names)
	return processRelationalData(message)
}

// determineScalarType determines the ScalarType enum based on the Go type and type string
func determineScalarType(value interface{}, valueType string) tagbrowserpluginprotobuf.ScalarType {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return tagbrowserpluginprotobuf.ScalarType_NUMERIC
	case bool:
		return tagbrowserpluginprotobuf.ScalarType_BOOLEAN
	case string:
		return tagbrowserpluginprotobuf.ScalarType_STRING
	default:
		// For other types (JSON objects, arrays, etc.), treat as string
		return tagbrowserpluginprotobuf.ScalarType_STRING
	}
}

// processTimeSeriesData extracts the timestamp and value from the structured data
// For UMH-Core, time-series data must have exactly these two keys:
// 1. "timestamp_ms" - timestamp in milliseconds (numeric)
// 2. "value" - the scalar value (any type)
// Any other format is considered relational data
func processTimeSeriesData(structured map[string]interface{}) (*tagbrowserpluginprotobuf.EventTableEntry, error) {
	var valueContent anypb.Any
	var timestampMs int64
	var scalarType tagbrowserpluginprotobuf.ScalarType
	var err error

	// Validate that we have exactly the required keys for UMH-Core time-series format
	timestampValue, hasTimestamp := structured["timestamp_ms"]
	value, hasValue := structured["value"]

	if !hasTimestamp || !hasValue {
		return nil, fmt.Errorf("time-series data must have exactly 'timestamp_ms' and 'value' keys")
	}

	// Process timestamp_ms
	timestampMs, err = interfaceToInt64(timestampValue)
	if err != nil {
		return nil, err
	}

	// Process value
	var byteValue []byte
	var valueType string
	byteValue, valueType, err = ToBytes(value)
	if err != nil {
		return nil, err
	}
	valueContent = anypb.Any{
		TypeUrl: fmt.Sprintf("golang/%s", valueType),
		Value:   byteValue,
	}
	// Determine the scalar type for the protobuf
	scalarType = determineScalarType(value, valueType)

	// Create the TimeSeriesPayload
	timeSeriesPayload := &tagbrowserpluginprotobuf.TimeSeriesPayload{
		ScalarType:  scalarType,
		Value:       &valueContent,
		TimestampMs: timestampMs,
	}

	// Return EventTableEntry with the TimeSeriesPayload using the oneof pattern
	return &tagbrowserpluginprotobuf.EventTableEntry{
		Payload: &tagbrowserpluginprotobuf.EventTableEntry_Ts{
			Ts: timeSeriesPayload,
		},
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

	// Create the RelationalPayload
	relationalPayload := &tagbrowserpluginprotobuf.RelationalPayload{
		Json: valueBytes,
	}

	// Return EventTableEntry with the RelationalPayload using the oneof pattern
	return &tagbrowserpluginprotobuf.EventTableEntry{
		Payload: &tagbrowserpluginprotobuf.EventTableEntry_Rel{
			Rel: relationalPayload,
		},
	}, nil
}

func interfaceToInt64(value interface{}) (int64, error) {
	var valueAsInt64 int64
	switch v := value.(type) {
	case int:
		valueAsInt64 = int64(v)
	case int8:
		valueAsInt64 = int64(v)
	case int16:
		valueAsInt64 = int64(v)
	case int32:
		valueAsInt64 = int64(v)
	case int64:
		valueAsInt64 = v
	case uint:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("value %d out of int64 range", v)
		}
		valueAsInt64 = int64(v)
	case uint8:
		valueAsInt64 = int64(v)
	case uint16:
		valueAsInt64 = int64(v)
	case uint32:
		valueAsInt64 = int64(v)
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("value %d out of int64 range", v)
		}
		valueAsInt64 = int64(v)
	case float32:
		if v > float32(math.MaxInt64) || v < float32(math.MinInt64) {
			return 0, fmt.Errorf("value %f out of int64 range", v)
		}
		valueAsInt64 = int64(v)
	case float64:
		if v > float64(math.MaxInt64) || v < float64(math.MinInt64) {
			return 0, fmt.Errorf("value %f out of int64 range", v)
		}
		valueAsInt64 = int64(v)
	default:
		return 0, fmt.Errorf("timestamp_ms must be numerical type, but was %T", v)
	}
	return valueAsInt64, nil
}
