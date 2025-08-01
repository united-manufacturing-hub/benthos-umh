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

	SIZE LIMITS SPECIFICATION:
	==========================

	Rule Statement:
	> Only time-series payloads are size-capped at 1 MiB (after JSON decoding); relational payloads have no hard limit.

	| Type                   | Typical content                                                      | Size limit enforced                    |
	| ---------------------- | -------------------------------------------------------------------- | --------------------------------------- |
	| **Time-series / Tags** | Exactly two keys: timestamp_ms and value                           | ≤ 1 MiB decoded                        |
	| **Relational / JSON**  | One self-contained business record or multi-field snapshot         | **No hard cap** (broker limit only)    |

	Implementation:
	- Time-series: must stay small – fits in one DB row / cache line
	- Relational: unbounded for now; rely on broker / infra defaults
*/

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	// Time-series: 1 MiB since this is the default Kafka message size limit
	// Usually our payloads will be much smaller
	MaxTimeSeriesPayloadBytes = 1_048_576 // 1 MiB - Kafka message size limit

	// Field names for UMH-Core time-series format
	FieldTimestamp = "timestamp_ms"
	FieldValue     = "value"
)

var (
	// ErrNotObject indicates that the payload is not a JSON object.
	ErrNotObject = errors.New("payload is not a JSON object")
	// ErrInvalidJSON indicates invalid JSON format.
	ErrInvalidJSON = errors.New("invalid JSON format")
	// ErrInvalidTimeSeries indicates time-series payload must have exactly 'timestamp_ms' and 'value' keys.
	ErrInvalidTimeSeries = errors.New("time-series payload must have exactly 'timestamp_ms' and 'value' keys")
	// ErrNaNOrInf indicates that the value may not be NaN or Inf.
	ErrNaNOrInf = errors.New("value may not be NaN or Inf")
	// ErrPayloadTooLarge indicates that payload size exceeds maximum allowed size.
	ErrPayloadTooLarge = errors.New("payload size exceeds maximum allowed size")
	// ErrPrecisionLoss indicates that timestamp_ms conversion would cause precision loss.
	ErrPrecisionLoss = errors.New("timestamp_ms conversion would cause precision loss")
	// ErrInvalidTimestamp indicates that timestamp_ms must be numerical type.
	ErrInvalidTimestamp = errors.New("timestamp_ms must be numerical type")
	// ErrNilValue indicates that a required value cannot be nil.
	ErrNilValue = errors.New("value cannot be nil")
	// ErrUnsupportedType indicates an unsupported value type for time-series.
	ErrUnsupportedType = errors.New("unsupported value type for time-series")
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
func messageToEvent(message *service.Message) (*proto.EventTableEntry, error) {
	// 1. Try to get structured data (valid JSON required for both time-series and relational)
	structured, err := message.AsStructured()
	if err != nil {
		// This is not valid JSON - return error for invalid format
		return nil, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}

	// 2. Relational data must be a JSON object (map), not arrays or primitives
	structuredAsMap, ok := structured.(map[string]interface{})
	if !ok {
		// Valid JSON but not an object - this is invalid (arrays, primitives, etc.)
		return nil, fmt.Errorf("%w, got %T", ErrNotObject, structured)
	}

	// 3. Check if it matches UMH-Core time-series format exactly
	if len(structuredAsMap) == 2 {
		_, hasTimestamp := structuredAsMap[FieldTimestamp]
		_, hasValue := structuredAsMap[FieldValue]
		if hasTimestamp && hasValue {
			// Valid UMH-Core time-series format
			return processTimeSeriesData(structuredAsMap)
		}
	}

	// 4. If it doesn't match time-series format, process as relational data
	// (This includes UMH Classic format with arbitrary key names)
	return processRelationalStructured(structuredAsMap)
}

// processTimeSeriesData extracts the timestamp and value from the structured data
// For UMH-Core, time-series data must have exactly these two keys:
// 1. "timestamp_ms" - timestamp in milliseconds (numeric)
// 2. "value" - the scalar value (any type)
// Any other format is considered relational data
func processTimeSeriesData(structured map[string]interface{}) (*proto.EventTableEntry, error) {
	var timestampMs int64
	var err error

	// Validate that we have exactly the required keys for UMH-Core time-series format
	timestampValue, hasTimestamp := structured[FieldTimestamp]
	value, hasValue := structured[FieldValue]

	if !hasTimestamp || !hasValue {
		return nil, fmt.Errorf("%w: must have exactly '%s' and '%s' keys", ErrInvalidTimeSeries, FieldTimestamp, FieldValue)
	}

	// Validate that neither timestamp nor value is nil
	if timestampValue == nil {
		return nil, fmt.Errorf("%w: %s cannot be nil", ErrNilValue, FieldTimestamp)
	}
	if value == nil {
		return nil, fmt.Errorf("%w: %s cannot be nil", ErrNilValue, FieldValue)
	}

	// Process timestamp_ms
	timestampMs, err = interfaceToInt64(timestampValue)
	if err != nil {
		return nil, err
	}

	// Validate special float values before processing
	if f, ok := value.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
		return nil, ErrNaNOrInf
	}

	// Check payload size by serializing the value (approximation)
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value for size check: %w", err)
	}
	if len(valueBytes) > MaxTimeSeriesPayloadBytes {
		return nil, fmt.Errorf("%w: time-series payload %d bytes exceeds maximum allowed size of %d bytes", ErrPayloadTooLarge, len(valueBytes), MaxTimeSeriesPayloadBytes)
	}

	// Create the TimeSeriesPayload with oneof wrapper types
	timeSeriesPayload := &proto.TimeSeriesPayload{
		TimestampMs: timestampMs,
	}

	// Set the value and scalar type based on the Go type
	switch v := value.(type) {
	case json.Number:
		// Handle json.Number type - try parsing as different types
		if floatVal, err := strconv.ParseFloat(string(v), 64); err == nil {
			// Successfully parsed as float - check for special values
			if math.IsNaN(floatVal) || math.IsInf(floatVal, 0) {
				return nil, ErrNaNOrInf
			}
			timeSeriesPayload.ScalarType = proto.ScalarType_NUMERIC
			timeSeriesPayload.Value = &proto.TimeSeriesPayload_NumericValue{
				NumericValue: &wrapperspb.DoubleValue{Value: floatVal},
			}
		} else {
			// If it can't be parsed as float, treat as string
			timeSeriesPayload.ScalarType = proto.ScalarType_STRING
			timeSeriesPayload.Value = &proto.TimeSeriesPayload_StringValue{
				StringValue: &wrapperspb.StringValue{Value: string(v)},
			}
		}
	case float64:
		timeSeriesPayload.ScalarType = proto.ScalarType_NUMERIC
		timeSeriesPayload.Value = &proto.TimeSeriesPayload_NumericValue{
			NumericValue: &wrapperspb.DoubleValue{Value: v},
		}
	case float32:
		timeSeriesPayload.ScalarType = proto.ScalarType_NUMERIC
		timeSeriesPayload.Value = &proto.TimeSeriesPayload_NumericValue{
			NumericValue: &wrapperspb.DoubleValue{Value: float64(v)},
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		// Convert all integer types to float64 for consistent handling
		var floatVal float64
		switch v := v.(type) {
		case int:
			// ✅ FIX: Check precision safety for potentially 64-bit int
			const maxSafeInt = 1 << 53 // 2^53 = 9,007,199,254,740,992
			if v > maxSafeInt || v < -maxSafeInt {
				return nil, fmt.Errorf("int value %d exceeds safe float64 precision range (±2^53)", v)
			}
			floatVal = float64(v)
		case int8:
			floatVal = float64(v)
		case int16:
			floatVal = float64(v)
		case int32:
			floatVal = float64(v)
		case int64:
			// ✅ FIX: Check precision safety before converting to float64
			const maxSafeInt64 = 1 << 53 // 2^53 = 9,007,199,254,740,992
			if v > maxSafeInt64 || v < -maxSafeInt64 {
				return nil, fmt.Errorf("int64 value %d exceeds safe float64 precision range (±2^53)", v)
			}
			floatVal = float64(v)
		case uint:
			// ✅ FIX: Check precision safety for potentially 64-bit uint
			const maxSafeUint = 1 << 53 // 2^53 = 9,007,199,254,740,992
			if v > maxSafeUint {
				return nil, fmt.Errorf("uint value %d exceeds safe float64 precision range (2^53)", v)
			}
			floatVal = float64(v)
		case uint8:
			floatVal = float64(v)
		case uint16:
			floatVal = float64(v)
		case uint32:
			floatVal = float64(v)
		case uint64:
			// ✅ FIX: Check precision safety before converting to float64
			const maxSafeUint64 = 1 << 53 // 2^53 = 9,007,199,254,740,992
			if v > maxSafeUint64 {
				return nil, fmt.Errorf("uint64 value %d exceeds safe float64 precision range (2^53)", v)
			}
			floatVal = float64(v)
		}
		timeSeriesPayload.ScalarType = proto.ScalarType_NUMERIC
		timeSeriesPayload.Value = &proto.TimeSeriesPayload_NumericValue{
			NumericValue: &wrapperspb.DoubleValue{Value: floatVal},
		}
	case string:
		timeSeriesPayload.ScalarType = proto.ScalarType_STRING
		timeSeriesPayload.Value = &proto.TimeSeriesPayload_StringValue{
			StringValue: &wrapperspb.StringValue{Value: v},
		}
	case bool:
		timeSeriesPayload.ScalarType = proto.ScalarType_BOOLEAN
		timeSeriesPayload.Value = &proto.TimeSeriesPayload_BooleanValue{
			BooleanValue: &wrapperspb.BoolValue{Value: v},
		}
	default:
		return nil, fmt.Errorf("%w: cannot handle type %T", ErrUnsupportedType, value)
	}

	// Return EventTableEntry with the TimeSeriesPayload using the oneof pattern
	return &proto.EventTableEntry{
		PayloadFormat: proto.PayloadFormat_TIMESERIES,
		Payload: &proto.EventTableEntry_Ts{
			Ts: timeSeriesPayload,
		},
	}, nil
}

// processRelationalStructured processes structured data directly as relational without re-serialization
func processRelationalStructured(structured map[string]interface{}) (*proto.EventTableEntry, error) {
	// Convert the structured data to JSON bytes for storage
	valueBytes, err := json.Marshal(structured)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal relational data: %w", err)
	}

	// Create the RelationalPayload
	relationalPayload := &proto.RelationalPayload{
		Json: valueBytes,
	}

	// Return EventTableEntry with the RelationalPayload using the oneof pattern
	return &proto.EventTableEntry{
		PayloadFormat: proto.PayloadFormat_RELATIONAL,
		Payload: &proto.EventTableEntry_Rel{
			Rel: relationalPayload,
		},
	}, nil
}

func interfaceToInt64(value interface{}) (int64, error) {
	var valueAsInt64 int64
	switch v := value.(type) {
	case json.Number:
		// Handle json.Number type which can occur when JSON is parsed with UseNumber()
		intVal, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			// Try parsing as float if it's not an integer
			floatVal, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return 0, fmt.Errorf("failed to parse json.Number '%s': %w", string(v), err)
			}
			if floatVal > float64(math.MaxInt64) || floatVal < float64(math.MinInt64) {
				return 0, fmt.Errorf("value %f out of int64 range", floatVal)
			}
			if floatVal != math.Trunc(floatVal) {
				return 0, fmt.Errorf("%w: %f is not an integer", ErrPrecisionLoss, floatVal)
			}
			valueAsInt64 = int64(floatVal)
		} else {
			valueAsInt64 = intVal
		}
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
		if float64(v) != math.Trunc(float64(v)) {
			return 0, fmt.Errorf("%w: %f is not an integer", ErrPrecisionLoss, float64(v))
		}
		valueAsInt64 = int64(v)
	case float64:
		if v > float64(math.MaxInt64) || v < float64(math.MinInt64) {
			return 0, fmt.Errorf("value %f out of int64 range", v)
		}
		if v != math.Trunc(v) {
			return 0, fmt.Errorf("%w: %f is not an integer", ErrPrecisionLoss, v)
		}
		valueAsInt64 = int64(v)
	default:
		return 0, fmt.Errorf("%w, but was %T", ErrInvalidTimestamp, v)
	}
	return valueAsInt64, nil
}
