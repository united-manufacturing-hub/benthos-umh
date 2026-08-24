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

package cache

import "encoding/json"

// ParsePayload validates a JS-shaped {value, timestamp_ms} map and returns a Payload.
func ParsePayload(m map[string]any) (Payload, error) {
	value, hasValue := m["value"]
	if !hasValue {
		return Payload{}, ErrMissingValue
	}
	tsRaw, hasTs := m["timestamp_ms"]
	if !hasTs {
		return Payload{}, ErrMissingTimestamp
	}
	ts, ok := int64FromAny(tsRaw)
	if !ok {
		return Payload{}, ErrTimestampNotNumeric
	}
	return Payload{Value: value, TimestampMs: ts}, nil
}

// int64FromAny bridges goja's mixed int64/float64 and bbolt's json.Number to int64.
func int64FromAny(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	case uint64:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint:
		return int64(n), true
	case float64:
		return int64(n), true
	case float32:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		if err == nil {
			return i, true
		}
		f, err := n.Float64()
		if err != nil {
			return 0, false
		}
		return int64(f), true
	default:
		return 0, false
	}
}
