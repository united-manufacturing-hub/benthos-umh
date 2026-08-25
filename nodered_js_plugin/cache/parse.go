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

import (
	"encoding/json"
	"strconv"
)

// WatermarkKeys are the field names accepted as the watermark; exactly one must be set.
var WatermarkKeys = []string{"watermark", "timestamp_ms", "kafka_offset"}

// ParsePayload validates a JS-shaped {value, <watermark>} map and returns a Payload.
func ParsePayload(m map[string]any) (Payload, error) {
	value, hasValue := m["value"]
	if !hasValue {
		return Payload{}, ErrMissingValue
	}

	var (
		found   bool
		raw     any
		nFields int
	)
	for _, k := range WatermarkKeys {
		v, ok := m[k]
		if !ok {
			continue
		}
		nFields++
		if !found {
			raw = v
			found = true
		}
	}
	if nFields > 1 {
		return Payload{}, ErrMultipleWatermarks
	}
	if !found {
		return Payload{}, ErrMissingWatermark
	}
	wm, ok := int64FromAny(raw)
	if !ok {
		return Payload{}, ErrWatermarkNotNumeric
	}
	return Payload{Value: value, Watermark: wm}, nil
}

// int64FromAny bridges goja's int64/float64, bbolt's json.Number, and Benthos meta strings to int64.
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
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		if err == nil {
			return i, true
		}
		f, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0, false
		}
		return int64(f), true
	default:
		return 0, false
	}
}
