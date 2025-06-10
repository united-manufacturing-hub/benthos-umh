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

package algorithms

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

func init() {
	Register("deadband", NewDeadbandAlgorithm)
}

// DeadbandAlgorithm implements deadband filtering
type DeadbandAlgorithm struct {
	threshold   float64
	maxInterval time.Duration
}

// NewDeadbandAlgorithm creates a new deadband algorithm instance
func NewDeadbandAlgorithm(config map[string]interface{}) (DownsampleAlgorithm, error) {
	threshold := 0.0
	var maxInterval time.Duration

	if t, ok := config["threshold"]; ok {
		switch v := t.(type) {
		case float64:
			threshold = v
		case int:
			threshold = float64(v)
		case string:
			var err error
			threshold, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid threshold value: %v", t)
			}
		default:
			return nil, fmt.Errorf("invalid threshold type: %T", t)
		}
	}

	if mi, ok := config["max_interval"]; ok {
		switch v := mi.(type) {
		case time.Duration:
			maxInterval = v
		case string:
			var err error
			maxInterval, err = time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid max_interval value: %v", mi)
			}
		default:
			return nil, fmt.Errorf("invalid max_interval type: %T", mi)
		}
	}

	return &DeadbandAlgorithm{
		threshold:   threshold,
		maxInterval: maxInterval,
	}, nil
}

// ShouldKeep determines if a data point should be kept based on deadband logic
func (d *DeadbandAlgorithm) ShouldKeep(current, previous interface{}, currentTime, prevTime time.Time) (bool, error) {
	// First point is always kept
	if previous == nil {
		return true, nil
	}

	// Check max interval if configured
	if d.maxInterval > 0 && !prevTime.IsZero() {
		if currentTime.Sub(prevTime) >= d.maxInterval {
			return true, nil
		}
	}

	// Convert values to numeric for comparison
	currentVal, err := d.toFloat64(current)
	if err != nil {
		return false, fmt.Errorf("cannot convert current value to numeric: %v", err)
	}

	prevVal, err := d.toFloat64(previous)
	if err != nil {
		return false, fmt.Errorf("cannot convert previous value to numeric: %v", err)
	}

	// Calculate absolute difference
	diff := math.Abs(currentVal - prevVal)

	// Keep if change exceeds threshold
	return diff >= d.threshold, nil
}

// Reset clears the algorithm's internal state (deadband is stateless)
func (d *DeadbandAlgorithm) Reset() {
	// Deadband algorithm doesn't maintain internal state
}

// GetMetadata returns metadata string for annotation
func (d *DeadbandAlgorithm) GetMetadata() string {
	if d.maxInterval > 0 {
		return fmt.Sprintf("deadband(threshold=%.3f,max_interval=%v)", d.threshold, d.maxInterval)
	}
	return fmt.Sprintf("deadband(threshold=%.3f)", d.threshold)
}

// GetName returns the algorithm name
func (d *DeadbandAlgorithm) GetName() string {
	return "deadband"
}

// toFloat64 converts various numeric types to float64
func (d *DeadbandAlgorithm) toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case bool:
		// Convert boolean to 0/1
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	case string:
		// Try to parse string as number
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("cannot convert string to number: %s", v)
	default:
		return 0, fmt.Errorf("cannot convert type %T to float64", val)
	}
}

// areEqual checks if two values are equal (for non-numeric types)
func (d *DeadbandAlgorithm) areEqual(a, b interface{}) bool {
	// First try numeric conversion
	aFloat, aErr := d.toFloat64(a)
	bFloat, bErr := d.toFloat64(b)

	if aErr == nil && bErr == nil {
		return aFloat == bFloat
	}

	// Handle complex types (maps, slices, etc.) using reflection
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}

	// Use deep equal for complex types
	if reflect.TypeOf(a).Kind() == reflect.Map ||
		reflect.TypeOf(a).Kind() == reflect.Slice ||
		reflect.TypeOf(a).Kind() == reflect.Array {
		return reflect.DeepEqual(a, b)
	}

	// For simple types, try JSON comparison as fallback
	aJSON, aJSONErr := json.Marshal(a)
	bJSON, bJSONErr := json.Marshal(b)

	if aJSONErr == nil && bJSONErr == nil {
		return string(aJSON) == string(bJSON)
	}

	// Last resort: use reflect.DeepEqual
	return reflect.DeepEqual(a, b)
}
