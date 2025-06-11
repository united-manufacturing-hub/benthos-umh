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
	"fmt"
	"math"
	"strconv"
	"time"
)

func init() {
	Register("deadband", NewDeadbandAlgorithm)
}

// DeadbandAlgorithm implements deadband filtering for numeric time-series data.
//
// The deadband algorithm keeps data points only when they differ from the last
// kept value by more than the configured threshold. This provides simple but
// effective compression for slowly changing values with noise.
//
// Algorithm behavior:
//   - First point is always kept
//   - Subsequent points are kept if: abs(value - lastKeptValue) >= threshold
//   - Optional max_time constraint forces periodic emission regardless of value change
//   - Special case: threshold=0 keeps any change but drops exact repeats
//
// Configuration parameters:
//   - threshold (required): minimum change magnitude to keep a point
//   - max_time (optional): maximum time between kept points for heartbeat emission
//
// Thread safety: NOT goroutine-safe. Use separate instances for concurrent processing.
type DeadbandAlgorithm struct {
	threshold float64
	maxTime   time.Duration

	// Internal state
	lastKeptValue float64
	lastKeptTime  time.Time
	hasKeptValue  bool // Track if we have kept any value yet
}

// NewDeadbandAlgorithm creates a new deadband algorithm instance.
//
// Configuration map keys:
//   - "threshold": float64, int, or string - minimum change required (>= 0)
//   - "max_time": time.Duration or string - maximum time between emissions (optional)
//
// Returns error for:
//   - Missing or invalid threshold values
//   - Negative threshold values
//   - Invalid max_time duration strings
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

	// Validate threshold is not negative
	if threshold < 0 {
		return nil, fmt.Errorf("threshold cannot be negative: %v", threshold)
	}

	if mi, ok := config["max_time"]; ok {
		switch v := mi.(type) {
		case time.Duration:
			maxInterval = v
		case string:
			var err error
			maxInterval, err = time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid max_time value: %v", mi)
			}
		default:
			return nil, fmt.Errorf("invalid max_time type: %T", mi)
		}
	}

	return &DeadbandAlgorithm{
		threshold: threshold,
		maxTime:   maxInterval,
	}, nil
}

// ProcessPoint processes a new data point and returns whether it should be kept
func (d *DeadbandAlgorithm) ProcessPoint(value float64, timestamp time.Time) (bool, error) {
	// First point is always kept
	if !d.hasKeptValue {
		d.lastKeptValue = value
		d.lastKeptTime = timestamp
		d.hasKeptValue = true
		return true, nil
	}

	// Handle time overflow and duration limits
	if d.lastKeptTime.IsZero() {
		d.lastKeptValue = value
		d.lastKeptTime = timestamp
		return true, nil
	}

	// Check maximum interval constraint
	if d.maxTime > 0 && timestamp.Sub(d.lastKeptTime) >= d.maxTime {
		d.lastKeptValue = value
		d.lastKeptTime = timestamp
		return true, nil
	}

	// Check threshold constraint
	delta := math.Abs(value - d.lastKeptValue)

	// Special case for zero threshold: keep any change, drop exact repeats
	if d.threshold == 0 {
		if delta > 0 {
			d.lastKeptValue = value
			d.lastKeptTime = timestamp
			return true, nil
		}
		return false, nil
	}

	if delta >= d.threshold {
		d.lastKeptValue = value
		d.lastKeptTime = timestamp
		return true, nil
	}

	return false, nil
}

// Reset clears the algorithm's internal state
func (d *DeadbandAlgorithm) Reset() {
	d.lastKeptValue = 0
	d.lastKeptTime = time.Time{}
	d.hasKeptValue = false
}

// GetMetadata returns metadata string for annotation
func (d *DeadbandAlgorithm) GetMetadata() string {
	if d.maxTime > 0 {
		return fmt.Sprintf("deadband(threshold=%.3f,max_time=%v)", d.threshold, d.maxTime)
	}
	return fmt.Sprintf("deadband(threshold=%.3f)", d.threshold)
}

// GetName returns the algorithm name
func (d *DeadbandAlgorithm) GetName() string {
	return "deadband"
}
