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

// TODO: The calling package needs to handle boolean values appropriately before
// passing them to these algorithms, as boolean logic should not be implemented
// at the algorithm level.

// Package algorithms provides data compression algorithms for downsampling.
//
// The deadband algorithm only supports numeric values (int, float types).
// Boolean values and other non-numeric types should be handled by the calling
// package before invoking the algorithm.
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

// DeadbandAlgorithm implements deadband filtering for numeric values only.
//
// Boolean values are NOT supported - they should be handled by the calling package.
// This algorithm maintains state and is NOT goroutine-safe. Use separate instances
// for concurrent processing or add external synchronization.
type DeadbandAlgorithm struct {
	threshold float64
	maxTime   time.Duration

	// Internal state
	lastKeptValue float64
	lastKeptTime  time.Time
	hasKeptValue  bool // Track if we have kept any value yet

	// Mutex for thread safety - currently commented out for performance
	// Uncomment if goroutine safety is needed
	// mu sync.Mutex
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
