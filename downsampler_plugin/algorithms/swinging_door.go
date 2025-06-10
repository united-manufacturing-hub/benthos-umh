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
	Register("swinging_door", NewSwingingDoorAlgorithm)
}

// Point represents a data point with value and timestamp
type Point struct {
	Value     float64
	Timestamp time.Time
}

// SwingingDoorAlgorithm implements the Swinging Door Trending (SDT) algorithm
type SwingingDoorAlgorithm struct {
	compDev     float64       // Compression deviation threshold
	compMinTime time.Duration // Minimum time interval
	compMaxTime time.Duration // Maximum time interval

	// Internal state - similar to deadband pattern
	basePoint     *Point  // Current base point (start of segment)
	lastKeptPoint *Point  // Last point that was kept
	maxLowerSlope float64 // Maximum slope of lower door
	minUpperSlope float64 // Minimum slope of upper door
}

// NewSwingingDoorAlgorithm creates a new SDT algorithm instance
func NewSwingingDoorAlgorithm(config map[string]interface{}) (DownsampleAlgorithm, error) {
	compDev := 1.0
	var compMinTime, compMaxTime time.Duration

	if cd, ok := config["comp_dev"]; ok {
		switch v := cd.(type) {
		case float64:
			compDev = v
		case int:
			compDev = float64(v)
		case string:
			var err error
			compDev, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid comp_dev value: %v", cd)
			}
		default:
			return nil, fmt.Errorf("invalid comp_dev type: %T", cd)
		}
	}

	// Validate comp_dev is not negative
	if compDev < 0 {
		return nil, fmt.Errorf("comp_dev cannot be negative, got: %f", compDev)
	}

	if cmin, ok := config["comp_min_time"]; ok {
		switch v := cmin.(type) {
		case time.Duration:
			compMinTime = v
		case string:
			var err error
			compMinTime, err = time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid comp_min_time value: %v", cmin)
			}
		default:
			return nil, fmt.Errorf("invalid comp_min_time type: %T", cmin)
		}
	}

	if cmax, ok := config["comp_max_time"]; ok {
		switch v := cmax.(type) {
		case time.Duration:
			compMaxTime = v
		case string:
			var err error
			compMaxTime, err = time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid comp_max_time value: %v", cmax)
			}
		default:
			return nil, fmt.Errorf("invalid comp_max_time type: %T", cmax)
		}
	}

	return &SwingingDoorAlgorithm{
		compDev:     compDev,
		compMinTime: compMinTime,
		compMaxTime: compMaxTime,
	}, nil
}

// ProcessPoint processes a new data point using SDT logic
func (s *SwingingDoorAlgorithm) ProcessPoint(value interface{}, timestamp time.Time) (bool, error) {
	// Convert value to float64
	floatVal, err := s.toFloat64(value)
	if err != nil {
		return false, fmt.Errorf("cannot convert value to numeric: %v", err)
	}

	currentPoint := &Point{
		Value:     floatVal,
		Timestamp: timestamp,
	}

	// First point is always kept
	if s.basePoint == nil {
		s.basePoint = currentPoint
		s.lastKeptPoint = currentPoint
		s.maxLowerSlope = math.Inf(-1)
		s.minUpperSlope = math.Inf(1)
		return true, nil
	}

	// Check maximum time constraint
	if s.compMaxTime > 0 {
		elapsed := currentPoint.Timestamp.Sub(s.lastKeptPoint.Timestamp)
		if elapsed >= s.compMaxTime {
			// Force keep due to max time
			s.basePoint = s.lastKeptPoint
			s.lastKeptPoint = currentPoint
			s.maxLowerSlope = math.Inf(-1)
			s.minUpperSlope = math.Inf(1)
			return true, nil
		}
	}

	// Check minimum time constraint
	if s.compMinTime > 0 {
		elapsed := currentPoint.Timestamp.Sub(s.lastKeptPoint.Timestamp)
		if elapsed < s.compMinTime {
			// Too soon to keep another point
			return false, nil
		}
	}

	// Calculate time difference from base
	deltaTime := currentPoint.Timestamp.Sub(s.basePoint.Timestamp).Seconds()
	if deltaTime <= 0 {
		// Same or earlier timestamp than base
		return false, nil
	}

	// Calculate slopes to current point's deviation bounds
	lowerSlope := (currentPoint.Value - s.compDev - s.basePoint.Value) / deltaTime
	upperSlope := (currentPoint.Value + s.compDev - s.basePoint.Value) / deltaTime

	// Update envelope bounds with this point
	newMaxLowerSlope := math.Max(s.maxLowerSlope, lowerSlope)
	newMinUpperSlope := math.Min(s.minUpperSlope, upperSlope)

	// Check if doors would intersect (envelope would collapse)
	if newMaxLowerSlope > newMinUpperSlope {
		// Doors intersect - this point violates the envelope, so keep it
		// and start a new segment
		s.basePoint = s.lastKeptPoint
		s.lastKeptPoint = currentPoint

		// Recalculate envelope from new base to current point
		newDeltaTime := currentPoint.Timestamp.Sub(s.basePoint.Timestamp).Seconds()
		if newDeltaTime > 0 {
			s.maxLowerSlope = (currentPoint.Value - s.compDev - s.basePoint.Value) / newDeltaTime
			s.minUpperSlope = (currentPoint.Value + s.compDev - s.basePoint.Value) / newDeltaTime
		} else {
			s.maxLowerSlope = math.Inf(-1)
			s.minUpperSlope = math.Inf(1)
		}

		return true, nil
	}

	// Point fits within current envelope - update envelope but don't keep point
	s.maxLowerSlope = newMaxLowerSlope
	s.minUpperSlope = newMinUpperSlope
	return false, nil
}

// Reset clears the algorithm's internal state
func (s *SwingingDoorAlgorithm) Reset() {
	s.basePoint = nil
	s.lastKeptPoint = nil
	s.maxLowerSlope = math.Inf(-1)
	s.minUpperSlope = math.Inf(1)
}

// GetMetadata returns metadata string for annotation
func (s *SwingingDoorAlgorithm) GetMetadata() string {
	metadata := fmt.Sprintf("swinging_door(comp_dev=%.3f", s.compDev)
	if s.compMinTime > 0 {
		metadata += fmt.Sprintf(",comp_min_time=%v", s.compMinTime)
	}
	if s.compMaxTime > 0 {
		metadata += fmt.Sprintf(",comp_max_time=%v", s.compMaxTime)
	}
	metadata += ")"
	return metadata
}

// GetName returns the algorithm name
func (s *SwingingDoorAlgorithm) GetName() string {
	return "swinging_door"
}

// toFloat64 converts various numeric types to float64
func (s *SwingingDoorAlgorithm) toFloat64(val interface{}) (float64, error) {
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
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("cannot convert string to number: %s", v)
	default:
		return 0, fmt.Errorf("cannot convert type %T to float64", val)
	}
}
