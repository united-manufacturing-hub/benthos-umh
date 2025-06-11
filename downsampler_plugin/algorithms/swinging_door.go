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
	threshold float64       // Compression deviation threshold
	minTime   time.Duration // Minimum time interval
	maxTime   time.Duration // Maximum time interval

	// Internal state - similar to deadband pattern
	basePoint      *Point  // Current base point (start of segment)
	lastKeptPoint  *Point  // Last point that was kept
	candidatePoint *Point  // Buffered point waiting for min_time to elapse
	maxLowerSlope  float64 // Maximum slope of lower door
	minUpperSlope  float64 // Minimum slope of upper door
}

// NewSwingingDoorAlgorithm creates a new SDT algorithm instance
func NewSwingingDoorAlgorithm(config map[string]interface{}) (DownsampleAlgorithm, error) {
	threshold := 1.0
	var minTime, maxTime time.Duration

	if cd, ok := config["threshold"]; ok {
		switch v := cd.(type) {
		case float64:
			threshold = v
		case int:
			threshold = float64(v)
		case string:
			var err error
			threshold, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid threshold value: %v", cd)
			}
		default:
			return nil, fmt.Errorf("invalid threshold type: %T", cd)
		}
	}

	// Validate threshold is not negative
	if threshold < 0 {
		return nil, fmt.Errorf("threshold cannot be negative, got: %f", threshold)
	}

	if cmin, ok := config["min_time"]; ok {
		switch v := cmin.(type) {
		case time.Duration:
			minTime = v
		case string:
			var err error
			minTime, err = time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid min_time value: %v", cmin)
			}
		default:
			return nil, fmt.Errorf("invalid min_time type: %T", cmin)
		}
	}

	if cmax, ok := config["max_time"]; ok {
		switch v := cmax.(type) {
		case time.Duration:
			maxTime = v
		case string:
			var err error
			maxTime, err = time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid max_time value: %v", cmax)
			}
		default:
			return nil, fmt.Errorf("invalid max_time type: %T", cmax)
		}
	}

	return &SwingingDoorAlgorithm{
		threshold: threshold,
		minTime:   minTime,
		maxTime:   maxTime,
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

	// Check if we have a candidate point that can now be processed
	if s.candidatePoint != nil {
		elapsed := currentPoint.Timestamp.Sub(s.lastKeptPoint.Timestamp)
		if elapsed >= s.minTime {
			// Process the buffered candidate point now that enough time has elapsed
			candidate := s.candidatePoint
			s.candidatePoint = nil

			// Process the candidate as if it just arrived
			shouldKeep, _ := s.processCandidatePoint(candidate)
			if shouldKeep {
				return true, nil
			}
			// If candidate wasn't kept, continue processing current point
		}
	}

	// Check maximum time constraint
	if s.maxTime > 0 {
		elapsed := currentPoint.Timestamp.Sub(s.lastKeptPoint.Timestamp)
		if elapsed >= s.maxTime {
			// Force keep due to max time
			s.candidatePoint = nil // Clear any buffered candidate
			s.basePoint = s.lastKeptPoint
			s.lastKeptPoint = currentPoint
			s.maxLowerSlope = math.Inf(-1)
			s.minUpperSlope = math.Inf(1)
			return true, nil
		}
	}

	// Process current point with envelope logic
	return s.processPointWithEnvelope(currentPoint)
}

// processCandidatePoint processes a buffered candidate point
func (s *SwingingDoorAlgorithm) processCandidatePoint(candidate *Point) (bool, error) {
	// Calculate time difference from base
	deltaTime := candidate.Timestamp.Sub(s.basePoint.Timestamp).Seconds()
	if deltaTime <= 0 {
		return false, nil
	}

	// Calculate slopes to candidate point's deviation bounds
	lowerSlope := (candidate.Value - s.threshold - s.basePoint.Value) / deltaTime
	upperSlope := (candidate.Value + s.threshold - s.basePoint.Value) / deltaTime

	// Update envelope bounds with candidate point
	newMaxLowerSlope := math.Max(s.maxLowerSlope, lowerSlope)
	newMinUpperSlope := math.Min(s.minUpperSlope, upperSlope)

	// Check if doors would intersect (envelope would collapse)
	if newMaxLowerSlope > newMinUpperSlope {
		// Candidate violates envelope, so keep it and start new segment
		s.basePoint = s.lastKeptPoint
		s.lastKeptPoint = candidate

		// Recalculate envelope from new base to candidate
		newDeltaTime := candidate.Timestamp.Sub(s.basePoint.Timestamp).Seconds()
		if newDeltaTime > 0 {
			s.maxLowerSlope = (candidate.Value - s.threshold - s.basePoint.Value) / newDeltaTime
			s.minUpperSlope = (candidate.Value + s.threshold - s.basePoint.Value) / newDeltaTime
		} else {
			s.maxLowerSlope = math.Inf(-1)
			s.minUpperSlope = math.Inf(1)
		}

		return true, nil
	}

	// Candidate fits within envelope, update bounds but don't keep
	s.maxLowerSlope = newMaxLowerSlope
	s.minUpperSlope = newMinUpperSlope
	return false, nil
}

// processPointWithEnvelope processes a point using envelope logic
func (s *SwingingDoorAlgorithm) processPointWithEnvelope(currentPoint *Point) (bool, error) {
	// Calculate time difference from base
	deltaTime := currentPoint.Timestamp.Sub(s.basePoint.Timestamp).Seconds()
	if deltaTime <= 0 {
		return false, nil
	}

	// Calculate slopes to current point's deviation bounds
	lowerSlope := (currentPoint.Value - s.threshold - s.basePoint.Value) / deltaTime
	upperSlope := (currentPoint.Value + s.threshold - s.basePoint.Value) / deltaTime

	// Update envelope bounds with current point
	newMaxLowerSlope := math.Max(s.maxLowerSlope, lowerSlope)
	newMinUpperSlope := math.Min(s.minUpperSlope, upperSlope)

	// Check if doors would intersect (envelope would collapse)
	if newMaxLowerSlope > newMinUpperSlope {
		// Current point violates envelope

		// Check min_time constraint before keeping
		if s.minTime > 0 {
			elapsed := currentPoint.Timestamp.Sub(s.lastKeptPoint.Timestamp)
			if elapsed < s.minTime {
				// Buffer this point for later processing
				s.candidatePoint = currentPoint
				return false, nil
			}
		}

		// Keep the point and start new segment
		s.basePoint = s.lastKeptPoint
		s.lastKeptPoint = currentPoint

		// Recalculate envelope from new base to current point
		newDeltaTime := currentPoint.Timestamp.Sub(s.basePoint.Timestamp).Seconds()
		if newDeltaTime > 0 {
			s.maxLowerSlope = (currentPoint.Value - s.threshold - s.basePoint.Value) / newDeltaTime
			s.minUpperSlope = (currentPoint.Value + s.threshold - s.basePoint.Value) / newDeltaTime
		} else {
			s.maxLowerSlope = math.Inf(-1)
			s.minUpperSlope = math.Inf(1)
		}

		return true, nil
	}

	// Point fits within envelope, update bounds but don't keep
	s.maxLowerSlope = newMaxLowerSlope
	s.minUpperSlope = newMinUpperSlope
	return false, nil
}

// Reset clears the algorithm's internal state
func (s *SwingingDoorAlgorithm) Reset() {
	s.basePoint = nil
	s.lastKeptPoint = nil
	s.candidatePoint = nil
	s.maxLowerSlope = math.Inf(-1)
	s.minUpperSlope = math.Inf(1)
}

// GetMetadata returns metadata string for annotation
func (s *SwingingDoorAlgorithm) GetMetadata() string {
	metadata := fmt.Sprintf("swinging_door(threshold=%.3f", s.threshold)
	if s.minTime > 0 {
		metadata += fmt.Sprintf(",min_time=%v", s.minTime)
	}
	if s.maxTime > 0 {
		metadata += fmt.Sprintf(",max_time=%v", s.maxTime)
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
		// Reject booleans - they should be handled by the plugin's equality logic
		return 0, fmt.Errorf("cannot convert boolean to number: %v", v)
	case string:
		// Reject strings - they should be handled by string-appropriate algorithms
		return 0, fmt.Errorf("cannot convert string to number: %s", v)
	default:
		return 0, fmt.Errorf("cannot convert type %T to float64", val)
	}
}
