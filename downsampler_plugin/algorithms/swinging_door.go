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

// Point represents a data point with value and timestamp.
//
// This is used internally by the SDT algorithm to track envelope calculations.
type Point struct {
	Value     float64
	Timestamp time.Time
}

// SwingingDoorAlgorithm implements the industry-standard "emit-previous"
// Swinging Door Trending (SDT) algorithm used by PI Server, WinCC, and other
// historians for numeric time-series data compression.
//
// SDT maintains upper and lower envelope lines (the "doors") to determine when
// linear interpolation error would exceed the configured threshold. This provides
// more sophisticated compression than simple deadband filtering, especially for
// trending data.
//
// Algorithm behavior (emit-previous variant):
//   - First point always kept as envelope base
//   - Each new point updates envelope bounds
//   - When envelope collapses: emit the PREVIOUS point (last in-bounds sample)
//   - Violating point becomes pending and starts new envelope
//   - Call Flush() at end-of-stream to emit final pending point
//   - Optional min_time prevents high-frequency noise emission
//   - Optional max_time forces periodic heartbeat emission
//   - Special handling for very small time deltas to prevent slope overflow
//
// Behavioral guarantee: All compressed segments respect an absolute error ≤ threshold
// for every raw point, matching PI Server and AVEVA historian behavior.
//
// NOTE: This implementation follows the canonical PI/OSIsoft "emit-previous" convention.
// On corridor collapse we flush the last in-bounds point and keep the violating sample
// as the first point of the next segment. This can cause consecutive collapses when
// violating points immediately break the new corridor (e.g., "double collapse" scenarios).
//
// References:
//   - OSIsoft PI Server documentation on Swinging Door Trending
//   - gfoidl/DataCompression (C#) reference implementation
//   - Wonderware Historian white papers on SDT compression
//
// Configuration parameters:
//   - threshold (required): maximum interpolation error tolerance (>= 0)
//   - min_time (optional): minimum time between emissions for noise filtering
//   - max_time (optional): maximum time between emissions for heartbeat
//
// References:
//   - Swinging Door Trending (SDT) in PI System documentation
//   - "Data Compression" by E. S. Bristol, Control Engineering, 1989
//
// Thread safety: NOT goroutine-safe. Use separate instances for concurrent processing.
type SwingingDoorAlgorithm struct {
	threshold float64       // Compression deviation threshold
	minTime   time.Duration // Minimum time interval
	maxTime   time.Duration // Maximum time interval

	// Internal state for envelope tracking
	basePoint      *Point  // Current base point (start of segment)
	lastKeptPoint  *Point  // Last point that was emitted
	pendingPoint   *Point  // Point to emit when envelope collapses or on flush
	candidatePoint *Point  // Buffered point waiting for min_time to elapse
	maxLowerSlope  float64 // Maximum slope of lower door
	minUpperSlope  float64 // Minimum slope of upper door
}

// NewSwingingDoorAlgorithm creates a new SDT algorithm instance.
//
// Configuration map keys:
//   - "threshold": float64, int, or string - maximum interpolation error (>= 0)
//   - "min_time": time.Duration or string - minimum emission interval (optional)
//   - "max_time": time.Duration or string - maximum emission interval (optional)
//
// Returns error for:
//   - Missing or invalid threshold values
//   - Negative threshold values
//   - Invalid time duration strings
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

// ProcessPoint processes a new data point using SDT logic with emit-previous behavior
func (s *SwingingDoorAlgorithm) ProcessPoint(value float64, timestamp time.Time) (bool, error) {
	currentPoint := &Point{
		Value:     value,
		Timestamp: timestamp,
	}

	// First point is always kept
	if s.basePoint == nil {
		s.basePoint = currentPoint
		s.lastKeptPoint = currentPoint
		s.pendingPoint = currentPoint // Track for potential emission
		s.maxLowerSlope = math.Inf(-1)
		s.minUpperSlope = math.Inf(1)
		return true, nil
	}

	// Handle candidate point processing for min_time constraint
	if s.candidatePoint != nil {
		elapsed := timestamp.Sub(s.candidatePoint.Timestamp)
		if elapsed >= s.minTime {
			// Process the buffered candidate point now that enough time has elapsed
			candidate := s.candidatePoint
			s.candidatePoint = nil

			// Process the candidate
			shouldEmit, _ := s.processCandidatePoint(candidate)
			if shouldEmit {
				// After processing candidate, continue with current point
				return s.processPointWithEnvelope(currentPoint)
			}
		}
	}

	// Check maximum time constraint
	if s.maxTime > 0 {
		elapsed := timestamp.Sub(s.lastKeptPoint.Timestamp)
		if elapsed >= s.maxTime {
			// Force emit due to max time
			s.candidatePoint = nil // Clear any buffered candidate

			// In emit-previous mode, emit the pending point, then set current as new base
			keptPoint := s.pendingPoint
			s.basePoint = keptPoint
			s.lastKeptPoint = keptPoint
			s.pendingPoint = currentPoint
			s.maxLowerSlope = math.Inf(-1)
			s.minUpperSlope = math.Inf(1)
			return true, nil
		}
	}

	// Process current point with envelope logic
	return s.processPointWithEnvelope(currentPoint)
}

// processCandidatePoint processes a buffered candidate point with emit-previous behavior
func (s *SwingingDoorAlgorithm) processCandidatePoint(candidate *Point) (bool, error) {
	// Calculate time difference from base
	deltaTime := candidate.Timestamp.Sub(s.basePoint.Timestamp).Seconds()

	// Handle very small deltaTime to prevent slope overflow
	if deltaTime <= 1e-9 {
		// Envelope collapse - emit pending point and start new segment
		keptPoint := s.pendingPoint
		s.basePoint = keptPoint
		s.lastKeptPoint = keptPoint
		s.pendingPoint = candidate
		s.maxLowerSlope = math.Inf(-1)
		s.minUpperSlope = math.Inf(1)
		return true, nil
	}

	// Calculate slopes to candidate point's deviation bounds
	lowerSlope := (candidate.Value - s.threshold - s.basePoint.Value) / deltaTime
	upperSlope := (candidate.Value + s.threshold - s.basePoint.Value) / deltaTime

	// Update envelope bounds with candidate point
	newMaxLowerSlope := math.Max(s.maxLowerSlope, lowerSlope)
	newMinUpperSlope := math.Min(s.minUpperSlope, upperSlope)

	// Check if doors would intersect (envelope would collapse)
	if newMaxLowerSlope > newMinUpperSlope {
		// Candidate violates envelope - emit PREVIOUS point
		keptPoint := s.pendingPoint
		s.basePoint = keptPoint
		s.lastKeptPoint = keptPoint
		s.pendingPoint = candidate // Candidate becomes new pending

		// Start new envelope from emitted point to candidate
		newDeltaTime := candidate.Timestamp.Sub(s.basePoint.Timestamp).Seconds()
		if newDeltaTime > 1e-9 {
			s.maxLowerSlope = (candidate.Value - s.threshold - s.basePoint.Value) / newDeltaTime
			s.minUpperSlope = (candidate.Value + s.threshold - s.basePoint.Value) / newDeltaTime
		} else {
			s.maxLowerSlope = math.Inf(-1)
			s.minUpperSlope = math.Inf(1)
		}

		return true, nil
	}

	// Candidate fits within envelope - update pending point and envelope bounds
	s.pendingPoint = candidate
	s.maxLowerSlope = newMaxLowerSlope
	s.minUpperSlope = newMinUpperSlope
	return false, nil
}

// processPointWithEnvelope processes a point using envelope logic with emit-previous behavior
func (s *SwingingDoorAlgorithm) processPointWithEnvelope(currentPoint *Point) (bool, error) {
	// Calculate time difference from base
	deltaTime := currentPoint.Timestamp.Sub(s.basePoint.Timestamp).Seconds()

	// Handle very small deltaTime to prevent slope overflow
	if deltaTime <= 1e-9 {
		// Envelope collapse - emit pending point and start new segment
		keptPoint := s.pendingPoint
		s.basePoint = keptPoint
		s.lastKeptPoint = keptPoint
		s.pendingPoint = currentPoint
		s.maxLowerSlope = math.Inf(-1)
		s.minUpperSlope = math.Inf(1)
		return true, nil
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

		// Check min_time constraint before emitting
		if s.minTime > 0 {
			elapsed := currentPoint.Timestamp.Sub(s.lastKeptPoint.Timestamp)
			if elapsed < s.minTime {
				// Buffer this point for later processing
				s.candidatePoint = currentPoint
				return false, nil
			}
		}

		// EMIT PREVIOUS: emit the pending point
		keptPoint := s.pendingPoint
		s.basePoint = keptPoint
		s.lastKeptPoint = keptPoint
		s.pendingPoint = currentPoint // Current becomes new pending

		// Start new envelope from emitted point to current point
		newDeltaTime := currentPoint.Timestamp.Sub(s.basePoint.Timestamp).Seconds()
		if newDeltaTime > 1e-9 {
			s.maxLowerSlope = (currentPoint.Value - s.threshold - s.basePoint.Value) / newDeltaTime
			s.minUpperSlope = (currentPoint.Value + s.threshold - s.basePoint.Value) / newDeltaTime
		} else {
			s.maxLowerSlope = math.Inf(-1)
			s.minUpperSlope = math.Inf(1)
		}

		return true, nil
	}

	// Point fits within envelope - update pending point and envelope bounds
	s.pendingPoint = currentPoint
	s.maxLowerSlope = newMaxLowerSlope
	s.minUpperSlope = newMinUpperSlope
	return false, nil
}

// Flush returns any pending final point that should be emitted at end-of-stream
func (s *SwingingDoorAlgorithm) Flush() (*Point, error) {
	if s.pendingPoint != nil {
		// Return the pending point and clear it
		result := s.pendingPoint
		s.pendingPoint = nil
		return result, nil
	}
	return nil, nil
}

// Reset clears the algorithm's internal state
func (s *SwingingDoorAlgorithm) Reset() {
	s.basePoint = nil
	s.lastKeptPoint = nil
	s.pendingPoint = nil
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
