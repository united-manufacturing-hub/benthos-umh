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
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// ProcessorWrapper provides a high-level interface for compression algorithms that
// handles type conversion, out-of-order data, and special data types automatically.
//
// The wrapper sits between your application and the core compression algorithms,
// providing these conveniences:
//
// Type Conversion:
//   - Automatically converts int, float32, uint64, etc. to float64 for algorithms
//   - Rejects unsupported types (complex numbers, structs, etc.) with clear errors
//
// Special Data Types:
//   - Handles booleans using change-based logic (keep when value changes)
//   - Handles strings using change-based logic (keep when value changes)
//   - Bypasses numeric algorithms for these types completely
//
// Out-of-Order Data:
//   - Can drop out-of-order data (PassThrough=false) to maintain algorithm assumptions
//   - Can pass through out-of-order data (PassThrough=true) for special use cases
//   - Tracks last timestamp to detect ordering violations
//
// When to use ProcessorWrapper vs direct algorithms:
//   - Use ProcessorWrapper for most applications (recommended)
//   - Use direct algorithms when you guarantee float64 input and chronological ordering
//   - Use direct algorithms for maximum performance in high-throughput scenarios
//
// Thread safety: NOT goroutine-safe. Use separate instances for concurrent processing.
type ProcessorWrapper struct {
	algorithm     StreamCompressor
	passThrough   bool // If true, pass older data through; if false, drop it
	lastTimestamp time.Time
	logger        *service.Logger
	seriesID      string // For debug logging context

	// Boolean handling state - using value fields to avoid heap allocations
	lastBoolValue bool
	haveBoolValue bool
	lastBoolTime  time.Time

	// String handling state - using value fields to avoid heap allocations
	lastStringValue string
	haveStringValue bool
	lastStringTime  time.Time
}

// ProcessorConfig configures the processor wrapper behavior.
//
// Fields:
//   - Algorithm: name of the underlying compression algorithm ("deadband", "swinging_door")
//   - AlgorithmConfig: configuration parameters passed to the algorithm
//   - PassThrough: how to handle out-of-order data (false=drop, true=pass through)
//   - Logger: benthos logger for debug output (optional)
//   - SeriesID: series identifier for debug context (optional)
type ProcessorConfig struct {
	Algorithm       string                 `json:"algorithm"`        // Algorithm name to use
	AlgorithmConfig map[string]interface{} `json:"algorithm_config"` // Algorithm-specific configuration
	PassThrough     bool                   `json:"pass_through"`     // Default: false (drop out-of-order data)
	Logger          *service.Logger        `json:"-"`                // Logger for debug output
	SeriesID        string                 `json:"-"`                // Series identifier for debug context
}

// NewProcessorWrapper creates a new processor wrapper instance.
//
// The wrapper will create the underlying algorithm using the provided configuration
// and prepare to handle mixed data types and ordering issues.
//
// Returns error if:
//   - Algorithm name is not registered
//   - Algorithm configuration is invalid
//   - Algorithm factory function fails
func NewProcessorWrapper(config ProcessorConfig) (*ProcessorWrapper, error) {
	// Create the underlying algorithm
	algo, err := Create(config.Algorithm, config.AlgorithmConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create algorithm: %w", err)
	}

	return &ProcessorWrapper{
		algorithm:   algo,
		passThrough: config.PassThrough,
		logger:      config.Logger,
		seriesID:    config.SeriesID,
	}, nil
}

// Ingest processes a data point with automatic type conversion and ordering
func (p *ProcessorWrapper) Ingest(value interface{}, timestamp time.Time) ([]GenericPoint, error) {
	// Handle boolean values with special logic
	if boolVal, isBool := value.(bool); isBool {
		return p.processBooleanValue(boolVal, timestamp)
	}

	// Handle string values with change-based logic
	if stringVal, isString := value.(string); isString {
		return p.processStringValue(stringVal, timestamp)
	}

	// Convert to float64
	floatVal, err := p.toFloat64(value)
	if err != nil {
		if p.logger != nil {
			p.logger.Debugf("Type conversion failed for series '%s' (algorithm: %s): value %v (type %T) cannot be converted to float64: %v",
				p.seriesID, p.algorithm.Name(), value, value, err)
		}
		return []GenericPoint{}, fmt.Errorf("type conversion failed: %w", err)
	}

	// Handle ordering - algorithms expect monotonic timestamps
	if !p.lastTimestamp.IsZero() && timestamp.Before(p.lastTimestamp) {
		if p.passThrough {
			// PassThrough=true means bypass algorithm and always keep out-of-order data
			// This ensures out-of-order messages are passed through unchanged
			if p.logger != nil {
				timeDiff := p.lastTimestamp.Sub(timestamp)
				p.logger.Debugf("Late arrival passthrough for series '%s' (algorithm: %s): message timestamp %v is %v behind last processed %v - passing through unchanged",
					p.seriesID, p.algorithm.Name(), timestamp, timeDiff, p.lastTimestamp)
			}
			return []GenericPoint{{Value: value, Timestamp: timestamp}}, nil
		} else {
			// Drop out-of-order data
			if p.logger != nil {
				timeDiff := p.lastTimestamp.Sub(timestamp)
				p.logger.Debugf("Late arrival drop for series '%s' (algorithm: %s): message timestamp %v is %v behind last processed %v - dropping message",
					p.seriesID, p.algorithm.Name(), timestamp, timeDiff, p.lastTimestamp)
			}
			return []GenericPoint{}, nil
		}
	}

	// Update last timestamp and process
	// Only update lastTimestamp for in-order data to maintain proper ordering detection
	if p.lastTimestamp.IsZero() || !timestamp.Before(p.lastTimestamp) {
		p.lastTimestamp = timestamp
	}

	// Call the underlying algorithm and convert Points to GenericPoints
	points, err := p.algorithm.Ingest(floatVal, timestamp)
	if err != nil {
		return []GenericPoint{}, err
	}

	// Convert []Point to []GenericPoint
	genericPoints := make([]GenericPoint, len(points))
	for i, point := range points {
		genericPoints[i] = GenericPoint{
			Value:     point.Value,
			Timestamp: point.Timestamp,
		}
	}

	return genericPoints, nil
}

// processBooleanValue handles boolean values with change-based logic
func (p *ProcessorWrapper) processBooleanValue(value bool, timestamp time.Time) ([]GenericPoint, error) {
	// First boolean value is always kept
	if !p.haveBoolValue {
		p.lastBoolValue = value
		p.haveBoolValue = true
		p.lastBoolTime = timestamp
		if p.logger != nil {
			p.logger.Debugf("Boolean value kept for series '%s' (algorithm: %s): first value %v at %v", p.seriesID, p.algorithm.Name(), value, timestamp)
		}
		return []GenericPoint{{Value: value, Timestamp: timestamp}}, nil
	}

	// Keep if value changed
	if p.lastBoolValue != value {
		// Capture the previous value before updating
		old := p.lastBoolValue
		p.lastBoolValue = value
		p.lastBoolTime = timestamp
		if p.logger != nil {
			p.logger.Debugf("Boolean value kept for series '%s' (algorithm: %s): changed from %v to %v at %v", p.seriesID, p.algorithm.Name(), old, value, timestamp)
		}
		return []GenericPoint{{Value: value, Timestamp: timestamp}}, nil
	}

	// Drop if no change
	if p.logger != nil {
		p.logger.Debugf("Boolean value dropped for series '%s' (algorithm: %s): unchanged value %v at %v", p.seriesID, p.algorithm.Name(), value, timestamp)
	}
	return []GenericPoint{}, nil
}

// processStringValue handles string values with change-based logic
func (p *ProcessorWrapper) processStringValue(value string, timestamp time.Time) ([]GenericPoint, error) {
	// First string value is always kept
	if !p.haveStringValue {
		p.lastStringValue = value
		p.haveStringValue = true
		p.lastStringTime = timestamp
		if p.logger != nil {
			p.logger.Debugf("String value kept for series '%s' (algorithm: %s): first value '%s' at %v", p.seriesID, p.algorithm.Name(), value, timestamp)
		}
		return []GenericPoint{{Value: value, Timestamp: timestamp}}, nil
	}

	// Keep if value changed
	if p.lastStringValue != value {
		oldValue := p.lastStringValue
		p.lastStringValue = value
		p.lastStringTime = timestamp
		if p.logger != nil {
			p.logger.Debugf("String value kept for series '%s' (algorithm: %s): changed from '%s' to '%s' at %v", p.seriesID, p.algorithm.Name(), oldValue, value, timestamp)
		}
		return []GenericPoint{{Value: value, Timestamp: timestamp}}, nil
	}

	// Drop if no change
	if p.logger != nil {
		p.logger.Debugf("String value dropped for series '%s' (algorithm: %s): unchanged value '%s' at %v", p.seriesID, p.algorithm.Name(), value, timestamp)
	}
	return []GenericPoint{}, nil
}

// toFloat64 converts various numeric types to float64
func (p *ProcessorWrapper) toFloat64(val interface{}) (float64, error) {
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
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, fmt.Errorf("json.Number cannot be converted to float64: %w", err)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("unsupported type: %T", val)
	}
}

// Reset clears the wrapper's internal state including the underlying algorithm
func (p *ProcessorWrapper) Reset() {
	p.algorithm.Reset()
	p.lastTimestamp = time.Time{}
	p.haveBoolValue = false
	p.lastBoolTime = time.Time{}
	p.haveStringValue = false
	p.lastStringTime = time.Time{}
}

// Config returns the underlying algorithm's configuration string
func (p *ProcessorWrapper) Config() string {
	return p.algorithm.Config()
}

// Name returns the underlying algorithm's name
func (p *ProcessorWrapper) Name() string {
	return p.algorithm.Name()
}

// Flush returns any pending final points from the underlying algorithm
func (p *ProcessorWrapper) Flush() ([]GenericPoint, error) {
	points, err := p.algorithm.Flush()
	if err != nil {
		return []GenericPoint{}, err
	}

	// Convert []Point to []GenericPoint
	genericPoints := make([]GenericPoint, len(points))
	for i, point := range points {
		genericPoints[i] = GenericPoint{
			Value:     point.Value,
			Timestamp: point.Timestamp,
		}
	}

	return genericPoints, nil
}

// NeedsPreviousPoint returns whether the underlying algorithm needs to emit previous points.
// This is used to optimize ACK buffering behavior.
func (p *ProcessorWrapper) NeedsPreviousPoint() bool {
	return p.algorithm.NeedsPreviousPoint()
}
