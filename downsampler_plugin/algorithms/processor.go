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
	"time"
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
	algorithm     DownsampleAlgorithm
	passThrough   bool // If true, pass older data through; if false, drop it
	lastTimestamp time.Time

	// Boolean handling state
	lastBoolValue *bool
	lastBoolTime  time.Time

	// String handling state
	lastStringValue *string
	lastStringTime  time.Time
}

// ProcessorConfig configures the processor wrapper behavior.
//
// Fields:
//   - Algorithm: name of the underlying compression algorithm ("deadband", "swinging_door")
//   - AlgorithmConfig: configuration parameters passed to the algorithm
//   - PassThrough: how to handle out-of-order data (false=drop, true=pass through)
type ProcessorConfig struct {
	Algorithm       string                 `json:"algorithm"`        // Algorithm name to use
	AlgorithmConfig map[string]interface{} `json:"algorithm_config"` // Algorithm-specific configuration
	PassThrough     bool                   `json:"pass_through"`     // Default: false (drop out-of-order data)
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
	}, nil
}

// ProcessPoint processes a data point with automatic type conversion and ordering
func (p *ProcessorWrapper) ProcessPoint(value interface{}, timestamp time.Time) (bool, error) {
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
		return false, fmt.Errorf("type conversion failed: %w", err)
	}

	// Handle ordering - algorithms expect monotonic timestamps
	if !p.lastTimestamp.IsZero() && timestamp.Before(p.lastTimestamp) {
		if p.passThrough {
			// Pass through older data to the algorithm
			return p.algorithm.ProcessPoint(floatVal, timestamp)
		} else {
			// Drop out-of-order data
			return false, nil
		}
	}

	// Update last timestamp and process
	p.lastTimestamp = timestamp
	return p.algorithm.ProcessPoint(floatVal, timestamp)
}

// processBooleanValue handles boolean values with change-based logic
func (p *ProcessorWrapper) processBooleanValue(value bool, timestamp time.Time) (bool, error) {
	// First boolean value is always kept
	if p.lastBoolValue == nil {
		p.lastBoolValue = &value
		p.lastBoolTime = timestamp
		return true, nil
	}

	// Keep if value changed
	if *p.lastBoolValue != value {
		p.lastBoolValue = &value
		p.lastBoolTime = timestamp
		return true, nil
	}

	// Drop if no change
	return false, nil
}

// processStringValue handles string values with change-based logic
func (p *ProcessorWrapper) processStringValue(value string, timestamp time.Time) (bool, error) {
	// First string value is always kept
	if p.lastStringValue == nil {
		p.lastStringValue = &value
		p.lastStringTime = timestamp
		return true, nil
	}

	// Keep if value changed
	if *p.lastStringValue != value {
		p.lastStringValue = &value
		p.lastStringTime = timestamp
		return true, nil
	}

	// Drop if no change
	return false, nil
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
	default:
		return 0, fmt.Errorf("cannot convert type %T to float64", val)
	}
}

// Reset resets both the wrapper and underlying algorithm state
func (p *ProcessorWrapper) Reset() {
	p.algorithm.Reset()
	p.lastTimestamp = time.Time{}
	p.lastBoolValue = nil
	p.lastBoolTime = time.Time{}
	p.lastStringValue = nil
	p.lastStringTime = time.Time{}
}

// GetMetadata returns metadata from the underlying algorithm
func (p *ProcessorWrapper) GetMetadata() string {
	metadata := p.algorithm.GetMetadata()
	if p.passThrough {
		metadata += ",out_of_order_handling=pass_through"
	} else {
		metadata += ",out_of_order_handling=drop"
	}
	return metadata
}

// GetName returns the underlying algorithm name
func (p *ProcessorWrapper) GetName() string {
	return p.algorithm.GetName()
}
