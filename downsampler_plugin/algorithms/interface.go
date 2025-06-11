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

// Package algorithms provides data compression algorithms for downsampling numeric time series data.
//
// # Assumptions and Requirements
//
// All algorithms in this package assume that:
//
//  1. **Data Type**: All input values are of type float64. Type conversion from other numeric types
//     (int, float32, etc.) must be handled by the calling package before invoking ProcessPoint.
//
//  2. **Data Ordering**: Input data points are provided in chronological order (timestamps are
//     monotonically increasing). Out-of-order data handling must be implemented by the calling package.
//
//  3. **Non-Numeric Data**: Boolean values, strings, and other non-numeric types are NOT supported
//     by these algorithms directly. Such data types require different handling logic and should be processed
//     by the calling package before reaching these algorithms. However, the ProcessorWrapper provides
//     change-based logic for booleans and strings automatically.
//
// # Thread Safety
//
// All algorithms maintain internal state and are NOT goroutine-safe. Use separate algorithm instances
// for concurrent processing or implement external synchronization in the calling package.
//
// # Usage Patterns
//
// ## Direct Algorithm Usage (Advanced)
//
// For direct control, use algorithms directly with pre-converted float64 values:
//
//	// Create algorithm instance
//	algo, err := algorithms.Create("deadband", config)
//	if err != nil {
//		return err
//	}
//
//	// Process each data point in chronological order
//	for _, point := range timeSortedData {
//		keep, err := algo.ProcessPoint(point.Value, point.Timestamp) // Value must be float64
//		if err != nil {
//			return err
//		}
//		if keep {
//			// Emit/store this point
//		}
//	}
//
// ## ProcessorWrapper Usage (Recommended)
//
// For convenience, use ProcessorWrapper which handles type conversion, boolean logic,
// and out-of-order data automatically:
//
//	config := ProcessorConfig{
//		Algorithm: "deadband",
//		AlgorithmConfig: map[string]interface{}{"threshold": 0.5},
//		PassThrough: false, // Drop out-of-order data
//	}
//	processor, err := NewProcessorWrapper(config)
//	if err != nil {
//		return err
//	}
//
//	// Process any data type in any order
//	for _, point := range mixedData {
//		keep, err := processor.ProcessPoint(point.Value, point.Timestamp) // Any numeric type, bool, or string
//		if err != nil {
//			return err
//		}
//		if keep {
//			// Emit/store this point
//		}
//	}
//
// The ProcessorWrapper automatically:
// - Converts numeric types (int, float32, etc.) to float64
// - Handles boolean values with change-based logic
// - Handles string values with change-based logic
// - Drops or passes through out-of-order data based on configuration
// - Rejects other non-convertible types
package algorithms

import (
	"time"
)

// DownsampleAlgorithm defines the interface for downsampling algorithms
type DownsampleAlgorithm interface {
	// ProcessPoint processes a new numeric data point and returns whether it should be kept.
	// This method is called for every point in sequence, allowing stateful algorithms
	// like SDT to maintain internal state across all points.
	ProcessPoint(value float64, timestamp time.Time) (bool, error)

	// Reset clears the algorithm's internal state
	Reset()

	// GetMetadata returns a string describing the algorithm and its configuration
	// for use in message metadata annotations
	GetMetadata() string

	// GetName returns the algorithm name
	GetName() string
}

// AlgorithmFactory creates new algorithm instances
type AlgorithmFactory func(config map[string]interface{}) (DownsampleAlgorithm, error)

// Registry holds all available algorithms
var Registry = make(map[string]AlgorithmFactory)

// Register adds an algorithm to the registry
func Register(name string, factory AlgorithmFactory) {
	Registry[name] = factory
}

// Create instantiates an algorithm by name
func Create(name string, config map[string]interface{}) (DownsampleAlgorithm, error) {
	factory, exists := Registry[name]
	if !exists {
		return nil, &AlgorithmNotFoundError{Name: name}
	}
	return factory(config)
}

// AlgorithmNotFoundError is returned when an unknown algorithm is requested
type AlgorithmNotFoundError struct {
	Name string
}

func (e *AlgorithmNotFoundError) Error() string {
	return "algorithm not found: " + e.Name
}
