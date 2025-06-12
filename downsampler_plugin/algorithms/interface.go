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
// This package implements industrial-grade compression algorithms commonly used in
// process historians and time-series databases to reduce storage requirements while
// preserving data integrity within configurable tolerances.
//
// # Available Algorithms
//
// ## Deadband Algorithm
//
// Deadband filtering keeps data points only when they differ from the last kept value
// by more than a specified threshold. Ideal for:
// - Temperature sensors with noise
// - Pressure readings with small fluctuations
// - Any slowly changing analog values
//
// Configuration:
//   - threshold: float64 - minimum change required to keep a point
//   - max_time: duration - maximum time between kept points (optional)
//
// ## Swinging Door Trending (SDT) Algorithm
//
// SDT implements the industry-standard "emit-previous" algorithm used by PI Server,
// AVEVA, and other historians. It maintains upper and lower envelope lines to
// determine when linear interpolation error would exceed the threshold. More
// sophisticated than deadband for:
// - Fast-changing process variables
// - Data with trends and gradual changes
// - Higher compression ratios with maintained accuracy
// - Guaranteed interpolation error ≤ threshold for all points
//
// Configuration:
//   - threshold: float64 - maximum interpolation error tolerance
//   - min_time: duration - minimum time between emissions (optional)
//   - max_time: duration - maximum time between kept points (optional)
//
// # Assumptions and Requirements
//
// All algorithms in this package assume that:
//
//  1. **Data Type**: Input values to the core algorithms are float64. The ProcessorWrapper
//     handles automatic type conversion from other numeric types (int, float32, etc.).
//
//  2. **Data Ordering**: Core algorithms expect data points in chronological order
//     (monotonically increasing timestamps). The ProcessorWrapper can handle out-of-order
//     data based on configuration.
//
//  3. **Non-Numeric Data**: Core algorithms only process numeric values. The ProcessorWrapper
//     automatically handles booleans and strings using change-based logic (keep when value changes).
//
// # Thread Safety
//
// - Core algorithms maintain internal state and are NOT goroutine-safe
// - ProcessorWrapper is also NOT goroutine-safe
// - Use separate instances for concurrent processing or implement external synchronization
//
// # Usage Patterns
//
// ## Recommended: ProcessorWrapper (High-level API)
//
// Use ProcessorWrapper for most applications - it handles type conversion, special data types,
// and out-of-order data automatically:
//
//	import "github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
//
//	config := algorithms.ProcessorConfig{
//		Algorithm: "deadband",
//		AlgorithmConfig: map[string]interface{}{
//			"threshold": 0.5,
//			"max_time":  "5m",
//		},
//		PassThrough: false, // Drop out-of-order data (vs pass through)
//	}
//
//	processor, err := algorithms.NewProcessorWrapper(config)
//	if err != nil {
//		return fmt.Errorf("failed to create processor: %w", err)
//	}
//
//	// Process any data type in any order
//	for _, point := range mixedOrderData {
//		emitted, err := processor.Ingest(point.Value, point.Timestamp)
//		if err != nil {
//			return fmt.Errorf("processing failed: %w", err)
//		}
//		for _, pt := range emitted {
//			// Emit/store these compressed points
//			fmt.Printf("Emit: %v at %v\n", pt.Value, pt.Timestamp)
//		}
//	}
//
//	// Flush any final pending points (important for SDT)
//	if finalPoints, err := processor.Flush(); err != nil {
//		return fmt.Errorf("flush failed: %w", err)
//	} else {
//		for _, pt := range finalPoints {
//			fmt.Printf("Final: %v at %v\n", pt.Value, pt.Timestamp)
//		}
//	}
//
// ## Advanced: Direct Algorithm Usage (Low-level API)
//
// Use algorithms directly when you need precise control and guarantee chronological
// ordering and float64 types:
//
//	import "github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
//
//	// Discover available algorithms
//	fmt.Println("Available algorithms:", algorithms.GetAlgorithmNames())
//
//	// Create algorithm instance
//	algo, err := algorithms.Create("swinging_door", map[string]interface{}{
//		"threshold": 1.0,
//		"min_time":  "100ms",
//		"max_time":  "30s",
//	})
//	if err != nil {
//		return fmt.Errorf("algorithm creation failed: %w", err)
//	}
//
//	// Process pre-sorted float64 data points
//	for _, point := range chronologicalData {
//		emitted, err := algo.Ingest(point.Value, point.Timestamp) // Must be float64
//		if err != nil {
//			return fmt.Errorf("processing failed: %w", err)
//		}
//		for _, pt := range emitted {
//			// Emit these points - add algorithm metadata
//			fmt.Printf("Emit: %v at %v (via %s)\n",
//				pt.Value, pt.Timestamp, algo.Config())
//		}
//	}
//
//	// Flush any final pending points (essential for SDT)
//	if finalPoints, err := algo.Flush(); err != nil {
//		return fmt.Errorf("flush failed: %w", err)
//	} else {
//		for _, pt := range finalPoints {
//			fmt.Printf("Final: %v at %v (via %s)\n",
//				pt.Value, pt.Timestamp, algo.Config())
//		}
//	}
//
// # Configuration Examples
//
// ## Deadband Configurations
//
//	// Basic deadband with 0.5 unit threshold
//	map[string]interface{}{
//		"threshold": 0.5,
//	}
//
//	// Deadband with heartbeat every 5 minutes
//	map[string]interface{}{
//		"threshold": 1.0,
//		"max_time":  "5m",
//	}
//
//	// High-sensitivity deadband for precise measurements
//	map[string]interface{}{
//		"threshold": 0.001,
//		"max_time":  "1h",
//	}
//
// ## SDT Configurations
//
//	// Basic SDT with 1.0 unit tolerance
//	map[string]interface{}{
//		"threshold": 1.0,
//	}
//
//	// SDT with noise filtering and heartbeat
//	map[string]interface{}{
//		"threshold": 0.5,
//		"min_time":  "1s",   // Suppress high-frequency noise
//		"max_time":  "10m",  // Force periodic emission
//	}
//
//	// High-compression SDT for historical data
//	map[string]interface{}{
//		"threshold": 2.0,
//		"max_time":  "1h",
//	}
//
// # Error Handling
//
// All functions return descriptive errors for:
//   - Invalid algorithm names
//   - Invalid configuration parameters
//   - Type conversion failures
//   - Runtime processing errors
//
// Check errors and handle them appropriately in production code.
package algorithms

import (
	"sort"
	"time"
)

// Point represents a single data point with a numeric value and timestamp.
type Point struct {
	Value     float64
	Timestamp time.Time
}

// StreamCompressor defines the interface for downsampling algorithms.
//
// All algorithms process numeric time-series data and maintain internal state
// for stateful compression (like SDT envelope tracking). Implementations must:
//   - Process data points in chronological order
//   - Accept only float64 values
//   - Return zero or more points that should be emitted
//   - Provide descriptive metadata for debugging/auditing
//
// StreamCompressor is 100% deterministic, has no Benthos types,
// and never allocates messages.
type StreamCompressor interface {
	// Ingest adds one point and returns zero or more points
	// the algorithm wants to emit immediately.
	//
	// Parameters:
	//   - value: the numeric measurement (must be float64)
	//   - ts: when the measurement was taken
	//
	// Returns:
	//   - []Point: slice of points to emit (may be empty, typically 0-2 points)
	//   - error: processing error (algorithm should recover on next call)
	//
	// This method is called for every point in sequence, allowing stateful algorithms
	// like SDT to maintain internal envelope state across all points and emit
	// both current and previous points as needed.
	Ingest(value float64, ts time.Time) ([]Point, error)

	// Flush returns any remaining points still buffered inside the algorithm.
	//
	// This method should be called exactly once at the logical end of a data series
	// to ensure that algorithms like SDT can emit their final pending points.
	//
	// Returns:
	//   - []Point: slice of final points to emit (at most a handful, never unbounded)
	//   - error: flush error (rare)
	//
	// After calling Flush, the algorithm state should be consistent for continued
	// use or Reset.
	Flush() ([]Point, error)

	// Reset clears the algorithm's internal state.
	//
	// Use this when:
	//   - Starting a new data stream
	//   - Handling stream reconnections
	//   - Recovering from errors
	//   - Processing multiple independent time series
	Reset()

	// Config returns a string describing the algorithm and its configuration
	// for use in message metadata annotations.
	//
	// Format: "algorithm_name(param1=value1,param2=value2)"
	// Example: "deadband(threshold=0.500,max_time=5m0s)"
	Config() string

	// Name returns the algorithm name as registered in the factory.
	//
	// This should match the name used in Create() calls.
	Name() string
}

// AlgorithmFactory creates new algorithm instances from configuration.
//
// The factory function should:
//   - Validate all configuration parameters
//   - Return descriptive errors for invalid config
//   - Set sensible defaults for optional parameters
//   - Initialize the algorithm in a clean state
type AlgorithmFactory func(config map[string]interface{}) (StreamCompressor, error)

// Registry holds all available algorithms by name.
// Algorithms register themselves during package initialization using Register().
var Registry = make(map[string]AlgorithmFactory)

// Register adds an algorithm to the global registry.
//
// This is typically called from init() functions in algorithm implementation files.
// Multiple registrations of the same name will overwrite previous entries.
func Register(name string, factory AlgorithmFactory) {
	Registry[name] = factory
}

// Create instantiates an algorithm by name using the provided configuration.
//
// Parameters:
//   - name: algorithm name as registered (e.g., "deadband", "swinging_door")
//   - config: configuration parameters specific to the algorithm
//
// Returns:
//   - StreamCompressor: ready-to-use algorithm instance
//   - error: if algorithm not found or configuration invalid
//
// Example:
//
//	algo, err := algorithms.Create("deadband", map[string]interface{}{
//		"threshold": 0.5,
//		"max_time":  "5m",
//	})
func Create(name string, config map[string]interface{}) (StreamCompressor, error) {
	factory, exists := Registry[name]
	if !exists {
		return nil, &AlgorithmNotFoundError{Name: name}
	}
	return factory(config)
}

// GetAlgorithmNames returns a sorted list of all registered algorithm names.
//
// Use this for discovery, validation, or building user interfaces.
//
// Example:
//
//	names := algorithms.GetAlgorithmNames()
//	fmt.Println("Available algorithms:", names) // ["deadband", "swinging_door"]
func GetAlgorithmNames() []string {
	names := make([]string, 0, len(Registry))
	for name := range Registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// AlgorithmNotFoundError is returned when Create() is called with an unknown algorithm name.
type AlgorithmNotFoundError struct {
	Name string
}

func (e *AlgorithmNotFoundError) Error() string {
	return "algorithm not found: " + e.Name
}
