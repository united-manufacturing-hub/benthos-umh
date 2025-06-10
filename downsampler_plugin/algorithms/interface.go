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

import "time"

// DownsampleAlgorithm defines the interface for downsampling algorithms
type DownsampleAlgorithm interface {
	// ProcessPoint processes a new data point and returns whether it should be kept.
	// This method is called for every point in sequence, allowing stateful algorithms
	// like SDT to maintain internal state across all points.
	ProcessPoint(value interface{}, timestamp time.Time) (bool, error)

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
