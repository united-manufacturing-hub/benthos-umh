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

package topic

import (
	"errors"
	"strings"
)

// Builder provides a fluent interface for constructing UMH topics programmatically.
//
// The Builder pattern allows for readable, maintainable topic construction with
// validation at build time. All builder methods are chainable for convenient usage.
//
// # Performance Characteristics
//
// The builder is optimized for:
//   - Minimal memory allocations during construction
//   - Efficient string operations using pre-calculated capacities
//   - Single validation pass during Build()
//   - Reusable builder instances via Reset()
//
// # Usage Patterns
//
//	// Fluent interface
//	topic, err := NewBuilder().
//	    SetLocationLevels("enterprise", "site", "area").
//	    SetDataContract("_historian").
//	    SetName("temperature").
//	    Build()
//
//	// Step-by-step construction
//	builder := NewBuilder()
//	builder.SetLevel0("enterprise")
//	builder.AddLocationLevel("site")
//	builder.SetDataContract("_historian")
//	topic, err := builder.Build()
//
//	// Reusable builder
//	builder := NewBuilder()
//	for _, sensor := range sensors {
//	    topic, err := builder.Reset().
//	        SetLocationPath(sensor.Location).
//	        SetDataContract("_historian").
//	        SetName(sensor.Name).
//	        Build()
//	}
type Builder struct {
	level0            string
	locationSublevels []string
	dataContract      string
	virtualPath       *string
	name              string
}

// NewBuilder creates a new topic builder with empty fields.
//
// The returned builder is ready for configuration via the fluent interface.
// All fields start empty and must be set before calling Build().
//
// Returns:
//   - *Builder: A new builder instance ready for configuration
//
// Example:
//
//	builder := NewBuilder()
//	topic, err := builder.
//	    SetLevel0("enterprise").
//	    SetDataContract("_historian").
//	    SetName("temperature").
//	    Build()
func NewBuilder() *Builder {
	return &Builder{}
}

// Reset clears all builder fields for reuse.
//
// This method allows efficient reuse of builder instances without
// creating new objects, which is beneficial in high-throughput scenarios.
//
// Returns:
//   - *Builder: The same builder instance with cleared fields (chainable)
//
// Example:
//
//	builder := NewBuilder()
//	for _, item := range items {
//	    topic, err := builder.Reset().
//	        SetLocationPath(item.Location).
//	        SetDataContract(item.Contract).
//	        SetName(item.Name).
//	        Build()
//	}
func (b *Builder) Reset() *Builder {
	b.level0 = ""
	b.locationSublevels = b.locationSublevels[:0] // Keep capacity, clear length
	b.dataContract = ""
	b.virtualPath = nil
	b.name = ""
	return b
}

// SetLevel0 sets the enterprise/root level of the location hierarchy.
//
// This is the mandatory first level of the location path and cannot start
// with an underscore.
//
// Parameters:
//   - level0: The enterprise level (e.g., "enterprise", "acme", "factory")
//
// Returns:
//   - *Builder: The builder instance for method chaining
//
// Example:
//
//	builder.SetLevel0("enterprise")
//	builder.SetLevel0("acme")
func (b *Builder) SetLevel0(level0 string) *Builder {
	b.level0 = level0
	return b
}

// SetLocationPath sets the complete location path by parsing a dot-separated string.
//
// This method splits the path on dots and assigns the first segment as Level0
// and remaining segments as LocationSublevels. It provides a convenient way
// to set the entire location hierarchy at once.
//
// Parameters:
//   - locationPath: Dot-separated location path (e.g., "enterprise.site.area.line")
//
// Returns:
//   - *Builder: The builder instance for method chaining
//
// Examples:
//
//	builder.SetLocationPath("enterprise")                    // Level0="enterprise", no sublevels
//	builder.SetLocationPath("enterprise.site")              // Level0="enterprise", sublevels=["site"]
//	builder.SetLocationPath("factory.area.line.station")    // Level0="factory", sublevels=["area","line","station"]
func (b *Builder) SetLocationPath(locationPath string) *Builder {
	if locationPath == "" {
		b.level0 = ""
		b.locationSublevels = b.locationSublevels[:0]
		return b
	}

	parts := strings.Split(locationPath, ".")
	b.level0 = parts[0]

	if len(parts) > 1 {
		// Ensure capacity and copy remaining parts
		if cap(b.locationSublevels) < len(parts)-1 {
			b.locationSublevels = make([]string, len(parts)-1)
		} else {
			b.locationSublevels = b.locationSublevels[:len(parts)-1]
		}
		copy(b.locationSublevels, parts[1:])
	} else {
		b.locationSublevels = b.locationSublevels[:0]
	}

	return b
}

// SetLocationLevels sets location levels individually with Level0 and optional additional levels.
//
// This method provides precise control over each location level and is more
// efficient than parsing a string when levels are already separated.
//
// Parameters:
//   - level0: The enterprise level (mandatory)
//   - additionalLevels: Additional location hierarchy levels (optional)
//
// Returns:
//   - *Builder: The builder instance for method chaining
//
// Examples:
//
//	builder.SetLocationLevels("enterprise")                           // Just enterprise level
//	builder.SetLocationLevels("enterprise", "site")                  // Enterprise + site
//	builder.SetLocationLevels("factory", "area", "line", "station")  // Four levels deep
func (b *Builder) SetLocationLevels(level0 string, additionalLevels ...string) *Builder {
	b.level0 = level0

	if len(additionalLevels) > 0 {
		// Ensure capacity and copy levels
		if cap(b.locationSublevels) < len(additionalLevels) {
			b.locationSublevels = make([]string, len(additionalLevels))
		} else {
			b.locationSublevels = b.locationSublevels[:len(additionalLevels)]
		}
		copy(b.locationSublevels, additionalLevels)
	} else {
		b.locationSublevels = b.locationSublevels[:0]
	}

	return b
}

// AddLocationLevel appends an additional location level to the hierarchy.
//
// This method is useful for building location paths incrementally or when
// the number of levels is determined dynamically.
//
// Parameters:
//   - level: The location level to append
//
// Returns:
//   - *Builder: The builder instance for method chaining
//
// Example:
//
//	builder.SetLevel0("enterprise").
//	    AddLocationLevel("site").
//	    AddLocationLevel("area").
//	    AddLocationLevel("line")
func (b *Builder) AddLocationLevel(level string) *Builder {
	b.locationSublevels = append(b.locationSublevels, level)
	return b
}

// SetDataContract sets the data contract for the topic.
//
// The data contract must start with an underscore and identifies the service
// or schema that will handle this data.
//
// Parameters:
//   - dataContract: The data contract (e.g., "_historian", "_analytics", "_raw")
//
// Returns:
//   - *Builder: The builder instance for method chaining
//
// Examples:
//
//	builder.SetDataContract("_historian")
//	builder.SetDataContract("_analytics")
//	builder.SetDataContract("_raw")
func (b *Builder) SetDataContract(dataContract string) *Builder {
	b.dataContract = dataContract
	return b
}

// SetVirtualPath sets the optional virtual path for logical grouping.
//
// Virtual path provides logical organization within a data contract and
// can contain multiple dot-separated segments. Segments can start with underscores.
//
// Parameters:
//   - virtualPath: The virtual path (e.g., "motor.diagnostics", "axis.x.position")
//     Empty string or calling with "" will clear the virtual path
//
// Returns:
//   - *Builder: The builder instance for method chaining
//
// Examples:
//
//	builder.SetVirtualPath("motor.diagnostics")       // Multi-segment path
//	builder.SetVirtualPath("axis.x.position")         // Coordinate-style path
//	builder.SetVirtualPath("_internal.debug")         // Internal path with underscore
//	builder.SetVirtualPath("")                        // Clear virtual path
func (b *Builder) SetVirtualPath(virtualPath string) *Builder {
	if virtualPath == "" {
		b.virtualPath = nil
	} else {
		b.virtualPath = &virtualPath
	}
	return b
}

// SetName sets the final name segment of the topic.
//
// The name is the final identifier for the specific data point and cannot be empty.
// Names can start with underscores for internal/system data.
//
// Parameters:
//   - name: The topic name (e.g., "temperature", "pressure", "_internal_counter")
//
// Returns:
//   - *Builder: The builder instance for method chaining
//
// Examples:
//
//	builder.SetName("temperature")
//	builder.SetName("pressure")
//	builder.SetName("_internal_state")
func (b *Builder) SetName(name string) *Builder {
	b.name = name
	return b
}

// Build constructs and validates the final UnsTopic.
//
// This method assembles all configured components into a topic string,
// then creates and validates a UnsTopic instance. The validation ensures
// the topic conforms to UMH specifications and is safe for use as a Kafka key.
//
// Returns:
//   - *UnsTopic: A validated topic instance
//   - error: Detailed error if validation fails
//
// The method performs validation for:
//   - Required fields (Level0, DataContract, Name)
//   - UMH topic structure rules
//   - Kafka key compatibility
//
// Example:
//
//	topic, err := NewBuilder().
//	    SetLocationLevels("enterprise", "site").
//	    SetDataContract("_historian").
//	    SetName("temperature").
//	    Build()
//	if err != nil {
//	    return fmt.Errorf("invalid topic: %w", err)
//	}
func (b *Builder) Build() (*UnsTopic, error) {
	topicStr, err := b.buildTopicString()
	if err != nil {
		return nil, err
	}

	// Create and validate UnsTopic
	return NewUnsTopic(topicStr)
}

// BuildString constructs and validates the final topic string.
//
// This method provides a convenient way to get just the topic string
// without creating a UnsTopic instance. It performs the same validation
// as Build() but returns only the string result.
//
// Returns:
//   - string: The validated topic string
//   - error: Detailed error if validation fails
//
// Example:
//
//	topicStr, err := NewBuilder().
//	    SetLocationPath("enterprise.site").
//	    SetDataContract("_historian").
//	    SetName("temperature").
//	    BuildString()
func (b *Builder) BuildString() (string, error) {
	topic, err := b.Build()
	if err != nil {
		return "", err
	}
	return topic.String(), nil
}

// GetLocationPath returns the complete location path as currently configured.
//
// This method provides inspection of the current location path without
// building the full topic. Useful for debugging or conditional logic.
//
// Returns:
//   - string: The current location path (e.g., "enterprise.site.area")
//   - Empty string if no location levels are set
//
// Example:
//
//	builder.SetLocationLevels("enterprise", "site", "area")
//	path := builder.GetLocationPath() // Returns: "enterprise.site.area"
func (b *Builder) GetLocationPath() string {
	if b.level0 == "" {
		return ""
	}
	if len(b.locationSublevels) == 0 {
		return b.level0
	}

	// Calculate capacity for efficient allocation
	totalLen := len(b.level0)
	for _, level := range b.locationSublevels {
		totalLen += 1 + len(level) // +1 for dot separator
	}

	// Build string efficiently
	result := make([]byte, 0, totalLen)
	result = append(result, b.level0...)
	for _, level := range b.locationSublevels {
		result = append(result, '.')
		result = append(result, level...)
	}

	return string(result)
}

// buildTopicString assembles the topic string from builder components.
//
// This internal method constructs the final topic string using efficient
// string building techniques with pre-calculated capacity.
func (b *Builder) buildTopicString() (string, error) {
	// Validate required fields
	if b.level0 == "" {
		return "", errors.New("level0 is required")
	}
	if b.dataContract == "" {
		return "", errors.New("data contract is required")
	}
	if b.name == "" {
		return "", errors.New("name is required")
	}

	// Build topic string efficiently
	parts := make([]string, 0, 4+len(b.locationSublevels))
	parts = append(parts, "umh", "v1")

	// Add location path
	parts = append(parts, b.level0)
	parts = append(parts, b.locationSublevels...)

	// Add data contract
	parts = append(parts, b.dataContract)

	// Add virtual path if present
	if b.virtualPath != nil && *b.virtualPath != "" {
		virtualParts := strings.Split(*b.virtualPath, ".")
		parts = append(parts, virtualParts...)
	}

	// Add name
	parts = append(parts, b.name)

	return strings.Join(parts, "."), nil
}
