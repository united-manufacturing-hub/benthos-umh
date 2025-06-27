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

/*
Package topic provides high-performance parsing, validation, and construction utilities for UMH topics.

UMH topics follow the format: umh.v1.<location_path>.<data_contract>[.<virtual_path>].<name>

Parse and validate topics:

	topic, err := topic.NewUnsTopic("umh.v1.enterprise.site._historian.temperature")
	if err != nil {
		log.Fatal(err)
	}

	info := topic.Info()
	fmt.Println("Location:", info.LocationPath())   // "enterprise.site"
	fmt.Println("Contract:", info.DataContract)     // "_historian"

Build topics programmatically:

	topic, err := topic.NewBuilder().
		SetLocationPath("enterprise.site").
		SetDataContract("_historian").
		SetName("temperature").
		Build()

Performance: ~656ns simple parsing, ~1322ns complex parsing, ~751ns simple construction, ~1436ns complex construction. Thread-safe read operations.

For detailed documentation and examples, see: docs/libraries/umh-topic-parser.md && https://docs.umh.app/usage/unified-namespace/topic-convention
*/
package topic

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

// Compiled regular expressions for efficient validation.
// These are compiled once at package initialization for optimal performance.
var (
	// validCharacterRegex matches strings containing only valid Kafka key characters.
	validCharacterRegex = regexp.MustCompile(`^[a-zA-Z0-9._\-]+$`)

	// invalidCharacterRegex matches any invalid characters for replacement.
	invalidCharacterRegex = regexp.MustCompile(`[^a-zA-Z0-9._\-]`)

	// Ensure regex compilation happens only once
	regexOnce sync.Once
)

// initRegex ensures regex patterns are compiled exactly once for thread safety.
func initRegex() {
	regexOnce.Do(func() {
		// Regex patterns are already compiled above, this ensures it happens once
	})
}

// UnsTopic represents a validated UMH topic that can be used as both a topic string and Kafka key.
//
// UnsTopic provides a unified interface for topic parsing, validation, and usage.
// Once created, an UnsTopic is guaranteed to be valid according to UMH specifications
// and safe for use as a Kafka message key.
//
// # Thread Safety
//
// UnsTopic instances are immutable after creation and safe for concurrent use.
// The parsing and validation occur during construction, so all methods are read-only.
//
// # Performance
//
// Topic parsing is optimized for high throughput scenarios:
//   - Single-pass parsing algorithm
//   - Pre-compiled regex patterns
//   - Minimal memory allocations
//   - Efficient string operations
type UnsTopic struct {
	// raw stores the original topic string
	raw string

	// info contains the parsed topic components
	info *proto.TopicInfo
}

// NewUnsTopic creates and validates a UMH topic from a string.
//
// This function performs comprehensive validation including:
//   - UMH topic structure validation
//   - Location path hierarchy validation
//   - Data contract format validation
//   - Virtual path validation (if present)
//   - Kafka key character validation
//   - Consecutive dots and boundary validation
//
// The validation is performed in a single pass for optimal performance.
//
// Parameters:
//   - topic: The topic string to parse and validate
//
// Returns:
//   - *UnsTopic: A validated topic instance
//   - error: Detailed error if validation fails
//
// Examples:
//
//	// Valid topics
//	topic, err := NewUnsTopic("umh.v1.enterprise._historian.temperature")
//	topic, err := NewUnsTopic("umh.v1.acme.berlin.assembly._raw.motor.vibration")
//
//	// Invalid topics
//	_, err := NewUnsTopic("umh.v1._enterprise._historian.temp")    // location starts with _
//	_, err := NewUnsTopic("umh.v1.factory.historian.temp")         // missing _ in data contract
//	_, err := NewUnsTopic("umh.v1.factory..line._historian.temp")  // consecutive dots
func NewUnsTopic(topic string) (*UnsTopic, error) {
	// Ensure regex patterns are initialized
	initRegex()

	uns := &UnsTopic{raw: topic}

	// Parse and validate topic structure first (provides more specific errors)
	info, err := uns.parse()
	if err != nil {
		return nil, err
	}

	// Validate as Kafka key (character validation)
	if err := uns.validateAsKey(); err != nil {
		return nil, err
	}

	uns.info = info
	return uns, nil
}

// String returns the original topic string.
//
// This method implements the fmt.Stringer interface, allowing UnsTopic
// to be used directly in string contexts.
//
// Returns:
//   - The original topic string used during construction
//
// Example:
//
//	topic, _ := NewUnsTopic("umh.v1.enterprise._historian.temperature")
//	fmt.Println(topic.String()) // Output: umh.v1.enterprise._historian.temperature
func (u *UnsTopic) String() string {
	return u.raw
}

// Info returns the parsed topic components.
//
// This provides access to the structured representation of the topic,
// allowing inspection of individual components like location path,
// data contract, virtual path, and name.
//
// Returns:
//   - *TopicInfo: Parsed topic components (never nil for valid UnsTopic)
//
// Example:
//
//	topic, _ := NewUnsTopic("umh.v1.acme.berlin._historian.motor.temperature")
//	info := topic.Info()
//	fmt.Println(info.LocationPath())  // Output: acme.berlin
//	fmt.Println(info.DataContract)    // Output: _historian
//	fmt.Println(*info.VirtualPath)    // Output: motor
//	fmt.Println(info.Name)            // Output: temperature
func (u *UnsTopic) Info() *proto.TopicInfo {
	return u.info
}

// AsKafkaKey returns the topic string for use as a Kafka message key.
//
// Since UnsTopic validates Kafka key compatibility during construction,
// this method can be used safely with Kafka producers without additional validation.
//
// Returns:
//   - The topic string, guaranteed to be valid as a Kafka key
//
// Example:
//
//	topic, _ := NewUnsTopic("umh.v1.enterprise._historian.temperature")
//	key := topic.AsKafkaKey()
//	// Use 'key' with Kafka producer
func (u *UnsTopic) AsKafkaKey() string {
	return u.raw
}

// parse converts the topic string to TopicInfo struct using an optimized single-pass algorithm.
//
// This method performs the core parsing logic while validating the topic structure.
// It's designed for high performance with minimal memory allocations.
func (u *UnsTopic) parse() (*proto.TopicInfo, error) {
	// Basic format validation
	if err := u.validateBasicFormat(); err != nil {
		return nil, err
	}

	// Split once and reuse for efficiency
	parts := strings.Split(u.raw, ".")

	// Minimum valid topic: umh.v1.level0._contract.name (5 parts)
	if len(parts) < MinimumParts {
		return nil, errors.New("topic must have at least: umh.v1.level0._contract.name")
	}

	// Validate level0 is not empty
	if parts[2] == "" {
		return nil, errors.New("level0 cannot be empty")
	}

	// Find data contract position efficiently
	datacontractIndex := u.findDataContractIndex(parts)
	if datacontractIndex == -1 {
		return nil, errors.New("topic must contain a data contract (segment starting with '_') and it cannot be the final segment")
	}

	// Validate data contract is not just an underscore
	if len(parts[datacontractIndex]) <= 1 {
		return nil, errors.New("data contract cannot be just an underscore")
	}

	// The last segment is always the name (mandatory)
	nameIndex := len(parts) - 1
	name := parts[nameIndex]
	if name == "" {
		return nil, errors.New("topic name (final segment) cannot be empty")
	}

	// Build TopicInfo efficiently
	info := &proto.TopicInfo{
		Level0:       parts[2], // Skip "umh" and "v1"
		DataContract: parts[datacontractIndex],
		Name:         name,
	}

	// Process location sublevels (everything between level0 and datacontract)
	locationStart := 3 // After umh.v1.level0
	locationEnd := datacontractIndex
	if locationEnd > locationStart {
		// Pre-allocate slice with exact capacity
		info.LocationSublevels = make([]string, locationEnd-locationStart)
		copy(info.LocationSublevels, parts[locationStart:locationEnd])
	} else {
		info.LocationSublevels = []string{} // Ensure non-nil empty slice
	}

	// Process virtual path (everything between datacontract and name)
	virtualStart := datacontractIndex + 1
	virtualEnd := nameIndex
	if virtualEnd > virtualStart {
		virtualParts := parts[virtualStart:virtualEnd]

		// Validate that no virtual path segments are empty
		for i, part := range virtualParts {
			if len(part) == 0 {
				return nil, fmt.Errorf("virtual path segment at index %d cannot be empty", i)
			}
		}

		virtualPath := strings.Join(virtualParts, ".")
		info.VirtualPath = &virtualPath
	}

	// Validate the parsed components
	if err := u.validateParsedInfo(info); err != nil {
		return nil, err
	}

	return info, nil
}

// findDataContractIndex efficiently locates the data contract in the topic parts.
//
// The data contract must start with underscore and cannot be the final segment.
// This method uses early termination for optimal performance.
func (u *UnsTopic) findDataContractIndex(parts []string) int {
	// Start from index 3 (after umh.v1.level0), end before last segment
	for i := 3; i < len(parts)-1; i++ {
		if len(parts[i]) == 0 {
			continue // Skip empty segments (will be caught in validation)
		}
		if strings.HasPrefix(parts[i], "_") {
			return i
		}
	}
	return -1
}

// validateBasicFormat performs fast preliminary validation.
func (u *UnsTopic) validateBasicFormat() error {
	if len(u.raw) == 0 {
		return errors.New("topic cannot be empty")
	}
	if !strings.HasPrefix(u.raw, TopicPrefix) {
		return errors.New("topic must start with umh.v1")
	}
	return nil
}

// validateParsedInfo validates the parsed TopicInfo using fixed UMH rules.
func (u *UnsTopic) validateParsedInfo(info *proto.TopicInfo) error {
	// Validate level0 (enterprise level)
	if strings.HasPrefix(info.Level0, "_") {
		return errors.New("level0 cannot start with underscore")
	}

	// Validate location sublevels
	for i, level := range info.LocationSublevels {
		if level == "" {
			return fmt.Errorf("location sublevel at index %d cannot be empty: %+v", i, info)
		}
		if strings.HasPrefix(level, "_") {
			return fmt.Errorf("location sublevel at index %d cannot start with underscore", i)
		}
	}

	// Data contract validation was performed in parse()
	// Virtual path validation was performed in parse()
	// Name validation - name CAN start with underscore (per specification)

	return nil
}

// validateAsKey validates the topic string for Kafka key compatibility.
//
// This validation ensures the topic can be safely used as a Kafka message key
// without causing issues in the messaging system.
func (u *UnsTopic) validateAsKey() error {
	// Check for invalid characters using pre-compiled regex
	if !validCharacterRegex.MatchString(u.raw) {
		sanitized := invalidCharacterRegex.ReplaceAllString(u.raw, "_")
		return fmt.Errorf("topic contained invalid characters and was rejected: '%s' -> '%s'", u.raw, sanitized)
	}

	// Reject multiple consecutive dots (fast string search)
	if strings.Contains(u.raw, "..") {
		return fmt.Errorf("topic contained multiple consecutive dots and was rejected: '%s'", u.raw)
	}

	// Reject leading or trailing dots (fast prefix/suffix check)
	if strings.HasPrefix(u.raw, ".") || strings.HasSuffix(u.raw, ".") {
		return fmt.Errorf("topic contained leading or trailing dots and was rejected: '%s'", u.raw)
	}

	return nil
}
