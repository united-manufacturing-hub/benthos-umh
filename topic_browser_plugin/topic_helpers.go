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

package topic_browser_plugin

import (
	"errors"
	"fmt"
	"strings"
)

// LocationPath returns the full location path by joining level0 with all location sublevels.
// This provides a convenient way to get the complete hierarchical location path as a single string,
// which is useful for logging, display, and filtering operations.
//
// The method includes defensive programming practices:
//   - Handles nil receiver gracefully to prevent panics
//   - Trims leading/trailing whitespace from all segments to ensure consistent hash equality
//
// Returns:
//   - string: Complete location path (e.g., "enterprise.site.area.line")
//   - Empty string if receiver is nil
//
// Example:
//   - If Level0 = "enterprise" and LocationSublevels = ["site", "area"]
//   - Returns: "enterprise.site.area"
//   - If LocationSublevels is empty, returns just "enterprise"
//   - If Level0 = " enterprise " and LocationSublevels = [" site ", " area "]
//   - Returns: "enterprise.site.area" (whitespace trimmed)
func (t *TopicInfo) LocationPath() string {
	if t == nil { // guard against accidental nil pointer deref
		return ""
	}
	// trim to ensure " enterprise " and "enterprise" hash identically
	base := strings.TrimSpace(t.Level0)
	if len(t.LocationSublevels) == 0 {
		return base
	}
	cleaned := make([]string, 0, len(t.LocationSublevels)+1)
	cleaned = append(cleaned, base)
	for _, s := range t.LocationSublevels {
		cleaned = append(cleaned, strings.TrimSpace(s))
	}
	return strings.Join(cleaned, ".")
}

// Validate performs comprehensive validation of the TopicInfo struct.
// This ensures that all required fields are present and valid according to UNS specifications.
//
// Returns:
//   - error: nil if valid, otherwise an error describing the validation failure
//
// Validation rules:
//   - Level0 must not be empty (enterprise level is mandatory)
//   - DataContract must not be empty and must start with underscore
//   - DataContract must be longer than just an underscore
//   - Name must not be empty
//   - Name must not start with underscore (to avoid confusion with data contracts)
//   - LocationSublevels entries must not be empty if present
//   - VirtualPath segments must not be empty if present
func (t *TopicInfo) Validate() error {
	if t.Level0 == "" {
		return errors.New("level0 (enterprise) cannot be empty")
	}

	if t.DataContract == "" {
		return errors.New("data contract cannot be empty")
	}

	if !strings.HasPrefix(t.DataContract, "_") {
		return errors.New("data contract must start with underscore")
	}

	if len(t.DataContract) <= 1 {
		return errors.New("data contract cannot be just an underscore")
	}

	if t.Name == "" {
		return errors.New("topic name cannot be empty")
	}

	if strings.HasPrefix(t.Name, "_") {
		return errors.New("topic name cannot start with underscore")
	}

	// Validate location sublevels are not empty
	for i, level := range t.LocationSublevels {
		if level == "" {
			return fmt.Errorf("location sublevel at index %d cannot be empty", i)
		}
	}

	// Validate virtual path segments are not empty if present
	if t.VirtualPath != nil && *t.VirtualPath != "" {
		virtualParts := strings.Split(*t.VirtualPath, ".")
		for i, part := range virtualParts {
			if part == "" {
				return fmt.Errorf("virtual path segment at index %d cannot be empty", i)
			}
		}
	}

	return nil
}
