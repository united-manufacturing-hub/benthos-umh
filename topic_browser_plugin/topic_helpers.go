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
	"strings"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
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
//   - Empty string if topicInfo is nil
//
// Example:
//   - If Level0 = "enterprise" and LocationSublevels = ["site", "area"]
//   - Returns: "enterprise.site.area"
//   - If LocationSublevels is empty, returns just "enterprise"
//   - If Level0 = " enterprise " and LocationSublevels = [" site ", " area "]
//   - Returns: "enterprise.site.area" (whitespace trimmed)
func LocationPath(t *proto.TopicInfo) string {
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
