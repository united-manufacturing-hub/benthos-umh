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

package open_protocol_plugin

import (
	"fmt"
	"time"
)

// controllerTimeLayout is the Open Protocol timestamp format (param 20/21):
// 19 ASCII chars, no timezone. See spec R2.16 timestamp semantics.
const controllerTimeLayout = "2006-01-02:15:04:05"

// ParseControllerTime parses an Open Protocol wall-clock timestamp
// (YYYY-MM-DD:HH:MM:SS) in loc. The protocol carries no zone, so loc supplies
// it. DST-ambiguous/nonexistent local times follow time.ParseInLocation.
func ParseControllerTime(s string, loc *time.Location) (time.Time, error) {
	if loc == nil {
		loc = time.UTC
	}
	t, err := time.ParseInLocation(controllerTimeLayout, s, loc)
	if err != nil {
		return time.Time{}, fmt.Errorf("open protocol: invalid timestamp %q: %w", s, err)
	}
	return t, nil
}
