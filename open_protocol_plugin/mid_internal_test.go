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
	"testing"
)

// TestAtoiTrimOverflow proves FIX 3: atoiTrim returns 0 on overflow/parse
// errors rather than the old behaviour of returning whatever strconv.Atoi
// returned (which on overflow was 0 anyway for negative overflow but
// math.MaxInt64 / math.MinInt64 for positive/negative overflows before Go 1.17
// changed to always returning 0 on overflow — regardless, the explicit check
// makes the intent clear and guards against any future changes).
func TestAtoiTrimOverflow(t *testing.T) {
	cases := []struct {
		input string
		want  int
	}{
		// 20-digit number that overflows int64 on all platforms.
		{"99999999999999999999", 0},
		// Leading/trailing whitespace must still parse correctly.
		{" 42 ", 42},
		// Non-numeric string.
		{"abc", 0},
		// Empty string.
		{"", 0},
		// Valid zero.
		{"0", 0},
		// Valid negative.
		{"-7", -7},
	}
	for _, tc := range cases {
		got := atoiTrim(tc.input)
		if got != tc.want {
			t.Errorf("atoiTrim(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}
