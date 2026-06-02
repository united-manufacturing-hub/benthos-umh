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

package open_protocol_plugin_test

import (
	"fmt"
	"testing"
	"time"

	"pgregory.net/rapid"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

// TestHeaderRoundTripProperty verifies that MID and Revision survive a
// BuildMessage → ParseHeader round-trip for all valid (mid, rev) pairs.
//
// TEST 6 strengthens this property: it also draws rev <= 0 (including
// negative values) and asserts the normalization invariant:
//   - rev <= 0  → parsed Revision == 1  (BuildMessage normalizes 0/negative to 1)
//   - rev >= 1  → parsed Revision == rev  (round-trip identity)
func TestHeaderRoundTripProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		mid := rapid.IntRange(1, 9999).Draw(rt, "mid")
		// TEST 6: include rev <= 0 to cover the BuildMessage normalization path.
		rev := rapid.IntRange(-5, 999).Draw(rt, "rev")
		h, err := op.ParseHeader(op.BuildMessage(mid, rev, []byte{}))
		if err != nil {
			rt.Fatalf("ParseHeader after BuildMessage(mid=%d,rev=%d) errored: %v", mid, rev, err)
		}
		if h.MID != mid {
			rt.Fatalf("MID round-trip: built %d, parsed %d", mid, h.MID)
		}
		// Normalization invariant: rev <= 0 must be stored as 1.
		expectedRev := rev
		if rev <= 0 {
			expectedRev = 1
		}
		if h.Revision != expectedRev {
			rt.Fatalf("Revision normalization: built rev=%d, expected parsed=%d, got %d",
				rev, expectedRev, h.Revision)
		}
	})
}

func TestParseControllerTimeRejectsGarbageProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Random alphabetic/space strings are never a valid YYYY-MM-DD:HH:MM:SS timestamp.
		s := rapid.StringMatching(`[A-Za-z ]{1,25}`).Draw(rt, "s")
		if _, err := op.ParseControllerTime(s, time.UTC); err == nil {
			rt.Fatalf("expected error for non-timestamp %q", s)
		}
	})
}

// TestParseControllerTimeRejectsStructuredInvalidProperty covers TEST 7:
// near-miss timestamps that are structurally shaped like YYYY-MM-DD:HH:MM:SS
// but carry out-of-range component values. Go's time.ParseInLocation rejects
// all of these (confirmed via direct testing: month 0/13+, day 32+, hour 24+,
// minute 60+, second 60+). A mutant that removes the error check in
// ParseControllerTime would pass these through silently.
func TestParseControllerTimeRejectsStructuredInvalidProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Draw an out-of-range component value for each field position.
		// Each sub-test picks ONE out-of-range field; others are in-range.
		year := rapid.IntRange(1900, 2100).Draw(rt, "year")

		// Choose which field to make invalid:
		//   0 = month (13..99)
		//   1 = day (32..99)
		//   2 = hour (24..99)
		//   3 = minute (60..99)
		//   4 = second (60..99)
		which := rapid.IntRange(0, 4).Draw(rt, "which")

		var month, day, hour, minute, second int
		switch which {
		case 0:
			month = rapid.IntRange(13, 99).Draw(rt, "month")
			day = rapid.IntRange(1, 28).Draw(rt, "day")
			hour = rapid.IntRange(0, 23).Draw(rt, "hour")
			minute = rapid.IntRange(0, 59).Draw(rt, "minute")
			second = rapid.IntRange(0, 59).Draw(rt, "second")
		case 1:
			month = rapid.IntRange(1, 12).Draw(rt, "month")
			day = rapid.IntRange(32, 99).Draw(rt, "day")
			hour = rapid.IntRange(0, 23).Draw(rt, "hour")
			minute = rapid.IntRange(0, 59).Draw(rt, "minute")
			second = rapid.IntRange(0, 59).Draw(rt, "second")
		case 2:
			month = rapid.IntRange(1, 12).Draw(rt, "month")
			day = rapid.IntRange(1, 28).Draw(rt, "day")
			hour = rapid.IntRange(24, 99).Draw(rt, "hour")
			minute = rapid.IntRange(0, 59).Draw(rt, "minute")
			second = rapid.IntRange(0, 59).Draw(rt, "second")
		case 3:
			month = rapid.IntRange(1, 12).Draw(rt, "month")
			day = rapid.IntRange(1, 28).Draw(rt, "day")
			hour = rapid.IntRange(0, 23).Draw(rt, "hour")
			minute = rapid.IntRange(60, 99).Draw(rt, "minute")
			second = rapid.IntRange(0, 59).Draw(rt, "second")
		case 4:
			month = rapid.IntRange(1, 12).Draw(rt, "month")
			day = rapid.IntRange(1, 28).Draw(rt, "day")
			hour = rapid.IntRange(0, 23).Draw(rt, "hour")
			minute = rapid.IntRange(0, 59).Draw(rt, "minute")
			second = rapid.IntRange(60, 99).Draw(rt, "second")
		}

		s := fmt.Sprintf("%04d-%02d-%02d:%02d:%02d:%02d",
			year, month, day, hour, minute, second)

		if _, err := op.ParseControllerTime(s, time.UTC); err == nil {
			rt.Fatalf("expected error for out-of-range structured timestamp %q (field=%d)", s, which)
		}
	})
}
