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
	"testing"
	"time"

	"pgregory.net/rapid"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

func TestHeaderRoundTripProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		mid := rapid.IntRange(1, 9999).Draw(rt, "mid")
		rev := rapid.IntRange(1, 999).Draw(rt, "rev")
		h, err := op.ParseHeader(op.BuildMessage(mid, rev, []byte{}))
		if err != nil {
			rt.Fatalf("ParseHeader after BuildMessage(mid=%d,rev=%d) errored: %v", mid, rev, err)
		}
		if h.MID != mid {
			rt.Fatalf("MID round-trip: built %d, parsed %d", mid, h.MID)
		}
		if h.Revision != rev {
			rt.Fatalf("Revision round-trip: built %d, parsed %d", rev, h.Revision)
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
