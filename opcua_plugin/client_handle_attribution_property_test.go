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

package opcua_plugin

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// The property-based companion to the scenario tests in
// read_client_handle_attribution_test.go (ENG-5553 step 3).
//
// Invariant under test, for an ARBITRARY node list, ARBITRARY batch size and
// an ARBITRARY set of filter-rejection positions:
//
//	Every notification resolves to the node the server monitors under its
//	handle - and a handle never identifies more than one MonitoredItem.
//
// The scenario tests pick specific shapes (5 nodes, rejection at index 2,
// ...). A property test generates them: list length, batch size, which
// indices the server rejects - including zero rejections, rejection at index
// 0, rejection at the last index, and several rejections.
//
// Expected handles are never computed here, exactly as in the scenario tests:
// the fake subscription is the authority for what the server accepted, and
// the assertions replay a notification for every MonitoredItem the server
// has live. If the property is violated, the failing subtest is named after
// (and the failure message repeats) the exact scenario: the seed, the list
// length, the batch size and the rejection set - so a human can reproduce it
// with a single `go test -run` invocation. Shrinking information beats
// scenario volume: the deterministic edge scenarios below are the minimal
// shapes the generator must always cover.

const obligationProperty = "PROPERTY (UMH policy, ENG-5553 step 3: for an arbitrary node list, batch size and " +
	"set of filter-rejection positions, every notification must resolve to the node the server monitors under " +
	"its handle, and a handle must identify exactly one MonitoredItem)"

// propertyScenario is one generated attribution scenario. All three
// dimensions named in the invariant are here: list length, batch size, and
// the set of indices whose filtered request the server rejects.
type propertyScenario struct {
	seed          int64
	listLength    int
	batchSize     int
	rejectedIdx   []int // indices into the node list; may be empty
	rejectAll     bool  // reject every filtered request (filterless server)
	fixedScenario bool  // true for the deterministic edge scenarios
}

// String renders the scenario so that a failing subtest name and failure
// message tell a human exactly how to reproduce it.
func (s propertyScenario) String() string {
	origin := fmt.Sprintf("seed=%d", s.seed)
	if s.fixedScenario {
		origin = "fixed-edge-scenario"
	}
	return fmt.Sprintf("%s n=%d b=%d rejected=%v rejectAllFiltered=%v",
		origin, s.listLength, s.batchSize, s.rejectedIdx, s.rejectAll)
}

// derivePropertyScenario generates an arbitrary scenario from a seed. Every
// derived value is recorded in the scenario so a failure is reproducible
// from the seed alone.
func derivePropertyScenario(seed int64) propertyScenario {
	rng := rand.New(rand.NewSource(seed))
	s := propertyScenario{
		seed:       seed,
		listLength: 1 + rng.Intn(8), // 1..8 nodes
		batchSize:  1 + rng.Intn(8), // 1..8 items per CreateMonitoredItems call
	}
	// Arbitrary rejection set: each index is rejected with probability 1/3.
	// This naturally covers zero rejections, index 0, the last index and
	// several rejections; the deterministic edge scenarios guarantee the
	// corners in every run.
	for i := 0; i < s.listLength; i++ {
		if rng.Intn(3) == 0 {
			s.rejectedIdx = append(s.rejectedIdx, i)
		}
	}
	if rng.Intn(16) == 0 {
		// Occasionally a server with no DataChangeFilter support at all:
		// every filtered request is rejected (the retry starts at index 0).
		s.rejectAll = true
	}
	return s
}

// fixedPropertyScenarios are the edge shapes the generator must always
// cover, run deterministically in addition to the generated ones: zero
// rejections, rejection at index 0, rejection at the last index, several
// rejections, batch size 1, a batch larger than the list, and a single-node
// list.
func fixedPropertyScenarios() []propertyScenario {
	return []propertyScenario{
		{fixedScenario: true, listLength: 5, batchSize: 5, rejectedIdx: nil},            // zero rejections, one batch
		{fixedScenario: true, listLength: 7, batchSize: 3, rejectedIdx: nil},            // zero rejections, multi-batch
		{fixedScenario: true, listLength: 5, batchSize: 5, rejectedIdx: []int{0}},       // rejection at index 0
		{fixedScenario: true, listLength: 6, batchSize: 4, rejectedIdx: []int{0}},       // rejection at index 0, multi-batch
		{fixedScenario: true, listLength: 5, batchSize: 5, rejectedIdx: []int{4}},       // rejection at the last index
		{fixedScenario: true, listLength: 7, batchSize: 3, rejectedIdx: []int{6}},       // rejection at the last index, multi-batch
		{fixedScenario: true, listLength: 8, batchSize: 3, rejectedIdx: []int{0, 3, 7}}, // several rejections, multi-batch
		{fixedScenario: true, listLength: 8, batchSize: 8, rejectedIdx: []int{1, 2, 3}}, // several rejections, one batch
		{fixedScenario: true, listLength: 3, batchSize: 1, rejectedIdx: []int{1}},       // batch size 1
		{fixedScenario: true, listLength: 3, batchSize: 10, rejectedIdx: []int{0}},      // batch larger than the list
		{fixedScenario: true, listLength: 1, batchSize: 1, rejectedIdx: nil},            // single node, no rejection
		{fixedScenario: true, listLength: 1, batchSize: 1, rejectedIdx: []int{0}},       // single node, rejected then retried
		{fixedScenario: true, listLength: 5, batchSize: 5, rejectAll: true},             // filterless server: rejects every filtered request
	}
}

// runPropertyScenario drives the real production path (MonitorBatched,
// then a replayed notification for every live MonitoredItem through
// ReadBatchSubscribe) for one generated scenario and asserts the invariant.
//
// It reuses the frozen harness (newAttributionHarness,
// checkHandleUniqueness, checkServerViewAttribution) from
// read_client_handle_attribution_test.go: the fake subscription records what
// the server accepted, and the assertions never recompute handles.
func runPropertyScenario(t *testing.T, s propertyScenario) {
	t.Helper()
	h := newAttributionHarness(t, harnessConfig{
		nodeCount:        s.listLength,
		maxBatchSize:     s.batchSize,
		filterCapability: FilterUnknown, // forces the trial + fallback path the defect lived in
		subscriptionID:   1001,
		namespace:        2,
		idBase:           1000,
		browsePrefix:     "sensor",
	})

	switch {
	case s.rejectAll:
		h.sub.reject = rejectAllFiltered
	case len(s.rejectedIdx) > 0:
		ids := make([]string, 0, len(s.rejectedIdx))
		for _, i := range s.rejectedIdx {
			if i < 0 || i >= len(h.nodes) {
				t.Fatalf("test infrastructure problem: rejection index %d out of range for %d nodes", i, len(h.nodes))
			}
			ids = append(ids, h.nodes[i].NodeID.String())
		}
		h.sub.reject = rejectFilteredFor(ids...)
	}

	h.subscribe(t)

	// Invariant part 1 - a handle identifies exactly one MonitoredItem of
	// the subscription (the B1 obligation, over the generated shape).
	h.checkHandleUniqueness(t, obligationProperty, s.String())

	// Invariant part 2 - every notification resolves to the node the server
	// monitors under its handle: replay one notification per live
	// MonitoredItem and require the published node identity to match the
	// server's record (the O1 obligation, over the generated shape).
	h.checkServerViewAttribution(t, obligationProperty, s.String())

	// Invariant part 3 - the registry itself: every handle the server knows
	// must resolve to exactly the node the server monitors under it. This
	// pins the new code directly (allocation, registry, resolution), where
	// parts 1 and 2 pin it end-to-end.
	var violations []string
	for _, item := range h.sub.serverView() {
		node, ok := h.g.handleRegistry.lookup(item.ClientHandle)
		if !ok {
			violations = append(violations, fmt.Sprintf(
				"%s [%s]: the server monitors node %s under handle %d (%s), but the handle registry does not "+
					"know the handle at all",
				obligationProperty, s.String(), item.NodeID, item.ClientHandle, item))
			continue
		}
		if node.NodeID.String() != item.NodeID {
			violations = append(violations, fmt.Sprintf(
				"%s [%s]: the server monitors node %s under handle %d (%s), but the handle registry resolves the "+
					"handle to node %s",
				obligationProperty, s.String(), item.NodeID, item.ClientHandle, item, node.NodeID))
		}
	}
	if len(violations) > 0 {
		t.Errorf("%s [%s]: the handle registry disagrees with the server for %d handles:\n%s",
			obligationProperty, s.String(), len(violations), joinStrings(violations))
	}
}

func joinStrings(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "\n"
		}
		out += p
	}
	return out
}

// TestClientHandleAttributionProperty is the property test: deterministic
// edge scenarios plus generated ones, all as parallel subtests named after
// the exact scenario. Each scenario costs at least one second (production's
// post-batch sleep in MonitorBatched), so the subtests run in parallel to
// keep the run time bounded; scenarios are independent - each builds its own
// harness.
func TestClientHandleAttributionProperty(t *testing.T) {
	scenarios := fixedPropertyScenarios()

	// Generated scenarios from consecutive seeds. Seed 0 is not used, so a
	// failure always names a non-zero seed that reproduces the scenario via
	// derivePropertyScenario.
	const generatedScenarios = 24
	if testing.Short() {
		// -short keeps the suite quick for mutation runs: the fixed edge
		// scenarios still cover every named corner shape.
		const shortGenerated = 6
		for seed := int64(1); seed <= shortGenerated; seed++ {
			scenarios = append(scenarios, derivePropertyScenario(seed))
		}
	} else {
		for seed := int64(1); seed <= generatedScenarios; seed++ {
			scenarios = append(scenarios, derivePropertyScenario(seed))
		}
	}

	for _, s := range scenarios {
		s := s
		t.Run(s.String(), func(t *testing.T) {
			t.Parallel()
			runPropertyScenario(t, s)
		})
	}
}

// FuzzClientHandleAttributionProperty is the Go-native fuzzing front end for
// the same invariant. The fuzz inputs ARE the scenario: list length, batch
// size, and a rejection bitmask over the node indices - so a failing input
// reported by the fuzzing engine is directly readable as the exact scenario
// (list length, batch size, rejection set) a human needs to reproduce it.
//
// The seed corpus pins the edge shapes. Under a plain `go test` run the
// corpus entries execute once each (a few seconds each, production's
// post-batch sleep); `go test -fuzz=FuzzClientHandleAttributionProperty`
// explores arbitrary combinations. Skipped in -short mode to keep mutation
// runs fast.
func FuzzClientHandleAttributionProperty(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping fuzz corpus in -short mode (property test covers the edge scenarios)")
	}
	f.Add(uint8(5), uint8(5), uint64(0))               // zero rejections
	f.Add(uint8(5), uint8(5), uint64(1)<<2)            // rejection at a nonzero index (the classic defect shape)
	f.Add(uint8(5), uint8(5), uint64(1))               // rejection at index 0
	f.Add(uint8(5), uint8(5), uint64(1)<<4)            // rejection at the last index
	f.Add(uint8(8), uint8(3), uint64(1)|(1<<3)|(1<<7)) // several rejections across batches
	f.Add(uint8(3), uint8(1), uint64(1)<<1)            // batch size 1
	f.Add(uint8(1), uint8(1), uint64(0))               // single node
	f.Add(uint8(3), uint8(10), uint64(1))              // batch larger than the list

	f.Fuzz(func(t *testing.T, listLength, batchSize uint8, rejectedMask uint64) {
		s := propertyScenario{
			seed:       -1, // derived directly from fuzz inputs, not from a seed
			listLength: int(listLength)%8 + 1,
			batchSize:  int(batchSize)%8 + 1,
		}
		for i := 0; i < s.listLength && i < 64; i++ {
			if rejectedMask&(uint64(1)<<uint(i)) != 0 {
				s.rejectedIdx = append(s.rejectedIdx, i)
			}
		}
		sort.Ints(s.rejectedIdx)
		t.Logf("fuzz input: list length=%d batch size=%d rejected indices=%v raw(listLength=%d batchSize=%d mask=%d)",
			s.listLength, s.batchSize, s.rejectedIdx, listLength, batchSize, rejectedMask)
		runPropertyScenario(t, s)
	})
}
