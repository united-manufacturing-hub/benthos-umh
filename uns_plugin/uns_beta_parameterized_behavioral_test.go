//go:build connect_patched

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

// R4 capstone (ENG-5105): the behavioral integration suite runs against the
// uns_beta plugin via a plugin-name parameter. This file introduces the
// parameterization helper (unsBetaInputYAML) and one parameterized behavioral
// Describe — the dropped_keyless counter scenario — driven via DescribeTable.
//
// Scope: this rung enforces dropped_keyless counter behavior for genuine (non-
// spoofed) keyless records only. uns_beta also wires filtered_records, but the
// spoof-detection primitive (classifyDrop/hasSpoofHeader) and the
// dropped_spoofed_key counter it feeds are deferred to ENG-5125, so drift
// remains possible for that drop class. The parameterized DescribeTable below
// proves dropped_keyless behavior; it does NOT prove full behavioral parity.

package uns_plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	// Registers benthos core components (incl. the "none" tracer that
	// StreamBuilder.Build defaults to). The production binary gets these via
	// its full component bundle; this minimal test package only blank-imports
	// the kafka components, so the StreamBuilder build needs pure here.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// unsBetaInputYAML builds the lean 4-field input config for either form from a
// plugin-name variable. The body is identical for both forms (the shared lean
// surface); only the top-level key differs. This is the parameterization seam
// the behavioral suite loops over, so a behavior fix applies to both forms.
func unsBetaInputYAML(pluginName, broker, group, umhTopics string) string {
	return fmt.Sprintf(`
%[1]s:
  broker_address: %[2]q
  consumer_group: %[3]q
  umh_topics:
    - %[4]q
`, pluginName, broker, group, umhTopics)
}

// droppedKeylessCounterScenario drives the uns_beta dropped_keyless counter
// behavioral Describe for a single form. Under a match-everything (".*")
// pattern the connect pre-filter builds a keyless record (it matches the empty
// key), the form's own `key != ""` guard drops it, and dropped_keyless must
// increment on the outer stream's metrics registry. A keyed matching record
// keeps the poll goroutine live so the counter tick is observable.
func droppedKeylessCounterScenario(pluginName string) {
	registerCaptureExporter()
	captureExporter.reset()

	addr := startBroker(GinkgoT())
	group := fmt.Sprintf("uns-beta-param-keyless-%s-%d", pluginName, time.Now().UnixNano())
	produce(GinkgoT(), addr,
		&kgo.Record{Key: nil, Value: []byte(`{"v":0}`)},
		rec("umh.v1.keep.live", `{"v":1}`))

	var mu sync.Mutex
	var delivered int
	sb := service.NewStreamBuilder()
	Expect(sb.AddInputYAML(unsBetaInputYAML(pluginName, addr, group, `.*`))).To(Succeed())
	Expect(sb.SetMetricsYAML(captureExporterName + ": {}")).To(Succeed())
	Expect(sb.AddBatchConsumerFunc(func(_ context.Context, b service.MessageBatch) error {
		mu.Lock()
		delivered += len(b)
		mu.Unlock()
		return nil
	})).To(Succeed())

	stream, err := sb.Build()
	Expect(err).NotTo(HaveOccurred())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = stream.Run(ctx) }()
	defer func() {
		cancel()
		<-done
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			Fail("stream run: " + runErr.Error())
		}
	}()

	// The keyless drop must increment dropped_keyless on the outer stream's
	// metrics registry. uns_beta wires this counter in filterAndAlias; a form
	// that diverges (no per-reason counters) leaves it at 0 and fails here.
	Eventually(func() int64 {
		return captureExporter.counterValue("dropped_keyless")
	}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).
		Should(Equal(int64(1)), pluginName+" must increment dropped_keyless for a keyless record that reaches it under a match-everything pattern")

	// Guard against over-counting: the keyless record must be counted exactly
	// once. If it were redelivered and re-dropped (the ENG-5094 infinite-
	// redelivery wedge this suite exists to detect), dropped_keyless would
	// climb past 1. The sibling key_omitted_records spec applies the same
	// Consistently guard.
	Consistently(func() int64 {
		return captureExporter.counterValue("dropped_keyless")
	}).WithTimeout(2 * time.Second).WithPolling(200 * time.Millisecond).
		Should(Equal(int64(1)), pluginName+" must not over-count dropped_keyless (a climb would indicate the ENG-5094 redelivery wedge)")

	// dropped_spoofed_key and filtered_records are registered (and, for
	// filtered_records, incremented on the keyed non-matching branch) by BOTH
	// forms' constructors. This scenario sends a keyless record and a keyed
	// MATCHING record, so neither triggers the filtered_records increment path;
	// counterValue==0 here reflects "no triggering record was sent", not a
	// map-miss absence and not "the counter is never incremented". The
	// filtered_records increment path is not exercised by any integration
	// scenario for either form (connect's key_pattern pre-filter omits keyed
	// non-matching records before ReadBatch); end-to-end coverage of the
	// increment is tracked in ENG-5125. dropped_spoofed_key is not registered
	// in the single form (no hasSpoofHeader), so a map miss returns 0 there.
	Expect(captureExporter.counterValue("dropped_spoofed_key")).To(Equal(int64(0)),
		pluginName+": a keyless record must not count as spoofed")
	Expect(captureExporter.counterValue("filtered_records")).To(Equal(int64(0)),
		pluginName+": a keyless record must not count as a filtered (real-keyed) drop")
	// The keyed matching record is delivered asynchronously by benthos's batch
	// consumer AFTER ReadBatch returns; the dropped_keyless tick can be
	// observable before the consumer callback runs (especially if franz-go
	// delivers the keyless and keyed records in separate polls), so poll for
	// delivery rather than asserting synchronously.
	Eventually(func() int {
		mu.Lock()
		defer mu.Unlock()
		return delivered
	}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).
		Should(Equal(1), pluginName+": the keyed matching record must be delivered")
}

// R4 parameterized behavioral suite: one behavioral Describe (the dropped_keyless
// counter scenario) driven via the plugin-name parameter. This proves the
// behavioral suite runs against uns_beta for the dropped_keyless counter; it is
// not a full-parity proof (spoofed-key classification and filtered_records are
// out of scope, ENG-5125).
var _ = DescribeTable("uns_beta dropped_keyless counter (parameterized)",
	Label("uns_beta"),
	droppedKeylessCounterScenario,
	Entry("uns_beta", "uns_beta"),
)

// filteredRecordsCounterScenario asserts that uns_beta's own filtered_records
// counter is registered on the outer stream's metrics registry. Registration is
// structural only: it proves the counter name exists on the registry, not that
// the increment fires.
//
// uns_beta registers filtered_records eagerly in its constructor
// (uns_beta_input.go newUnsBeta). The constructor runs when the stream starts
// the input (during stream.Run, not at StreamBuilder.Build), so the assertion
// polls for registration rather than checking synchronously after Build.
//
// The increment is not exercised here. A single shared umh_topics list cannot
// reach the Go-level !matches branch for a keyed record: connect's key_pattern
// pre-filter and the Go-level keyFilter apply the same pattern, so a keyed
// non-matching record is dropped by connect before ReadBatch. Exercising the
// increment requires a connect-level seam or direct ReadBatch injection, which
// belongs to the ENG-5125 rung.
func filteredRecordsCounterScenario(pluginName string) {
	registerCaptureExporter()
	captureExporter.reset()

	addr := startBroker(GinkgoT())
	group := fmt.Sprintf("uns-beta-param-filtered-%s-%d", pluginName, time.Now().UnixNano())

	sb := service.NewStreamBuilder()
	Expect(sb.AddInputYAML(unsBetaInputYAML(pluginName, addr, group, `^only-this$`))).To(Succeed())
	Expect(sb.SetMetricsYAML(captureExporterName + ": {}")).To(Succeed())
	Expect(sb.AddBatchConsumerFunc(func(_ context.Context, _ service.MessageBatch) error {
		return nil
	})).To(Succeed())

	stream, err := sb.Build()
	Expect(err).NotTo(HaveOccurred())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = stream.Run(ctx) }()
	defer func() {
		cancel()
		<-done
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			Fail("stream run: " + runErr.Error())
		}
	}()

	// The input constructor runs when the stream starts it (during Run), and
	// registers filtered_records eagerly there. A form that omits the
	// NewCounter call never registers it and fails this poll.
	Eventually(func() bool {
		return captureExporter.registered("filtered_records")
	}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).
		Should(BeTrue(),
			pluginName+": filtered_records counter must be registered on the outer stream's metrics registry")
}

// The filtered_records counter registration behavioral Describe. Registration
// is structural only: it proves the counter name exists on the registry, not
// that the increment fires. The increment path (keyed records whose key matches
// no umh_topics pattern) is tracked in ENG-5125, which requires a harness that
// bypasses connect's key_pattern pre-filter.
//
// Coverage gap (ENG-5125): the Entry below does not exercise the filtered_records
// INCREMENT. Connect's key_pattern pre-filter omits keyed non-matching records
// before ReadBatch, so the Go-level !matches branch (where the increment lives)
// is never reached by this scenario, which sends no records at all. A regression
// that removes the filtered.Incr would leave this Describe green. Closing the
// gap requires a connect-level seam or direct ReadBatch injection with a keyed
// non-matching record, which belongs to the ENG-5125 rung.
var _ = DescribeTable("uns_beta filtered_records counter registration only (structural)",
	Label("uns_beta"),
	filteredRecordsCounterScenario,
	Entry("uns_beta — registration only; increment path NOT covered (ENG-5125)", "uns_beta"),
)
