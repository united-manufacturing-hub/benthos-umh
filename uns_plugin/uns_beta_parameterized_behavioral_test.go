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

// Per-reason drop counter specs for uns_beta, plus the unsBetaInputYAML
// config helper the integration suite uses. They cover dropped_keyless for
// genuine (non-spoofed) keyless records and filtered_records registration.

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

// unsBetaInputYAML builds the 4-field input config under the given plugin name.
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
// spec. Under a match-everything (".*")
// pattern the connect pre-filter builds a keyless record (it matches the empty
// key), uns_beta's own `key != ""` guard drops it, and dropped_keyless must
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
	// metrics registry. uns_beta increments this counter in ReadBatch.
	Eventually(func() int64 {
		return captureExporter.counterValue("dropped_keyless")
	}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
		Should(Equal(int64(1)), pluginName+" must increment dropped_keyless for a keyless record that reaches it under a match-everything pattern")

	// Guard against over-counting: the keyless record must be counted exactly
	// once. If it were redelivered and re-dropped (the ENG-5094 infinite-
	// redelivery wedge this suite exists to detect), dropped_keyless would
	// climb past 1. The key_omitted_records spec applies the same
	// Consistently guard.
	Consistently(func() int64 {
		return captureExporter.counterValue("dropped_keyless")
	}).WithTimeout(2*time.Second).WithPolling(200*time.Millisecond).
		Should(Equal(int64(1)), pluginName+" must not over-count dropped_keyless (a climb would indicate the ENG-5094 redelivery wedge)")

	// This scenario sends a keyless record and a keyed MATCHING record, so
	// neither triggers the filtered_records increment; 0 here means "no
	// triggering record was sent".
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
	}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
		Should(Equal(1), pluginName+": the keyed matching record must be delivered")
}

// The dropped_keyless counter spec, driven via the plugin-name parameter.
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
// increment requires a connect-level seam or direct ReadBatch injection
// (ENG-5125).
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
	// registers filtered_records eagerly there. Without the NewCounter call
	// it is never registered and this poll fails.
	Eventually(func() bool {
		return captureExporter.registered("filtered_records")
	}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
		Should(BeTrue(),
			pluginName+": filtered_records counter must be registered on the outer stream's metrics registry")
}

// The filtered_records counter registration spec. It does not cover the
// increment (see filteredRecordsCounterScenario): a regression that removes
// filtered.Incr would leave it green.
var _ = DescribeTable("uns_beta filtered_records counter registration only (structural)",
	Label("uns_beta"),
	filteredRecordsCounterScenario,
	Entry("uns_beta — registration only; increment path NOT covered (ENG-5125)", "uns_beta"),
)
