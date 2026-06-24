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

// R3 (ENG-5105): the redpanda_lag routing gate for the single-plugin form.
// The registerCaptureExporter / captureExporter / startBroker / produce / rec
// helpers live in uns_beta_integration_test.go and uns_input_nack_commit_repro_test.go
// (same package).

package uns_plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// This is the gate rung for the single-plugin form. The source-level claim
// (SPEC.md "Research facts", franz_reader_ordered.go:534) is that redpanda_lag
// is created from f.res.Metrics() inside the reader's own poll goroutine, bound
// to whatever *service.Resources is handed the constructor — and uns_beta_single
// hands the constructor the OUTER res (via forwarder.NewRedpandaInput(conf, res)).
// So redpanda_lag must reach the outer stream's metrics exporter regardless of
// construction path. This spec mirrors the uns_beta metrics-routing Describe
// (uns_beta_integration_test.go:1146) but drives uns_beta_single. If this fails,
// the single-plugin form is dead: the source-level finding about metrics routing
// is wrong. Do NOT weaken the assertion to make it pass.
var _ = Describe("uns_beta_single inner-input metrics routing", Label("uns_beta"), func() {
	It("routes the inner redpanda input's redpanda_lag gauge through the outer stream's metrics registry under Go-side construction", func() {
		registerCaptureExporter()
		captureExporter.reset()

		addr := startBroker(GinkgoT())
		group := fmt.Sprintf("uns-beta-single-metrics-routing-%d", time.Now().UnixNano())
		produce(GinkgoT(), addr, rec("umh.v1.acme.berlin.temp", `{"v":1}`))

		sb := service.NewStreamBuilder()
		Expect(sb.AddInputYAML(fmt.Sprintf(`
uns_beta_single:
  broker_address: %q
  consumer_group: %q
  umh_topics: [".*"]
`, addr, group))).To(Succeed())
		// Select the capturing exporter on the OUTER stream builder. The
		// forwarder hands the constructor this same outer *service.Resources,
		// so redpanda_lag — created on f.res.Metrics() in the reader's poll
		// goroutine — must land here.
		Expect(sb.SetMetricsYAML(captureExporterName + ": {}")).To(Succeed())

		var mu sync.Mutex
		var got int
		Expect(sb.AddBatchConsumerFunc(func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			got += len(b)
			mu.Unlock()
			return nil // ack so the inner input keeps polling/connected
		})).To(Succeed())

		stream, err := sb.Build()
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var runErr error // written before close(done), read after <-done
		go func() { defer close(done); runErr = stream.Run(ctx) }()
		defer func() {
			cancel()
			<-done
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				Fail("stream run: " + runErr.Error())
			}
		}()

		// Connect + read at least one record so the inner input's poll
		// goroutine (which registers redpanda_lag) is running.
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return got
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeNumerically(">=", 1), "message never arrived through uns_beta_single")

		// The routing assertion: redpanda_lag — a metric created ONLY by the
		// inner redpanda input on f.res.Metrics() — must have been created on
		// the outer-attached capturer. If this fails, the forwarder is not
		// threading the outer res into the constructor (or the source-level
		// finding is wrong), and the single-plugin form is dead.
		Eventually(func() bool {
			return captureExporter.registered("redpanda_lag")
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeTrue(), "the inner redpanda input's redpanda_lag gauge never reached the outer stream's metrics registry under uns_beta_single's Go-side construction — the forwarder is not threading the outer res into NewRedpandaInput")
	})
})
