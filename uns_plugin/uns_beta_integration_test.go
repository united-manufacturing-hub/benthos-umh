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

// The startBroker/produce/rec/committedOffsetE helpers used here live in
// uns_input_nack_commit_repro_test.go (same package).

package uns_plugin

import (
	"context"
	"errors"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// runUnsBetaStream starts a StreamBuilder pipeline: uns_beta input -> consumerFn.
// consumerFn's returned error NACKs the batch. Returns a stop func.
func runUnsBetaStream(t testingT, unsBetaYAML string, consumerFn func(context.Context, service.MessageBatch) error) (stop func()) {
	t.Helper()
	sb := service.NewStreamBuilder()
	if err := sb.AddInputYAML(unsBetaYAML); err != nil {
		t.Fatalf("input yaml: %v", err)
	}
	if err := sb.AddBatchConsumerFunc(consumerFn); err != nil {
		t.Fatalf("consumer: %v", err)
	}
	stream, err := sb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error // written before close(done), read after <-done
	go func() { defer close(done); runErr = stream.Run(ctx) }()
	return func() {
		cancel()
		<-done
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Fatalf("stream run: %v", runErr)
		}
	}
}

var _ = Describe("uns_beta input delivery", Label("uns_beta"), func() {
	It("delivers a produced message and commits its offset on ack", func() {
		addr := startBroker(GinkgoT())
		const group = "uns-beta-delivery"
		produce(GinkgoT(), addr, rec("umh.v1.acme.berlin.temp", `{"v":1}`))

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
`, func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(got)
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeNumerically(">=", 1), "message never arrived through uns_beta")
		mu.Lock()
		first := got[0]
		mu.Unlock()
		Expect(first).To(Equal(`{"v":1}`))

		// Stop the stream, then verify the ack actually committed the offset. A
		// mis-wired ack path (or a consumer_group dropped from the innerYAML
		// redpanda config built in newUnsBetaInput) would deliver fine here but
		// replay the full topic on every restart in production. The inner
		// redpanda input commits on the 5s commit_period tick pinned in
		// renderRedpandaFragment, so Gomega's 1s default timeout would flake.
		stop()
		Eventually(func(g Gomega) {
			off, ok, err := committedOffsetE(addr, group)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue(), "no offset committed yet")
			g.Expect(off).To(Equal(int64(1)))
		}).WithTimeout(15 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})
})
