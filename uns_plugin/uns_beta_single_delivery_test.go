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

// The startBroker/produce/rec helpers used here live in
// uns_input_nack_commit_repro_test.go (same package).

package uns_plugin

import (
	"context"
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
)

// A record whose key matches umh_topics must flow end-to-end through the
// Go-registered uns_beta_single input.
var _ = Describe("uns_beta_single delivery", Label("uns_beta"), func() {
	It("delivers a produced record whose key matches umh_topics end-to-end through the Go-registered input", func() {
		addr := startBroker(GinkgoT())
		// A unique consumer group so the fresh group reads from the earliest
		// offset (the translation pins start_offset: earliest).
		group := fmt.Sprintf("uns-beta-single-delivery-%d", time.Now().UnixNano())
		// Produce one record whose key matches the umh_topics filter below.
		produce(GinkgoT(), addr, rec("umh.v1.acme.temp", `{"v":1}`))

		var mu sync.Mutex
		var got []string
		stop := runUnsBetaStream(GinkgoT(), fmt.Sprintf(`
uns_beta_single:
  broker_address: %q
  consumer_group: %q
  umh_topics: [".*"]
`, addr, group), func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			defer mu.Unlock()
			for _, m := range b {
				bs, _ := m.AsBytes()
				got = append(got, string(bs))
			}
			return nil
		})
		defer stop()

		// The record must arrive through uns_beta_single.
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(got)
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeNumerically(">=", 1), "message never arrived through uns_beta_single")

		mu.Lock()
		first := got[0]
		mu.Unlock()
		Expect(first).To(Equal(`{"v":1}`))
	})
})
