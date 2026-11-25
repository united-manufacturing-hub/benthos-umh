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

package uns_plugin

import (
	"context"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// The redpanda reader creates redpanda_lag on the resources it is built with.
// buildUnsBetaInner passes uns_beta's own resources, so the gauge reaches the
// stream's metrics exporter.
var _ = Describe("uns_beta metrics routing", Label("uns_beta"), func() {
	It("routes the redpanda reader's redpanda_lag gauge through the stream's metrics registry", func() {
		registerCaptureExporter()
		captureExporter.reset()

		addr := startBroker(GinkgoT())
		group := fmt.Sprintf("uns-beta-metrics-routing-%d", time.Now().UnixNano())
		produce(GinkgoT(), addr, rec("umh.v1.acme.berlin.temp", `{"v":1}`))

		sb := service.NewStreamBuilder()
		Expect(sb.AddInputYAML(fmt.Sprintf(`
uns_beta:
  broker_address: %q
  consumer_group: %q
  umh_topics: [".*"]
`, addr, group))).To(Succeed())
		Expect(sb.SetMetricsYAML(captureExporterName + ": {}")).To(Succeed())

		var mu sync.Mutex
		var got int
		Expect(sb.AddBatchConsumerFunc(func(_ context.Context, b service.MessageBatch) error {
			mu.Lock()
			got += len(b)
			mu.Unlock()
			return nil
		})).To(Succeed())

		stream, err := sb.Build()
		Expect(err).NotTo(HaveOccurred())

		defer startStream(GinkgoT(), stream)()

		// A delivered record means the reader's poll loop, which creates
		// redpanda_lag, is running.
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return got
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeNumerically(">=", 1), "message never arrived through uns_beta")

		Eventually(func() bool {
			return captureExporter.registered("redpanda_lag")
		}).WithTimeout(15*time.Second).WithPolling(100*time.Millisecond).
			Should(BeTrue(), "the redpanda reader's redpanda_lag gauge never reached the stream's metrics registry")
	})
})
