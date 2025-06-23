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

package topic_browser_plugin

import (
	"context"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Topic Browser Integration Tests", func() {

	Describe("Pipeline Configuration", func() {

		It("should emit UNS bundles at regular intervals when implemented", func() {
			Skip("FIXME: Current implementation emits on every batch, but should buffer and emit once per second")

			// This test should verify time-based aggregation behavior:
			// 1. Messages are buffered internally
			// 2. UNS bundles are emitted at most once per second
			// 3. All buffered messages are included in the bundle
			// 4. No data is lost during buffering

			builder := service.NewStreamBuilder()

			// Add a producer to inject test messages
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			// Add topic browser processor
			err = builder.AddProcessorYAML(`
topic_browser:
  emit_interval: 1s  # This configuration doesn't exist yet - needs to be implemented
`)
			Expect(err).NotTo(HaveOccurred())

			// Collect output messages with timestamps
			var outputMessages []*service.Message
			var outputTimestamps []time.Time
			var outputMutex sync.Mutex

			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
				outputMutex.Lock()
				outputMessages = append(outputMessages, msg)
				outputTimestamps = append(outputTimestamps, time.Now())
				outputMutex.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// ✅ FIX: Use WaitGroup to prevent goroutine leak
			var streamWg sync.WaitGroup
			streamWg.Add(1)
			go func() {
				defer streamWg.Done()
				_ = stream.Run(ctx)
			}()
			defer streamWg.Wait() // Ensure stream goroutine completes before test exits

			// Send multiple messages rapidly (more than once per second)
			startTime := time.Now()
			for i := 0; i < 10; i++ {
				msg := service.NewMessage(nil)
				msg.MetaSet("umh_topic", "umh.v1.enterprise._historian.temperature")
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": startTime.Add(time.Duration(i) * 100 * time.Millisecond).UnixMilli(),
					"value":        20.0 + float64(i),
				})

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())
				time.Sleep(100 * time.Millisecond)
			}

			// Wait for aggregation
			time.Sleep(2 * time.Second)

			outputMutex.Lock()
			outputCount := len(outputMessages)
			outputMutex.Unlock()

			// Should have emitted fewer bundles than input messages due to time-based aggregation
			Expect(outputCount).To(BeNumerically("<", 10))
			Expect(outputCount).To(BeNumerically(">=", 1))

			// Verify timing between emissions (should be ~1 second apart)
			if len(outputTimestamps) > 1 {
				for i := 1; i < len(outputTimestamps); i++ {
					interval := outputTimestamps[i].Sub(outputTimestamps[i-1])
					Expect(interval.Seconds()).To(BeNumerically("~", 1.0, 0.1))
				}
			}
		})
	})
})
