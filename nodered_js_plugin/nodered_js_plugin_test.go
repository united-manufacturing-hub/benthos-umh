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

package nodered_js_plugin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin"
)

var _ = Describe("NodeREDJS Processor", func() {
	BeforeEach(func() {
		testActivated := os.Getenv("TEST_NODERED_JS")

		// Check if environment variables are set
		if testActivated == "" {
			Skip("Skipping Node-RED JS tests: TEST_NODERED_JS not set")
			return
		}
	})

	When("using a stream builder", func() {
		It("should pass through messages unchanged", func() {
			builder := service.NewStreamBuilder()

			// Add producer function
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			// Capture messages for validation
			var messages []*service.Message
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messages = append(messages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			// Run stream in background
			go func() {
				_ = stream.Run(ctx)
			}()

			// Create and send test message
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Verify message content
			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			jsonStr, err := json.Marshal(structured)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(jsonStr)).To(Equal(`"test"`))
		})

		It("should modify message payload", func() {
			builder := service.NewStreamBuilder()

			// Add producer function
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    msg.payload = msg.payload.length;
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messages = append(messages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Create and send test message
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			jsonStr, err := json.Marshal(structured)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(jsonStr)).To(Equal(`4`))
		})

		It("should create new message", func() {
			builder := service.NewStreamBuilder()

			// Add producer function
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    var newMsg = { payload: "new message" };
    return newMsg;
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messages = append(messages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Create and send test message
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			jsonStr, err := json.Marshal(structured)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(jsonStr)).To(Equal(`"new message"`))
		})

		It("should fan out one output per element when the function returns an array", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    return [
      {payload: {value: 1}, meta: {tag_name: "a"}},
      {payload: {value: 2}, meta: {tag_name: "b"}}
    ];
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			var messagesMutex sync.Mutex
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messagesMutex.Lock()
				messages = append(messages, msg)
				messagesMutex.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("ignored")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// The function returned a 2-element array, so exactly two outputs
			// must reach the consumer, one per element, order preserved.
			Eventually(func() int {
				messagesMutex.Lock()
				defer messagesMutex.Unlock()
				return len(messages)
			}).Should(Equal(2))

			// First output: element 0's payload and meta.
			messagesMutex.Lock()
			msg0 := messages[0]
			msg1 := messages[1]
			messagesMutex.Unlock()
			structured0, err := msg0.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			jsonStr0, err := json.Marshal(structured0)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(jsonStr0)).To(Equal(`{"value":1}`))
			tagName0, exists := msg0.MetaGet("tag_name")
			Expect(exists).To(BeTrue())
			Expect(tagName0).To(Equal("a"))

			// Second output: element 1's payload and meta.
			structured1, err := msg1.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			jsonStr1, err := json.Marshal(structured1)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(jsonStr1)).To(Equal(`{"value":2}`))
			tagName1, exists := msg1.MetaGet("tag_name")
			Expect(exists).To(BeTrue())
			Expect(tagName1).To(Equal("b"))
		})

		It("should skip nil array elements and fan out the rest", func() {
			env := service.NewEnvironment()

			var mu sync.Mutex
			counts := map[string]int64{}
			exporter := &counterCaptureMetrics{mu: &mu, counts: counts}

			Expect(env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec(),
				func(_ *service.ParsedConfig, _ *service.Logger) (service.MetricsExporter, error) {
					return exporter, nil
				})).To(Succeed())

			builder := env.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			Expect(builder.SetMetricsYAML("testmetrics: {}")).To(Succeed())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    return [
      {payload: {value: 1}, meta: {tag_name: "a"}},
      null,
      {payload: {value: 2}, meta: {tag_name: "b"}}
    ];
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			var messagesMutex sync.Mutex
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messagesMutex.Lock()
				messages = append(messages, msg)
				messagesMutex.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("ignored")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// The null element in the middle is skipped rather than erroring
			// the batch, so the two non-nil elements survive.
			Eventually(func() int {
				messagesMutex.Lock()
				defer messagesMutex.Unlock()
				return len(messages)
			}).Should(Equal(2))

			// A partial-nil array still produces outputs, so the whole-input
			// drop counter must stay at 0.
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}, "500ms").Should(Equal(int64(0)))

			messagesMutex.Lock()
			msg0 := messages[0]
			msg1 := messages[1]
			messagesMutex.Unlock()

			// First output: element 0's payload and meta.
			structured0, err := msg0.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			jsonStr0, err := json.Marshal(structured0)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(jsonStr0)).To(Equal(`{"value":1}`))
			tagName0, exists := msg0.MetaGet("tag_name")
			Expect(exists).To(BeTrue())
			Expect(tagName0).To(Equal("a"))

			// Second output: element 2's payload and meta (element 1 was nil, skipped).
			structured1, err := msg1.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			jsonStr1, err := json.Marshal(structured1)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(jsonStr1)).To(Equal(`{"value":2}`))
			tagName1, exists := msg1.MetaGet("tag_name")
			Expect(exists).To(BeTrue())
			Expect(tagName1).To(Equal("b"))
		})

		It("should skip undefined array elements and fan out the rest", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    return [
      {payload: {value: 1}, meta: {tag_name: "a"}},
      undefined,
      {payload: {value: 2}, meta: {tag_name: "b"}}
    ];
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			var messagesMutex sync.Mutex
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messagesMutex.Lock()
				messages = append(messages, msg)
				messagesMutex.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("ignored")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// goja exports JS undefined as Go nil, so it hits the same
			// skip branch as null.
			Eventually(func() int {
				messagesMutex.Lock()
				defer messagesMutex.Unlock()
				return len(messages)
			}).Should(Equal(2))

			messagesMutex.Lock()
			msg0 := messages[0]
			msg1 := messages[1]
			messagesMutex.Unlock()

			structured0, err := msg0.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			jsonStr0, err := json.Marshal(structured0)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(jsonStr0)).To(Equal(`{"value":1}`))

			structured1, err := msg1.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			jsonStr1, err := json.Marshal(structured1)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(jsonStr1)).To(Equal(`{"value":2}`))
		})

		It("should error the batch when an array element is a non-object primitive", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    return [
      {payload: {value: 1}, meta: {tag_name: "a"}},
      42,
      {payload: {value: 2}, meta: {tag_name: "b"}}
    ];
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			var messagesMutex sync.Mutex
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messagesMutex.Lock()
				messages = append(messages, msg)
				messagesMutex.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("ignored")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// A non-object array element errors the batch atomically: zero
			// fan-out outputs survive, and the single error-marked input is
			// passed through to the consumer (HaveLen(1)), observed via GetError().
			var batchErr error
			Eventually(func() bool {
				messagesMutex.Lock()
				defer messagesMutex.Unlock()
				if len(messages) == 0 {
					return false
				}
				batchErr = messages[0].GetError()
				return batchErr != nil
			}).Should(BeTrue(), "expected the batch-fatal error to reach the consumer message")

			messagesMutex.Lock()
			Expect(batchErr.Error()).To(ContainSubstring("must be a message object"))
			messagesMutex.Unlock()
		})

		It("should drop all messages when returning an all-nil array", func() {
			env := service.NewEnvironment()

			var mu sync.Mutex
			counts := map[string]int64{}
			exporter := &counterCaptureMetrics{mu: &mu, counts: counts}

			Expect(env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec(),
				func(_ *service.ParsedConfig, _ *service.Logger) (service.MetricsExporter, error) {
					return exporter, nil
				})).To(Succeed())

			builder := env.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			Expect(builder.SetMetricsYAML("testmetrics: {}")).To(Succeed())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    return [null, null];
`)
			Expect(err).NotTo(HaveOccurred())

			var count int64
			err = builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
				atomic.AddInt64(&count, 1)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("ignored")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Every element is nil, so the fan-out produces an empty
			// batch: no consumer outputs and no error.
			Consistently(func() int64 {
				return atomic.LoadInt64(&count)
			}, "500ms").Should(Equal(int64(0)))

			// The whole input was dropped, so messagesDropped is bumped once.
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}).Should(Equal(int64(1)))
		})

		It("should bump messagesDropped once when the function returns an empty array", func() {
			// An empty array return is a whole-input drop: 0 outputs AND
			// messagesDropped incremented exactly once.
			// Default StreamBuilder metrics are no-op, so the counter is
			// observed via a registered MetricsExporter.
			env := service.NewEnvironment()

			var mu sync.Mutex
			counts := map[string]int64{}
			exporter := &counterCaptureMetrics{mu: &mu, counts: counts}

			Expect(env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec(),
				func(_ *service.ParsedConfig, _ *service.Logger) (service.MetricsExporter, error) {
					return exporter, nil
				})).To(Succeed())

			builder := env.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			Expect(builder.SetMetricsYAML("testmetrics: {}")).To(Succeed())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    return [];
`)
			Expect(err).NotTo(HaveOccurred())

			var consumerCount int64
			Expect(builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
				atomic.AddInt64(&consumerCount, 1)
				return nil
			})).To(Succeed())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() { _ = stream.Run(ctx) }()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("ignored")
			Expect(msgHandler(ctx, testMsg)).To(Succeed())

			// (a) 0 consumer outputs: the whole input was dropped.
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(0)))

			// (b) messagesDropped bumped exactly once for the whole input.
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}).Should(Equal(int64(1)))
		})

		It("should fan out one UNS message per ERP record from an array payload end-to-end", func() {
			// Capstone (R8): Sebastian's actual use case — an ERP API returns a
			// JSON array of records and the nodered_js function maps each record
			// to one UNS message, propagating the input's metadata to every
			// child. This composes R1 (array fan-out) + R2 (nil-skip, latent
			// here — no nil records) + the per-element payload/meta construction
			// through the full service.NewStreamBuilder pipeline.
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    return msg.payload.records.map(r => ({payload: r, meta: msg.meta}));
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			var messagesMutex sync.Mutex
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messagesMutex.Lock()
				messages = append(messages, msg)
				messagesMutex.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Input carries the ERP array payload plus metadata that must be
			// propagated to every fan-out child.
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(map[string]any{
				"records": []any{
					map[string]any{"id": float64(1), "temp": float64(22)},
					map[string]any{"id": float64(2), "temp": float64(23)},
					map[string]any{"id": float64(3), "temp": float64(24)},
				},
			})
			testMsg.MetaSet("source", "erp-api")
			testMsg.MetaSet("location_path", "enterprise.site.area")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// One input → exactly three consumer outputs, one per record.
			Eventually(func() int {
				messagesMutex.Lock()
				defer messagesMutex.Unlock()
				return len(messages)
			}).Should(Equal(3))

			expected := []string{
				`{"id":1,"temp":22}`,
				`{"id":2,"temp":23}`,
				`{"id":3,"temp":24}`,
			}

			for i, want := range expected {
				messagesMutex.Lock()
				msg := messages[i]
				messagesMutex.Unlock()

				structured, err := msg.AsStructured()
				Expect(err).NotTo(HaveOccurred())
				jsonStr, err := json.Marshal(structured)
				Expect(err).NotTo(HaveOccurred())
				Expect(string(jsonStr)).To(Equal(want))

				// Each child carries the input's metadata, propagated via the
				// meta map returned by the function.
				source, exists := msg.MetaGet("source")
				Expect(exists).To(BeTrue())
				Expect(source).To(Equal("erp-api"))
				loc, exists := msg.MetaGet("location_path")
				Expect(exists).To(BeTrue())
				Expect(loc).To(Equal("enterprise.site.area"))
			}
		})

		It("should drop messages when returning null", func() {
			builder := service.NewStreamBuilder()

			// Add producer function
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    if (msg.payload === "test") {
      return null;
    }
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var count int64
			err = builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
				atomic.AddInt64(&count, 1)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send multiple test messages
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")

			for i := 0; i < 5; i++ {
				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())
			}

			// Wait a bit to ensure all messages are processed
			time.Sleep(500 * time.Millisecond)

			// Expect no messages as all should be dropped
			Expect(atomic.LoadInt64(&count)).To(Equal(int64(0)))
		})

		It("should handle JavaScript errors gracefully", func() {
			builder := service.NewStreamBuilder()

			var messages []*service.Message
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    // This should cause an error
    undefinedFunction();
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messages = append(messages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Verify no messages make it through
			Consistently(func() int {
				return len(messages)
			}, "500ms").Should(Equal(0))
		})
	})

	When("handling different input types", func() {
		It("should handle string input", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    msg.payload = msg.payload.toUpperCase();
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messages = append(messages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Create and send test message with string payload
			testMsg := service.NewMessage([]byte("hello world"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			jsonStr, err := json.Marshal(structured)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(jsonStr)).To(Equal(`"HELLO WORLD"`))
		})

		It("should handle number input", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    msg.payload = msg.payload * 2;
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messages = append(messages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Create and send test message with number payload
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(42)
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			jsonStr, err := json.Marshal(structured)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(jsonStr)).To(Equal(`84`))
		})

		It("should handle metadata", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    // Add new metadata
    msg.meta.processed = "true";
    msg.meta.count = "1";

    // Modify existing metadata
    msg.meta.source = "modified-" + msg.meta.source;

    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messages = append(messages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Create message with metadata
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")
			testMsg.MetaSet("source", "original")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			// Check metadata
			processed, exists := msg.MetaGet("processed")
			Expect(exists).To(BeTrue())
			Expect(processed).To(Equal("true"))

			count, exists := msg.MetaGet("count")
			Expect(exists).To(BeTrue())
			Expect(count).To(Equal("1"))

			source, exists := msg.MetaGet("source")
			Expect(exists).To(BeTrue())
			Expect(source).To(Equal("modified-original"))
		})

		It("should preserve metadata when not modified", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    // Only modify payload, leave metadata unchanged
    msg.payload = "modified";
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messages = append(messages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Create message with metadata
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")
			testMsg.MetaSet("original", "value")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			// Check metadata is preserved
			original, exists := msg.MetaGet("original")
			Expect(exists).To(BeTrue())
			Expect(original).To(Equal("value"))
		})
	})

	Context("Performance testing", func() {
		It("compares JavaScript and Bloblang processing performance", func() {
			experiment := gmeasure.NewExperiment("Processing Performance Comparison")
			AddReportEntry(experiment.Name, experiment)

			// Test JavaScript processor
			jsBuilder := service.NewStreamBuilder()
			var jsMsgHandler service.MessageHandlerFunc
			jsMsgHandler, err := jsBuilder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = jsBuilder.AddProcessorYAML(`
nodered_js:
  code: |
    msg.payload = msg.payload * 2;
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var jsMessages []*service.Message
			err = jsBuilder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				jsMessages = append(jsMessages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			jsStream, err := jsBuilder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()

			go func() {
				_ = jsStream.Run(ctx)
			}()

			// Run JavaScript measurement 5 times
			for i := 0; i < 5; i++ {
				jsMessages = nil // Reset messages slice
				experiment.MeasureDuration("JavaScript processing", func() {
					for j := 0; j < 1000; j++ {
						testMsg := service.NewMessage(nil)
						testMsg.SetStructured(j)
						err = jsMsgHandler(ctx, testMsg)
						Expect(err).NotTo(HaveOccurred())
					}

					Eventually(func() int {
						return len(jsMessages)
					}).Should(Equal(1000))
				})
			}

			// Test Bloblang processor
			bloblangBuilder := service.NewStreamBuilder()
			var bloblangMsgHandler service.MessageHandlerFunc
			bloblangMsgHandler, err = bloblangBuilder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = bloblangBuilder.AddProcessorYAML(`
bloblang: 'root = this * 2'
`)
			Expect(err).NotTo(HaveOccurred())

			var bloblangMessages []*service.Message
			err = bloblangBuilder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				bloblangMessages = append(bloblangMessages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			bloblangStream, err := bloblangBuilder.Build()
			Expect(err).NotTo(HaveOccurred())

			go func() {
				_ = bloblangStream.Run(ctx)
			}()

			// Run Bloblang measurement 5 times
			for i := 0; i < 5; i++ {
				bloblangMessages = nil // Reset messages slice
				experiment.MeasureDuration("Bloblang processing", func() {
					for j := 0; j < 1000; j++ {
						testMsg := service.NewMessage(nil)
						testMsg.SetStructured(j)
						err = bloblangMsgHandler(ctx, testMsg)
						Expect(err).NotTo(HaveOccurred())
					}

					Eventually(func() int {
						return len(bloblangMessages)
					}).Should(Equal(1000))
				})
			}

			// Verify last messages for sanity check
			jsLastMsg := jsMessages[len(jsMessages)-1]
			jsStructured, err := jsLastMsg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			Expect(jsStructured).To(Equal(int64(1998))) // 999 * 2

			bloblangLastMsg := bloblangMessages[len(bloblangMessages)-1]
			bloblangStructured, err := bloblangLastMsg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			Expect(bloblangStructured).To(Equal(int64(1998))) // 999 * 2
		})

		It("should handle VM state cleanup and prevent global variable leakage", func() {
			testActivated := os.Getenv("TEST_NODERED_JS")
			if testActivated == "" {
				Skip("Skipping Node-RED JS tests: TEST_NODERED_JS not set")
				return
			}

			// Test msg variable cleanup between messages using single processor
			builder := service.NewStreamBuilder()
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			// JavaScript code that checks if msg properties persist between messages
			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    // Check if msg has a leftover property from previous execution
    if (msg.leftover) {
      // This indicates msg wasn't properly cleaned
      msg.payload = "LEAKED: " + msg.leftover;
    } else {
      // Normal processing - add a property that should be cleaned
      msg.leftover = "should_be_cleaned";
      msg.payload = "CLEAN: " + msg.payload;
    }

    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messages = append(messages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send first message
			testMsg1 := service.NewMessage(nil)
			testMsg1.SetStructured("test1")
			err = msgHandler(ctx, testMsg1)
			Expect(err).NotTo(HaveOccurred())

			// Wait for first message to be processed
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Verify first message result
			structured1, err := messages[0].AsStructured()
			Expect(err).NotTo(HaveOccurred())
			Expect(structured1).To(Equal("CLEAN: test1"))

			// Send second message - should NOT see leaked msg properties
			testMsg2 := service.NewMessage(nil)
			testMsg2.SetStructured("test2")
			err = msgHandler(ctx, testMsg2)
			Expect(err).NotTo(HaveOccurred())

			// Wait for second message to be processed
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))

			// Verify second message result - should be "CLEAN", not "LEAKED"
			structured2, err := messages[1].AsStructured()
			Expect(err).NotTo(HaveOccurred())
			// This should be "CLEAN: test2", proving msg state was cleaned
			Expect(structured2).To(Equal("CLEAN: test2"))
		})

		It("should enforce strict mode and prevent accidental global variables", func() {
			testActivated := os.Getenv("TEST_NODERED_JS")
			if testActivated == "" {
				Skip("Skipping Node-RED JS tests: TEST_NODERED_JS not set")
				return
			}

			// Test that accidental global variable creation throws an error
			builder := service.NewStreamBuilder()
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			// JavaScript code that tries to create an accidental global (should fail in strict mode)
			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    // This should throw an error in strict mode
    accidentalGlobal = "this should fail";
    msg.payload = "should not reach here";
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messages = append(messages, msg)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send a message - this should cause an error due to strict mode
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Wait a bit to see if message gets processed (it shouldn't)
			time.Sleep(100 * time.Millisecond)

			// Verify no messages were processed due to the strict mode error
			Expect(messages).To(BeEmpty())
		})

		It("should handle concurrent processing safely", func() {
			testActivated := os.Getenv("TEST_NODERED_JS")
			if testActivated == "" {
				Skip("Skipping Node-RED JS tests: TEST_NODERED_JS not set")
				return
			}

			// Test concurrent processing
			builder := service.NewStreamBuilder()
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    // Simple transformation to test concurrent access
    msg.payload = "processed_" + msg.payload;
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			var messagesMutex sync.Mutex
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messagesMutex.Lock()
				messages = append(messages, msg)
				messagesMutex.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send multiple messages concurrently
			const numMessages = 20
			var wg sync.WaitGroup

			for i := 0; i < numMessages; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					testMsg := service.NewMessage(nil)
					testMsg.SetStructured(fmt.Sprintf("test_%d", index))
					err := msgHandler(ctx, testMsg)
					Expect(err).NotTo(HaveOccurred())
				}(i)
			}

			wg.Wait()

			// Wait for all messages to be processed
			Eventually(func() int {
				messagesMutex.Lock()
				count := len(messages)
				messagesMutex.Unlock()
				return count
			}).Should(Equal(numMessages))

			// Verify all messages were processed correctly
			messagesMutex.Lock()
			processedPayloads := make(map[string]bool)
			for _, msg := range messages {
				structured, err := msg.AsStructured()
				Expect(err).NotTo(HaveOccurred())
				payload, ok := structured.(string)
				Expect(ok).To(BeTrue())
				processedPayloads[payload] = true
			}
			messagesMutex.Unlock()

			// Verify we have the expected number of unique processed messages
			Expect(processedPayloads).To(HaveLen(numMessages))
		})
	})
})

var _ = Describe("NodeREDJS cache", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_NODERED_JS") == "" {
			Skip("Skipping Node-RED JS tests: TEST_NODERED_JS not set")
		}
	})

	buildStream := func(code string) (service.MessageHandlerFunc, *[]*service.Message, context.CancelFunc) {
		builder := service.NewStreamBuilder()
		handler, err := builder.AddProducerFunc()
		Expect(err).NotTo(HaveOccurred())

		err = builder.AddProcessorYAML("nodered_js:\n  code: |\n" + indentLines(code, "    "))
		Expect(err).NotTo(HaveOccurred())

		var msgs []*service.Message
		err = builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
			msgs = append(msgs, m)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())

		stream, err := builder.Build()
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		go func() { _ = stream.Run(ctx) }()
		return handler, &msgs, cancel
	}

	When("using cache", func() {
		It("set then get returns the stored value", func() {
			handler, msgs, cancel := buildStream(`
cache.set("k", 42);
msg.payload = cache.get("k");
return msg;
`)
			defer cancel()

			err := handler(context.Background(), newMsg("ignored"))
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() int { return len(*msgs) }).Should(Equal(1))
			Expect(payloadFloat(*msgs, 0)).To(Equal(float64(42)))
		})

		It("get on unknown key returns undefined", func() {
			handler, msgs, cancel := buildStream(`
var v = cache.get("nope");
msg.payload = (typeof v === "undefined") ? "is_undefined" : "not_undefined";
return msg;
`)
			defer cancel()

			err := handler(context.Background(), newMsg("ignored"))
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() int { return len(*msgs) }).Should(Equal(1))
			Expect(payloadString(*msgs, 0)).To(Equal("is_undefined"))
		})

		It("exists returns false for missing key", func() {
			handler, msgs, cancel := buildStream(`
msg.payload = cache.exists("nope");
return msg;
`)
			defer cancel()

			err := handler(context.Background(), newMsg("ignored"))
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() int { return len(*msgs) }).Should(Equal(1))
			s, sErr := (*msgs)[0].AsStructured()
			Expect(sErr).NotTo(HaveOccurred())
			Expect(s).To(BeFalse())
		})

		It("exists returns true for existing key", func() {
			handler, msgs, cancel := buildStream(`
cache.set("k", "v");
msg.payload = cache.exists("k");
return msg;
`)
			defer cancel()

			err := handler(context.Background(), newMsg("ignored"))
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() int { return len(*msgs) }).Should(Equal(1))
			s, sErr := (*msgs)[0].AsStructured()
			Expect(sErr).NotTo(HaveOccurred())
			Expect(s).To(BeTrue())
		})

		It("delete removes a key", func() {
			handler, msgs, cancel := buildStream(`
cache.set("x", 1);
cache.delete("x");
msg.payload = cache.exists("x");
return msg;
`)
			defer cancel()

			err := handler(context.Background(), newMsg("ignored"))
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() int { return len(*msgs) }).Should(Equal(1))
			s, sErr := (*msgs)[0].AsStructured()
			Expect(sErr).NotTo(HaveOccurred())
			Expect(s).To(BeFalse())
		})

		It("value persists across consecutive messages", func() {
			handler, msgs, cancel := buildStream(`
var count = 0;
if (cache.exists("count")) {
  count = cache.get("count");
}
count++;
cache.set("count", count);
msg.payload = count;
return msg;
`)
			defer cancel()

			ctx := context.Background()
			for i := 0; i < 3; i++ {
				err := handler(ctx, newMsg("tick"))
				Expect(err).NotTo(HaveOccurred())
			}
			Eventually(func() int { return len(*msgs) }).Should(Equal(3))
			Expect(payloadFloat(*msgs, 2)).To(Equal(float64(3)))
		})

		It("stores and retrieves an object value", func() {
			handler, msgs, cancel := buildStream(`
cache.set("obj", { temperature: 42.5, unit: "C" });
var obj = cache.get("obj");
msg.payload = obj.temperature;
return msg;
`)
			defer cancel()

			err := handler(context.Background(), newMsg("ignored"))
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() int { return len(*msgs) }).Should(Equal(1))
			Expect(payloadFloat(*msgs, 0)).To(Equal(42.5))
		})

		It("is safe under concurrent message processing", func() {
			handler, msgs, cancel := buildStream(`
var n = 0;
if (cache.exists("n")) { n = cache.get("n"); }
cache.set("n", n + 1);
msg.payload = "ok";
return msg;
`)
			defer cancel()

			const numMsgs = 30
			ctx := context.Background()
			var wg sync.WaitGroup
			wg.Add(numMsgs)
			for i := 0; i < numMsgs; i++ {
				go func() {
					defer wg.Done()
					_ = handler(ctx, newMsg("concurrent"))
				}()
			}
			wg.Wait()
			Eventually(func() int { return len(*msgs) }).Should(Equal(numMsgs))
		})

		It("cache is shared across VM pool instances", func() {
			handler, msgs, cancel := buildStream(`
if (!cache.exists("shared")) {
  cache.set("shared", "seeded");
  msg.payload = "first";
} else {
  msg.payload = cache.get("shared");
}
return msg;
`)
			defer cancel()

			ctx := context.Background()
			for i := 0; i < 5; i++ {
				err := handler(ctx, newMsg("x"))
				Expect(err).NotTo(HaveOccurred())
			}
			Eventually(func() int { return len(*msgs) }).Should(Equal(5))
			for i := 1; i < 5; i++ {
				Expect(payloadString(*msgs, i)).To(Equal("seeded"))
			}
		})

		It("numeric key coercion: number passed as key is coerced to string", func() {
			handler, msgs, cancel := buildStream(`
cache.set("42", "byStringKey");
msg.payload = cache.get("42");
return msg;
`)
			defer cancel()

			err := handler(context.Background(), newMsg("ignored"))
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() int { return len(*msgs) }).Should(Equal(1))
			Expect(payloadString(*msgs, 0)).To(Equal("byStringKey"))
		})

		It("exists + get pattern with object", func() {
			handler, msgs, cancel := buildStream(`
if (!cache.exists("state")) {
  cache.set("state", { alarm: false, count: 0 });
}
var state = cache.get("state");
state.count++;
cache.set("state", state);
msg.payload = state.count;
return msg;
`)
			defer cancel()

			ctx := context.Background()
			for i := 0; i < 2; i++ {
				err := handler(ctx, newMsg("tick"))
				Expect(err).NotTo(HaveOccurred())
			}
			Eventually(func() int { return len(*msgs) }).Should(Equal(2))
			Expect(payloadFloat(*msgs, 1)).To(Equal(float64(2)))
		})
	})
})

// newMsg creates a service.Message with the given string payload.
func newMsg(payload string) *service.Message {
	return service.NewMessage([]byte(payload))
}

// payloadString extracts the string payload from messages[i].
func payloadString(msgs []*service.Message, i int) string {
	s, err := msgs[i].AsStructured()
	Expect(err).NotTo(HaveOccurred())
	str, ok := s.(string)
	Expect(ok).To(BeTrue(), "expected string payload, got %T: %v", s, s)
	return str
}

// payloadFloat extracts a numeric payload as float64 (goja may return int64 for whole numbers).
func payloadFloat(msgs []*service.Message, i int) float64 {
	s, err := msgs[i].AsStructured()
	Expect(err).NotTo(HaveOccurred())
	switch v := s.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case int:
		return float64(v)
	default:
		Fail(fmt.Sprintf("expected numeric payload, got %T: %v", s, s))
		return 0
	}
}

// indentLines prepends prefix to every line of s.
func indentLines(s string, prefix string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if l != "" {
			lines[i] = prefix + l
		}
	}
	return strings.Join(lines, "\n")
}

// counterCaptureMetrics is a service.MetricsExporter that aggregates integer
// counter increments by counter name, ignoring labels. It is the only public
// seam (outside the benthos module) to observe processor-level MetricCounter
// increments such as nodered_js's internal messagesDropped, which is not
// readable through the default StreamBuilder (its metrics are no-op).
type counterCaptureMetrics struct {
	mu     *sync.Mutex
	counts map[string]int64
}

func (m *counterCaptureMetrics) NewCounterCtor(name string, _ ...string) service.MetricsExporterCounterCtor {
	return func(_ ...string) service.MetricsExporterCounter {
		return &capturedCounter{name: name, mu: m.mu, counts: m.counts}
	}
}

func (m *counterCaptureMetrics) NewTimerCtor(string, ...string) service.MetricsExporterTimerCtor {
	return func(...string) service.MetricsExporterTimer { return noopTimer{} }
}

func (m *counterCaptureMetrics) NewGaugeCtor(string, ...string) service.MetricsExporterGaugeCtor {
	return func(...string) service.MetricsExporterGauge { return noopGauge{} }
}

func (m *counterCaptureMetrics) Close(context.Context) error { return nil }

type capturedCounter struct {
	name   string
	mu     *sync.Mutex
	counts map[string]int64
}

func (c *capturedCounter) Incr(n int64) {
	c.mu.Lock()
	c.counts[c.name] += n
	c.mu.Unlock()
}

type noopTimer struct{}

func (noopTimer) Timing(int64) {}

type noopGauge struct{}

func (noopGauge) Set(int64) {}

var _ = Describe("js logmessage", func() {
	DescribeTable("format",
		func(input []any, expected string) {
			result := nodered_js_plugin.FormatConsoleLogMsg(input)
			Expect(result).To(Equal(expected))
		},
		Entry(`handles empty input`, []any{}, ``),
		Entry(`escapes standard string`, []any{`hello world`}, `'hello world'`),
		Entry(`escapes empty string`, []any{""}, `''`),
		Entry(`escapes strings with single quote`, []any{`hello ' world`}, `'hello \' world'`),
		Entry(`escapes strings with surrounding single quote`, []any{`'hello world'`}, `'\'hello world\''`),
		Entry(`escapes strings with double quote`, []any{`hello " world`}, `'hello " world'`),
		Entry(`escapes strings with backtick`, []any{"hello ` world"}, "'hello ` world'"),
		Entry(`escapes strings with backslash`, []any{"hello\\world"}, `'hello\\world'`),
		Entry(`escapes strings with newline`, []any{"hello\nworld"}, `'hello\nworld'`),
		Entry(`escapes strings with carriage return`, []any{"hello\rworld"}, `'hello\rworld'`),
		Entry(`escapes strings with horizontal tab`, []any{"hello\tworld"}, `'hello\tworld'`),
		Entry(`escapes strings with backspace`, []any{"hello\bworld"}, `'hello\bworld'`),
		Entry(`escapes strings with formfeed`, []any{"hello\fworld"}, `'hello\fworld'`),
		Entry(`handles nil values`, []any{nil}, `null`),
		Entry(`handles boolean true`, []any{true}, `true`),
		Entry(`handles boolean false`, []any{false}, `false`),
		Entry(`handles zero int64`, []any{int64(0)}, `0`),
		Entry(`handles positive int64`, []any{int64(42)}, `42`),
		Entry(`handles negative int64`, []any{int64(-42)}, `-42`),
		Entry(`handles max int64`, []any{math.MaxInt64}, `9223372036854775807`),
		Entry(`handles min int64`, []any{math.MinInt64}, `-9223372036854775808`),
		Entry(`handles zero float64`, []any{float64(0)}, `0`),
		Entry(`handles float64 values without fractional part`, []any{float64(42)}, `42`),
		Entry(`handles float64 values with precision of 2`, []any{float64(42.42)}, `42.42`),
		Entry(`handles float64 values with precision of 4`, []any{float64(42.4242)}, `42.4242`),
		Entry(`handles float64 values with precision of 6`, []any{float64(42.424242)}, `42.424242`),
		Entry(`handles float64 values with precision of 8`, []any{float64(42.42424242)}, `42.42424242`),
		Entry(`handles float64 Infinity`, []any{math.Inf(1)}, `Infinity`),
		Entry(`handles float64 -Infinity`, []any{math.Inf(-1)}, `-Infinity`),
		Entry(`handles float64 NaN`, []any{math.NaN()}, `NaN`),
		Entry(`handles negative zero float64`, []any{math.Copysign(0, -1)}, `-0`),
		Entry(`handles BigInt values`, []any{big.NewInt(42)}, `42`),
		Entry(`handles BigFloat values`, []any{big.NewFloat(42)}, `42`),
		Entry(`handles empty slices`, []any{[]any{}}, `[]`),
		Entry(`handles slice with single element`, []any{[]any{1}}, `[ 1 ]`),
		Entry(`handles slices of numbers`, []any{[]any{1, 2, 3}}, `[ 1, 2, 3 ]`),
		Entry(`handles mixed slices`, []any{[]any{1, "2", 3}}, `[ 1, '2', 3 ]`),
		Entry(`handles slices within slices`, []any{[]any{[]any{}}}, `[ [] ]`),
		Entry(`handles empty maps`, []any{map[string]any{}}, `{}`),
		Entry(`handles maps with single value`, []any{map[string]any{"foo": "bar"}}, `{ foo: 'bar' }`),
		Entry(`handles maps with multiple values`, []any{map[string]any{"foo": 1, "bar": 2, "baz": []any{1, 2, 3}}}, `{ bar: 2, baz: [ 1, 2, 3 ], foo: 1 }`),
		Entry(`handles map keys with spaces`, []any{map[string]any{"foo bar": 1}}, `{ 'foo bar': 1 }`),
		Entry(`handles map keys with single quotes`, []any{map[string]any{"foo'bar": 1}}, `{ 'foo\'bar': 1 }`),
		Entry(`handles map keys with double quotes`, []any{map[string]any{`foo"bar`: 1}}, `{ 'foo"bar': 1 }`),
		Entry(`handles map keys with backticks`, []any{map[string]any{"foo`bar": 1}}, "{ 'foo`bar': 1 }"),
		Entry(`handles map keys with surrounding quotes`, []any{map[string]any{"'foo bar'": 1}}, `{ '\'foo bar\'': 1 }`),
		Entry(`handles maps within maps`, []any{map[string]any{"foo": map[string]any{}}}, `{ foo: {} }`),
		Entry(`handles slices within maps`, []any{map[string]any{"foo": []any{}}}, `{ foo: [] }`),
		Entry(`handles maps within slices`, []any{[]any{map[string]any{}}}, `[ {} ]`),
		Entry(`handles multiple arguments`, []any{1, "foo", map[string]any{"foo": "bar"}}, `1 'foo' { foo: 'bar' }`),
	)
})

var _ = Describe("ConvertMessageToJSObject", func() {
	DescribeTable("parses payload",
		func(input string, expectedPayload any) {
			expectedOutput := map[string]any{"payload": expectedPayload}
			msg := service.NewMessage([]byte(input))
			output, err := nodered_js_plugin.ConvertMessageToJSObject(msg)
			Expect(err).ToNot(HaveOccurred())
			Expect(output).To(Equal(expectedOutput))
		},
		Entry(`empty input`, "", ""),
		Entry(`generic string input as string`, "foo", "foo"),
		Entry(`boolean input as boolean`, "true", true),
		Entry(`null string as null`, "null", nil),
		Entry(`quoted number as string`, `"42"`, "42"),
		Entry(`number input as float`, "42", float64(42)),
		Entry(`decimal input as float`, "42.42", float64(42.42)),
		Entry(`empty json string as empty object`, "{}", map[string]any{}),
		Entry(`json string with key and value as string`, `{"foo":"bar"}`, map[string]any{"foo": "bar"}),
		Entry(`json string with value as null`, `{"foo":null}`, map[string]any{"foo": nil}),
		Entry(`json string with value as boolean`, `{"foo":true}`, map[string]any{"foo": true}),
		Entry(`json string with numeric value as float`, `{"foo":42}`, map[string]any{"foo": float64(42)}),
		Entry(`json string with negative numeric value as negative float`, `{"foo":-42}`, map[string]any{"foo": float64(-42)}),
		Entry(`json string with value as decimal number`, `{"foo":42.42}`, map[string]any{"foo": float64(42.42)}),
		Entry(`json string nested object`, `{"foo":{}}`, map[string]any{"foo": map[string]any{}}),
		Entry(`json string with value as array`, `{"foo":[1,2,3]}`, map[string]any{"foo": []any{float64(1), float64(2), float64(3)}}),
		Entry(`json array with numbers as float array`, `[1,2,3]`, []any{float64(1), float64(2), float64(3)}),
	)
})
