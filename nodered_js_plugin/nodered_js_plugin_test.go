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

		It("should drop the message when an array element is a non-object primitive", func() {
			// A non-object array element (42) causes a bad_array_element drop:
			// zero fan-out outputs survive, the input is dropped (not
			// forwarded), and messages_dropped{reason=bad_array_element}==1.
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
      42,
      {payload: {value: 2}, meta: {tag_name: "b"}}
    ];
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

			go func() {
				_ = stream.Run(ctx)
			}()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("ignored")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// No consumer outputs: the whole input was dropped (bad array element).
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(0)))

			// messages_dropped{reason=bad_array_element} == 1.
			Eventually(func() int64 {
				return exporter.labeledValue("messages_dropped", "bad_array_element")
			}, "2s").Should(Equal(int64(1)))
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
			// A real ERP fan-out use case: an ERP API returns a JSON array of
			// records and the nodered_js function maps each record to one UNS
			// message, propagating the input's metadata to every child. This
			// exercises the array fan-out, nil-skip, and per-element payload/meta
			// construction through the full service.NewStreamBuilder pipeline.
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
			// A JS throw (undefinedFunction) causes a drop-loudly: the
			// input is absent from the output, not forwarded with SetError.
			var consumerCount int64
			builder := service.NewStreamBuilder()

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

			Expect(builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
				atomic.AddInt64(&consumerCount, 1)
				return nil
			})).To(Succeed())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// No consumer outputs: the throwing input was dropped, not forwarded.
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(0)))
		})

		It("should drop the throwing message and continue the batch when a mid-batch message throws", func() {
			// A 3-message batch [good, bad, good] where "bad" throws. Under
			// drop-loudly, the throwing message is dropped (absent from
			// output), the two good messages flow, messages_errored stays 0,
			// and messages_dropped{reason=js_throw}==1.
			env := service.NewEnvironment()

			var mu sync.Mutex
			counts := map[string]int64{}
			exporter := &counterCaptureMetrics{mu: &mu, counts: counts}

			Expect(env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec(),
				func(_ *service.ParsedConfig, _ *service.Logger) (service.MetricsExporter, error) {
					return exporter, nil
				})).To(Succeed())

			builder := env.NewStreamBuilder()

			var batchHandler service.MessageBatchHandlerFunc
			batchHandler, err := builder.AddBatchProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			Expect(builder.SetMetricsYAML("testmetrics: {}")).To(Succeed())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    if (msg.payload === "throw") { throw new Error("boom"); }
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var consumerCount int64
			var messages []*service.Message
			var messagesMutex sync.Mutex
			Expect(builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				atomic.AddInt64(&consumerCount, 1)
				messagesMutex.Lock()
				messages = append(messages, msg)
				messagesMutex.Unlock()
				return nil
			})).To(Succeed())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() { _ = stream.Run(ctx) }()

			// Feed a multi-message batch: [good, bad, good].
			msg0 := service.NewMessage(nil)
			msg0.SetStructured("good1")
			msg1 := service.NewMessage(nil)
			msg1.SetStructured("throw")
			msg2 := service.NewMessage(nil)
			msg2.SetStructured("good2")
			batch := service.MessageBatch{msg0, msg1, msg2}
			Expect(batchHandler(ctx, batch)).To(Succeed())

			// (a) Only 2 reach the consumer: the good messages. The throw is dropped.
			Eventually(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "2s").Should(Equal(int64(2)))

			// (b) messages_errored == 0 (drops don't bump errored).
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_errored"]
			}, "500ms").Should(Equal(int64(0)))

			// (c) messages_dropped{reason=js_throw} == 1.
			Eventually(func() int64 {
				return exporter.labeledValue("messages_dropped", "js_throw")
			}, "2s").Should(Equal(int64(1)))

			// (d) messages_processed == 2 (the two good outputs).
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_processed"]
			}, "500ms").Should(Equal(int64(2)))

			// (e) No output message carries an error (no forward-on-error).
			messagesMutex.Lock()
			for _, m := range messages {
				Expect(m.GetError()).To(Succeed(), "no output message should carry an error")
			}
			messagesMutex.Unlock()
		})

		It("should drop a null-returning message and independently drop a later throwing one", func() {
			// [drop, throw]: msg0 returns null (a genuine drop) and msg1
			// throws (dropped via RecordDrop). Both messages are absent from
			// the output. messages_dropped is 2 (1 deliberate + 1 js_throw),
			// messages_errored stays 0.
			env := service.NewEnvironment()

			var mu sync.Mutex
			counts := map[string]int64{}
			exporter := &counterCaptureMetrics{mu: &mu, counts: counts}

			Expect(env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec(),
				func(_ *service.ParsedConfig, _ *service.Logger) (service.MetricsExporter, error) {
					return exporter, nil
				})).To(Succeed())

			builder := env.NewStreamBuilder()

			var batchHandler service.MessageBatchHandlerFunc
			batchHandler, err := builder.AddBatchProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			Expect(builder.SetMetricsYAML("testmetrics: {}")).To(Succeed())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    if (msg.payload === "drop") { return null; }
    if (msg.payload === "throw") { throw new Error("boom"); }
    return msg;
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

			msg0 := service.NewMessage(nil)
			msg0.SetStructured("drop")
			msg1 := service.NewMessage(nil)
			msg1.SetStructured("throw")
			batch := service.MessageBatch{msg0, msg1}
			Expect(batchHandler(ctx, batch)).To(Succeed())

			// No consumer outputs: both messages are dropped.
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(0)))

			// messages_errored == 0 (drops don't bump errored).
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_errored"]
			}, "500ms").Should(Equal(int64(0)))

			// messages_dropped total == 2 (1 deliberate + 1 js_throw).
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}, "2s").Should(Equal(int64(2)))

			// messages_dropped{reason=js_throw} == 1 (only the throw).
			Eventually(func() int64 {
				return exporter.labeledValue("messages_dropped", "js_throw")
			}, "2s").Should(Equal(int64(1)))

			// messages_processed == 0: no successful outputs.
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_processed"]
			}, "500ms").Should(Equal(int64(0)))
		})

		It("should bump messages_processed by the output count for a fan-out return", func() {
			// 1 input -> 2 outputs (array fan-out). messages_processed counts
			// successfully produced OUTPUTS, so it must be 2, not 1.
			env := service.NewEnvironment()

			var mu sync.Mutex
			counts := map[string]int64{}
			exporter := &counterCaptureMetrics{mu: &mu, counts: counts}

			Expect(env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec(),
				func(_ *service.ParsedConfig, _ *service.Logger) (service.MetricsExporter, error) {
					return exporter, nil
				})).To(Succeed())

			builder := env.NewStreamBuilder()

			var batchHandler service.MessageBatchHandlerFunc
			batchHandler, err := builder.AddBatchProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			Expect(builder.SetMetricsYAML("testmetrics: {}")).To(Succeed())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    return [
      {payload: {value: 1}, meta: {tag_name: "a"}},
      {payload: {value: 2}, meta: {tag_name: "b"}}
    ];
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
			batch := service.MessageBatch{testMsg}
			Expect(batchHandler(ctx, batch)).To(Succeed())

			// 2 outputs reach the consumer.
			Eventually(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "2s").Should(Equal(int64(2)))

			// messages_processed == 2 (output count), not 1 (input count).
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_processed"]
			}, "2s").Should(Equal(int64(2)))

			// No drops, no errors.
			mu.Lock()
			Expect(counts["messages_dropped"]).To(Equal(int64(0)))
			Expect(counts["messages_errored"]).To(Equal(int64(0)))
			mu.Unlock()
		})

		It("should drop the throwing message and continue the batch (drop-loudly)", func() {
			// [good, boom, good]: msg1 throws. Under drop-loudly, the
			// throwing message is dropped (absent from output), the two
			// good messages flow, messages_dropped{reason=js_throw}==1,
			// and NO output message carries an error (no forward).
			env := service.NewEnvironment()

			var mu sync.Mutex
			counts := map[string]int64{}
			labeledCounts := map[string]map[string]int64{}
			exporter := &counterCaptureMetrics{
				mu:            &mu,
				counts:        counts,
				labeledCounts: labeledCounts,
			}

			Expect(env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec(),
				func(_ *service.ParsedConfig, _ *service.Logger) (service.MetricsExporter, error) {
					return exporter, nil
				})).To(Succeed())

			builder := env.NewStreamBuilder()

			var batchHandler service.MessageBatchHandlerFunc
			batchHandler, err := builder.AddBatchProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			Expect(builder.SetMetricsYAML("testmetrics: {}")).To(Succeed())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    if (msg.payload === "boom") { throw new Error("boom"); }
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			var consumerCount int64
			var messages []*service.Message
			var messagesMutex sync.Mutex
			Expect(builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				atomic.AddInt64(&consumerCount, 1)
				messagesMutex.Lock()
				messages = append(messages, msg)
				messagesMutex.Unlock()
				return nil
			})).To(Succeed())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() { _ = stream.Run(ctx) }()

			// Feed [good, boom, good].
			msg0 := service.NewMessage(nil)
			msg0.SetStructured("good1")
			msg1 := service.NewMessage(nil)
			msg1.SetStructured("boom")
			msg2 := service.NewMessage(nil)
			msg2.SetStructured("good2")
			batch := service.MessageBatch{msg0, msg1, msg2}
			Expect(batchHandler(ctx, batch)).To(Succeed())

			// (a) Only 2 messages reach the consumer (the good ones).
			Eventually(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "2s").Should(Equal(int64(2)))

			// (b) messages_dropped{reason=js_throw} == 1 (only the throw).
			Eventually(func() int64 {
				return exporter.labeledValue("messages_dropped", "js_throw")
			}, "2s").Should(Equal(int64(1)))

			// (c) messages_errored == 0 (drops don't bump errored).
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_errored"]
			}, "500ms").Should(Equal(int64(0)))

			// (d) messages_processed == 2 (the two good outputs).
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_processed"]
			}, "500ms").Should(Equal(int64(2)))

			// (e) NO output message carries an error (no forward-on-error).
			messagesMutex.Lock()
			for _, m := range messages {
				Expect(m.GetError()).To(Succeed(), "no output message should carry an error")
			}
			messagesMutex.Unlock()
		})

		It("capstone: [good, boom, good] through nodered_js + key-guard stub (keyless→nack poison-pill)", func() {
			// End-to-end capstone for the standalone nodered_js → uns case.
			// Unlike tag_processor, nodered_js does NOT construct umh_topic:
			// the user's JS must set msg.meta.umh_topic on good messages. A
			// forwarded errored message is keyless → uns nacks → poison pill.
			//
			// Under drop-loudly: the boom msg is dropped at the processor
			// (never reaches the output), the 2 good msgs are written with
			// umh_topic, 0 keyless-nacks, 1 messages_dropped{reason=js_throw}.
			//
			// Mutant (spec property 5): flipping ProcessBatch's drop site
			// back to SetError+append (forward-on-error) makes the test go
			// RED: the forwarded boom msg is keyless (JS threw before
			// setting umh_topic) → key-guard stub nacks → "0 keyless-nacks"
			// fails (1 instead of 0).
			env := service.NewEnvironment()

			var mu sync.Mutex
			counts := map[string]int64{}
			labeledCounts := map[string]map[string]int64{}
			exporter := &counterCaptureMetrics{
				mu:            &mu,
				counts:        counts,
				labeledCounts: labeledCounts,
			}

			Expect(env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec(),
				func(_ *service.ParsedConfig, _ *service.Logger) (service.MetricsExporter, error) {
					return exporter, nil
				})).To(Succeed())

			builder := env.NewStreamBuilder()

			var batchHandler service.MessageBatchHandlerFunc
			batchHandler, err := builder.AddBatchProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			Expect(builder.SetMetricsYAML("testmetrics: {}")).To(Succeed())

			// nodered_js sets umh_topic on good messages; throws on "boom".
			// The JS must set umh_topic BEFORE the throw point, otherwise the
			// errored message (if forwarded) is keyless → poison pill.
			Expect(builder.AddProcessorYAML(strings.TrimSpace(`
nodered_js:
  code: |
    if (msg.payload === 'boom') { throw new Error('boom'); }
    msg.meta.umh_topic = "umh.v1.enterprise._raw.tag";
    return msg;
`))).To(Succeed())

			// Key-guard stub output: mirrors uns_output.go:403-404.
			// Messages with umh_topic are written; keyless messages are
			// counted as nacks (returns nil to avoid benthos retry loops;
			// the count is the signal, not the error return).
			var writtenCount int64
			var keylessCount int64
			Expect(builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				topic, exists := msg.MetaGet("umh_topic")
				if !exists || topic == "" || topic == "null" {
					atomic.AddInt64(&keylessCount, 1)
					return nil
				}
				atomic.AddInt64(&writtenCount, 1)
				return nil
			})).To(Succeed())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() { _ = stream.Run(ctx) }()

			// [good, boom, good]
			msg0 := service.NewMessage([]byte("good1"))
			msg1 := service.NewMessage([]byte("boom"))
			msg2 := service.NewMessage([]byte("good2"))
			batch := service.MessageBatch{msg0, msg1, msg2}
			Expect(batchHandler(ctx, batch)).To(Succeed())

			// Exactly 2 messages written (the good ones), both with umh_topic.
			Eventually(func() int64 {
				return atomic.LoadInt64(&writtenCount)
			}, "2s").Should(Equal(int64(2)))

			// 0 keyless messages reached the output (no nack / poison pill).
			Consistently(func() int64 {
				return atomic.LoadInt64(&keylessCount)
			}, "500ms").Should(Equal(int64(0)))

			// Exactly 1 messages_dropped{reason=js_throw} (the boom one).
			Eventually(func() int64 {
				return exporter.labeledValue("messages_dropped", "js_throw")
			}, "2s").Should(Equal(int64(1)))
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

		It("should not emit literal <nil> or Go-syntax garbage for nested-nil or non-scalar meta values", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    msg.meta = {nested:{sub:null}, arr:[null,1], count:42, flag:true, name:"x"};
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

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				messagesMutex.Lock()
				defer messagesMutex.Unlock()
				return len(messages)
			}).Should(Equal(1))

			messagesMutex.Lock()
			msg := messages[0]
			messagesMutex.Unlock()

			// (1) nested nil must not produce literal <nil> in the header value.
			nested, exists := msg.MetaGet("nested")
			Expect(exists).To(BeTrue())
			Expect(nested).NotTo(ContainSubstring("<nil>"))

			// (2) nil-in-slice must not produce literal <nil> in the header value.
			arr, exists := msg.MetaGet("arr")
			Expect(exists).To(BeTrue())
			Expect(arr).NotTo(ContainSubstring("<nil>"))

			// (3) number must be stringified (not skipped, not JSON-quoted).
			count, exists := msg.MetaGet("count")
			Expect(exists).To(BeTrue())
			Expect(count).To(Equal("42"))

			// (4) bool must be stringified via strconv, not Go-syntax.
			flag, exists := msg.MetaGet("flag")
			Expect(exists).To(BeTrue())
			Expect(flag).To(Equal("true"))

			// (5) string must pass through unchanged (no JSON quotes).
			name, exists := msg.MetaGet("name")
			Expect(exists).To(BeTrue())
			Expect(name).To(Equal("x"))

			// (6) non-scalar meta must be valid JSON, so nested nil becomes null.
			Expect(nested).To(Equal(`{"sub":null}`))
			Expect(arr).To(Equal(`[null,1]`))
		})

		It("should not emit an empty header when a non-scalar meta value contains NaN or Infinity", func() {
			// json.Marshal errors on NaN/+Inf nested inside a map or slice; the
			// helper must fall back to a non-empty value rather than writing an
			// empty Kafka header (which is indistinguishable from a user setting
			// the meta to "" and is silent data corruption).
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    msg.meta = {x: 0/0, arr: [1/0]};
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

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				messagesMutex.Lock()
				defer messagesMutex.Unlock()
				return len(messages)
			}).Should(Equal(1))

			messagesMutex.Lock()
			msg := messages[0]
			messagesMutex.Unlock()

			x, exists := msg.MetaGet("x")
			Expect(exists).To(BeTrue())
			Expect(x).NotTo(BeEmpty(), "NaN nested in a map meta must not produce an empty header")

			arr, exists := msg.MetaGet("arr")
			Expect(exists).To(BeTrue())
			Expect(arr).NotTo(BeEmpty(), "Infinity nested in a slice meta must not produce an empty header")
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

			// Strict mode throws on accidental globals; under drop-loudly
			// the input is dropped (not forwarded).
			var consumerCount int64
			builder := service.NewStreamBuilder()
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
nodered_js:
  code: |
    // This should throw an error in strict mode
    accidentalGlobal = "this should fail";
    msg.payload = "should not reach here";
    return msg;
`)
			Expect(err).NotTo(HaveOccurred())

			Expect(builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
				atomic.AddInt64(&consumerCount, 1)
				return nil
			})).To(Succeed())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("test")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// No consumer outputs: the throwing input was dropped, not forwarded.
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(0)))
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
// counter increments by counter name and label values. It is the only public
// seam (outside the benthos module) to observe processor-level MetricCounter
// increments such as nodered_js's internal messagesDropped, which is not
// readable through the default StreamBuilder (its metrics are no-op).
//
// counts holds the total per counter name (summed across all label values),
// preserving backward compatibility with existing assertions. labeledCounts
// holds per-(counter name, label-values) counts so tests can assert the
// reason label on messages_dropped.
type counterCaptureMetrics struct {
	mu            *sync.Mutex
	counts        map[string]int64
	labeledCounts map[string]map[string]int64
}

func (m *counterCaptureMetrics) NewCounterCtor(name string, _ ...string) service.MetricsExporterCounterCtor {
	m.mu.Lock()
	if m.labeledCounts == nil {
		m.labeledCounts = make(map[string]map[string]int64)
	}
	m.mu.Unlock()
	return func(labelValues ...string) service.MetricsExporterCounter {
		return &capturedCounter{
			name:          name,
			labelValues:   labelValues,
			mu:            m.mu,
			counts:        m.counts,
			labeledCounts: m.labeledCounts,
		}
	}
}

func (m *counterCaptureMetrics) NewTimerCtor(string, ...string) service.MetricsExporterTimerCtor {
	return func(...string) service.MetricsExporterTimer { return noopTimer{} }
}

func (m *counterCaptureMetrics) NewGaugeCtor(string, ...string) service.MetricsExporterGaugeCtor {
	return func(...string) service.MetricsExporterGauge { return noopGauge{} }
}

func (m *counterCaptureMetrics) Close(context.Context) error { return nil }

// labeledValue returns the captured count for the given counter name and
// label values. Benthos prepends internal label values (e.g. "",
// "root.pipeline.processors.0") to the user-provided ones, so the match is a
// suffix match on the joined key. When no label values are given, the total
// across all label combinations is returned. Returns 0 if the counter or label
// combination was never incremented.
func (m *counterCaptureMetrics) labeledValue(name string, labelValues ...string) int64 {
	suffix := strings.Join(labelValues, ",")
	m.mu.Lock()
	defer m.mu.Unlock()
	inner := m.labeledCounts[name]
	if inner == nil {
		return 0
	}
	if suffix == "" {
		var total int64
		for _, v := range inner {
			total += v
		}
		return total
	}
	var total int64
	for k, v := range inner {
		if k == suffix || strings.HasSuffix(k, ","+suffix) {
			total += v
		}
	}
	return total
}

type capturedCounter struct {
	name          string
	labelValues   []string
	mu            *sync.Mutex
	counts        map[string]int64
	labeledCounts map[string]map[string]int64
}

func (c *capturedCounter) Incr(n int64) {
	key := strings.Join(c.labelValues, ",")
	c.mu.Lock()
	c.counts[c.name] += n
	if c.labeledCounts != nil {
		if c.labeledCounts[c.name] == nil {
			c.labeledCounts[c.name] = make(map[string]int64)
		}
		c.labeledCounts[c.name][key] += n
	}
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

var _ = Describe("counterCaptureMetrics labeled capture", func() {
	It("should capture the label value when a labeled counter is incremented", func() {
		var mu sync.Mutex
		counts := map[string]int64{}
		labeledCounts := map[string]map[string]int64{}
		exporter := &counterCaptureMetrics{
			mu:            &mu,
			counts:        counts,
			labeledCounts: labeledCounts,
		}

		// Simulate the framework call chain:
		//   metrics.NewCounter("messages_dropped", "reason")  -> NewCounterCtor
		//   counter.Incr(1, "js_throw")                        -> ctor("js_throw").Incr(1)
		ctor := exporter.NewCounterCtor("messages_dropped", "reason")
		counter := ctor("js_throw")
		counter.Incr(1)

		// Backward compat: total still captured
		Expect(counts["messages_dropped"]).To(Equal(int64(1)))

		// Label-specific capture
		Expect(exporter.labeledValue("messages_dropped", "js_throw")).To(Equal(int64(1)))
	})

	It("should distinguish increments with different label values", func() {
		var mu sync.Mutex
		counts := map[string]int64{}
		labeledCounts := map[string]map[string]int64{}
		exporter := &counterCaptureMetrics{
			mu:            &mu,
			counts:        counts,
			labeledCounts: labeledCounts,
		}

		ctor := exporter.NewCounterCtor("messages_dropped", "reason")
		ctor("js_throw").Incr(1)
		ctor("infra_failed").Incr(1)
		ctor("js_throw").Incr(1)

		Expect(counts["messages_dropped"]).To(Equal(int64(3)))
		Expect(exporter.labeledValue("messages_dropped", "js_throw")).To(Equal(int64(2)))
		Expect(exporter.labeledValue("messages_dropped", "infra_failed")).To(Equal(int64(1)))
	})

	It("should handle unlabeled counters", func() {
		var mu sync.Mutex
		counts := map[string]int64{}
		labeledCounts := map[string]map[string]int64{}
		exporter := &counterCaptureMetrics{
			mu:            &mu,
			counts:        counts,
			labeledCounts: labeledCounts,
		}

		ctor := exporter.NewCounterCtor("messages_processed")
		counter := ctor()
		counter.Incr(5)

		Expect(counts["messages_processed"]).To(Equal(int64(5)))
		Expect(exporter.labeledValue("messages_processed")).To(Equal(int64(5)))
	})
})
