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
	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/cache"
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

		It("should drop the message when meta is a non-object value", func() {
			// A returned meta that is not an object (here a string) is a bad
			// return: the input is dropped (not forwarded without metadata),
			// and messages_dropped{reason=bad_return}==1.
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
    return {payload: {value: 1}, meta: "not-an-object"};
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

			// No consumer output: the input was dropped (bad meta).
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(0)))

			// messages_dropped{reason=bad_return} == 1.
			Eventually(func() int64 {
				return exporter.labeledValue("messages_dropped", "bad_return")
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

			for range 5 {
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
			// output), the two good messages flow,
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

			// (b) messages_dropped{reason=js_throw} == 1.
			Eventually(func() int64 {
				return exporter.labeledValue("messages_dropped", "js_throw")
			}, "2s").Should(Equal(int64(1)))

			// (c) messages_processed == 2 (the two good outputs).
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_processed"]
			}, "500ms").Should(Equal(int64(2)))

			// (d) No output message carries an error (no forward-on-error).
			messagesMutex.Lock()
			for _, m := range messages {
				Expect(m.GetError()).To(Succeed(), "no output message should carry an error")
			}
			messagesMutex.Unlock()
		})

		It("should drop a null-returning message and independently drop a later throwing one", func() {
			// [drop, throw]: msg0 returns null (a genuine drop) and msg1
			// throws (dropped via RecordDrop). Both messages are absent from
			// the output. messages_dropped is 2 (1 deliberate + 1 js_throw).
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

			// No drops.
			mu.Lock()
			Expect(counts["messages_dropped"]).To(Equal(int64(0)))
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

			// (c) messages_processed == 2 (the two good outputs).
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_processed"]
			}, "500ms").Should(Equal(int64(2)))

			// (d) NO output message carries an error (no forward-on-error).
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
			for range 5 {
				jsMessages = nil // Reset messages slice
				experiment.MeasureDuration("JavaScript processing", func() {
					for j := range 1000 {
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
			for range 5 {
				bloblangMessages = nil // Reset messages slice
				experiment.MeasureDuration("Bloblang processing", func() {
					for j := range 1000 {
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

			for i := range numMessages {
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

		err = builder.AddProcessorYAML(fmt.Sprintf("nodered_js:\n  cache:\n    name: %q\n  code: |\n%s",
			fmt.Sprintf("test-%d", time.Now().UnixNano()),
			indentLines(code, "    ")))
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
				err := handler(ctx, newMsg(fmt.Sprintf("tick-%d", i)))
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
				id := i
				go func() {
					defer wg.Done()
					_ = handler(ctx, newMsg(fmt.Sprintf("concurrent-%d", id)))
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
				err := handler(ctx, newMsg(fmt.Sprintf("x-%d", i)))
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
				err := handler(ctx, newMsg(fmt.Sprintf("tick-%d", i)))
				Expect(err).NotTo(HaveOccurred())
			}
			Eventually(func() int { return len(*msgs) }).Should(Equal(2))
			Expect(payloadFloat(*msgs, 1)).To(Equal(float64(2)))
		})

		buildStreamDedup := func(dedupKey, code string) (service.MessageHandlerFunc, *[]*service.Message, context.CancelFunc) {
			builder := service.NewStreamBuilder()
			handler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			yaml := fmt.Sprintf("nodered_js:\n  cache:\n    name: %q\n    dedupKey: %q\n  code: |\n%s",
				fmt.Sprintf("test-%d", time.Now().UnixNano()),
				dedupKey,
				indentLines(code, "    "))
			err = builder.AddProcessorYAML(yaml)
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

		// Same shape as buildStreamDedup, emitting cache.monotonicKey instead. The
		// key is an interpolated string, so the same syntax reaches metadata
		// ('${! meta("kafka_offset") }') or payload ('${! this.timestamp_ms }').
		buildStreamMonotonic := func(monotonicKey, code string) (service.MessageHandlerFunc, *[]*service.Message, context.CancelFunc) {
			builder := service.NewStreamBuilder()
			handler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			yaml := fmt.Sprintf("nodered_js:\n  cache:\n    name: %q\n    monotonicKey: %q\n  code: |\n%s",
				fmt.Sprintf("test-%d", time.Now().UnixNano()),
				monotonicKey,
				indentLines(code, "    "))
			err = builder.AddProcessorYAML(yaml)
			Expect(err).NotTo(HaveOccurred(),
				"cache.monotonicKey does not exist yet — this is the field the design settled on 2026-08-07, "+
					"and until it is implemented this test is red at config parse rather than at an assertion")

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

		It("suppresses cache writes when dedupKey value was seen before", func() {
			handler, msgs, cancel := buildStreamDedup("kafka_offset", `
var n = cache.exists("n") ? cache.get("n") : 0;
n = n + 1;
cache.set("n", n);
msg.payload = n;
return msg;
`)
			defer cancel()

			ctx := context.Background()
			// Same dedup value across three messages — only the first write commits.
			for i := 0; i < 3; i++ {
				Expect(handler(ctx, msgWithMeta(fmt.Sprintf("tick-%d", i), "kafka_offset", "42"))).To(Succeed())
			}
			Eventually(func() int { return len(*msgs) }).Should(Equal(3))

			Expect(payloadFloat(*msgs, 0)).To(Equal(float64(1)))
			Expect(payloadFloat(*msgs, 1)).To(Equal(float64(2)))
			Expect(payloadFloat(*msgs, 2)).To(Equal(float64(2)))
		})

		It("allows writes when dedupKey values differ", func() {
			handler, msgs, cancel := buildStreamDedup("kafka_offset", `
var n = cache.exists("n") ? cache.get("n") : 0;
n = n + 1;
cache.set("n", n);
msg.payload = n;
return msg;
`)
			defer cancel()

			ctx := context.Background()
			for i := range 3 {
				Expect(handler(ctx, msgWithMeta(fmt.Sprintf("tick-%d", i), "kafka_offset", fmt.Sprintf("%d", i)))).To(Succeed())
			}
			Eventually(func() int { return len(*msgs) }).Should(Equal(3))

			Expect(payloadFloat(*msgs, 0)).To(Equal(float64(1)))
			Expect(payloadFloat(*msgs, 1)).To(Equal(float64(2)))
			Expect(payloadFloat(*msgs, 2)).To(Equal(float64(3)))
		})

		It("does not suppress when dedupKey meta field is missing", func() {
			handler, msgs, cancel := buildStreamDedup("kafka_offset", `
var n = cache.exists("n") ? cache.get("n") : 0;
n = n + 1;
cache.set("n", n);
msg.payload = n;
return msg;
`)
			defer cancel()

			ctx := context.Background()
			// No meta set — dedup skipped, all writes commit.
			for i := range 2 {
				Expect(handler(ctx, newMsg(fmt.Sprintf("tick-%d", i)))).To(Succeed())
			}
			Eventually(func() int { return len(*msgs) }).Should(Equal(2))

			Expect(payloadFloat(*msgs, 0)).To(Equal(float64(1)))
			Expect(payloadFloat(*msgs, 1)).To(Equal(float64(2)))
		})

		It("also suppresses cache.delete on a retried dedupKey value", func() {
			handler, msgs, cancel := buildStreamDedup("kafka_offset", `
if (!cache.exists("keep")) {
  cache.set("keep", "alive");
}
cache.delete("keep");
msg.payload = cache.exists("keep") ? "present" : "gone";
return msg;
`)
			defer cancel()

			ctx := context.Background()
			// First msg: dedup fresh, cache.set + cache.delete both run → key gone.
			Expect(handler(ctx, msgWithMeta("first", "kafka_offset", "77"))).To(Succeed())
			Expect(handler(ctx, msgWithMeta("second", "kafka_offset", "77"))).To(Succeed())
			Eventually(func() int { return len(*msgs) }).Should(Equal(2))

			Expect(payloadString(*msgs, 0)).To(Equal("gone"))
			Expect(payloadString(*msgs, 1)).To(Equal("gone"))
		})

		// RED, and it has to go green before this PR merges.
		//
		// dedupKey keeps the cache correct but not the message that leaves the
		// processor. That was previously written up as an accepted trade-off, with
		// the docs telling readers to dedup the output downstream themselves. That
		// framing has been rejected, because it is not actionable here: the UNS
		// writes every message to one compacted topic keyed by umh_topic, and
		// compaction keeps the LAST value per key, so it preserves the wrong count
		// rather than dropping it. Anything reading the stream live — the historian
		// writing time series, for instance — records a wrong value at a real
		// timestamp, and no downstream filter can recover it.
		//
		// The summary claim that counters "stay correct across retries" has already
		// been narrowed in javascript-api.md to cover what is stored rather than
		// what is published. What still describes today's behaviour, and becomes
		// wrong the moment this test goes green, is the walkthrough under "What a
		// retry looks like": it says the redelivered message leaves carrying the 43
		// it computed locally and that the stored value is the authoritative one.
		// Edit that in the same commit, and state whichever remedy was chosen next
		// to the guarantee at the top of the section.
		//
		// The plugin-level options are genuinely constrained, which is why the
		// choice belongs to the review rather than to whoever implements it: (a) is
		// one field inside a counter()-style helper that owns the operation, but at
		// plugin level needs a copy of the whole outgoing message, because the
		// plugin cannot know which field is "the result"; (b) does not do what it
		// looks like, because a processor cannot refuse a message — see the note on
		// candidate (b) below.
		//
		// Invariant under test: a message may only carry a counter value that is
		// actually committed. A consumer cannot distinguish a committed value from
		// one that was computed and then discarded.
		//
		// The root cause is where the guard sits. DDIA 2nd ed, Ch. 12 "Stream
		// Processing" -> Processing Streams -> Fault Tolerance -> "Idempotence"
		// (this edition has no page numbers; cite by section):
		//
		//   "Even if an operation is not naturally idempotent, it can often be
		//    made idempotent with a bit of extra metadata. For example, when
		//    consuming messages from Kafka, every message has a persistent,
		//    monotonically increasing offset. When writing a value to an external
		//    database, you can include the offset of the message that triggered
		//    the last write with the value. Thus, you can tell whether an update
		//    has already been applied and avoid performing the same update again."
		//
		// "...with the value" is the whole design: one record, two fields, one
		// write. Read {value, offset}, compare, and on a replay you already hold
		// the committed value — so re-emitting it costs nothing. This PR stores
		// the guard in a separate namespace (__dedup__:<v>) from the value, which
		// is why the emitted message can diverge from committed state at all.
		//
		// The separate-marker-table shape is also blessed by the book, in a
		// section new to the 2nd edition (Ch. 8 "Transactions" -> Distributed
		// Transactions -> "Exactly-Once Message Processing Revisited"): keep "a
		// table of message IDs that have been processed". But there it is one
		// transaction with the processing, and it assumes the ID is deleted once
		// the broker is acknowledged — "you will have an old message ID lying
		// around, which doesn't do any harm besides taking up a little bit of
		// storage space". A Benthos processor cannot observe the broker ack, so
		// that cleanup is not implementable here. Which argues for the Ch. 12
		// shape.
		//
		// The assertion pins the invariant, not a mechanism. Four candidates for
		// the design discussion; this test takes no position between them:
		//
		//   a) Store the offset with the value (the Ch. 12 shape) and re-emit the
		//      committed result when the write is suppressed. One extra field —
		//      NOT a second copy of the message, so not the "rebuild the prior
		//      message from cache" idea rejected in July. Cheap inside a counter()
		//      helper; expensive at plugin level, because the plugin cannot know
		//      which part of the message is "the result".
		//   b) Error the replayed message instead of forwarding it. ⚠️ This does NOT
		//      hold the message back: a processor cannot nack. On a non-nil return
		//      the engine flags every part and appends the original batch to the
		//      output anyway, logging at DEBUG level
		//      (benthos internal/component/processor/auto_observed.go:261-269), so
		//      the uncommitted value still reaches the output. Making the flag
		//      actually stop the message needs error handling configured on the
		//      pipeline, which is outside this plugin — realistically a umh-core
		//      template concern, alongside the threads and dedupKey injection.
		//   c) Keep the behaviour and put the guard in user code instead —
		//      if (offset > last_offset) { ... } — where the branch is skipped
		//      before the arithmetic rather than after it. Correct, but it means
		//      documenting the pattern rather than fixing the plugin, so this test
		//      would be deleted rather than made green.
		//   d) Publish a correction afterwards, and optionally retract the earlier
		//      value. This is the standard answer for an aggregate that has already
		//      been published, and it needs nothing stored in advance — but the
		//      wrong value still leaves first, and the consumer has to understand a
		//      second message.
		//
		// Be precise about what satisfies the assertion, because only one of these
		// does. The invariant is that no message carries a counter value that was
		// never stored, and that can only be met by PREVENTING the wrong value:
		//   - (a) meets it.
		//   - (b) does not, on its own — the flagged message still reaches the
		//     output unless the pipeline is configured to drop errored messages.
		//   - (c) meets it only by changing the user's code, so the test goes away
		//     rather than passing.
		//   - (d) repairs rather than prevents, so the assertion still fails.
		// The test is neutral about the mechanism that makes published == committed
		// (helper, plugin-level snapshot, or userspace guard all qualify). It is not
		// neutral between preventing the wrong value and correcting it afterwards.
		It("never publishes a counter value that is not committed in the cache", func() {
			handler, msgs, cancel := buildStreamDedup("kafka_offset", `
var n = cache.exists("n") ? cache.get("n") : 0;
n = n + 1;
cache.set("n", n);
// Re-reading after the write yields what is actually committed: on a suppressed
// message the set was a no-op, so this is the value the consumer must agree with.
msg.payload = { published: n, committed: cache.get("n") };
return msg;
`)
			defer cancel()

			ctx := context.Background()
			// Three distinct source events, then a redelivery of the last one.
			for _, offset := range []string{"42", "43", "44", "44"} {
				Expect(handler(ctx, msgWithMeta("tick", "kafka_offset", offset))).To(Succeed())
			}
			// All four are forwarded today. Remedy (b) drops the replay instead, and
			// would change this expectation deliberately.
			Eventually(func() int { return len(*msgs) }).Should(Equal(4))

			for i := range *msgs {
				published := payloadMapFloat(*msgs, i, "published")
				committed := payloadMapFloat(*msgs, i, "committed")
				Expect(published).To(Equal(committed),
					"message %d published counter %v, but the cache committed %v", i, published, committed)
			}
		})

		// RED, and it has to go green before this PR merges.
		//
		// Unlike the counter case above, this one has a clean answer at plugin
		// level, and it pays for itself twice: remember the highest dedupKey value
		// seen instead of remembering every value. "Already processed" then becomes
		// a comparison rather than a set lookup, which detects a straggler for free
		// AND replaces the unbounded __dedup__: namespace with one entry — see the
		// Limitations note about the cache growing without bound, which the user
		// cannot mitigate for these keys because the docs reserve the prefix.
		//
		// Nothing checks that dedupKey values arrive in order.
		//
		// The docs section used to be titled "Idempotency and monotonicity under
		// retries" while only idempotency was implemented; it has since been renamed
		// to "Retries and duplicate messages" for exactly that reason, and it now
		// states plainly that no value is compared against the ones before it. That
		// sentence is what this test forbids, so it goes when this goes green.
		// checkDedup asks "have I seen this exact value?" (a set-membership test
		// against __dedup__:<v>); it never compares the incoming value with the
		// highest one seen. Grep confirms it: no occurrence of out-of-order /
		// monotonic / watermark / straggler anywhere in nodered_js_plugin or
		// tag_processor_plugin. The old heading's word "monotonicity" in
		// the heading is doing no work.
		//
		// Consequence: a straggler — an older event arriving after a newer one —
		// looks brand new, so its write is applied on top of state derived from a
		// later event. A membership test cannot detect this; a high-water mark
		// would, for free.
		//
		// The downsampler is the one component in this repo that does compare
		// timestamps — but check what it actually does before treating it as
		// precedent. Its `late_policy` defaults to `passthrough`, so a late sample is
		// forwarded unchanged rather than dropped. docs/processing/downsampler.md:124
		// says it is "flagging it with meta:late_oos=true", and nothing in the code
		// sets that flag — zero non-test occurrences of `late_oos`. The drop path
		// logs at Debug only, and none of its eight metrics counts late arrivals.
		// So in the default configuration a late sample passes through silently: no
		// marker, no counter, nothing above debug.
		//
		// Two plugins, two answers, and neither is observable. The 2026-07-16
		// proposal asked for one ordered-stream check and one warning shape across
		// stateful components, naming the downsampler as the next adopter; it got
		// there first, independently, and without the instrumentation.
		//
		// DDIA 2nd ed gives exactly two options, and names dropping first — Ch. 12
		// "Stream Processing" -> Processing Streams -> Reasoning About Time ->
		// "Handling straggler events" (no page numbers in this edition):
		//
		//   "You need to be able to handle such straggler events that arrive after
		//    the window has already been declared complete. Broadly, you have two
		//    options:
		//      - Ignore the straggler events, as they are probably a small
		//        percentage of events in normal circumstances. You can track the
		//        number of dropped events as a metric and alert if you start
		//        dropping a significant amount of data.
		//      - Publish a correction, an updated value for the window with
		//        stragglers included. You may also need to retract the previous
		//        output."
		//
		// Note the instrumentation is the book's advice, not ours — and neither
		// plugin follows it. The downsampler's default is neither of the two options
		// above: it forwards the late sample unchanged, with the marker its own docs
		// promise never actually set, no counter, and only a Debug line. The cache
		// path does not detect the case at all.
		//
		// The book also draws the line we already drew by operation, Ch. 12 ->
		// Transmitting Event Streams -> "Messaging Systems":
		//
		//   "with sensor readings and metrics that are transmitted periodically, an
		//    occasional missing data point is perhaps not important, since an
		//    updated value will be sent a short time later anyway. However, beware
		//    that if a large number of messages are dropped, it may not be
		//    immediately apparent that the metrics are incorrect. If you are
		//    counting events, it is more important that they are delivered
		//    reliably, since every lost message means incorrect counters."
		//
		// So: dropping is defensible for a gauge or a delta, and not for a count —
		// the same split as idempotent-vs-accumulate. Whatever we choose has to be
		// stated per operation, not once for the whole cache.
		//
		// One vocabulary caution: "watermark" is NOT this book's term. It appears
		// twice in the whole 2nd edition, both about Netflix's DBLog CDC snapshot
		// algorithm. The concept is there unnamed — "a special message to indicate
		// that 'from now on, there will be no more messages with a timestamp
		// earlier than t'" (attributed to MillWheel) — with the caveat that it gets
		// hard when several producers each have their own threshold. Our single
		// partition is what makes it tractable here. Don't cite DDIA for the word.
		//
		// Note what suppression alone cannot fix. For a retry, skipping the write
		// protects the cache. For a straggler it does not: if the write is skipped
		// but the message is still forwarded and the JS still runs, the arithmetic
		// has already read newer state and will publish a garbage difference. So
		// "warn and forward unchanged" is not a sufficient remedy here, whereas
		// dropping the straggler, rejecting it, or guarding in user code all are.
		// That asymmetry is worth an explicit decision.
		//
		// The assertion: no message may be processed whose ordering key is older
		// than the state already in the cache. Sequence 1000, 3000, 2000, 4000 —
		// 2000 is the straggler. Today it sails through and clobbers "last" back
		// to an older value, so the 4000 message then computes its difference from
		// 2000 instead of 3000.
		It("does not process a dedupKey value older than the state already committed", func() {
			handler, msgs, cancel := buildStreamDedup("event_ts", `
var last = cache.exists("last") ? cache.get("last") : 0;
var ts = Number(msg.meta.event_ts);
cache.set("last", ts);
msg.payload = { ts: ts, last_before: last };
return msg;
`)
			defer cancel()

			ctx := context.Background()
			for _, ts := range []string{"1000", "3000", "2000", "4000"} {
				Expect(handler(ctx, msgWithMeta("sample", "event_ts", ts))).To(Succeed())
			}
			Eventually(func() int { return len(*msgs) }).Should(Equal(4))

			for i := range *msgs {
				ts := payloadMapFloat(*msgs, i, "ts")
				lastBefore := payloadMapFloat(*msgs, i, "last_before")
				Expect(ts).To(BeNumerically(">=", lastBefore),
					"message %d carries ordering key %v but the cache already reflected %v — a straggler was processed as if fresh",
					i, ts, lastBefore)
			}
		})

		When("specifying monotonicKey, the field that REPLACES dedupKey (not implemented yet)", func() {

			// `monotonicKey` REPLACES `dedupKey`. It is not a second field to build
			// alongside it, and nothing should ever accept both.
			//
			// | | `dedupKey` (ships today) | `monotonicKey` (this block) |
			// |---|---|---|
			// | reads | metadata only, via `msg.MetaGet` | metadata OR payload, one interpolated syntax |
			// | compares | set membership on `__dedup__:<v>` | strictly `>` a stored mark |
			// | stores | one entry per message, forever | one mark per cache key |
			// | catches | a redelivery | a redelivery, a straggler, AND a non-monotonic key |
			// | misses | anything about order | tells the three cases apart (it cannot) |
			//
			// So when `monotonicKey` lands, `dedupKey` is REMOVED, not deprecated next to
			// it. The five tests above still use `dedupKey` only because it is what ships
			// in this PR; they get renamed by the change that implements this block, and
			// the config parser should reject `dedupKey` at that point rather than quietly
			// honouring both. That rejection cannot be tested here — it would contradict
			// the five tests above — so it belongs to the implementation, and it is named
			// here so it is not forgotten.
			//
			// The two tests below fail at config parse, not at an assertion, until
			// `cache.monotonicKey` exists. That is deliberate: they are the design decided
			// on 2026-08-07, written in the one form that cannot be misread. Classify them
			// as a follow-up ticket, NOT as a merge gate — a red test for an unbuilt API
			// must not block this PR, only the rollout.
			//
			// Why a rename rather than teaching `dedupKey` to read the payload:
			// `dedupKey` is set membership on a value (`__dedup__:<v>`), and set
			// membership on a timestamp silently drops a second genuine sample that lands
			// in the same millisecond. The two mechanisms want different comparisons, so
			// they get different names. `monotonicKey` also states its own precondition,
			// which `dedupKey` does not.
			//
			// The shape:
			//
			//   cache:
			//     monotonicKey: '${! meta("kafka_offset") }'   # or '${! this.timestamp_ms }'
			//
			// An interpolated string reaches metadata or payload with one syntax, so no
			// new lookup logic is needed — `service.NewInterpolatedStringField` is already
			// used in this repo (uns_output.go:97, opcua_plugin/write.go:57). This is what
			// makes `timestamp_ms` usable at all: it lives in the payload, and `dedupKey`
			// resolves through `msg.MetaGet`, so today it can never be found.
			//
			// Both candidate values are already monotonic — a `kafka_offset` because
			// `umh.messages` is hard-enforced single-partition (uns_output.go:43,326), a
			// `timestamp_ms` within one series — so ONE comparison covers both, and it
			// does three jobs at once: it spots a retry, it spots a straggler, and it is
			// itself the check that the key is monotonic.
			//
			// Accept a write when the key is strictly greater than the mark. Not-greater
			// means one of retry, genuinely-late, or a key that was never monotonic; the
			// comparison cannot tell them apart and should not pretend to (Flink's
			// late-data counter has the same limitation). Document it, don't design
			// around it.
			//
			// This test takes no position on where the mark is stored, only on what must
			// hold. The recommendation is with the value, per cache key — DDIA Ch. 12,
			// "Idempotence": "you can include the offset of the message that triggered the
			// last write with the value". That bounds the state at one mark per key
			// instead of one `__dedup__:` entry per message forever, which is the same
			// fix the straggler gate above needs. The second test below is what rules out
			// the tempting shortcut of a single global mark.

			// The payload-reading counterpart to the straggler gate above. Same invariant,
			// reached through the proposed field and an ordering value that lives in the
			// payload — which is the case `dedupKey` structurally cannot serve.
			It("catches the straggler that dedupKey structurally cannot, because the value is in the payload", func() {
				handler, msgs, cancel := buildStreamMonotonic(`${! this.timestamp_ms }`, `
var last = cache.exists("last") ? cache.get("last") : 0;
var ts = msg.payload.timestamp_ms;
cache.set("last", ts);
msg.payload = { ts: ts, last_before: last };
return msg;
`)
				defer cancel()

				ctx := context.Background()
				for _, ts := range []int{1000, 3000, 2000, 4000} {
					payload := fmt.Sprintf(`{"timestamp_ms":%d,"value":1}`, ts)
					Expect(handler(ctx, service.NewMessage([]byte(payload)))).To(Succeed())
				}
				Eventually(func() int { return len(*msgs) }).Should(Equal(4))

				for i := range *msgs {
					ts := payloadMapFloat(*msgs, i, "ts")
					lastBefore := payloadMapFloat(*msgs, i, "last_before")
					Expect(ts).To(BeNumerically(">=", lastBefore),
						"message %d carries monotonicKey %v but the cache already reflected %v — the straggler was written as if fresh",
						i, ts, lastBefore)
				}
			})

			// The implementation detail that a passing first test does not prove. One
			// global high-water mark satisfies the test above and is still wrong: two
			// tags have unrelated clocks and rates, so the fastest series would suppress
			// every slower one, permanently and silently.
			//
			// Interleaved so that a global mark is fatal: series A runs at 5000/6000
			// while series B runs at 1000/2000. Under a global mark, B's first write is
			// below A's mark and is suppressed, so B's second message reads 0 for its
			// previous value instead of 1000 — a whole series of state quietly lost.
			It("keeps a separate mark per cache key so one series cannot suppress another", func() {
				handler, msgs, cancel := buildStreamMonotonic(`${! this.timestamp_ms }`, `
var key = "last_" + msg.meta.series;
var last = cache.exists(key) ? cache.get(key) : 0;
var ts = msg.payload.timestamp_ms;
cache.set(key, ts);
msg.payload = { ts: ts, last_before: last };
return msg;
`)
				defer cancel()

				ctx := context.Background()
				samples := []struct {
					series string
					ts     int
				}{
					{"a", 5000},
					{"b", 1000},
					{"a", 6000},
					{"b", 2000},
				}
				for _, s := range samples {
					msg := service.NewMessage([]byte(fmt.Sprintf(`{"timestamp_ms":%d,"value":1}`, s.ts)))
					msg.MetaSet("series", s.series)
					Expect(handler(ctx, msg)).To(Succeed())
				}
				Eventually(func() int { return len(*msgs) }).Should(Equal(4))

				// Series B's second sample must see B's own first sample, not zero.
				Expect(payloadMapFloat(*msgs, 3, "last_before")).To(BeNumerically("==", 1000),
					"series b saw %v as its previous value instead of 1000 — series a's higher mark suppressed b's write, so the mark is global rather than per cache key",
					payloadMapFloat(*msgs, 3, "last_before"))

				// And series A must be unaffected, which rules out the mirror error.
				Expect(payloadMapFloat(*msgs, 2, "last_before")).To(BeNumerically("==", 5000),
					"series a saw %v as its previous value instead of 5000",
					payloadMapFloat(*msgs, 2, "last_before"))
			})

		})

		// The alarm case: the counter's mirror image. There the retry publishes a
		// value that is too high; here it publishes nothing at all.
		//
		// The JS below is the shipped "Alarm state tracking" example from
		// javascript-api.md, unchanged. It is a latch: the state lives in the cache
		// (alarm_active) and the event lives on the message (meta.alarm), and the
		// event is only stamped on the transition. Run 1 sets the state and stamps
		// "triggered", then its output fails. The redelivery finds the state already
		// set, so neither branch fires, and the message leaves with no annotation —
		// the transition is never published to anyone. The clear side loses
		// "cleared" the same way, which leaves a consumer showing an alarm that
		// never ends while the cache says it ended.
		//
		// dedupKey neither causes nor fixes this: the branch conditions have already
		// evaluated false, so there was no write to suppress. It is what at-least-
		// once does to any level-to-edge conversion, and the 2026-07-16 proposal
		// recorded the same failure for delta()/changed().
		//
		// Two remedies, and the choice is open:
		//   - Store the result the first run produced and re-emit it when the write
		//     is suppressed. Note this is the ONLY remedy that works here — erroring
		//     the replay instead (an option for the counter) does not help, because
		//     every retry recomputes an empty annotation, so the alarm can never be
		//     delivered at all.
		//   - Change the pattern: publish the state on every message and let
		//     consumers derive the transition. If that is the answer, this test is
		//     deleted along with the example rather than made green.
		It("re-publishes the alarm edge when the first attempt's output failed", func() {
			// This builds its own stream rather than using buildStreamDedup, because
			// the whole point is an output that FAILS on the first attempt. Sending
			// the same message twice through a succeeding output proves nothing: the
			// first delivery would carry the annotation and the duplicate lacking it
			// is harmless under at-least-once.
			builder := service.NewStreamBuilder()
			handler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			yaml := fmt.Sprintf("nodered_js:\n  cache:\n    name: %q\n    dedupKey: \"kafka_offset\"\n  code: |\n%s",
				fmt.Sprintf("test-alarm-%d", time.Now().UnixNano()),
				indentLines(`
var alarmed = false;
if (cache.exists("alarm_active")) { alarmed = cache.get("alarm_active"); }
if (msg.payload.value > 100 && !alarmed) {
  cache.set("alarm_active", true);
  msg.meta.alarm = "triggered";
  return msg;
}
if (msg.payload.value <= 100 && alarmed) {
  cache.set("alarm_active", false);
  msg.meta.alarm = "cleared";
  return msg;
}
return msg;
`, "    "))
			Expect(builder.AddProcessorYAML(yaml)).To(Succeed())

			// The output rejects exactly once, then accepts. Only messages the output
			// accepted are recorded — those are the ones that actually reached the UNS.
			var delivered []*service.Message
			var rejectOnce atomic.Bool
			rejectOnce.Store(true)
			Expect(builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
				if rejectOnce.CompareAndSwap(true, false) {
					return fmt.Errorf("simulated output failure")
				}
				delivered = append(delivered, m)
				return nil
			})).To(Succeed())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go func() { _ = stream.Run(ctx) }()

			newSample := func() *service.Message {
				m := service.NewMessage([]byte(`{"value": 150}`))
				m.MetaSet("kafka_offset", "42")
				return m
			}

			// Attempt 1: JS stamps "triggered" and commits alarm_active, then the
			// output rejects — nothing reaches the UNS.
			_ = handler(ctx, newSample())
			// At-least-once: the same record is redelivered.
			Expect(handler(ctx, newSample())).To(Succeed())

			Eventually(func() int { return len(delivered) }).Should(Equal(1))

			alarm, ok := delivered[0].MetaGet("alarm")
			Expect(ok).To(BeTrue(),
				"the only message that reached the output carries no alarm annotation, so the transition was published to nobody")
			Expect(alarm).To(Equal("triggered"))
		})

		// RED — and unlike the three above, this one is a defect in this PR's own
		// code rather than a hazard inherited from at-least-once delivery.
		//
		// checkDedup builds the marker key from the dedup value alone —
		// DedupCacheKeyPrefix + v — with nothing recording which processor wrote it.
		// Cache instances are shared by backend+name, and name DEFAULTS to "shared",
		// so two nodered_js processors in one pipeline share a cache unless someone
		// deliberately opts out. Processor 1 records __dedup__:<v>; processor 2 then
		// finds it on the FIRST delivery of that message and suppresses its writes.
		// Its state is never written at all, and every message logs a spurious retry
		// WARN pointing at an upstream that is behaving perfectly.
		//
		// This is reachable by doing nothing unusual: the "Sharing across processors"
		// section of javascript-api.md shows exactly this shape, two nodered_js stages
		// on implicit defaults. Set dedupKey on both and the second silently stops
		// keeping state.
		//
		// Fix direction: scope the marker to whichever processor wrote it, so two
		// processors guarding on the same value cannot collide. That also decides
		// ENG-5051 (sharing a cache across nodered_js processors) — sharing and
		// dedupKey cannot both work until this is fixed.
		It("lets two processors sharing a cache dedup independently", func() {
			builder := service.NewStreamBuilder()
			handler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			// Both stages name the same cache. The default name is "shared", so this
			// is the arrangement a user gets by configuring nothing at all.
			cacheName := fmt.Sprintf("test-shared-%d", time.Now().UnixNano())
			stages := []string{`
var a = cache.exists("a") ? cache.get("a") : 0;
a = a + 1;
cache.set("a", a);
msg.payload = { a: a };
return msg;
`, `
var b = cache.exists("b") ? cache.get("b") : 0;
b = b + 1;
cache.set("b", b);
msg.payload.b = b;
return msg;
`}
			for _, code := range stages {
				yaml := fmt.Sprintf("nodered_js:\n  cache:\n    name: %q\n    dedupKey: \"kafka_offset\"\n  code: |\n%s",
					cacheName, indentLines(code, "    "))
				Expect(builder.AddProcessorYAML(yaml)).To(Succeed())
			}

			var msgs []*service.Message
			Expect(builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
				msgs = append(msgs, m)
				return nil
			})).To(Succeed())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go func() { _ = stream.Run(ctx) }()

			// Three distinct messages — no retries anywhere in this test.
			for _, offset := range []string{"1", "2", "3"} {
				Expect(handler(ctx, msgWithMeta("tick", "kafka_offset", offset))).To(Succeed())
			}
			Eventually(func() int { return len(msgs) }).Should(Equal(3))

			for i := range msgs {
				want := float64(i + 1)
				Expect(payloadMapFloat(msgs, i, "a")).To(Equal(want),
					"the first processor should have counted %v distinct messages", want)
				Expect(payloadMapFloat(msgs, i, "b")).To(Equal(want),
					"the second processor's counter stalled at 1: the first processor had already written the marker for this message, so the second one's own writes never commit")
			}
		})

		// RED, and it has to go green before this PR merges.
		//
		// ProcessBatch opens a cache transaction, buffers every write the batch
		// makes, and closes it with `defer u.cacheCommit(ctx)`. cacheCommit logs a
		// failed Commit and returns nothing, so ProcessBatch still reports success.
		// The pipeline then acknowledges the batch: the messages go to the output
		// and the input is told it may forget them, while the state they were
		// supposed to produce went away with the transaction. Nothing is left to
		// redeliver, so the loss is permanent, and the only trace is one log line
		// in a stream where every other signal says the batch succeeded.
		//
		// The discard is real, not hypothetical. Under the persistent backend
		// BboltStore.Set writes into the bolt tx that Begin opened, and a failed
		// tx.Commit throws that tx away with every write in it. There is no
		// Rollback on the Cache interface and nothing reconciles afterwards, so a
		// batch that failed to commit leaves no record of what it meant to write.
		// The store injected below models that: buffered between Begin and Commit,
		// visible to reads inside the transaction, dropped when the Commit fails.
		//
		// Invariant under test: committed state must account for every message the
		// processor hands on as good. A batch whose writes did not persist may not
		// leave looking like a batch that succeeded, because the acknowledgement
		// that follows is what makes the loss unrecoverable.
		//
		// "As good" and not "acknowledged", because a Benthos processor cannot stop
		// an acknowledgement, and this was checked rather than assumed: with
		// cacheCommit's error plumbed through to ProcessBatch's return, all three
		// messages below were still acknowledged. The engine wrapper around every
		// registered processor discards the returned error — it stamps each part
		// via MarkErr and forwards the original batch (auto_observed.go, the
		// `if err != nil` branch of v2BatchedToV1Processor.ProcessBatch, which then
		// returns a nil error). What a processor controls is whether the message
		// carries an error, and an errored message is one an error-handling output
		// can reject; a clean one is a claim that the batch worked.
		//
		// Fix direction: cacheCommit has to be able to fail the batch, so the
		// commit error reaches ProcessBatch's return instead of only the log. What
		// happens around that is open — whether the commit is retried before giving
		// up, whether the error is returned once for the batch or attached per
		// message, whether a failed commit should also tear the store down, and
		// what a pipeline is then expected to configure downstream so the flagged
		// messages are actually rejected rather than written. The assertion pins
		// the invariant, not any of those: it compares messages certified against
		// increments committed, so returning the error and making the commit
		// durable enough not to lose the writes satisfy it equally. Doing nothing
		// does not.
		It("does not report success for a batch whose cache commit failed", func() {
			// Attach a store whose second commit fails by pre-seeding it in the
			// registry under the key openCacheStore computes for this cache.name;
			// Acquire then hands the processor this instance instead of building a
			// MemoryStore, whose no-op Commit could never fail.
			cacheName := fmt.Sprintf("test-commit-fail-%d", time.Now().UnixNano())
			store := newCommitFailingStore(2)
			seeded, err := cache.Acquire("mem:"+cacheName, func() (cache.Cache, error) { return store, nil })
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = seeded.Close() }()

			builder := service.NewStreamBuilder()
			handler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			Expect(builder.AddProcessorYAML(fmt.Sprintf("nodered_js:\n  cache:\n    name: %q\n  code: |\n%s",
				cacheName, indentLines(`
var n = cache.exists("n") ? cache.get("n") : 0;
n = n + 1;
cache.set("n", n);
msg.payload = n;
return msg;
`, "    ")))).To(Succeed())

			// certified counts the messages that reached the output carrying no
			// error — the ones the processor vouched for. flagged counts the rest.
			var certified, flagged atomic.Int64
			Expect(builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
				if m.GetError() != nil {
					flagged.Add(1)
				} else {
					certified.Add(1)
				}
				return nil
			})).To(Succeed())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go func() { _ = stream.Run(ctx) }()

			// One message per batch, sent one at a time. A nil return from handler
			// is the acknowledgement: the input may forget that message.
			acknowledged := 0
			for i := range 3 {
				if handler(ctx, newMsg(fmt.Sprintf("tick-%d", i))) == nil {
					acknowledged++
				}
			}
			Eventually(func() int64 { return certified.Load() + flagged.Load() }).
				Should(Equal(int64(acknowledged)), "the pipeline never settled")

			// Positive control. Without a commit that actually failed and actually
			// discarded its writes, the assertion below would prove nothing.
			Eventually(store.failedCommits).Should(Equal(1),
				"the injected commit failure never fired, so this spec is not exercising the case it claims to")

			committed, found := store.committedValue("n")
			Expect(found).To(BeTrue(), "no counter value was ever committed")
			Expect(float64(certified.Load())).To(BeNumerically("<=", committed),
				"%d of the %d acknowledged messages left the processor as successes, but the counter committed to the cache is only %v: a failed commit discarded a batch's writes, was logged and dropped, and the batch went out indistinguishable from one that worked — nothing will redeliver it",
				certified.Load(), acknowledged, committed)
		})

		It("plain get+set counter is atomic across concurrent messages (auto-lock)", func() {
			handler, msgs, cancel := buildStream(`
var n = cache.exists("counter") ? cache.get("counter") : 0;
n = n + 1;
cache.set("counter", n);
msg.payload = n;
return msg;
`)
			defer cancel()

			const numMsgs = 100
			ctx := context.Background()
			var wg sync.WaitGroup
			wg.Add(numMsgs)
			for i := range numMsgs {
				id := i
				go func() {
					defer wg.Done()
					_ = handler(ctx, newMsg(fmt.Sprintf("tick-%d", id)))
				}()
			}
			wg.Wait()

			Eventually(func() int { return len(*msgs) }).Should(Equal(numMsgs))

			// ProcessBatch auto-locks the cache for the whole batch, so plain
			// get+set is safe under concurrent messages. Max reported counter
			// must equal numMsgs — no lost increments.
			maxVal := float64(0)
			for i := range numMsgs {
				v := payloadFloat(*msgs, i)
				if v > maxVal {
					maxVal = v
				}
			}
			Expect(maxVal).To(Equal(float64(numMsgs)), "expected counter to reach N exactly")
		})
	})
})

// newMsg creates a service.Message with the given string payload.
func newMsg(payload string) *service.Message {
	return service.NewMessage([]byte(payload))
}

// msgWithMeta creates a service.Message with the given payload and one meta field.
func msgWithMeta(payload string, metaKey string, metaValue string) *service.Message {
	m := service.NewMessage([]byte(payload))
	m.MetaSet(metaKey, metaValue)
	return m
}

// payloadString extracts the string payload from messages[i].
func payloadString(msgs []*service.Message, i int) string {
	s, err := msgs[i].AsStructured()
	Expect(err).NotTo(HaveOccurred())
	str, ok := s.(string)
	Expect(ok).To(BeTrue(), "expected string payload, got %T: %v", s, s)
	return str
}

// payloadMapFloat extracts a numeric field from an object payload in messages[i].
func payloadMapFloat(msgs []*service.Message, i int, key string) float64 {
	s, err := msgs[i].AsStructured()
	Expect(err).NotTo(HaveOccurred())
	m, ok := s.(map[string]any)
	Expect(ok).To(BeTrue(), "expected object payload, got %T: %v", s, s)
	v, present := m[key]
	Expect(present).To(BeTrue(), "payload has no %q field: %v", key, m)
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	default:
		Fail(fmt.Sprintf("expected numeric %q, got %T: %v", key, v, v))
		return 0
	}
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

// commitFailingStore is a cache.Cache that fails one Commit on demand.
//
// It reproduces the persistent backend's transaction semantics rather than the
// memory backend's: writes made between Begin and Commit are buffered, reads
// inside the transaction see them, and they only reach the underlying store
// when the Commit succeeds. That is BboltStore's shape — Set and Delete write
// into the bolt tx opened by Begin, and a failed tx.Commit throws the tx away
// with everything in it. MemoryStore, whose Begin and Commit are both no-ops,
// cannot express the case at all.
//
// A test attaches one of these to a processor by pre-seeding it in the cache
// registry under the key the processor will compute for its cache.name, so
// cache.Acquire hands the processor this store instead of building a new one.
type commitFailingStore struct {
	inner *cache.MemoryStore

	mu      sync.Mutex
	pending map[string]pendingWrite
	inTx    bool
	begun   int
	failOn  int // 1-based index of the batch whose Commit fails
	failed  int
}

// pendingWrite is one buffered mutation: a value to store, or a tombstone.
type pendingWrite struct {
	value   any
	deleted bool
}

var _ cache.Cache = (*commitFailingStore)(nil)

// newCommitFailingStore returns a store whose failOn'th Commit (counting from
// the first Begin) fails and discards that batch's buffered writes.
func newCommitFailingStore(failOn int) *commitFailingStore {
	return &commitFailingStore{inner: cache.NewMemoryStore(0), failOn: failOn}
}

func (s *commitFailingStore) Set(ctx context.Context, key string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inTx {
		return s.inner.Set(ctx, key, value)
	}
	s.pending[key] = pendingWrite{value: value}
	return nil
}

func (s *commitFailingStore) Get(ctx context.Context, key string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inTx {
		w, buffered := s.pending[key]
		if buffered {
			if w.deleted {
				return nil, false
			}
			return w.value, true
		}
	}
	return s.inner.Get(ctx, key)
}

func (s *commitFailingStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inTx {
		return s.inner.Delete(ctx, key)
	}
	s.pending[key] = pendingWrite{deleted: true}
	return nil
}

func (s *commitFailingStore) Lock() { s.inner.Lock() }

func (s *commitFailingStore) Unlock() { s.inner.Unlock() }

func (s *commitFailingStore) Begin(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inTx {
		return fmt.Errorf("commitFailingStore: begin called with an active batch")
	}
	s.inTx = true
	s.begun++
	s.pending = make(map[string]pendingWrite)
	return nil
}

func (s *commitFailingStore) Commit(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inTx {
		return nil
	}
	s.inTx = false
	pending := s.pending
	s.pending = nil

	if s.begun == s.failOn {
		s.failed++
		return fmt.Errorf("commitFailingStore: injected commit failure on batch %d (%d buffered writes discarded)", s.begun, len(pending))
	}

	for key, w := range pending {
		var err error
		if w.deleted {
			err = s.inner.Delete(ctx, key)
		} else {
			err = s.inner.Set(ctx, key, w.value)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *commitFailingStore) Stats(ctx context.Context) (cache.Stats, error) {
	return s.inner.Stats(ctx)
}

func (s *commitFailingStore) Close() error { return s.inner.Close() }

// failedCommits reports how many Commits were failed by injection.
func (s *commitFailingStore) failedCommits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed
}

// committedValue returns the numeric value the underlying store actually holds
// for key. found is false when nothing was ever committed under that key.
func (s *commitFailingStore) committedValue(key string) (float64, bool) {
	v, ok := s.inner.Get(context.Background(), key)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	default:
		Fail(fmt.Sprintf("expected numeric committed value for %q, got %T: %v", key, v, v))
		return 0, false
	}
}

var _ = Describe("js logmessage", func() {
	DescribeTable(
		"format",
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
	DescribeTable(
		"parses payload",
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
