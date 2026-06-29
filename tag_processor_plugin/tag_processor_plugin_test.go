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

package tag_processor_plugin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("TagProcessor", func() {
	BeforeEach(func() {
		testActivated := os.Getenv("TEST_TAG_PROCESSOR")

		// Check if environment variables are set
		if testActivated == "" {
			Skip("Skipping Tag Processor tests: TEST_TAG_PROCESSOR not set")
			return
		}
	})

	When("using a stream builder", func() {
		It("should process basic defaults only", func() {
			builder := service.NewStreamBuilder()

			// Add producer function
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
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
			testMsg := service.NewMessage([]byte("23.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]

			// Check metadata
			location_path, exists := msg.MetaGet("location_path")
			Expect(exists).To(BeTrue())
			Expect(location_path).To(Equal("enterprise"))

			data_contract, exists := msg.MetaGet("data_contract")
			Expect(exists).To(BeTrue())
			Expect(data_contract).To(Equal("_historian"))

			tag_name, exists := msg.MetaGet("tag_name")
			Expect(exists).To(BeTrue())
			Expect(tag_name).To(Equal("temperature"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise._historian.temperature"))

			umh_topic, exists := msg.MetaGet("umh_topic")
			Expect(exists).To(BeTrue())
			Expect(umh_topic).To(Equal("umh.v1.enterprise._historian.temperature"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["value"]
			Expect(ok).To(BeTrue())

			// Convert json.Number to float64
			GinkgoWriter.Printf("Value: %v, Type: %T\n", value, value)
			numValue, ok := value.(json.Number)
			Expect(ok).To(BeTrue())
			floatValue, err := numValue.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(floatValue).To(BeNumerically("==", 23.5))

			_, ok = payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
		})

		It("should not emit literal <nil> or Go-syntax garbage for nested-nil or non-scalar meta", func() {
			// tag_processor's JS-return meta loop (processMessageBatchWithProgram)
			// used fmt.Sprintf("%v") on every non-nil meta value, so a nested nil
			// (meta:{sub:null}) or a non-scalar produced literal <nil> / Go-syntax
			// garbage in Kafka headers. It must share nodered_js's SetMetaFromJS
			// so non-scalars serialize as JSON.
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_raw";
    msg.meta.tag_name = "temp";
    msg.meta.nested = {sub: null};
    msg.meta.arr = [null, 1];
    msg.meta.count = 42;
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

			testMsg := service.NewMessage([]byte("23.5"))
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

			nested, exists := msg.MetaGet("nested")
			Expect(exists).To(BeTrue())
			Expect(nested).NotTo(ContainSubstring("<nil>"))
			Expect(nested).To(Equal(`{"sub":null}`))

			arr, exists := msg.MetaGet("arr")
			Expect(exists).To(BeTrue())
			Expect(arr).NotTo(ContainSubstring("<nil>"))
			Expect(arr).To(Equal(`[null,1]`))

			count, exists := msg.MetaGet("count")
			Expect(exists).To(BeTrue())
			Expect(count).To(Equal("42"))
		})

		It("should forward a condition-error message with SetError, skipping later stages", func() {
			// A condition `then` that throws must not swallow the message
			// (silent drop, no forward, no retry). It is forwarded to the
			// consumer with SetError, UNCHANGED by later stages (remaining
			// conditions, advanced processing, construction), so downstream
			// error handling/DLQ can act on the original message. This
			// matches the batch-fatal path's semantics at per-message
			// granularity.
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

			Expect(builder.AddProcessorYAML(strings.TrimSpace(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_raw";
    msg.meta.tag_name = "temp";
    return msg;
  conditions:
    - if: msg.payload === "throw"
      then: |
        throw new Error("cond boom");
    - if: true
      then: |
        msg.meta.cond1 = "ran";
        return msg;
  advancedProcessing: |
    msg.payload = "advanced-" + msg.payload;
    return msg;
`))).To(Succeed())

			var messages []*service.Message
			var messagesMutex sync.Mutex
			Expect(builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
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

			msgA := service.NewMessage([]byte("keep"))
			msgB := service.NewMessage([]byte("throw"))
			batch := service.MessageBatch{msgA, msgB}
			Expect(batchHandler(ctx, batch)).To(Succeed())

			// Both messages reach the consumer: the good one transformed,
			// the condition-error one forwarded with SetError.
			Eventually(func() int {
				messagesMutex.Lock()
				defer messagesMutex.Unlock()
				return len(messages)
			}, "2s").Should(Equal(2))

			messagesMutex.Lock()
			var errored, ok *service.Message
			for _, m := range messages {
				if m.GetError() != nil {
					errored = m
				} else {
					ok = m
				}
			}
			messagesMutex.Unlock()

			// msgA: passed both conditions + advanced, payload value-wrapped
			// by construction into {value, timestamp_ms}.
			Expect(ok).NotTo(BeNil())
			okStruct, err := ok.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			Expect(okStruct).To(BeAssignableToTypeOf(map[string]interface{}{}))
			Expect(okStruct.(map[string]interface{})["value"]).To(Equal("advanced-keep"))
			cond1, exists := ok.MetaGet("cond1")
			Expect(exists).To(BeTrue())
			Expect(cond1).To(Equal("ran"))

			// msgB: condition 0 threw. Forwarded with SetError, UNCHANGED by
			// condition 1 (no cond1 meta), advanced (payload still "throw",
			// not "advanced-throw"), and construction (payload not value-
			// wrapped into a JSON object).
			Expect(errored).NotTo(BeNil())
			Expect(errored.GetError()).NotTo(Succeed())
			// Payload is the original "throw" (a structured string set by
			// defaults): NOT "advanced-throw" (advanced skipped) and NOT a
			// {value, timestamp_ms} object (construction skipped).
			errStruct, e := errored.AsStructured()
			Expect(e).NotTo(HaveOccurred())
			Expect(errStruct).To(Equal("throw"))
			_, hasCond1 := errored.MetaGet("cond1")
			Expect(hasCond1).To(BeFalse(), "errored message must skip later conditions")

			// messages_errored == 1 (one condition error, per-message).
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_errored"]
			}, "2s").Should(Equal(int64(1)))
		})

		It("should process conditions", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			yamlConfig := strings.TrimSpace(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.plant1.machiningArea.cnc-line.cnc5.plc123";
    msg.meta.data_contract = "_historian";
    return msg;
  conditions:
    - if: msg.meta.opcua_node_id === "ns=1;i=2245"
      then: |
        msg.meta.virtual_path = "axis.x.position";
        msg.meta.tag_name = "actual";
        return msg;
`)
			err = builder.AddProcessorYAML(yamlConfig)
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
			testMsg := service.NewMessage([]byte("23.5"))
			testMsg.MetaSet("opcua_node_id", "ns=1;i=2245")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]

			// Check metadata
			location_path, exists := msg.MetaGet("location_path")
			Expect(exists).To(BeTrue())
			Expect(location_path).To(Equal("enterprise.plant1.machiningArea.cnc-line.cnc5.plc123"))

			data_contract, exists := msg.MetaGet("data_contract")
			Expect(exists).To(BeTrue())
			Expect(data_contract).To(Equal("_historian"))

			virtual_path, exists := msg.MetaGet("virtual_path")
			Expect(exists).To(BeTrue())
			Expect(virtual_path).To(Equal("axis.x.position"))

			tag_name, exists := msg.MetaGet("tag_name")
			Expect(exists).To(BeTrue())
			Expect(tag_name).To(Equal("actual"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise.plant1.machiningArea.cnc-line.cnc5.plc123._historian.axis.x.position.actual"))

			umh_topic, exists := msg.MetaGet("umh_topic")
			Expect(exists).To(BeTrue())
			Expect(umh_topic).To(Equal("umh.v1.enterprise.plant1.machiningArea.cnc-line.cnc5.plc123._historian.axis.x.position.actual"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["value"]
			Expect(ok).To(BeTrue())

			// Convert json.Number to float64
			numValue, ok := value.(json.Number)
			Expect(ok).To(BeTrue())
			floatValue, err := numValue.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(floatValue).To(BeNumerically("==", 23.5))

			_, ok = payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
		})

		It("should process advanced processing", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    return msg;
  advancedProcessing: |
    msg.payload = parseFloat(msg.payload) * 2;
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
			testMsg := service.NewMessage([]byte("23.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]

			// Check metadata
			location_path, exists := msg.MetaGet("location_path")
			Expect(exists).To(BeTrue())
			Expect(location_path).To(Equal("enterprise"))

			data_contract, exists := msg.MetaGet("data_contract")
			Expect(exists).To(BeTrue())
			Expect(data_contract).To(Equal("_historian"))

			tag_name, exists := msg.MetaGet("tag_name")
			Expect(exists).To(BeTrue())
			Expect(tag_name).To(Equal("temperature"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise._historian.temperature"))

			umh_topic, exists := msg.MetaGet("umh_topic")
			Expect(exists).To(BeTrue())
			Expect(umh_topic).To(Equal("umh.v1.enterprise._historian.temperature"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["value"]
			Expect(ok).To(BeTrue())

			// Convert json.Number to float64
			numValue, ok := value.(json.Number)
			Expect(ok).To(BeTrue())
			floatValue, err := numValue.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(floatValue).To(BeNumerically("==", 47))

			_, ok = payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
		})

		It("should handle missing required fields", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    // Missing location_path
    msg.meta.data_contract = "_historian";
    // Missing tag_name
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
			testMsg := service.NewMessage([]byte("23.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Message should be dropped due to missing required fields
			Consistently(func() int {
				return len(messages)
			}, "500ms").Should(Equal(0))
		})

		It("should process multiple conditions", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "default";
    msg.meta.virtual_path = "OEE";
    return msg;
  conditions:
    - if: msg.meta.virtual_path.startsWith("OEE")
      then: |
        msg.meta.location_path += ".OEEArea";
        return msg;
    - if: msg.meta.opcua_node_id === "ns=1;i=2245"
      then: |
        msg.meta.tag_name = "temperature";
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
			testMsg := service.NewMessage([]byte("23.5"))
			testMsg.MetaSet("opcua_node_id", "ns=1;i=2245")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]

			// Check metadata
			location_path, exists := msg.MetaGet("location_path")
			Expect(exists).To(BeTrue())
			Expect(location_path).To(Equal("enterprise.OEEArea"))

			data_contract, exists := msg.MetaGet("data_contract")
			Expect(exists).To(BeTrue())
			Expect(data_contract).To(Equal("_historian"))

			virtual_path, exists := msg.MetaGet("virtual_path")
			Expect(exists).To(BeTrue())
			Expect(virtual_path).To(Equal("OEE"))

			tag_name, exists := msg.MetaGet("tag_name")
			Expect(exists).To(BeTrue())
			Expect(tag_name).To(Equal("temperature"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise.OEEArea._historian.OEE.temperature"))

			umh_topic, exists := msg.MetaGet("umh_topic")
			Expect(exists).To(BeTrue())
			Expect(umh_topic).To(Equal("umh.v1.enterprise.OEEArea._historian.OEE.temperature"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["value"]
			Expect(ok).To(BeTrue())

			// Convert json.Number to float64
			numValue, ok := value.(json.Number)
			Expect(ok).To(BeTrue())
			floatValue, err := numValue.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(floatValue).To(BeNumerically("==", 23.5))

			_, ok = payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
		})

		It("should process work order data (skipped)", func() {
			Skip("Skipping until umh.getHistorianValue is implemented")

			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  advancedProcessing: |
    msg.meta.location_path = "enterprise.site.area.line.workcell";
    msg.meta.data_contract = "_analytics";
    msg.meta.virtual_path = "work_order";
    msg.payload = {
      "work_order_id": msg.payload.work_order_id,
      "work_order_start_time": umh.getHistorianValue("enterprise.site.area.line.workcell._historian.workorder.work_order_start_time"),
      "work_order_end_time": umh.getHistorianValue("enterprise.site.area.line.workcell._historian.workorder.work_order_end_time")
    };
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

			// Create and send test message with work order data
			testPayload := map[string]interface{}{
				"work_order_id": "WO123",
			}
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(testPayload)

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]

			// Check metadata
			location_path, exists := msg.MetaGet("location_path")
			Expect(exists).To(BeTrue())
			Expect(location_path).To(Equal("enterprise.site.area.line.workcell"))

			data_contract, exists := msg.MetaGet("data_contract")
			Expect(exists).To(BeTrue())
			Expect(data_contract).To(Equal("_analytics"))

			virtual_path, exists := msg.MetaGet("virtual_path")
			Expect(exists).To(BeTrue())
			Expect(virtual_path).To(Equal("work_order"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			workOrderID, ok := payload["work_order_id"]
			Expect(ok).To(BeTrue())
			Expect(workOrderID).To(Equal("WO123"))

			// These checks will be enabled once umh.getHistorianValue is implemented
			_, ok = payload["work_order_start_time"]
			Expect(ok).To(BeTrue())

			_, ok = payload["work_order_end_time"]
			Expect(ok).To(BeTrue())
		})

		It("should process message with only advanced processing", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  advancedProcessing: |
    msg.meta = {
      location_path: "enterprise",
      data_contract: "_historian",
      tag_name: "temperature"
    };
    msg.payload = parseFloat(msg.payload) * 2;
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
			testMsg := service.NewMessage([]byte("23.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]

			// Check metadata
			location_path, exists := msg.MetaGet("location_path")
			Expect(exists).To(BeTrue())
			Expect(location_path).To(Equal("enterprise"))

			data_contract, exists := msg.MetaGet("data_contract")
			Expect(exists).To(BeTrue())
			Expect(data_contract).To(Equal("_historian"))

			tag_name, exists := msg.MetaGet("tag_name")
			Expect(exists).To(BeTrue())
			Expect(tag_name).To(Equal("temperature"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise._historian.temperature"))

			umh_topic, exists := msg.MetaGet("umh_topic")
			Expect(exists).To(BeTrue())
			Expect(umh_topic).To(Equal("umh.v1.enterprise._historian.temperature"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["value"]
			Expect(ok).To(BeTrue())

			// Convert json.Number to float64
			numValue, ok := value.(json.Number)
			Expect(ok).To(BeTrue())
			floatValue, err := numValue.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(floatValue).To(BeNumerically("==", 47))

			_, ok = payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
		})

		It("should drop messages when returning null", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    return msg;
  advancedProcessing: |
    if (msg.payload < 0) {
      // Drop negative values
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

			// Send a negative value that should be dropped
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(-10)
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Send a positive value that should pass through
			testMsg = service.NewMessage(nil)
			testMsg.SetStructured(10)
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Wait a bit to ensure all messages are processed
			time.Sleep(500 * time.Millisecond)

			// Expect only one message (the positive value) to pass through
			Expect(atomic.LoadInt64(&count)).To(Equal(int64(1)))
		})

		It("should handle multiple message returns", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    return msg;
  advancedProcessing: |
    // Prepare two separate messages: one original and one backup
    let msg1 = {
      payload: msg.payload,
      meta: { ...msg.meta }
    };

    let msg2 = {
      payload: msg.payload,
      meta: { ...msg.meta, tag_name: msg.meta.tag_name + "_backup" }
    };

    return [msg1, msg2];
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

			// Send test message
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(23.5)
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Should get two messages
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))

			// Check first message
			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["value"]).To(Equal(json.Number("23.5")))

			// Check second message
			msg = messages[1]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["value"]).To(Equal(json.Number("23.5")))
		})

		It("should process messages duplicated in advancedProcessing through all stages", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    return msg;

  conditions:
    - if: true
      then: |
        msg.meta.location_path += ".production";
        return msg;

  advancedProcessing: |
    let doubledValue = msg.payload * 2;

    let msg1 = {
        payload: msg.payload,
        meta: { ...msg.meta, data_contract: "_historian" }
    };

    let msg2 = {
        payload: doubledValue,
        meta: { ...msg.meta, data_contract: "_analytics", tag_name: msg.meta.tag_name + "_doubled" }
    };

    return [msg1, msg2];
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

			// Send test message
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(23.5)
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Should get two messages:
			// 1. Original historian message
			// 2. Analytics message with doubled value
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))

			// Check historian message
			msg := messages[0]
			data_contract, exists := msg.MetaGet("data_contract")
			Expect(exists).To(BeTrue())
			Expect(data_contract).To(Equal("_historian"))
			location_path, exists := msg.MetaGet("location_path")
			Expect(exists).To(BeTrue())
			Expect(location_path).To(Equal("enterprise.production"))
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["value"]).To(Equal(json.Number("23.5")))

			// Check analytics message
			msg = messages[1]
			data_contract, exists = msg.MetaGet("data_contract")
			Expect(exists).To(BeTrue())
			Expect(data_contract).To(Equal("_analytics"))
			location_path, exists = msg.MetaGet("location_path")
			Expect(exists).To(BeTrue())
			Expect(location_path).To(Equal("enterprise.production"))
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["value"]).To(Equal(json.Number("47")))
		})

		It("should process messages duplicated in defaults through all stages", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    let msg1 = {
      payload: msg.payload,
      meta: {
        location_path: "enterprise",
        data_contract: "_historian",
        tag_name: "temperature"
      }
    };

    let msg2 = {
      payload: msg.payload,
      meta: {
        location_path: "enterprise",
        data_contract: "_analytics",
        tag_name: "temperature_raw"
      }
    };

    return [msg1, msg2];

  conditions:
    - if: msg.meta.data_contract === "_historian"
      then: |
        msg.meta.location_path += ".production";
        return msg;
    - if: msg.meta.data_contract === "_analytics"
      then: |
        msg.meta.location_path += ".analytics";
        msg.payload = msg.payload * 2;
        return msg;

  advancedProcessing: |
    if (msg.meta.data_contract === "_analytics") {
      msg.meta.virtual_path = "raw_data";
    } else {
      msg.meta.virtual_path = "processed";
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

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send test message
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(23.5)
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Should get two messages that went through all stages
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))

			// Check historian message
			msg := messages[0]
			data_contract, exists := msg.MetaGet("data_contract")
			Expect(exists).To(BeTrue())
			Expect(data_contract).To(Equal("_historian"))
			location_path, exists := msg.MetaGet("location_path")
			Expect(exists).To(BeTrue())
			Expect(location_path).To(Equal("enterprise.production"))
			virtual_path, exists := msg.MetaGet("virtual_path")
			Expect(exists).To(BeTrue())
			Expect(virtual_path).To(Equal("processed"))
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["value"]).To(Equal(json.Number("23.5")))

			// Check analytics message
			msg = messages[1]
			data_contract, exists = msg.MetaGet("data_contract")
			Expect(exists).To(BeTrue())
			Expect(data_contract).To(Equal("_analytics"))
			location_path, exists = msg.MetaGet("location_path")
			Expect(exists).To(BeTrue())
			Expect(location_path).To(Equal("enterprise.analytics"))
			virtual_path, exists = msg.MetaGet("virtual_path")
			Expect(exists).To(BeTrue())
			Expect(virtual_path).To(Equal("raw_data"))
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["value"]).To(Equal(json.Number("47")))
		})

		It("should handle different data types correctly", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "value";
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

			// Test boolean
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(true)
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Test string
			testMsg = service.NewMessage(nil)
			testMsg.SetStructured("test string")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Test integer
			testMsg = service.NewMessage(nil)
			testMsg.SetStructured(42)
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Test float
			testMsg = service.NewMessage(nil)
			testMsg.SetStructured(23.5)
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Test float encoded as string
			testMsg = service.NewMessage(nil)
			testMsg.SetStructured("23.5")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Test string array (should be JSON serialized)
			testMsg = service.NewMessage(nil)
			testMsg.SetStructured([]interface{}{"a", "b", "c"})
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Test integer array (should be JSON serialized)
			testMsg = service.NewMessage(nil)
			testMsg.SetStructured([]interface{}{1, 2, 3})
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Test mixed type array (should be JSON serialized)
			testMsg = service.NewMessage(nil)
			testMsg.SetStructured([]interface{}{"text", 42, true})
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Test float string array
			testMsg = service.NewMessage(nil)
			testMsg.SetStructured([]interface{}{"1.23", "2.34", "3.45"})
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Test object (should be preserved as object)
			testMsg = service.NewMessage(nil)
			testMsg.SetStructured(map[string]interface{}{
				"key1": "value1",
				"key2": 42,
			})
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Should get ten messages
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(10))

			// Check boolean value
			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			value, ok := payload["value"]
			Expect(ok).To(BeTrue())
			boolValue, ok := value.(bool)
			Expect(ok).To(BeTrue())
			Expect(boolValue).To(BeTrue())

			// Check string value
			msg = messages[1]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			value, ok = payload["value"]
			Expect(ok).To(BeTrue())
			strValue, ok := value.(string)
			Expect(ok).To(BeTrue())
			Expect(strValue).To(Equal("test string"))

			// Check integer value
			msg = messages[2]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			value, ok = payload["value"]
			Expect(ok).To(BeTrue())
			numValue, ok := value.(json.Number)
			Expect(ok).To(BeTrue())
			intValue, err := numValue.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(intValue).To(Equal(int64(42)))

			// Check float value
			msg = messages[3]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			value, ok = payload["value"]
			Expect(ok).To(BeTrue())
			numValue, ok = value.(json.Number)
			Expect(ok).To(BeTrue())
			floatValue, err := numValue.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(floatValue).To(BeNumerically("==", 23.5))

			// Check float value of string encoded float
			msg = messages[4]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			value, ok = payload["value"]
			Expect(ok).To(BeTrue())
			numValue, ok = value.(json.Number)
			Expect(ok).To(BeTrue())
			floatValue, err = numValue.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(floatValue).To(BeNumerically("==", 23.5))

			// Check string array value (should be JSON serialized)
			msg = messages[5]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			value, ok = payload["value"]
			Expect(ok).To(BeTrue())
			strValue, ok = value.(string)
			Expect(ok).To(BeTrue())
			Expect(strValue).To(Equal("[\"a\",\"b\",\"c\"]"))

			// Check integer array value (should be JSON serialized)
			msg = messages[6]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			value, ok = payload["value"]
			Expect(ok).To(BeTrue())
			strValue, ok = value.(string)
			Expect(ok).To(BeTrue())
			Expect(strValue).To(Equal("[1,2,3]"))

			// Check mixed type array value (should be JSON serialized)
			msg = messages[7]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			value, ok = payload["value"]
			Expect(ok).To(BeTrue())
			strValue, ok = value.(string)
			Expect(ok).To(BeTrue())
			Expect(strValue).To(Equal("[\"text\",42,true]"))

			// Check float string array value (should be preserved as-is)
			msg = messages[8]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			value, ok = payload["value"]
			Expect(ok).To(BeTrue())
			strValue, ok = value.(string)
			Expect(ok).To(BeTrue())
			Expect(strValue).To(Equal("[\"1.23\",\"2.34\",\"3.45\"]"))

			// Check object value (should be preserved)
			msg = messages[9]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			value, ok = payload["value"]
			GinkgoWriter.Printf("payload: %v \n", payload)
			Expect(ok).To(BeTrue())
			objValue, ok := value.(string)
			Expect(ok).To(BeTrue())
			Expect(objValue).To(Equal("{\"key1\":\"value1\",\"key2\":42}"))
		})

		It("should construct umh_topic correctly with all metadata fields", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site.area.line.workcell";
    msg.meta.data_contract = "_historian";
    msg.meta.virtual_path = "axis.x.position";
    msg.meta.tag_name = "actual_value";
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
			testMsg := service.NewMessage([]byte("42.7"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]

			// Verify all metadata fields are set correctly
			location_path, exists := msg.MetaGet("location_path")
			Expect(exists).To(BeTrue())
			Expect(location_path).To(Equal("enterprise.site.area.line.workcell"))

			data_contract, exists := msg.MetaGet("data_contract")
			Expect(exists).To(BeTrue())
			Expect(data_contract).To(Equal("_historian"))

			virtual_path, exists := msg.MetaGet("virtual_path")
			Expect(exists).To(BeTrue())
			Expect(virtual_path).To(Equal("axis.x.position"))

			tag_name, exists := msg.MetaGet("tag_name")
			Expect(exists).To(BeTrue())
			Expect(tag_name).To(Equal("actual_value"))

			// Verify both topic (deprecated) and umh_topic are set correctly
			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			expectedTopic := "umh.v1.enterprise.site.area.line.workcell._historian.axis.x.position.actual_value"
			Expect(topic).To(Equal(expectedTopic))

			umh_topic, exists := msg.MetaGet("umh_topic")
			Expect(exists).To(BeTrue())
			Expect(umh_topic).To(Equal(expectedTopic))

			// Verify topic and umh_topic have the same value
			Expect(topic).To(Equal(umh_topic))

			// Verify payload structure
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["value"]
			Expect(ok).To(BeTrue())

			numValue, ok := value.(json.Number)
			Expect(ok).To(BeTrue())
			floatValue, err := numValue.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(floatValue).To(BeNumerically("==", 42.7))

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			Expect(timestamp_ms).To(BeNumerically(">", 0))
		})

		It("should construct umh_topic correctly without virtual_path", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site";
    msg.meta.data_contract = "_analytics";
    msg.meta.tag_name = "temperature";
    // Note: virtual_path is intentionally not set
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
			testMsg := service.NewMessage([]byte("25.3"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]

			// Verify virtual_path is not set
			_, exists := msg.MetaGet("virtual_path")
			Expect(exists).To(BeFalse())

			// Verify umh_topic is constructed without virtual_path
			umh_topic, exists := msg.MetaGet("umh_topic")
			Expect(exists).To(BeTrue())
			expectedTopic := "umh.v1.enterprise.site._analytics.temperature"
			Expect(umh_topic).To(Equal(expectedTopic))

			// Verify topic (deprecated) has the same value
			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal(expectedTopic))
		})

		It("should ignore virtual_path when explicitly set to null", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site";
    msg.meta.data_contract = "_analytics";
    msg.meta.tag_name = "temperature";
    msg.meta.virtual_path = null;
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

			testMsg := service.NewMessage([]byte("25.3"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]

			// virtual_path should be treated as unset
			_, exists := msg.MetaGet("virtual_path")
			Expect(exists).To(BeFalse())

			umh_topic, exists := msg.MetaGet("umh_topic")
			Expect(exists).To(BeTrue())
			expectedTopic := "umh.v1.enterprise.site._analytics.temperature"
			Expect(umh_topic).To(Equal(expectedTopic))
			Expect(strings.Contains(umh_topic, "..")).To(BeFalse())
		})

		It("should handle consecutive dots in location_path correctly", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "UMH-Systems-GmbH---Dev-Team......";
    msg.meta.data_contract = "_historian";
    msg.meta.virtual_path = "Root.Objects.SiemensPLC_fallback._System";
    msg.meta.tag_name = "_EnableDiagnostics";
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
			testMsg := service.NewMessage([]byte("false"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(0))
		})

		It("should use timestamp_ms from metadata when provided", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site.area";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
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

			// Create test message with custom timestamp in metadata
			testMsg := service.NewMessage([]byte("25.5"))
			customTimestamp := int64(1640995200000) // 2022-01-01T00:00:00Z
			// convert to RFC3339Nano format that matches opc-ua implementation
			customTime := time.UnixMilli(customTimestamp)
			testMsg.MetaSet("timestamp_ms", customTime.Format(time.RFC3339Nano))

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			Expect(timestamp_ms).To(Equal(customTimestamp))
		})

		It("should fall back to current time when timestamp_ms metadata is invalid", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site.area";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
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

			// Create test message with invalid timestamp in metadata
			testMsg := service.NewMessage([]byte("25.5"))
			testMsg.MetaSet("timestamp_ms", "invalid_timestamp")

			beforeTime := time.Now().UnixMilli()

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())

			afterTime := time.Now().UnixMilli()

			// Should fall back to current time when metadata is invalid
			Expect(timestamp_ms).To(BeNumerically(">=", beforeTime))
			Expect(timestamp_ms).To(BeNumerically("<=", afterTime))
		})

		It("should parse timestamp_ms as unix milliseconds string", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site.area";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    msg.meta.timestamp_ms = "1640995200000";
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

			testMsg := service.NewMessage([]byte("25.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			// Should parse the raw millisecond value directly
			Expect(timestamp_ms).To(Equal(int64(1640995200000)))
		})

		It("should parse timestamp_ms as unix milliseconds number", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site.area";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    msg.meta.timestamp_ms = 1640995200000;
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

			testMsg := service.NewMessage([]byte("25.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			// Should parse the numeric value and convert to milliseconds
			Expect(timestamp_ms).To(Equal(int64(1640995200000)))
		})

		It("should handle zero timestamp (Unix epoch) correctly", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site.area";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    msg.meta.timestamp_ms = "0";
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

			testMsg := service.NewMessage([]byte("25.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			// Zero is a valid Unix timestamp (1970-01-01T00:00:00Z)
			// The bug: current code treats 0 as a sentinel "no timestamp" value
			// and falls back to current time instead of using epoch
			Expect(timestamp_ms).To(Equal(int64(0)))
		})

		It("should parse timestamp as RFC3339 string", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site.area";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    msg.meta.timestamp = "2022-01-01T00:00:00.000000000Z";
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

			testMsg := service.NewMessage([]byte("25.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			// Should parse RFC3339Nano and convert to milliseconds
			Expect(timestamp_ms).To(Equal(int64(1640995200000)))
		})

		It("should parse timestamp as unix milliseconds string", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site.area";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    msg.meta.timestamp = "1640995200000";
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

			testMsg := service.NewMessage([]byte("25.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			// Should parse the raw millisecond value directly
			Expect(timestamp_ms).To(Equal(int64(1640995200000)))
		})

		It("should fallback from timestamp_ms to timestamp when timestamp_ms fails", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site.area";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    msg.meta.timestamp_ms = "invalid_timestamp";
    msg.meta.timestamp = "1640995200000";
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

			testMsg := service.NewMessage([]byte("25.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			// Should fallback to timestamp field and parse successfully
			Expect(timestamp_ms).To(Equal(int64(1640995200000)))
		})

		It("should handle Sparkplug B timestamp as Unix milliseconds", func() {
			// Test the documentation example for Sparkplug B
			// docs/processing/tag-processor.md:672-674
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    // Sparkplug B provides timestamp in milliseconds
    if (msg.meta.spb_timestamp) {
      msg.meta.timestamp_ms = msg.meta.spb_timestamp;
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

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Create test message with Sparkplug B timestamp
			testMsg := service.NewMessage([]byte("25.5"))
			testMsg.MetaSet("spb_timestamp", "1640995200000")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			Expect(timestamp_ms).To(Equal(int64(1640995200000)))
		})

		It("should parse timestamp created via JavaScript new Date().getTime().toString()", func() {
			// Test the documentation example for JavaScript date conversion
			// Simulates: msg.meta.timestamp_ms = new Date("2022-01-01T00:00:00Z").getTime().toString();
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    // JavaScript pattern: convert RFC3339 to Unix ms
    msg.meta.timestamp_ms = new Date("2022-01-01T00:00:00Z").getTime().toString();
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

			testMsg := service.NewMessage([]byte("25.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			// JavaScript new Date("2022-01-01T00:00:00Z").getTime() = 1640995200000
			Expect(timestamp_ms).To(Equal(int64(1640995200000)))
		})

		// Phase 2: Edge Cases for Timestamp Parsing
		It("should handle negative Unix timestamp (pre-1970)", func() {
			// Test that pre-1970 timestamps are valid and preserved
			// -86400000 milliseconds = 1969-12-31T00:00:00Z (one day before epoch)

			builder := service.NewStreamBuilder()
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    msg.meta.timestamp_ms = "-86400000";
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

			testMsg := service.NewMessage([]byte("25.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			Expect(timestamp_ms).To(Equal(int64(-86400000)))
		})

		It("should parse RFC3339 with timezone offset correctly", func() {
			// Test various timezone offsets normalize to same Unix ms
			// All these represent the same instant: 2022-01-01T00:00:00Z
			builder := service.NewStreamBuilder()
			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			// Test UTC+05:30 (India)
			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    // 2022-01-01T05:30:00+05:30 = 2022-01-01T00:00:00Z
    msg.meta.timestamp_ms = "2022-01-01T05:30:00+05:30";
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

			testMsg := service.NewMessage([]byte("25.5"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			timestamp_ms, ok := payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
			// Should normalize to UTC: 2022-01-01T00:00:00Z = 1640995200000
			Expect(timestamp_ms).To(Equal(int64(1640995200000)))
		})

		It("should parse RFC3339 with varying subsecond precision", func() {
			// Test different levels of precision all parse to correct milliseconds

			testCases := []struct {
				rfc3339    string
				expectedMs int64
				precision  string
			}{
				{"2022-01-01T00:00:00Z", 1640995200000, "no subseconds"},
				{"2022-01-01T00:00:00.123Z", 1640995200123, "milliseconds"},
				{"2022-01-01T00:00:00.123456Z", 1640995200123, "microseconds"},
				{"2022-01-01T00:00:00.123456789Z", 1640995200123, "nanoseconds"},
			}

			for _, tc := range testCases {
				GinkgoWriter.Printf("Testing %s precision: %s\n", tc.precision, tc.rfc3339)

				builder := service.NewStreamBuilder()
				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				yamlConfig := fmt.Sprintf(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    msg.meta.timestamp_ms = "%s";
    return msg;
`, tc.rfc3339)

				err = builder.AddProcessorYAML(yamlConfig)
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

				testMsg := service.NewMessage([]byte("25.5"))
				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(messages)
				}).Should(Equal(1))

				msg := messages[0]
				structured, err := msg.AsStructured()
				Expect(err).NotTo(HaveOccurred())

				payload, ok := structured.(map[string]interface{})
				Expect(ok).To(BeTrue())

				timestamp_ms, ok := payload["timestamp_ms"]
				Expect(ok).To(BeTrue())
				Expect(timestamp_ms).To(Equal(tc.expectedMs),
					fmt.Sprintf("Failed for %s precision: %s", tc.precision, tc.rfc3339))
			}
		})

		It("should bump messagesDropped once when a defaults function returns null", func() {
			// A tag_processor stage that yields 0 outputs via an explicit
			// drop (null return) is a per-message-entering-stage drop: 0
			// consumer outputs AND messagesDropped incremented exactly
			// once. tag_processor now increments messagesDropped on a
			// null-return drop; this test guards that increment against
			// regression. Default StreamBuilder metrics are no-op, so the
			// counter is observed via a registered MetricsExporter.
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
tag_processor:
  defaults: |
    return null;
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

			// (a) messagesDropped bumped exactly once for the message
			// entering the stage that dropped: confirms the drop happened
			// before asserting no leak, so a slow leak is still caught.
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}, "5s", "50ms").Should(Equal(int64(1)))

			// (b) 0 consumer outputs: the input was dropped and did not
			// leak to the consumer.
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(0)))
		})

		It("should bump messagesDropped once when a defaults function returns an empty array", func() {
			// An empty array return is a per-message-entering-stage drop:
			// 0 consumer outputs AND messagesDropped incremented exactly once.
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
tag_processor:
  defaults: |
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

			// (a) messagesDropped bumped exactly once for the message
			// entering the stage that dropped: confirms the drop happened
			// before asserting no leak, so a slow leak is still caught.
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}, "5s", "50ms").Should(Equal(int64(1)))

			// (b) 0 consumer outputs: the input was dropped and did not
			// leak to the consumer.
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(0)))
		})

		It("should bump messagesDropped once when a defaults function returns an all-nil array", func() {
			// An all-nil array return is a per-message-entering-stage drop:
			// every element is nil and skipped, yielding 0 consumer outputs
			// AND messagesDropped incremented exactly once.
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
tag_processor:
  defaults: |
    return [null, null];
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

			// (a) messagesDropped bumped exactly once for the message
			// entering the stage that dropped: confirms the drop happened
			// before asserting no leak, so a slow leak is still caught.
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}, "5s", "50ms").Should(Equal(int64(1)))

			// (b) 0 consumer outputs: every element was nil and dropped,
			// and nothing leaked to the consumer.
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(0)))
		})

		It("should bump messagesDropped once per message when a batch of N drops", func() {
			// messagesDropped is incremented inside the `for _, msg := range batch`
			// loop, so a single multi-message batch that drops every message must
			// increment once per dropped message (N), not once per batch or once
			// total. A single-message drop test cannot distinguish these, so a
			// 3-message batch pins the per-message semantic.
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
tag_processor:
  defaults: |
    return null;
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

			batch := service.MessageBatch{
				service.NewMessage(nil),
				service.NewMessage(nil),
				service.NewMessage(nil),
			}
			for _, m := range batch {
				m.SetStructured("ignored")
			}
			Expect(batchHandler(ctx, batch)).To(Succeed())

			// (a) messagesDropped bumped once per dropped message (3), not
			// once per batch or once total.
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}, "5s", "50ms").Should(Equal(int64(3)))

			// (b) 0 consumer outputs: every message dropped, none leaked.
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(0)))
		})

		It("should not bump messagesDropped when a defaults function returns a partial-nil array", func() {
			// A partial-nil array (some nil elements, at least one surviving
			// output) is NOT a drop: the surviving elements fan out and
			// messagesDropped stays at 0. Guards against an over-broad
			// regression that treats any-nil-in-array as a drop or increments
			// once per nil element. Symmetric with the nodered_js partial-nil
			// fan-out guard.
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
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site";
    msg.meta.data_contract = "_raw";
    msg.meta.tag_name = "temperature";
    return [null, msg];
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

			// (a) 1 consumer output: the surviving (non-nil) element fanned out.
			Eventually(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "5s", "50ms").Should(Equal(int64(1)))

			// (b) messagesDropped stays at 0: a partial-nil array is not a drop.
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}, "500ms").Should(Equal(int64(0)))
		})

		It("should not bump messagesDropped when a defaults function returns a non-dropping message", func() {
			// A non-dropping defaults function (return msg) must leave
			// messagesDropped at 0, guarding against a regression that
			// increments unconditionally.
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
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site";
    msg.meta.data_contract = "_raw";
    msg.meta.tag_name = "temperature";
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

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured("ignored")
			Expect(msgHandler(ctx, testMsg)).To(Succeed())

			// (a) 1 consumer output: the message passed through.
			Eventually(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "5s", "50ms").Should(Equal(int64(1)))

			// (b) messagesDropped stays at 0: no message was dropped.
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}, "500ms").Should(Equal(int64(0)))
		})

		It("should bump messagesDropped exactly once when a condition then returns null (no double-count)", func() {
			// R6 double-count guard: a condition whose `then` returns null drops
			// the message. The then-action runs via processMessageBatchWithProgram,
			// whose len==0 guard (R5) bumps messagesDropped once. The conditions
			// loop must NOT add a second bump. This pins already-correct behavior
			// (R5 covered it; R6 was vacuous) so a future refactor introducing a
			// conditions-loop bump is caught.
			env := service.NewEnvironment()
			var mu sync.Mutex
			counts := map[string]int64{}
			exporter := &counterCaptureMetrics{mu: &mu, counts: counts}
			Expect(env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec(),
				func(_ *service.ParsedConfig, _ *service.Logger) (service.MetricsExporter, error) {
					return exporter, nil
				})).To(Succeed())
			builder := env.NewStreamBuilder()
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())
			Expect(builder.SetMetricsYAML("testmetrics: {}")).To(Succeed())
			Expect(builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.tag_name = "x";
    return msg;
  conditions:
    - if: 'msg.meta.tag_name === "x"'
      then: |
        return null;
`)).To(Succeed())
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
			Expect(msgHandler(ctx, service.NewMessage(nil))).To(Succeed())
			// 0 consumer outputs (the then returned null → drop).
			Consistently(func() int64 { return atomic.LoadInt64(&consumerCount) }, "500ms").Should(Equal(int64(0)))
			// messagesDropped == 1, NOT 2 (no double-count from the conditions loop).
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}, "5s", "50ms").Should(Equal(int64(1)))
		})

		It("should not bump messagesDropped when a condition if is false (pass-through)", func() {
			// R6 pass-through guard: a false `if` is a pass-through, not a drop.
			// processConditionForMessageWithProgram returns the original message
			// unchanged (1 output) when the if is false; messagesDropped must
			// stay 0. Pins that a non-matching condition is NOT mis-counted as a drop.
			env := service.NewEnvironment()
			var mu sync.Mutex
			counts := map[string]int64{}
			exporter := &counterCaptureMetrics{mu: &mu, counts: counts}
			Expect(env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec(),
				func(_ *service.ParsedConfig, _ *service.Logger) (service.MetricsExporter, error) {
					return exporter, nil
				})).To(Succeed())
			builder := env.NewStreamBuilder()
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())
			Expect(builder.SetMetricsYAML("testmetrics: {}")).To(Succeed())
			Expect(builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_raw";
    msg.meta.tag_name = "temperature";
    return msg;
  conditions:
    - if: 'msg.meta.tag_name === "nonexistent"'
      then: |
        return null;
`)).To(Succeed())
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
			Expect(msgHandler(ctx, service.NewMessage(nil))).To(Succeed())
			// 1 consumer output (the false-if condition passes the original
			// message through unchanged, and the required meta fields are set
			// so it survives validation/constructFinalMessage).
			Eventually(func() int64 { return atomic.LoadInt64(&consumerCount) }, "5s", "50ms").Should(Equal(int64(1)))
			// messagesDropped == 0 (false-if is a pass-through, not a drop).
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}, "500ms").Should(Equal(int64(0)))
		})

		It("should bump messagesErrored when a condition then-action throws a JS error", func() {
			// A condition whose then action throws a JS error is forwarded to
			// the consumer with SetError (not swallowed), so downstream error
			// handling/DLQ can act. messages_errored bumps to make it visible.
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
tag_processor:
  conditions:
    - if: 'true'
      then: |
        throw new Error("boom");
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

			// messages_errored bumps to exactly 1: the condition JS error is
			// now visible in metrics.
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_errored"]
			}, "5s", "50ms").Should(Equal(int64(1)))

			// The errored message is forwarded to the consumer with SetError
			// (not swallowed), so downstream error handling can act.
			Eventually(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "2s").Should(Equal(int64(1)))
			mu.Lock()
			Expect(counts["messages_dropped"]).To(Equal(int64(0)))
			Expect(counts["messages_processed"]).To(Equal(int64(0)))
			mu.Unlock()
		})

		It("should bump messagesErrored when a condition if-expression throws a JS error", func() {
			// Triangulates the then-action case: the if-expression throw
			// returns the same error to the conditions-loop bump site, so
			// messages_errored must rise here too. Guards against a future
			// refactor that splits the two error returns.
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
tag_processor:
  conditions:
    - if: |
        throw new Error("if boom");
      then: 'return msg;'
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

			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_errored"]
			}, "5s", "50ms").Should(Equal(int64(1)))

			// The errored message is forwarded to the consumer with SetError
			// (not swallowed), so downstream error handling can act.
			Eventually(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "2s").Should(Equal(int64(1)))
			mu.Lock()
			Expect(counts["messages_dropped"]).To(Equal(int64(0)))
			Expect(counts["messages_processed"]).To(Equal(int64(0)))
			mu.Unlock()
		})

		It("should bump messagesErrored when a defaults JS error aborts the batch", func() {
			// A defaults program that throws aborts the batch. The error bumps
			// messages_errored and propagates to benthos via (nil, fmt.Errorf),
			// so the v2BatchedToV1Processor wrapper marks the original input
			// with SetError and forwards it to the consumer.
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
tag_processor:
  defaults: |
    throw new Error("defaults boom");
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

			// messages_errored bumps to exactly 1: the batch-fatal defaults
			// JS error is now visible in metrics.
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_errored"]
			}, "5s", "50ms").Should(Equal(int64(1)))

			// Batch-fatal propagate: the engine wrapper forwards the errored
			// input to the consumer (marked with SetError), so 1 consumer output.
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(1)))

			// The batch-fatal return skips the processed/dropped accounting.
			mu.Lock()
			Expect(counts["messages_processed"]).To(Equal(int64(0)))
			Expect(counts["messages_dropped"]).To(Equal(int64(0)))
			mu.Unlock()
		})

		It("should reclassify an earlier drop as errored when a later defaults message throws (deferred drop bump)", func() {
			// [drop, throw] in defaults: msg0 returns null (dropped,
			// droppedCount=1), msg1 throws (batch-fatal). The deferred
			// messagesDropped bump inside processMessageBatchWithProgram is
			// skipped by the early return, so the drop is counted as errored
			// (part of the batchSize bump), not dropped. Pins the symmetric
			// invariant to nodered_js.
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
tag_processor:
  defaults: |
    if (msg.payload === "drop") { return null; }
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

			msg0 := service.NewMessage(nil)
			msg0.SetStructured("drop")
			msg1 := service.NewMessage(nil)
			msg1.SetStructured("throw")
			batch := service.MessageBatch{msg0, msg1}
			Expect(batchHandler(ctx, batch)).To(Succeed())

			// All 2 inputs forwarded with SetError.
			Eventually(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "2s").Should(Equal(int64(2)))

			// messages_errored == batchSize (2).
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_errored"]
			}, "2s").Should(Equal(int64(2)))

			// messages_dropped == 0: deferred bump skipped on batch-fatal.
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_dropped"]
			}, "500ms").Should(Equal(int64(0)))

			// messages_processed == 0.
			Consistently(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_processed"]
			}, "500ms").Should(Equal(int64(0)))

			// Every forwarded input carries the batch error.
			messagesMutex.Lock()
			for _, m := range messages {
				Expect(m.GetError()).NotTo(Succeed(), "expected every forwarded input to carry the batch error")
			}
			messagesMutex.Unlock()
		})

		It("should bump messagesErrored when an advancedProcessing JS error aborts the batch", func() {
			// An advancedProcessing program that throws aborts the batch. The
			// error bumps messages_errored and propagates to benthos via
			// (nil, fmt.Errorf), so the v2BatchedToV1Processor wrapper marks
			// the original input with SetError and forwards it to the consumer.
			// A minimal defaults that returns msg is required so execution
			// reaches the advanced stage (guarded on advancedProgram != nil).
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
tag_processor:
  defaults: |
    return msg;
  advancedProcessing: |
    throw new Error("advanced boom");
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

			// messages_errored bumps to exactly 1: the batch-fatal
			// advancedProcessing JS error is now visible in metrics.
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_errored"]
			}, "5s", "50ms").Should(Equal(int64(1)))

			// Batch-fatal propagate: the engine wrapper forwards the errored
			// input to the consumer (marked with SetError), so 1 consumer output.
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(1)))

			// The batch-fatal return skips the processed/dropped accounting.
			mu.Lock()
			Expect(counts["messages_processed"]).To(Equal(int64(0)))
			Expect(counts["messages_dropped"]).To(Equal(int64(0)))
			mu.Unlock()
		})

		It("should not double-count messages_errored when a condition errors and advanced then aborts batch-fatal", func() {
			// [a, b, c]: condition throws on b (forwarded with SetError,
			// messages_errored+1), then advanced throws on c (batch-fatal).
			// advanced's batchFatalErr must bump only the non-errored
			// messages (a and c), not the already-counted b, so
			// messages_errored == 3 (distinct), not 4.
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

			Expect(builder.AddProcessorYAML(strings.TrimSpace(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_raw";
    msg.meta.tag_name = "temp";
    return msg;
  conditions:
    - if: msg.payload === "b"
      then: |
        throw new Error("cond boom");
  advancedProcessing: |
    if (msg.payload === "c") { throw new Error("adv boom"); }
    return msg;
`))).To(Succeed())

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

			msgA := service.NewMessage([]byte("a"))
			msgB := service.NewMessage([]byte("b"))
			msgC := service.NewMessage([]byte("c"))
			batch := service.MessageBatch{msgA, msgB, msgC}
			Expect(batchHandler(ctx, batch)).To(Succeed())

			// All 3 original inputs are forwarded (engine marks them on the
			// advanced batch-fatal).
			Eventually(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "2s").Should(Equal(int64(3)))

			// messages_errored == 3 (1 from the condition + 2 from advanced's
			// non-errored-only bump), NOT 4: b is not double-counted.
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_errored"]
			}, "2s").Should(Equal(int64(3)))

			// No drops, no successful outputs (construction never ran).
			mu.Lock()
			Expect(counts["messages_dropped"]).To(Equal(int64(0)))
			Expect(counts["messages_processed"]).To(Equal(int64(0)))
			mu.Unlock()
		})

		It("should bump messagesErrored by the batch size when a defaults JS error aborts a multi-message batch", func() {
			// processMessageBatchWithProgram aborts the whole batch on the
			// first per-message JS failure, so every message in the batch is
			// dropped. messages_errored must reflect the full batch size, not
			// a single Incr(1), otherwise the unprocessed tail vanishes from
			// all plugin counters.
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
tag_processor:
  defaults: |
    throw new Error("defaults boom");
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

			const batchSize = 3
			batch := make(service.MessageBatch, batchSize)
			for i := range batch {
				batch[i] = service.NewMessage(nil)
				batch[i].SetStructured("ignored")
			}
			Expect(batchHandler(ctx, batch)).To(Succeed())

			// messages_errored bumps to the full batch size: the whole batch
			// is dropped, so all N messages must be counted.
			Eventually(func() int64 {
				mu.Lock()
				defer mu.Unlock()
				return counts["messages_errored"]
			}, "5s", "50ms").Should(Equal(int64(batchSize)))

			// Batch-fatal propagate: the engine wrapper forwards every errored
			// input in the batch to the consumer (marked with SetError), so
			// consumerCount == batchSize.
			Consistently(func() int64 {
				return atomic.LoadInt64(&consumerCount)
			}, "500ms").Should(Equal(int64(batchSize)))

			mu.Lock()
			Expect(counts["messages_processed"]).To(Equal(int64(0)))
			Expect(counts["messages_dropped"]).To(Equal(int64(0)))
			mu.Unlock()
		})
	})
})

var _ = Describe("VM Pooling Optimization", func() {
	BeforeEach(func() {
		testActivated := os.Getenv("TEST_TAG_PROCESSOR")
		if testActivated == "" {
			Skip("Skipping Tag Processor tests: TEST_TAG_PROCESSOR not set")
			return
		}
	})

	Context("VM Pool Infrastructure", func() {
		It("should reuse VMs from pool for condition evaluation", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			// Configuration with multiple conditions to test VM pooling
			yamlConfig := strings.TrimSpace(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    return msg;
  conditions:
    - if: msg.meta.test_value === "condition1"
      then: |
        msg.meta.tag_name = "processed_condition1";
        msg.meta.condition_result = "true";
        return msg;
    - if: msg.meta.test_value === "condition2"
      then: |
        msg.meta.tag_name = "processed_condition2";
        msg.meta.condition_result = "true";
        return msg;
`)
			err = builder.AddProcessorYAML(yamlConfig)
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

			// Send multiple messages that will trigger different conditions
			msg1 := service.NewMessage([]byte("42"))
			msg1.MetaSet("test_value", "condition1")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			msg2 := service.NewMessage([]byte("43"))
			msg2.MetaSet("test_value", "condition2")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			msg3 := service.NewMessage([]byte("44"))
			msg3.MetaSet("test_value", "condition1")
			err = msgHandler(ctx, msg3)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(3))

			// Verify all messages were processed correctly
			for i, msg := range messages {
				GinkgoWriter.Printf("Processing message %d\n", i+1)

				// All should have defaults applied
				location_path, exists := msg.MetaGet("location_path")
				Expect(exists).To(BeTrue())
				Expect(location_path).To(Equal("enterprise"))

				data_contract, exists := msg.MetaGet("data_contract")
				Expect(exists).To(BeTrue())
				Expect(data_contract).To(Equal("_historian"))

				// Should have condition results
				condition_result, exists := msg.MetaGet("condition_result")
				Expect(exists).To(BeTrue())
				Expect(condition_result).To(Equal("true"))

				tag_name, exists := msg.MetaGet("tag_name")
				Expect(exists).To(BeTrue())
				Expect(tag_name).To(MatchRegexp("processed_condition[12]"))
			}
		})

		It("should handle VM pool with complex JavaScript execution", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			// Configuration with complex JavaScript to stress VM pooling
			yamlConfig := strings.TrimSpace(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "complex_test";

    // Complex JavaScript operations to test VM reuse
    let calculations = [];
    for(let i = 0; i < 10; i++) {
      calculations.push(Math.pow(i, 2));
    }
    msg.meta.calculation_sum = calculations.reduce((a, b) => a + b, 0).toString();

    return msg;
  conditions:
    - if: parseFloat(msg.payload) > 40
      then: |
        // More complex operations
        let value = parseFloat(msg.payload);
        let result = {
          original: value,
          squared: value * value,
          cubed: value * value * value,
          sqrt: Math.sqrt(value)
        };
        msg.meta.complex_result = JSON.stringify(result);
        return msg;
`)
			err = builder.AddProcessorYAML(yamlConfig)
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

			// Send multiple messages to test VM reuse with complex operations
			for i := 0; i < 5; i++ {
				testMsg := service.NewMessage([]byte("42.5"))
				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())
			}

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(5))

			// Verify all messages were processed with complex calculations
			for i, msg := range messages {
				GinkgoWriter.Printf("Verifying complex message %d\n", i+1)

				msg.MetaWalk(func(key, value string) error {
					GinkgoWriter.Printf("Key: %s, Value: %s\n", key, value)
					return nil
				})

				// Check defaults calculation
				calculation_sum, exists := msg.MetaGet("calculation_sum")
				Expect(exists).To(BeTrue())
				Expect(calculation_sum).To(Equal("285")) // Sum of squares 0² + 1² + ... + 9² = 285

				// Check condition calculation
				complex_result, exists := msg.MetaGet("complex_result")
				Expect(exists).To(BeTrue())

				var result map[string]interface{}
				err := json.Unmarshal([]byte(complex_result), &result)
				Expect(err).NotTo(HaveOccurred())

				Expect(result["original"]).To(BeNumerically("==", 42.5))
				Expect(result["squared"]).To(BeNumerically("==", 1806.25))
				Expect(result["cubed"]).To(BeNumerically("~", 76765.625, 0.001))
			}
		})

		It("should handle VM pool under concurrent load", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			yamlConfig := strings.TrimSpace(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "load_test";
    msg.meta.processed_at = Date.now().toString();
    return msg;
  conditions:
    - if: true  // Always execute condition
      then: |
        msg.meta.condition_processed = "true";
        msg.meta.condition_timestamp = Date.now().toString();
        return msg;
`)
			err = builder.AddProcessorYAML(yamlConfig)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			var messageCount int64
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				messages = append(messages, msg)
				atomic.AddInt64(&messageCount, 1)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send many messages concurrently to stress VM pool
			const numMessages = 20
			for i := 0; i < numMessages; i++ {
				testMsg := service.NewMessage([]byte(`{"test": "data"}`))
				testMsg.MetaSet("message_id", strings.TrimSpace(string(rune('A'+i))))
				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())
			}

			Eventually(func() int64 {
				return atomic.LoadInt64(&messageCount)
			}).Should(Equal(int64(numMessages)))

			// Verify all messages were processed correctly
			Expect(messages).To(HaveLen(numMessages))

			for i, msg := range messages {
				GinkgoWriter.Printf("Verifying concurrent message %d\n", i+1)

				// All should have basic metadata
				location_path, exists := msg.MetaGet("location_path")
				Expect(exists).To(BeTrue())
				Expect(location_path).To(Equal("enterprise"))

				// All should have condition processing
				condition_processed, exists := msg.MetaGet("condition_processed")
				Expect(exists).To(BeTrue())
				Expect(condition_processed).To(Equal("true"))

				// Should have timestamps
				processed_at, exists := msg.MetaGet("processed_at")
				Expect(exists).To(BeTrue())
				Expect(processed_at).NotTo(BeEmpty())

				condition_timestamp, exists := msg.MetaGet("condition_timestamp")
				Expect(exists).To(BeTrue())
				Expect(condition_timestamp).NotTo(BeEmpty())
			}
		})
	})

	Context("VM Pool Error Handling", func() {
		It("should handle JavaScript errors without VM pool corruption", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			yamlConfig := strings.TrimSpace(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "error_test";
    return msg;
  conditions:
    - if: msg.meta.error_test === "true"
      then: |
        // This will cause a JavaScript error
        nonExistentFunction();
        return msg;
    - if: msg.meta.error_test === "false"
      then: |
        // This should work fine
        msg.meta.processed = "success";
        return msg;
`)
			err = builder.AddProcessorYAML(yamlConfig)
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

			// Send a message that will cause JS error
			errorMsg := service.NewMessage([]byte(`{"test": "error"}`))
			errorMsg.MetaSet("error_test", "true")
			err = msgHandler(ctx, errorMsg)
			Expect(err).NotTo(HaveOccurred())

			// Send a message that should work fine (testing VM pool recovery)
			successMsg := service.NewMessage([]byte(`{"test": "success"}`))
			successMsg.MetaSet("error_test", "false")
			err = msgHandler(ctx, successMsg)
			Expect(err).NotTo(HaveOccurred())

			// We should get at least the success message (error message might be dropped)
			Eventually(func() int {
				return len(messages)
			}).Should(BeNumerically(">=", 1))

			// Find the success message (if any messages were processed)
			var successProcessed bool
			for _, msg := range messages {
				if processed, exists := msg.MetaGet("processed"); exists && processed == "success" {
					successProcessed = true
					break
				}
			}

			// If we got any messages, at least one should be the success message
			if len(messages) > 0 {
				Expect(successProcessed).To(BeTrue(), "Success message should be processed even after JS error")
			}
		})
	})

	When("using msg.meta.datatype override", func() {
		It("should preserve numeric string as string when datatype is string", func() {
			builder := service.NewStreamBuilder()

			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "sap_number";
    msg.meta.datatype = "string";
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

			testMsg := service.NewMessage([]byte("1234567890"))
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]any)
			Expect(ok).To(BeTrue())

			value, ok := payload["value"]
			Expect(ok).To(BeTrue())

			strValue, ok := value.(string)
			Expect(ok).To(BeTrue(), fmt.Sprintf("expected string but got %T: %v", value, value))
			Expect(strValue).To(Equal("1.23456789e+09"))
		})
	})
})

// counterCaptureMetrics is a service.MetricsExporter that aggregates integer
// counter increments by counter name, ignoring labels. It is the only public
// seam (outside the benthos module) to observe processor-level MetricCounter
// increments such as tag_processor's internal messagesDropped, which is not
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
