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
	"os"
	"strings"
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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

    msg1 = {
        payload: msg.payload,
        meta: { ...msg.meta, data_contract: "_historian" }
    };

    msg2 = {
        payload: doubledValue,
        meta: { ...msg.meta, data_contract: "_analytics", tag_name: msg.meta.tag_name + "_doubled" }
    };

    return [msg1, msg2];
`)
			Expect(err).NotTo(HaveOccurred())

			var messages []*service.Message
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
    msg1 = {
      payload: msg.payload,
      meta: {
        location_path: "enterprise",
        data_contract: "_historian",
        tag_name: "temperature"
      }
    };

    msg2 = {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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

			// Test array (should be converted to string)
			testMsg = service.NewMessage(nil)
			testMsg.SetStructured([]interface{}{"a", "b", "c"})
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

			// Should get six messages
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(6))

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

			// Check array value (should be string)
			msg = messages[4]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			value, ok = payload["value"]
			Expect(ok).To(BeTrue())
			strValue, ok = value.(string)
			Expect(ok).To(BeTrue())
			Expect(strValue).To(Equal("[a b c]"))

			// Check object value (should be preserved)
			msg = messages[5]
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
	})
})
