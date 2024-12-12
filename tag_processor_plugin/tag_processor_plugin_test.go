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
    msg.meta.location0 = "enterprise";
    msg.meta.datacontract = "_historian";
    msg.meta.tagName = "temperature";
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
			location0, exists := msg.MetaGet("location0")
			Expect(exists).To(BeTrue())
			Expect(location0).To(Equal("enterprise"))

			datacontract, exists := msg.MetaGet("datacontract")
			Expect(exists).To(BeTrue())
			Expect(datacontract).To(Equal("_historian"))

			tagName, exists := msg.MetaGet("tagName")
			Expect(exists).To(BeTrue())
			Expect(tagName).To(Equal("temperature"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise._historian.temperature"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["temperature"]
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

		It("should process conditions", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			yamlConfig := strings.TrimSpace(`
tag_processor:
  defaults: |
    msg.meta.location0 = "enterprise";
    msg.meta.location1 = "plant1";
    msg.meta.location2 = "machiningArea";
    msg.meta.location3 = "cnc-line";
    msg.meta.location4 = "cnc5";
    msg.meta.location5 = "plc123";
    msg.meta.datacontract = "_historian";
    return msg;
  conditions:
    - if: msg.meta.opcua_node_id === "ns=1;i=2245"
      then: |
        msg.meta.path0 = "axis";
        msg.meta.path1 = "x";
        msg.meta.path2 = "position";
        msg.meta.tagName = "actual";
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
			location0, exists := msg.MetaGet("location0")
			Expect(exists).To(BeTrue())
			Expect(location0).To(Equal("enterprise"))

			location1, exists := msg.MetaGet("location1")
			Expect(exists).To(BeTrue())
			Expect(location1).To(Equal("plant1"))

			location2, exists := msg.MetaGet("location2")
			Expect(exists).To(BeTrue())
			Expect(location2).To(Equal("machiningArea"))

			location3, exists := msg.MetaGet("location3")
			Expect(exists).To(BeTrue())
			Expect(location3).To(Equal("cnc-line"))

			location4, exists := msg.MetaGet("location4")
			Expect(exists).To(BeTrue())
			Expect(location4).To(Equal("cnc5"))

			location5, exists := msg.MetaGet("location5")
			Expect(exists).To(BeTrue())
			Expect(location5).To(Equal("plc123"))

			datacontract, exists := msg.MetaGet("datacontract")
			Expect(exists).To(BeTrue())
			Expect(datacontract).To(Equal("_historian"))

			path0, exists := msg.MetaGet("path0")
			Expect(exists).To(BeTrue())
			Expect(path0).To(Equal("axis"))

			path1, exists := msg.MetaGet("path1")
			Expect(exists).To(BeTrue())
			Expect(path1).To(Equal("x"))

			path2, exists := msg.MetaGet("path2")
			Expect(exists).To(BeTrue())
			Expect(path2).To(Equal("position"))

			tagName, exists := msg.MetaGet("tagName")
			Expect(exists).To(BeTrue())
			Expect(tagName).To(Equal("actual"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise.plant1.machiningArea.cnc-line.cnc5.plc123._historian.axis.x.position.actual"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["actual"]
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
    msg.meta.location0 = "enterprise";
    msg.meta.datacontract = "_historian";
    msg.meta.tagName = "temperature";
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
			location0, exists := msg.MetaGet("location0")
			Expect(exists).To(BeTrue())
			Expect(location0).To(Equal("enterprise"))

			datacontract, exists := msg.MetaGet("datacontract")
			Expect(exists).To(BeTrue())
			Expect(datacontract).To(Equal("_historian"))

			tagName, exists := msg.MetaGet("tagName")
			Expect(exists).To(BeTrue())
			Expect(tagName).To(Equal("temperature"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise._historian.temperature"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["temperature"]
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
    // Missing location0
    msg.meta.datacontract = "_historian";
    // Missing tagName
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
    msg.meta.location0 = "enterprise";
    msg.meta.datacontract = "_historian";
    msg.meta.tagName = "default";
    msg.meta.path0 = "OEE";
    return msg;
  conditions:
    - if: msg.meta.path0.startsWith("OEE")
      then: |
        msg.meta.location2 = "OEEArea";
        return msg;
    - if: msg.meta.opcua_node_id === "ns=1;i=2245"
      then: |
        msg.meta.tagName = "temperature";
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
			location0, exists := msg.MetaGet("location0")
			Expect(exists).To(BeTrue())
			Expect(location0).To(Equal("enterprise"))

			location2, exists := msg.MetaGet("location2")
			Expect(exists).To(BeTrue())
			Expect(location2).To(Equal("OEEArea"))

			datacontract, exists := msg.MetaGet("datacontract")
			Expect(exists).To(BeTrue())
			Expect(datacontract).To(Equal("_historian"))

			path0, exists := msg.MetaGet("path0")
			Expect(exists).To(BeTrue())
			Expect(path0).To(Equal("OEE"))

			tagName, exists := msg.MetaGet("tagName")
			Expect(exists).To(BeTrue())
			Expect(tagName).To(Equal("temperature"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise.OEEArea._historian.OEE.temperature"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["temperature"]
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
    msg.meta.location0 = "enterprise";
    msg.meta.location1 = "site";
    msg.meta.location2 = "area";
    msg.meta.location3 = "line";
    msg.meta.location4 = "workcell";
    msg.meta.datacontract = "_analytics";
    msg.meta.path0 = "work_order";
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
			location0, exists := msg.MetaGet("location0")
			Expect(exists).To(BeTrue())
			Expect(location0).To(Equal("enterprise"))

			location1, exists := msg.MetaGet("location1")
			Expect(exists).To(BeTrue())
			Expect(location1).To(Equal("site"))

			location2, exists := msg.MetaGet("location2")
			Expect(exists).To(BeTrue())
			Expect(location2).To(Equal("area"))

			location3, exists := msg.MetaGet("location3")
			Expect(exists).To(BeTrue())
			Expect(location3).To(Equal("line"))

			location4, exists := msg.MetaGet("location4")
			Expect(exists).To(BeTrue())
			Expect(location4).To(Equal("workcell"))

			datacontract, exists := msg.MetaGet("datacontract")
			Expect(exists).To(BeTrue())
			Expect(datacontract).To(Equal("_analytics"))

			path0, exists := msg.MetaGet("path0")
			Expect(exists).To(BeTrue())
			Expect(path0).To(Equal("work_order"))

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
			Skip("Skipping until multiple message returns are implemented")
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  advancedProcessing: |
    msg.meta = {
      location0: "enterprise",
      datacontract: "_historian",
      tagName: "temperature"
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
			location0, exists := msg.MetaGet("location0")
			Expect(exists).To(BeTrue())
			Expect(location0).To(Equal("enterprise"))

			datacontract, exists := msg.MetaGet("datacontract")
			Expect(exists).To(BeTrue())
			Expect(datacontract).To(Equal("_historian"))

			tagName, exists := msg.MetaGet("tagName")
			Expect(exists).To(BeTrue())
			Expect(tagName).To(Equal("temperature"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise._historian.temperature"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["temperature"]
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
    msg.meta.location0 = "enterprise";
    msg.meta.datacontract = "_historian";
    msg.meta.tagName = "temperature";
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
    msg.meta.location0 = "enterprise";
    msg.meta.datacontract = "_historian";
    msg.meta.tagName = "temperature";
    return msg;
  advancedProcessing: |
    // Prepare two separate messages: one original and one backup
    let msg1 = {
      payload: msg.payload,
      meta: { ...msg.meta }
    };

    let msg2 = {
      payload: msg.payload,
      meta: { ...msg.meta, tagName: msg.meta.tagName + "_backup" }
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
			Expect(payload["temperature"]).To(Equal(json.Number("23.5")))

			// Check second message
			msg = messages[1]
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["temperature_backup"]).To(Equal(json.Number("23.5")))
		})

		It("should process messages duplicated in advancedProcessing through all stages", func() {
			Skip("Skipping until multiple message returns are implemented")
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.location0 = "enterprise";
    msg.meta.datacontract = "_historian";
    msg.meta.tagName = "temperature";
    return msg;

  conditions:
    - if: true
      then: |
        msg.meta.location2 = "production";
        return msg;

  advancedProcessing: |
    let doubledValue = msg.payload * 2;

	msg1 = {
		payload: msg.payload,
		meta: { ...msg.meta, datacontract: "_historian" }
	};

	msg2 = {
		payload: doubledValue,
		meta: { ...msg.meta, datacontract: "_analytics", tagName: msg.meta.tagName + "_doubled" }
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
			datacontract, exists := msg.MetaGet("datacontract")
			Expect(exists).To(BeTrue())
			Expect(datacontract).To(Equal("_historian"))
			level2, exists := msg.MetaGet("location2")
			Expect(exists).To(BeTrue())
			Expect(level2).To(Equal("production"))
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["temperature"]).To(Equal(json.Number("23.5")))

			// Check analytics message
			msg = messages[1]
			datacontract, exists = msg.MetaGet("datacontract")
			Expect(exists).To(BeTrue())
			Expect(datacontract).To(Equal("_analytics"))
			level2, exists = msg.MetaGet("location2")
			Expect(exists).To(BeTrue())
			Expect(level2).To(Equal("production"))
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["temperature_doubled"]).To(Equal(json.Number("47")))
		})

		It("should process messages duplicated in defaults through all stages", func() {
			Skip("Skipping until multiple message returns are implemented")
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
        location0: "enterprise",
        datacontract: "_historian",
        tagName: "temperature"
      }
    };

    msg2 = {
      payload: msg.payload,
      meta: {
        location0: "enterprise",
        datacontract: "_analytics",
        tagName: "temperature_raw"
      }
    };

    return [msg1, msg2];

  conditions:
    - if: msg.meta.datacontract === "_historian"
      then: |
        msg.meta.location2 = "production";
        return msg;
    - if: msg.meta.datacontract === "_analytics"
      then: |
        msg.meta.location2 = "analytics";
        msg.payload = msg.payload * 2;
        return msg;

  advancedProcessing: |
    if (msg.meta.datacontract === "_analytics") {
      msg.meta.path0 = "raw_data";
    } else {
      msg.meta.path0 = "processed";
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
			datacontract, exists := msg.MetaGet("datacontract")
			Expect(exists).To(BeTrue())
			Expect(datacontract).To(Equal("_historian"))
			level2, exists := msg.MetaGet("location2")
			Expect(exists).To(BeTrue())
			Expect(level2).To(Equal("production"))
			path0, exists := msg.MetaGet("path0")
			Expect(exists).To(BeTrue())
			Expect(path0).To(Equal("processed"))
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["temperature"]).To(Equal(json.Number("23.5")))

			// Check analytics message
			msg = messages[1]
			datacontract, exists = msg.MetaGet("datacontract")
			Expect(exists).To(BeTrue())
			Expect(datacontract).To(Equal("_analytics"))
			level2, exists = msg.MetaGet("location2")
			Expect(exists).To(BeTrue())
			Expect(level2).To(Equal("analytics"))
			path0, exists = msg.MetaGet("path0")
			Expect(exists).To(BeTrue())
			Expect(path0).To(Equal("raw_data"))
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["temperature_raw"]).To(Equal(json.Number("47")))
		})
	})
})
