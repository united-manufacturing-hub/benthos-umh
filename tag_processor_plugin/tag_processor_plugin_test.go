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
    msg.meta.level0 = "enterprise";
    msg.meta.schema = "_historian";
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
			level0, exists := msg.MetaGet("level0")
			Expect(exists).To(BeTrue())
			Expect(level0).To(Equal("enterprise"))

			schema, exists := msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_historian"))

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
    msg.meta.level0 = "enterprise";
    msg.meta.schema = "_historian";
    msg.meta.tagName = "default";
    msg.meta.virtualPath = "OEE";
    return msg;
  conditions:
    - if: msg.meta.opcua_node_id === "ns=1;i=2245"
      then: |
        msg.meta.level2 = "SpecialArea";
        msg.meta.tagName = "temperature";
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
			level0, exists := msg.MetaGet("level0")
			Expect(exists).To(BeTrue())
			Expect(level0).To(Equal("enterprise"))

			level2, exists := msg.MetaGet("level2")
			Expect(exists).To(BeTrue())
			Expect(level2).To(Equal("SpecialArea"))

			schema, exists := msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_historian"))

			tagName, exists := msg.MetaGet("tagName")
			Expect(exists).To(BeTrue())
			Expect(tagName).To(Equal("temperature"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise.SpecialArea._historian.OEE.temperature"))

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

		It("should process advanced processing", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.level0 = "enterprise";
    msg.meta.schema = "_historian";
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
			level0, exists := msg.MetaGet("level0")
			Expect(exists).To(BeTrue())
			Expect(level0).To(Equal("enterprise"))

			schema, exists := msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_historian"))

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
    // Missing level0
    msg.meta.schema = "_historian";
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
    msg.meta.level0 = "enterprise";
    msg.meta.schema = "_historian";
    msg.meta.tagName = "default";
    msg.meta.virtualPath = "OEE";
    return msg;
  conditions:
    - if: msg.meta.virtualPath.startsWith("OEE")
      then: |
        msg.meta.level2 = "OEEArea";
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
			level0, exists := msg.MetaGet("level0")
			Expect(exists).To(BeTrue())
			Expect(level0).To(Equal("enterprise"))

			level2, exists := msg.MetaGet("level2")
			Expect(exists).To(BeTrue())
			Expect(level2).To(Equal("OEEArea"))

			schema, exists := msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_historian"))

			tagName, exists := msg.MetaGet("tagName")
			Expect(exists).To(BeTrue())
			Expect(tagName).To(Equal("temperature"))

			virtualPath, exists := msg.MetaGet("virtualPath")
			Expect(exists).To(BeTrue())
			Expect(virtualPath).To(Equal("OEE"))

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
    msg.meta.level0 = "enterprise";
    msg.meta.level1 = "site";
    msg.meta.level2 = "area";
    msg.meta.level3 = "line";
    msg.meta.level4 = "workcell";
    msg.meta.schema = "_analytics";
    msg.meta.folder = "work_order";
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
			level0, exists := msg.MetaGet("level0")
			Expect(exists).To(BeTrue())
			Expect(level0).To(Equal("enterprise"))

			level1, exists := msg.MetaGet("level1")
			Expect(exists).To(BeTrue())
			Expect(level1).To(Equal("site"))

			level2, exists := msg.MetaGet("level2")
			Expect(exists).To(BeTrue())
			Expect(level2).To(Equal("area"))

			level3, exists := msg.MetaGet("level3")
			Expect(exists).To(BeTrue())
			Expect(level3).To(Equal("line"))

			level4, exists := msg.MetaGet("level4")
			Expect(exists).To(BeTrue())
			Expect(level4).To(Equal("workcell"))

			schema, exists := msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_analytics"))

			folder, exists := msg.MetaGet("folder")
			Expect(exists).To(BeTrue())
			Expect(folder).To(Equal("work_order"))

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
      level0: "enterprise",
      schema: "_historian",
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
			level0, exists := msg.MetaGet("level0")
			Expect(exists).To(BeTrue())
			Expect(level0).To(Equal("enterprise"))

			schema, exists := msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_historian"))

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
    msg.meta.level0 = "enterprise";
    msg.meta.schema = "_historian";
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
			Skip("Skipping until multiple message returns are implemented")
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tag_processor:
  defaults: |
    msg.meta.level0 = "enterprise";
    msg.meta.schema = "_historian";
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
    msg.meta.level0 = "enterprise";
    msg.meta.schema = "_historian";
    msg.meta.tagName = "temperature";
    return msg;

  conditions:
    - if: true
      then: |
        msg.meta.level2 = "production";
        return msg;

  advancedProcessing: |
    let doubledValue = msg.payload * 2;

	msg1 = {
		payload: msg.payload,
		meta: { ...msg.meta, schema: "_historian" }
	};

	msg2 = {
		payload: doubledValue,
		meta: { ...msg.meta, schema: "_analytics", tagName: msg.meta.tagName + "_doubled" }
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
			schema, exists := msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_historian"))
			level2, exists := msg.MetaGet("level2")
			Expect(exists).To(BeTrue())
			Expect(level2).To(Equal("production"))
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["temperature"]).To(Equal(json.Number("23.5")))

			// Check analytics message
			msg = messages[1]
			schema, exists = msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_analytics"))
			level2, exists = msg.MetaGet("level2")
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
        level0: "enterprise",
        schema: "_historian",
        tagName: "temperature"
      }
    };

    msg2 = {
      payload: msg.payload,
      meta: {
        level0: "enterprise",
        schema: "_analytics",
        tagName: "temperature_raw"
      }
    };

    return [msg1, msg2];

  conditions:
    - if: msg.meta.schema === "_historian"
      then: |
        msg.meta.level2 = "production";
        return msg;
    - if: msg.meta.schema === "_analytics"
      then: |
        msg.meta.level2 = "analytics";
        msg.payload = msg.payload * 2;
        return msg;

  advancedProcessing: |
    if (msg.meta.schema === "_analytics") {
      msg.meta.virtualPath = "raw_data";
    } else {
      msg.meta.virtualPath = "processed";
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
			schema, exists := msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_historian"))
			level2, exists := msg.MetaGet("level2")
			Expect(exists).To(BeTrue())
			Expect(level2).To(Equal("production"))
			virtualPath, exists := msg.MetaGet("virtualPath")
			Expect(exists).To(BeTrue())
			Expect(virtualPath).To(Equal("processed"))
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["temperature"]).To(Equal(json.Number("23.5")))

			// Check analytics message
			msg = messages[1]
			schema, exists = msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_analytics"))
			level2, exists = msg.MetaGet("level2")
			Expect(exists).To(BeTrue())
			Expect(level2).To(Equal("analytics"))
			virtualPath, exists = msg.MetaGet("virtualPath")
			Expect(exists).To(BeTrue())
			Expect(virtualPath).To(Equal("raw_data"))
			structured, err = msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload, ok = structured.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(payload["temperature_raw"]).To(Equal(json.Number("47")))
		})
	})
})
