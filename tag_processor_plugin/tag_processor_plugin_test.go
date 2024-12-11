package tag_processor_plugin_test

import (
	"context"
	"os"
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
tagProcessor:
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
			Expect(value).To(Equal("23.5"))

			_, ok = payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
		})

		It("should process conditions", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tagProcessor:
  defaults: |
    msg.meta.level0 = "enterprise";
    msg.meta.schema = "_historian";
    msg.meta.tagName = "default";
    return msg;
  conditions:
    - if: msg.meta.opcua_node_id === "ns=1;i=2245"
      then: |
        msg.meta.level2 = "SpecialArea";
        msg.meta.tagName = "temperature";
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
			Expect(level2).To(Equal("SpecialArea"))

			schema, exists := msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_historian"))

			tagName, exists := msg.MetaGet("tagName")
			Expect(exists).To(BeTrue())
			Expect(tagName).To(Equal("temperature"))

			topic, exists := msg.MetaGet("topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.enterprise.SpecialArea._historian.temperature"))

			// Check payload
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			payload, ok := structured.(map[string]interface{})
			Expect(ok).To(BeTrue())

			value, ok := payload["temperature"]
			Expect(ok).To(BeTrue())
			Expect(value).To(Equal("23.5"))

			_, ok = payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
		})

		It("should process advanced processing", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tagProcessor:
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
			Expect(value).To(Equal(float64(47)))

			_, ok = payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
		})

		It("should handle missing required fields", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
tagProcessor:
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
tagProcessor:
  defaults: |
    msg.meta.level0 = "enterprise";
    msg.meta.schema = "_historian";
    msg.meta.tagName = "default";
    msg.meta.folder = "OEE";
    return msg;
  conditions:
    - if: msg.meta.folder.startsWith("OEE")
      then: |
        msg.meta.level2 = "OEEArea";
    - if: msg.meta.opcua_node_id === "ns=1;i=2245"
      then: |
        msg.meta.tagName = "temperature";
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

			folder, exists := msg.MetaGet("folder")
			Expect(exists).To(BeTrue())
			Expect(folder).To(Equal("OEE"))

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
			Expect(value).To(Equal("23.5"))

			_, ok = payload["timestamp_ms"]
			Expect(ok).To(BeTrue())
		})
	})
})
