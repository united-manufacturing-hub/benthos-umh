package nodered_js_plugin_test

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			testMsg.SetStructured(map[string]interface{}{
				"payload": "test",
			})
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
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(map[string]interface{}{
				"payload": []interface{}{1, 2, 3},
			})
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
			Expect(string(jsonStr)).To(Equal(`3`))
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
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(map[string]interface{}{
				"original": "data",
			})
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

			// Send multiple test messages
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(map[string]interface{}{
				"payload": "test",
			})

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

			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(map[string]interface{}{
				"payload": "test",
			})
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

			// Create message with metadata
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(map[string]interface{}{
				"payload": "test",
			})
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

			// Create message with metadata
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(map[string]interface{}{
				"payload": "test",
			})
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
})