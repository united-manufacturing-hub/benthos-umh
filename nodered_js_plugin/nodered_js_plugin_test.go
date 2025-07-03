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
	"os"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
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
			err = jsBuilder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = bloblangBuilder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
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
			Expect(len(processedPayloads)).To(Equal(numMessages))
		})
	})
})
