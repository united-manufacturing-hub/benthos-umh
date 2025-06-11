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

package downsampler_plugin_test

import (
	"context"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"

	downsampler "github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin"
)

var _ = Describe("Downsampler Plugin", func() {
	BeforeEach(func() {
		testActivated := os.Getenv("TEST_DOWNSAMPLER")

		// Check if environment variables are set
		if testActivated == "" {
			Skip("Skipping Downsampler tests: TEST_DOWNSAMPLER not set")
			return
		}
	})

	When("using basic deadband functionality", func() {
		It("should pass through non-historian messages", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 0.5
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send a non-historian message
			msg := service.NewMessage(nil)
			msg.SetStructured(map[string]interface{}{
				"some_field": "some_value",
			})
			err = msgHandler(ctx, msg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Check that message passed through unchanged
			structured, err := messages[0].AsStructured()
			Expect(err).NotTo(HaveOccurred())
			Expect(structured).To(Equal(map[string]interface{}{
				"some_field": "some_value",
			}))
		})

		It("should keep the first data point and add metadata", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 0.5
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send first historian message
			msg := service.NewMessage(nil)
			msg.SetStructured(map[string]interface{}{
				"value":        10.5,
				"timestamp_ms": 1609459200000,
			})
			msg.MetaSet("data_contract", "_historian")
			msg.MetaSet("umh_topic", "umh.v1.acme._historian.temperature")
			err = msgHandler(ctx, msg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Check data
			structured, err := messages[0].AsStructured()
			Expect(err).NotTo(HaveOccurred())
			Expect(structured).To(HaveKeyWithValue("value", 10.5))

			// Check metadata
			metadata, exists := messages[0].MetaGet("downsampled_by")
			Expect(exists).To(BeTrue())
			Expect(metadata).To(ContainSubstring("deadband"))
		})

		It("should filter out changes below threshold", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 0.5
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// First message (always kept)
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"value":        10.0,
				"timestamp_ms": 1609459200000,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.acme._historian.temperature")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Second message below threshold (should be filtered)
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"value":        10.3, // 0.3 < 0.5 threshold
				"timestamp_ms": 1609459200100,
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.acme._historian.temperature")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1)) // Only first message should pass through
		})

		It("should keep changes above threshold", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 0.5
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// First message
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"value":        10.0,
				"timestamp_ms": 1609459200000,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.acme._historian.temperature")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Second message above threshold
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"value":        10.8, // 0.8 > 0.5 threshold
				"timestamp_ms": 1609459200100,
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.acme._historian.temperature")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2)) // Both messages should pass through
		})
	})

	When("using max interval functionality", func() {
		It("should force output after max interval", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 10.0
      max_time: 1s
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

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// First message (always kept)
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"value":        10.0,
				"timestamp_ms": 1609459200000, // 2021-01-01 00:00:00
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.acme._historian.temperature")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Second message (small change, normally filtered but max_time exceeded)
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"value":        11.0,          // 1.0 < 10.0 threshold, normally filtered
				"timestamp_ms": 1609459202000, // 2 seconds later, exceeds max_time
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.acme._historian.temperature")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Should force output due to max interval exceeded
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))
		})
	})

	When("handling different data types", func() {
		It("should handle integer values", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 2.0
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// First integer message
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"value":        42,
				"timestamp_ms": 1609459200000,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.acme._historian.counter")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Second integer message (change of 1 < 2.0 threshold)
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"value":        43,
				"timestamp_ms": 1609459200100,
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.acme._historian.counter")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Only first message should pass (second filtered due to small change)
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))
		})

		It("should handle boolean values", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 1.0
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// First boolean message
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"value":        true,
				"timestamp_ms": 1609459200000,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.acme._historian.enabled")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Second boolean message (same value - should be filtered)
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"value":        true,
				"timestamp_ms": 1609459200100,
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.acme._historian.enabled")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Third boolean message (different value - should be kept)
			msg3 := service.NewMessage(nil)
			msg3.SetStructured(map[string]interface{}{
				"value":        false,
				"timestamp_ms": 1609459200200,
			})
			msg3.MetaSet("data_contract", "_historian")
			msg3.MetaSet("umh_topic", "umh.v1.acme._historian.enabled")
			err = msgHandler(ctx, msg3)
			Expect(err).NotTo(HaveOccurred())

			// Should have first and third messages
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))
		})

		It("should handle string values", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 1.0
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// First string message
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"value":        "RUNNING",
				"timestamp_ms": 1609459200000,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.acme._historian.status")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Second string message (same value - should be filtered)
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"value":        "RUNNING",
				"timestamp_ms": 1609459200100,
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.acme._historian.status")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Third string message (different value - should be kept)
			msg3 := service.NewMessage(nil)
			msg3.SetStructured(map[string]interface{}{
				"value":        "STOPPED",
				"timestamp_ms": 1609459200200,
			})
			msg3.MetaSet("data_contract", "_historian")
			msg3.MetaSet("umh_topic", "umh.v1.acme._historian.status")
			err = msgHandler(ctx, msg3)
			Expect(err).NotTo(HaveOccurred())

			// Should have first and third messages
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))
		})

		It("should process UMH-core format with single value field", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 0.5
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// UMH-core format (single value field)
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"value":        23.4,
				"timestamp_ms": 1609459200000,
			})
			msg1.MetaSet("umh_topic", "umh.v1.acme.weather.temperature")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Check data preserved
			structured, err := messages[0].AsStructured()
			Expect(err).NotTo(HaveOccurred())
			Expect(structured).To(HaveKeyWithValue("value", 23.4))

			// Check metadata annotation
			metadata, exists := messages[0].MetaGet("downsampled_by")
			Expect(exists).To(BeTrue())
			Expect(metadata).To(ContainSubstring("deadband"))
		})

		It("should use topic-specific thresholds with pattern matching", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 2.0
  overrides:
    - pattern: "*.temperature"
      deadband:
        threshold: 0.1
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send temperature message (should use 0.1 threshold from pattern match)
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"value":        20.0,
				"timestamp_ms": 1609459200000,
			})
			msg1.MetaSet("umh_topic", "umh.v1.acme._historian.temperature")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Small change that exceeds temperature threshold
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"value":        20.15, // 0.15 > 0.1 threshold for temperature
				"timestamp_ms": 1609459200100,
			})
			msg2.MetaSet("umh_topic", "umh.v1.acme._historian.temperature")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Should have both messages (temperature uses stricter threshold)
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))
		})

		It("should use topic-specific thresholds with exact topic matching", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 2.0
  overrides:
    - pattern: "umh.v1.acme._historian.critical_temp"
      deadband:
        threshold: 0.01
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send exact topic match message
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"value":        25.0,
				"timestamp_ms": 1609459200000,
			})
			msg1.MetaSet("umh_topic", "umh.v1.acme._historian.critical_temp")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Very small change that exceeds the exact topic threshold
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"value":        25.02, // 0.02 > 0.01 threshold for exact topic
				"timestamp_ms": 1609459200100,
			})
			msg2.MetaSet("umh_topic", "umh.v1.acme._historian.critical_temp")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Should have both messages (exact topic uses very strict threshold)
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))
		})

		It("should fallback to default threshold when no topic match found", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 5.0
  overrides:
    - pattern: "*.temperature"
      deadband:
        threshold: 0.1
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send message that doesn't match any override patterns
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"value":        100.0,
				"timestamp_ms": 1609459200000,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.acme._historian.pressure")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Small change that would be filtered with default threshold
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"value":        103.0, // 3.0 < 5.0 default threshold
				"timestamp_ms": 1609459200100,
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.acme._historian.pressure")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Should only have first message (second filtered by default threshold)
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))
		})
	})

	Describe("when handling errors gracefully", func() {
		It("should pass through messages with missing umh_topic", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 0.5
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send message missing umh_topic metadata
			msg := service.NewMessage(nil)
			msg.SetStructured(map[string]interface{}{
				"value":        10.0,
				"timestamp_ms": 1609459200000,
			})
			msg.MetaSet("data_contract", "_historian")
			// Note: missing umh_topic metadata
			err = msgHandler(ctx, msg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Message should pass through unchanged without downsampling
			_, exists := messages[0].MetaGet("downsampled_by")
			Expect(exists).To(BeFalse())
		})

		It("should pass through messages with missing timestamp_ms", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 0.5
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

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Send message missing timestamp_ms field
			msg := service.NewMessage(nil)
			msg.SetStructured(map[string]interface{}{
				"value": 10.0,
				// Note: missing timestamp_ms field
			})
			msg.MetaSet("data_contract", "_historian")
			msg.MetaSet("umh_topic", "umh.v1.acme._historian.temperature")
			err = msgHandler(ctx, msg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Message should pass through unchanged without downsampling
			_, exists := messages[0].MetaGet("downsampled_by")
			Expect(exists).To(BeFalse())
		})
	})

	Describe("getConfigForTopic function", func() {
		var config downsampler.DownsamplerConfig

		BeforeEach(func() {
			// Reset config for each test
			config = downsampler.DownsamplerConfig{}
		})

		Context("with default configuration only", func() {
			BeforeEach(func() {
				config.Default.Deadband.Threshold = 1.5
				config.Default.Deadband.MaxTime = 30 * time.Second
			})

			It("should return default values for any topic", func() {
				algorithm, configMap := config.GetConfigForTopic("any.topic.here")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 1.5))
				Expect(configMap).To(HaveKeyWithValue("max_time", 30*time.Second))
			})
		})

		Context("with pattern matching overrides", func() {
			BeforeEach(func() {
				config.Default.Deadband.Threshold = 2.0
				config.Overrides = []downsampler.OverrideConfig{
					{
						Pattern:  "*.temperature",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.1},
					},
					{
						Pattern:  "*.pressure",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.5},
					},
				}
			})

			It("should match wildcard patterns correctly", func() {
				// Test temperature pattern match
				algorithm, configMap := config.GetConfigForTopic("umh.v1.acme._historian.temperature")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.1))
			})

			It("should match different patterns", func() {
				// Test pressure pattern match
				algorithm, configMap := config.GetConfigForTopic("umh.v1.factory.line1._historian.pressure")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.5))
			})

			It("should fall back to default for non-matching topics", func() {
				// Test non-matching topic
				algorithm, configMap := config.GetConfigForTopic("umh.v1.acme._historian.humidity")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 2.0))
			})
		})

		Context("with exact topic matching overrides", func() {
			BeforeEach(func() {
				config.Default.Deadband.Threshold = 2.0
				config.Overrides = []downsampler.OverrideConfig{
					{
						Topic:    "umh.v1.acme._historian.critical_temp",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.01},
					},
				}
			})

			It("should match exact topics correctly", func() {
				algorithm, configMap := config.GetConfigForTopic("umh.v1.acme._historian.critical_temp")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.01))
			})

			It("should fall back to default for non-exact topics", func() {
				algorithm, configMap := config.GetConfigForTopic("umh.v1.acme._historian.critical_temp_2")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 2.0))
			})
		})

		Context("with mixed pattern and exact topic overrides", func() {
			BeforeEach(func() {
				config.Default.Deadband.Threshold = 10.0
				config.Overrides = []downsampler.OverrideConfig{
					{
						Pattern:  "*.temperature",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.5},
					},
					{
						Topic:    "umh.v1.acme._historian.critical_temperature",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.01},
					},
				}
			})

			It("should apply first matching rule (exact topic before pattern)", func() {
				// This should match the exact topic rule first
				algorithm, configMap := config.GetConfigForTopic("umh.v1.acme._historian.critical_temperature")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.01))
			})

			It("should apply pattern rule when exact doesn't match", func() {
				// This should match the pattern rule
				algorithm, configMap := config.GetConfigForTopic("umh.v1.factory._historian.temperature")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.5))
			})

			It("should use default values when no overrides match", func() {
				// This topic should not match any pattern or exact topic override
				algorithm, configMap := config.GetConfigForTopic("umh.v1.plant._historian.flow_rate")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 10.0)) // Default threshold
			})
		})

		Context("with complex wildcard patterns", func() {
			BeforeEach(func() {
				config.Default.Deadband.Threshold = 5.0
				config.Overrides = []downsampler.OverrideConfig{
					{
						Pattern:  "umh.v1.*.temperature",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.2},
					},
					{
						Pattern:  "umh.v1.factory.*",
						Deadband: &downsampler.DeadbandConfig{Threshold: 1.0},
					},
				}
			})

			It("should match specific wildcard patterns", func() {
				// Should match umh.v1.*.temperature
				algorithm, configMap := config.GetConfigForTopic("umh.v1.plant1.temperature")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.2))
			})

			It("should match broader wildcard patterns", func() {
				// Should match umh.v1.factory.*
				algorithm, configMap := config.GetConfigForTopic("umh.v1.factory.pressure")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 1.0))
			})

			It("should use default values when no complex patterns match", func() {
				// This topic should not match any of the complex wildcard patterns
				algorithm, configMap := config.GetConfigForTopic("umh.v2.plant1.humidity")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 5.0)) // Default threshold
			})
		})

		Context("with swinging door algorithm", func() {
			BeforeEach(func() {
				config.Default.SwingingDoor.Threshold = 0.3
				config.Default.SwingingDoor.MinTime = 5 * time.Second
				config.Default.SwingingDoor.MaxTime = 1 * time.Hour
				config.Overrides = []downsampler.OverrideConfig{
					{
						Pattern: "*.vibration",
						SwingingDoor: &downsampler.SwingingDoorConfig{
							Threshold: 0.01,
							MinTime:   1 * time.Second,
						},
					},
				}
			})

			It("should return swinging door algorithm for default", func() {
				algorithm, configMap := config.GetConfigForTopic("any.topic")

				Expect(algorithm).To(Equal("swinging_door"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.3))
				Expect(configMap).To(HaveKeyWithValue("min_time", 5*time.Second))
				Expect(configMap).To(HaveKeyWithValue("max_time", 1*time.Hour))
			})

			It("should apply swinging door overrides", func() {
				algorithm, configMap := config.GetConfigForTopic("umh.v1.machine._historian.vibration")

				Expect(algorithm).To(Equal("swinging_door"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.01))
				Expect(configMap).To(HaveKeyWithValue("min_time", 1*time.Second))
			})

			It("should use default swinging door values when no overrides match", func() {
				// This topic should not match the vibration pattern
				algorithm, configMap := config.GetConfigForTopic("umh.v1.machine._historian.temperature")

				Expect(algorithm).To(Equal("swinging_door"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.3))          // Default threshold
				Expect(configMap).To(HaveKeyWithValue("min_time", 5*time.Second)) // Default min_time
				Expect(configMap).To(HaveKeyWithValue("max_time", 1*time.Hour))   // Default max_time
			})
		})
	})
})
