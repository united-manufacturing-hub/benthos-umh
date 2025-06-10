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
  algorithm: "deadband"
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

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Create and send non-historian message
			testMsg := service.NewMessage([]byte(`{"value": 42}`))
			testMsg.MetaSet("data_contract", "_analytics")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Message should pass through unchanged
			_, exists := messages[0].MetaGet("downsampled_by")
			Expect(exists).To(BeFalse())
		})

		It("should keep the first data point and add metadata", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  algorithm: "deadband" 
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

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Create and send historian message
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459200000, // 2021-01-01 00:00:00
				"value":        10.0,
			})
			testMsg.MetaSet("data_contract", "_historian")
			testMsg.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.temperature")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Check metadata annotation
			metadata, exists := messages[0].MetaGet("downsampled_by")
			Expect(exists).To(BeTrue())
			Expect(metadata).To(ContainSubstring("deadband"))
			Expect(metadata).To(ContainSubstring("threshold=0.500"))
		})

		It("should filter out changes below threshold", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  algorithm: "deadband"
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

			// Send first message (baseline)
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459200000,
				"value":        10.0,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.temperature")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Send second message (small change - should be filtered)
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459201000,
				"value":        10.3, // Change of 0.3 < 0.5 threshold
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.temperature")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Should still be only one message (second filtered out)
			Consistently(func() int {
				return len(messages)
			}, 500*time.Millisecond).Should(Equal(1))
		})

		It("should keep changes above threshold", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  algorithm: "deadband"
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

			// Send first message (baseline)
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459200000,
				"value":        10.0,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.temperature")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Send second message (large change - should be kept)
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459201000,
				"value":        10.6, // Change of 0.6 >= 0.5 threshold
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.temperature")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Should have both messages
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))
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
  algorithm: "deadband"
  threshold: 0.5
  max_interval: "60s"
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

			// Send first message (baseline)
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459200000, // 2021-01-01 00:00:00
				"value":        10.0,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.temperature")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Send second message (small change but after max interval)
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459260000, // 1 minute later
				"value":        10.3,          // Change of 0.3 < 0.5 threshold
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.temperature")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Should be kept due to max interval
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))

			// Check metadata includes max_interval
			metadata, exists := messages[1].MetaGet("downsampled_by")
			Expect(exists).To(BeTrue())
			Expect(metadata).To(ContainSubstring("max_interval=1m0s"))
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
  algorithm: "deadband"
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

			// Send integer messages
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459200000,
				"value":        10,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.count")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459201000,
				"value":        11, // Change of 1.0 = threshold
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.count")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Should be kept (change = threshold)
			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))
		})

		It("should handle boolean values", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  algorithm: "deadband"
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

			// Send boolean messages
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459200000,
				"value":        false,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.alarm")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459201000,
				"value":        true, // Change of 1.0 = threshold
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.alarm")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Boolean change should be kept
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
  algorithm: "deadband"
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

			// Send string messages
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459200000,
				"value":        "running",
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.status")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Same string - should be filtered
			msg2 := service.NewMessage(nil)
			msg2.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459201000,
				"value":        "running",
			})
			msg2.MetaSet("data_contract", "_historian")
			msg2.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.status")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Should still be only one message
			Consistently(func() int {
				return len(messages)
			}, 500*time.Millisecond).Should(Equal(1))

			// Different string - should be kept
			msg3 := service.NewMessage(nil)
			msg3.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459202000,
				"value":        "stopped",
			})
			msg3.MetaSet("data_contract", "_historian")
			msg3.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.status")
			err = msgHandler(ctx, msg3)
			Expect(err).NotTo(HaveOccurred())

			// Should have two messages now
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
  algorithm: "deadband"
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

			// UMH-core format with single "value" field
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"value":        23.4,
				"timestamp_ms": 1609459200000,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.acme._historian.weather.temperature")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Verify value is processed correctly
			structured, err := messages[0].AsStructured()
			Expect(err).NotTo(HaveOccurred())
			Expect(structured).To(HaveKeyWithValue("value", 23.4))

			// Check metadata annotation
			metadata, exists := messages[0].MetaGet("downsampled_by")
			Expect(exists).To(BeTrue())
			Expect(metadata).To(ContainSubstring("deadband"))
		})

		It("should process UMH classic format with multiple fields", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  algorithm: "deadband"
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

			// UMH classic format with multiple fields (first message processed)
			msg1 := service.NewMessage(nil)
			msg1.SetStructured(map[string]interface{}{
				"temperature":  23.4,
				"humidity":     42.1,
				"timestamp_ms": 1609459200000,
			})
			msg1.MetaSet("data_contract", "_historian")
			msg1.MetaSet("umh_topic", "umh.v1.acme._historian.weather")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Should process the first non-timestamp field found (temperature or humidity)
			structured, err := messages[0].AsStructured()
			Expect(err).NotTo(HaveOccurred())
			// Should have either temperature or humidity value preserved
			Expect(structured).To(SatisfyAny(
				HaveKeyWithValue("temperature", 23.4),
				HaveKeyWithValue("humidity", 42.1),
			))

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
  algorithm: "deadband"
  threshold: 1.0
  topic_thresholds:
    - pattern: "*.temperature"
      threshold: 0.5
    - pattern: "*.pressure"
      threshold: 50.0
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

			// Temperature message - should use 0.5 threshold
			tempMsg := service.NewMessage(nil)
			tempMsg.SetStructured(map[string]interface{}{
				"value":        20.0,
				"timestamp_ms": 1609459200000,
			})
			tempMsg.MetaSet("data_contract", "_historian")
			tempMsg.MetaSet("umh_topic", "umh.v1.plant1.line1._historian.temperature")
			err = msgHandler(ctx, tempMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Small temperature change - should be filtered (0.3 < 0.5)
			tempMsg2 := service.NewMessage(nil)
			tempMsg2.SetStructured(map[string]interface{}{
				"value":        20.3,
				"timestamp_ms": 1609459201000,
			})
			tempMsg2.MetaSet("data_contract", "_historian")
			tempMsg2.MetaSet("umh_topic", "umh.v1.plant1.line1._historian.temperature")
			err = msgHandler(ctx, tempMsg2)
			Expect(err).NotTo(HaveOccurred())

			// Should still be only one message (filtered)
			Consistently(func() int {
				return len(messages)
			}, 500*time.Millisecond).Should(Equal(1))

			// Large temperature change - should be kept (0.7 >= 0.5)
			tempMsg3 := service.NewMessage(nil)
			tempMsg3.SetStructured(map[string]interface{}{
				"value":        20.7,
				"timestamp_ms": 1609459202000,
			})
			tempMsg3.MetaSet("data_contract", "_historian")
			tempMsg3.MetaSet("umh_topic", "umh.v1.plant1.line1._historian.temperature")
			err = msgHandler(ctx, tempMsg3)
			Expect(err).NotTo(HaveOccurred())

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
  algorithm: "deadband"
  threshold: 1.0
  topic_thresholds:
    - topic: "umh.v1.plant1.line1._historian.temperature"
      threshold: 0.2
    - pattern: "*.temperature"
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

			// Exact topic match - should use 0.2 threshold (highest priority)
			tempMsg := service.NewMessage(nil)
			tempMsg.SetStructured(map[string]interface{}{
				"value":        20.0,
				"timestamp_ms": 1609459200000,
			})
			tempMsg.MetaSet("data_contract", "_historian")
			tempMsg.MetaSet("umh_topic", "umh.v1.plant1.line1._historian.temperature")
			err = msgHandler(ctx, tempMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Small change - should be filtered (0.15 < 0.2)
			tempMsg2 := service.NewMessage(nil)
			tempMsg2.SetStructured(map[string]interface{}{
				"value":        20.15,
				"timestamp_ms": 1609459201000,
			})
			tempMsg2.MetaSet("data_contract", "_historian")
			tempMsg2.MetaSet("umh_topic", "umh.v1.plant1.line1._historian.temperature")
			err = msgHandler(ctx, tempMsg2)
			Expect(err).NotTo(HaveOccurred())

			// Should still be only one message (filtered)
			Consistently(func() int {
				return len(messages)
			}, 500*time.Millisecond).Should(Equal(1))

			// Change meeting exact topic threshold - should be kept (0.25 >= 0.2)
			tempMsg3 := service.NewMessage(nil)
			tempMsg3.SetStructured(map[string]interface{}{
				"value":        20.25,
				"timestamp_ms": 1609459202000,
			})
			tempMsg3.MetaSet("data_contract", "_historian")
			tempMsg3.MetaSet("umh_topic", "umh.v1.plant1.line1._historian.temperature")
			err = msgHandler(ctx, tempMsg3)
			Expect(err).NotTo(HaveOccurred())

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
  algorithm: "deadband"
  threshold: 2.0
  topic_thresholds:
    - pattern: "*.temperature"
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

			// Pressure message - no specific threshold, should use default (2.0)
			pressureMsg := service.NewMessage(nil)
			pressureMsg.SetStructured(map[string]interface{}{
				"value":        100.0,
				"timestamp_ms": 1609459200000,
			})
			pressureMsg.MetaSet("data_contract", "_historian")
			pressureMsg.MetaSet("umh_topic", "umh.v1.plant1.line1._historian.pressure")
			err = msgHandler(ctx, pressureMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Small pressure change - should be filtered (1.5 < 2.0)
			pressureMsg2 := service.NewMessage(nil)
			pressureMsg2.SetStructured(map[string]interface{}{
				"value":        101.5,
				"timestamp_ms": 1609459201000,
			})
			pressureMsg2.MetaSet("data_contract", "_historian")
			pressureMsg2.MetaSet("umh_topic", "umh.v1.plant1.line1._historian.pressure")
			err = msgHandler(ctx, pressureMsg2)
			Expect(err).NotTo(HaveOccurred())

			// Should still be only one message (filtered)
			Consistently(func() int {
				return len(messages)
			}, 500*time.Millisecond).Should(Equal(1))

			// Large pressure change - should be kept (2.5 >= 2.0)
			pressureMsg3 := service.NewMessage(nil)
			pressureMsg3.SetStructured(map[string]interface{}{
				"value":        102.5,
				"timestamp_ms": 1609459202000,
			})
			pressureMsg3.MetaSet("data_contract", "_historian")
			pressureMsg3.MetaSet("umh_topic", "umh.v1.plant1.line1._historian.pressure")
			err = msgHandler(ctx, pressureMsg3)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(2))
		})
	})

	When("handling errors gracefully", func() {
		It("should pass through messages with missing umh_topic", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  algorithm: "deadband"
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

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Message with missing umh_topic
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(map[string]interface{}{
				"timestamp_ms": 1609459200000,
				"value":        10.0,
			})
			testMsg.MetaSet("data_contract", "_historian")
			// Missing umh_topic
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Should not have downsampled_by metadata (passed through due to error)
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
  algorithm: "deadband"
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

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Message with missing timestamp_ms
			testMsg := service.NewMessage(nil)
			testMsg.SetStructured(map[string]interface{}{
				"value": 10.0,
				// Missing timestamp_ms
			})
			testMsg.MetaSet("data_contract", "_historian")
			testMsg.MetaSet("umh_topic", "umh.v1.enterprise.site.area.line.workcell._historian.temperature")
			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Should not have downsampled_by metadata (passed through due to error)
			_, exists := messages[0].MetaGet("downsampled_by")
			Expect(exists).To(BeFalse())
		})
	})
})
