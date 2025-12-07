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
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"

	// Import the downsampler plugin to register it
	_ "github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin"
)

var _ = Describe("DownsamplerPlugin", func() {
	BeforeEach(func() {
		testActivated := os.Getenv("TEST_DOWNSAMPLER")

		// Check if environment variables are set
		if testActivated == "" {
			Skip("Skipping Downsampler tests: TEST_DOWNSAMPLER not set")
			return
		}
	})

	When("using metadata-driven compression hints", func() {
		It("should apply metadata overrides for algorithm switching", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			// Configure downsampler with default deadband and allow metadata overrides
			err = builder.AddProcessorYAML(`
downsampler:
  allow_meta_overrides: true
  default:
    deadband:
      threshold: 1.0
      max_time: 30s
`)
			Expect(err).NotTo(HaveOccurred())

			var (
				messages []*service.Message
				mtx      sync.Mutex
			)
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				mtx.Lock()
				messages = append(messages, msg)
				mtx.Unlock()
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

			// Create test messages with metadata overrides
			baseTime := time.Now()

			// Message 1: Use default deadband (should pass - first message always passes)
			msg1 := createTimeSeriesMessage(10.0, baseTime, "test.series1")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Message 2: Same series, large change (should pass with default threshold)
			msg2 := createTimeSeriesMessage(12.0, baseTime.Add(time.Second), "test.series1")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Message 3: Different series with metadata override to swinging_door
			msg3 := createTimeSeriesMessage(20.0, baseTime, "test.series2")
			msg3.MetaSet("ds_algorithm", "swinging_door")
			msg3.MetaSet("ds_threshold", "0.1")
			msg3.MetaSet("ds_min_time", "1s")
			msg3.MetaSet("ds_max_time", "10s")
			err = msgHandler(ctx, msg3)
			Expect(err).NotTo(HaveOccurred())

			// Message 4: Same series with small change (should pass with lower threshold)
			msg4 := createTimeSeriesMessage(20.05, baseTime.Add(time.Second), "test.series2")
			msg4.MetaSet("ds_algorithm", "swinging_door")
			msg4.MetaSet("ds_threshold", "0.1")
			msg4.MetaSet("ds_min_time", "1s")
			msg4.MetaSet("ds_max_time", "10s")
			err = msgHandler(ctx, msg4)
			Expect(err).NotTo(HaveOccurred())

			// Message 5: Third series with threshold override only
			msg5 := createTimeSeriesMessage(30.0, baseTime, "test.series3")
			msg5.MetaSet("ds_threshold", "0.01")
			err = msgHandler(ctx, msg5)
			Expect(err).NotTo(HaveOccurred())

			// Message 6: Same series with tiny change (should pass with lower threshold)
			msg6 := createTimeSeriesMessage(30.02, baseTime.Add(time.Second), "test.series3")
			msg6.MetaSet("ds_threshold", "0.01")
			err = msgHandler(ctx, msg6)
			Expect(err).NotTo(HaveOccurred())

			// Wait for processing
			Eventually(func() int {
				mtx.Lock()
				n := len(messages)
				mtx.Unlock()
				return n
			}, "3s").Should(BeNumerically(">=", 5))

			// Verify we got the expected messages
			// Should get: msg1, msg2, msg3, msg4, msg5, msg6 (all should pass)
			mtx.Lock()
			messageCount := len(messages)
			messagesCopy := make([]*service.Message, len(messages))
			copy(messagesCopy, messages)
			mtx.Unlock()

			Expect(messageCount).To(BeNumerically(">=", 5))

			// Check that series1 messages passed
			series1Count := 0
			for _, msg := range messagesCopy {
				if topic, exists := msg.MetaGet("umh_topic"); exists && topic == "test.series1" {
					series1Count++
				}
			}
			Expect(series1Count).To(BeNumerically(">=", 1), "Should have at least one series1 message")

			// Check that series2 messages passed (swinging_door with low threshold)
			series2Count := 0
			for _, msg := range messagesCopy {
				if topic, exists := msg.MetaGet("umh_topic"); exists && topic == "test.series2" {
					series2Count++
				}
			}
			Expect(series2Count).To(BeNumerically(">=", 1), "Should have at least one series2 message")

			// Check that series3 messages passed (deadband with low threshold)
			series3Count := 0
			for _, msg := range messagesCopy {
				if topic, exists := msg.MetaGet("umh_topic"); exists && topic == "test.series3" {
					series3Count++
				}
			}
			Expect(series3Count).To(BeNumerically(">=", 1), "Should have at least one series3 message")
		})

		It("should reject invalid metadata overrides", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  allow_meta_overrides: true
  default:
    deadband:
      threshold: 1.0
      max_time: 30s
`)
			Expect(err).NotTo(HaveOccurred())

			var (
				messages []*service.Message
				mtx      sync.Mutex
			)
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				mtx.Lock()
				messages = append(messages, msg)
				mtx.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Message with invalid algorithm
			msg1 := createTimeSeriesMessage(10.0, time.Now(), "test.series1")
			msg1.MetaSet("ds_algorithm", "invalid_algorithm")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Message with invalid threshold
			msg2 := createTimeSeriesMessage(20.0, time.Now(), "test.series2")
			msg2.MetaSet("ds_threshold", "not_a_number")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Message with invalid duration
			msg3 := createTimeSeriesMessage(30.0, time.Now(), "test.series3")
			msg3.MetaSet("ds_max_time", "invalid_duration")
			err = msgHandler(ctx, msg3)
			Expect(err).NotTo(HaveOccurred())

			// Wait for processing
			Eventually(func() int {
				mtx.Lock()
				n := len(messages)
				mtx.Unlock()
				return n
			}, "2s").Should(Equal(3))

			// All messages should pass through using default configuration
			// since invalid metadata should be rejected
			mtx.Lock()
			messageCount := len(messages)
			mtx.Unlock()
			Expect(messageCount).To(Equal(3))
		})

		It("should respect allow_meta_overrides=false", func() {
			// This test verifies that when allow_meta_overrides=false,
			// the downsampler still processes messages using default configuration
			// (metadata overrides are simply ignored)

			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  allow_meta_overrides: false
  default:
    deadband:
      threshold: 0.1
      max_time: 30s
`)
			Expect(err).NotTo(HaveOccurred())

			var (
				messages []*service.Message
				mtx      sync.Mutex
			)
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				mtx.Lock()
				messages = append(messages, msg)
				mtx.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			baseTime := time.Now()

			// Send a message that should pass through (first message always passes)
			msg1 := createTimeSeriesMessage(10.0, baseTime, "test.series1")
			msg1.MetaSet("ds_threshold", "0.01") // This should be ignored
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Wait for processing
			Eventually(func() int {
				mtx.Lock()
				n := len(messages)
				mtx.Unlock()
				return n
			}, "2s").Should(BeNumerically(">=", 1))

			// Verify we got at least one message (proving the downsampler works)
			mtx.Lock()
			messageCount := len(messages)
			firstMessage := messages[0]
			mtx.Unlock()

			Expect(messageCount).To(BeNumerically(">=", 1))

			// Verify the message content
			structured, err := firstMessage.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			data := structured.(map[string]interface{})

			// Handle both float64 (from direct SetStructured) and json.Number (from JSON parsing)
			var val float64
			switch v := data["value"].(type) {
			case float64:
				val = v
			case json.Number:
				val, err = v.Float64()
				Expect(err).NotTo(HaveOccurred())
			default:
				Fail(fmt.Sprintf("Unexpected value type: %T", v))
			}

			Expect(val).To(Equal(10.0))

			// Verify downsampler metadata is present (proving it was processed)
			downsampledBy, exists := firstMessage.MetaGet("downsampled_by")
			Expect(exists).To(BeTrue(), "Message should have downsampled_by metadata")
			Expect(downsampledBy).To(ContainSubstring("deadband"))
		})

		It("should handle late_policy metadata override", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  allow_meta_overrides: true
  default:
    deadband:
      threshold: 0.1
      max_time: 30s
    late_policy: passthrough
`)
			Expect(err).NotTo(HaveOccurred())

			var (
				messages []*service.Message
				mtx      sync.Mutex
			)
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				mtx.Lock()
				messages = append(messages, msg)
				mtx.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			baseTime := time.Now()

			// Message 1: Current time - should pass (first message)
			msg1 := createTimeSeriesMessage(10.0, baseTime, "test.series1")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Message 2: Future time - should pass
			msg2 := createTimeSeriesMessage(20.0, baseTime.Add(2*time.Second), "test.series1")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Wait for processing
			Eventually(func() int {
				mtx.Lock()
				n := len(messages)
				mtx.Unlock()
				return n
			}, "2s").Should(BeNumerically(">=", 2))

			// Should get both messages
			mtx.Lock()
			messageCount := len(messages)
			mtx.Unlock()
			Expect(messageCount).To(BeNumerically(">=", 2))
		})

		It("should work with pattern overrides and metadata", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  allow_meta_overrides: true
  default:
    deadband:
      threshold: 1.0
      max_time: 30s
  overrides:
    - pattern: "*.temperature"
      deadband:
        threshold: 0.5
        max_time: 60s
`)
			Expect(err).NotTo(HaveOccurred())

			var (
				messages []*service.Message
				mtx      sync.Mutex
			)
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				mtx.Lock()
				messages = append(messages, msg)
				mtx.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			baseTime := time.Now()

			// Message 1: Temperature series (matches pattern override) - first message always passes
			msg1 := createTimeSeriesMessage(10.0, baseTime, "test.temperature")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Wait for first message
			Eventually(func() int {
				mtx.Lock()
				n := len(messages)
				mtx.Unlock()
				return n
			}, "1s").Should(Equal(1))

			// Message 2: Large change that passes both pattern and metadata overrides
			msg2 := createTimeSeriesMessage(11.0, baseTime.Add(time.Second), "test.temperature")
			msg2.MetaSet("ds_threshold", "0.1") // Metadata should override pattern
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Wait for second message
			Eventually(func() int {
				mtx.Lock()
				n := len(messages)
				mtx.Unlock()
				return n
			}, "1s").Should(Equal(2))

			// Should get both messages
			mtx.Lock()
			messageCount := len(messages)
			mtx.Unlock()
			Expect(messageCount).To(Equal(2))
		})

		It("should respect ds_ignore metadata to bypass downsampling", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 0.1
      max_time: 30s
`)
			Expect(err).NotTo(HaveOccurred())

			var (
				messages []*service.Message
				mtx      sync.Mutex
			)
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				mtx.Lock()
				messages = append(messages, msg)
				mtx.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			go func() {
				_ = stream.Run(ctx)
			}()

			baseTime := time.Now()

			// Message 1: Normal message (should be processed)
			msg1 := createTimeSeriesMessage(10.0, baseTime, "test.series1")
			err = msgHandler(ctx, msg1)
			Expect(err).NotTo(HaveOccurred())

			// Message 2: Same series, small change (should be filtered)
			msg2 := createTimeSeriesMessage(10.05, baseTime.Add(time.Second), "test.series1")
			err = msgHandler(ctx, msg2)
			Expect(err).NotTo(HaveOccurred())

			// Message 3: Different series with ds_ignore metadata
			msg3 := createTimeSeriesMessage(20.0, baseTime, "test.series2")
			msg3.MetaSet("ds_ignore", "true")
			err = msgHandler(ctx, msg3)
			Expect(err).NotTo(HaveOccurred())

			// Message 4: Same series, small change but still with ds_ignore
			msg4 := createTimeSeriesMessage(20.01, baseTime.Add(time.Second), "test.series2")
			msg4.MetaSet("ds_ignore", "bypass")
			err = msgHandler(ctx, msg4)
			Expect(err).NotTo(HaveOccurred())

			// Message 5: Non-time-series message with ds_ignore (should still be ignored)
			msg5 := service.NewMessage([]byte(`{"some": "data"}`))
			msg5.MetaSet("ds_ignore", "yes")
			err = msgHandler(ctx, msg5)
			Expect(err).NotTo(HaveOccurred())

			// Wait for processing
			Eventually(func() int {
				mtx.Lock()
				n := len(messages)
				mtx.Unlock()
				return n
			}, "2s").Should(Equal(4))

			// Verify we got the expected messages
			mtx.Lock()
			messageCount := len(messages)
			messagesCopy := make([]*service.Message, len(messages))
			copy(messagesCopy, messages)
			mtx.Unlock()

			Expect(messageCount).To(Equal(4))

			// Verify message 1 (normal processing)
			msg1Out := messagesCopy[0]
			structured, err := msg1Out.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			data := structured.(map[string]interface{})
			Expect(data["value"]).To(Equal(10.0))

			downsampledBy, exists := msg1Out.MetaGet("downsampled_by")
			Expect(exists).To(BeTrue())
			Expect(downsampledBy).To(Equal("deadband"))

			// Verify message 3 (ignored)
			msg3Out := messagesCopy[1]
			structured, err = msg3Out.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			data = structured.(map[string]interface{})
			Expect(data["value"]).To(Equal(20.0))

			downsampledBy, exists = msg3Out.MetaGet("downsampled_by")
			Expect(exists).To(BeTrue())
			Expect(downsampledBy).To(Equal("ignored"))

			// Verify message 4 (ignored)
			msg4Out := messagesCopy[2]
			structured, err = msg4Out.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			data = structured.(map[string]interface{})
			Expect(data["value"]).To(Equal(20.01))

			downsampledBy, exists = msg4Out.MetaGet("downsampled_by")
			Expect(exists).To(BeTrue())
			Expect(downsampledBy).To(Equal("ignored"))

			// Verify message 5 (ignored non-time-series)
			msg5Out := messagesCopy[3]
			structured, err = msg5Out.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			data = structured.(map[string]interface{})
			Expect(data["some"]).To(Equal("data"))

			downsampledBy, exists = msg5Out.MetaGet("downsampled_by")
			Expect(exists).To(BeTrue())
			Expect(downsampledBy).To(Equal("ignored"))
		})
	})

	When("processing non-time-series messages", func() {
		It("should pass through non-UMH messages unchanged", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 1.0
      max_time: 30s
`)
			Expect(err).NotTo(HaveOccurred())

			var (
				messages []*service.Message
				mtx      sync.Mutex
			)
			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				mtx.Lock()
				messages = append(messages, msg)
				mtx.Unlock()
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

			// Send non-time-series message
			msg := service.NewMessage([]byte(`{"some": "data", "not": "timeseries"}`))
			err = msgHandler(ctx, msg)
			Expect(err).NotTo(HaveOccurred())

			// Wait for processing
			Eventually(func() int {
				mtx.Lock()
				n := len(messages)
				mtx.Unlock()
				return n
			}, "1s").Should(Equal(1))

			// Message should pass through unchanged
			mtx.Lock()
			messageCount := len(messages)
			firstMessage := messages[0]
			mtx.Unlock()

			Expect(messageCount).To(Equal(1))

			structured, err := firstMessage.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			data := structured.(map[string]interface{})
			Expect(data["some"]).To(Equal("data"))
			Expect(data["not"]).To(Equal("timeseries"))
		})
	})
})

// Helper function to create UMH time-series messages
func createTimeSeriesMessage(value float64, timestamp time.Time, topic string) *service.Message {
	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]interface{}{
		"value":        value,
		"timestamp_ms": timestamp.UnixNano() / int64(time.Millisecond),
	})

	if topic != "" {
		msg.MetaSet("umh_topic", topic)
	}

	return msg
}
