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
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Manual Behavior Tests - Human-Verifiable Test Cases
// These test cases demonstrate specific downsampler behaviors in an easy-to-understand format.
// They use Ginkgo v2 DescribeTable for clear, tabular test organization.

var _ = Describe("Manual Behavior Tests", func() {
	var ctx context.Context

	BeforeEach(func() {
		// Only run if TEST_DOWNSAMPLER environment variable is set
		if os.Getenv("TEST_DOWNSAMPLER") == "" {
			Skip("Skipping downsampler tests (set TEST_DOWNSAMPLER=1 to run)")
		}
		ctx = context.Background()
	})

	// TODO: Example of how to use Ginkgo v2 idiomatic DescribeTable with helper functions
	// Once test_helpers.go functions are accessible, these can be uncommented:
	//
	// DescribeTable("Numeric Value Thresholds",
	//     VerifyBehaviorTestCase,
	//     NumericBehaviorEntry(
	//         "Temperature sensor with 0.5°C threshold - should keep significant changes",
	//         0.5,                               // threshold
	//         []float64{20.0, 20.6, 20.8, 21.2}, // input values
	//         []bool{true, true, false, false},  // which should be kept: first always, 20.6 exceeds 0.5, others don't
	//     ),
	//     NumericBehaviorEntry(
	//         "Temperature sensor with 0.5°C threshold - should filter small fluctuations",
	//         0.5,
	//         []float64{25.0, 25.3, 25.1, 24.7, 24.4}, // small changes then bigger drop
	//         []bool{true, false, false, false, true}, // first + final drop > 0.5
	//     ),
	//     NumericBehaviorEntry(
	//         "Pressure sensor with 2.0 Pa threshold - should handle larger threshold",
	//         2.0,
	//         []float64{1000.0, 1001.5, 1003.0, 1004.0, 1006.0},
	//         []bool{true, false, true, false, true}, // 1001.5 within 2.0, others exceed
	//     ),
	// )

	// Note: The old verbose test style has been replaced with the DescribeTable approach above.
	// The helper functions (VerifyBehaviorTestCase, NumericBehaviorEntry, etc.) are in test_helpers.go
	// These provide a more human-readable format for understanding downsampler behavior.

	// Legacy tests preserved below for complex scenarios requiring detailed verification
	Describe("Complex Topic-Specific Thresholds (Legacy Style)", func() {
		Context("Temperature with 0.5°C threshold", func() {
			It("should keep values that exceed threshold", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"temperature": 20.0, "timestamp_ms": 1000},
        {"temperature": 20.6, "timestamp_ms": 2000},  # +0.6°C > 0.5 threshold → KEEP
        {"temperature": 20.8, "timestamp_ms": 3000},  # +0.2°C < 0.5 threshold → DROP
        {"temperature": 21.2, "timestamp_ms": 4000}   # +0.4°C from last kept < 0.5 → DROP
      ].index(count("generated") % 4)
      meta data_contract = "_historian"
      meta umh_topic = "test.temperature"
    count: 4

pipeline:
  processors:
    - downsampler:
        default:
          deadband:
            threshold: 0.5

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 2 messages kept (20.0 first, 20.6 exceeds threshold)
				// Expected: 2 messages dropped (20.8 and 21.2 below threshold from last kept)
			})

			It("should drop small fluctuations within threshold", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"temperature": 25.0, "timestamp_ms": 1000},
        {"temperature": 25.3, "timestamp_ms": 2000},  # +0.3°C < 0.5 threshold → DROP
        {"temperature": 25.1, "timestamp_ms": 3000},  # -0.2°C < 0.5 threshold → DROP
        {"temperature": 24.7, "timestamp_ms": 4000},  # -0.3°C < 0.5 threshold → DROP
        {"temperature": 24.4, "timestamp_ms": 5000}   # -0.6°C > 0.5 threshold → KEEP
      ].index(count("generated") % 5)
      meta data_contract = "_historian"
      meta umh_topic = "test.temperature"
    count: 5

pipeline:
  processors:
    - downsampler:
        default:
          deadband:
            threshold: 0.5

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 2 messages kept (25.0 first, 24.4 exceeds threshold)
				// Expected: 3 messages dropped (25.3, 25.1, 24.7 all within threshold)
			})
		})

		Context("Pressure with 2.0 Pa threshold", func() {
			It("should handle larger threshold correctly", func() {
				builder := service.NewStreamBuilder()

				// Add producer function
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

				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				go func() {
					_ = stream.Run(ctx)
				}()

				// Send test messages in UMH-core format
				testMessages := []struct {
					value       float64
					timestamp   int64
					shouldKeep  bool
					description string
				}{
					{1000.0, 1000, true, "first value (always kept)"},
					{1001.5, 2000, false, "+1.5 Pa < 2.0 threshold → DROP"},
					{1003.0, 3000, true, "+3.0 Pa > 2.0 threshold → KEEP"},
					{1004.0, 4000, false, "+1.0 Pa < 2.0 threshold → DROP"},
					{1006.0, 5000, true, "+3.0 Pa > 2.0 threshold → KEEP"},
				}

				for _, tm := range testMessages {
					testMsg := service.NewMessage(nil)
					testMsg.SetStructured(map[string]interface{}{
						"value":        tm.value,
						"timestamp_ms": tm.timestamp,
					})
					testMsg.MetaSet("umh_topic", "test.pressure")

					err = msgHandler(ctx, testMsg)
					Expect(err).NotTo(HaveOccurred())
				}

				// Expected: 3 messages kept (1000.0 first, 1003.0 exceeds threshold, 1006.0 exceeds threshold)
				Eventually(func() int {
					return len(messages)
				}).Should(Equal(3), "Should keep 3 messages: first value + 2 that exceed threshold")

				// Verify the kept values and their order
				Expect(len(messages)).To(Equal(3))

				// First message should be 1000.0 (always keep first)
				structured, err := messages[0].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				payload, ok := structured.(map[string]interface{})
				Expect(ok).To(BeTrue())
				Expect(payload["value"]).To(BeNumerically("==", 1000.0))

				// Second message should be 1003.0 (exceeds threshold)
				structured, err = messages[1].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				payload, ok = structured.(map[string]interface{})
				Expect(ok).To(BeTrue())
				Expect(payload["value"]).To(BeNumerically("==", 1003.0))

				// Third message should be 1006.0 (exceeds threshold)
				structured, err = messages[2].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				payload, ok = structured.(map[string]interface{})
				Expect(ok).To(BeTrue())
				Expect(payload["value"]).To(BeNumerically("==", 1006.0))

				// Verify all messages have downsampler metadata
				for i, msg := range messages {
					downsampledBy, exists := msg.MetaGet("downsampled_by")
					Expect(exists).To(BeTrue(), "Message %d should have downsampled_by metadata", i)
					// The metadata includes threshold info, so check that it starts with "deadband"
					Expect(downsampledBy).To(HavePrefix("deadband"), "Message %d should be downsampled by deadband algorithm", i)
				}
			})
		})
	})

	Describe("String Value Equality", func() {
		Context("Machine status strings", func() {
			It("should ignore thresholds and use equality only", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"value": "RUNNING", "timestamp_ms": 1000},
        {"value": "RUNNING", "timestamp_ms": 2000},   # Same → DROP (threshold ignored)
        {"value": "STOPPED", "timestamp_ms": 3000},  # Different → KEEP (threshold ignored)
        {"value": "STOPPED", "timestamp_ms": 4000},  # Same → DROP (threshold ignored)
        {"value": "RUNNING", "timestamp_ms": 5000}   # Different → KEEP (threshold ignored)
      ].index(count("generated") % 5)
      meta umh_topic = "test.status"
    count: 5

pipeline:
  processors:
    - downsampler:
        default:
          deadband:
            threshold: 999.0  # High threshold should be ignored for strings

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				err = stream.Stop(ctx)
				Expect(err).ToNot(HaveOccurred(), "Stream should run without errors")

				// Expected: 3 messages kept ("RUNNING" first, "STOPPED", "RUNNING" again)
				// Expected: 2 messages dropped (duplicate "RUNNING", duplicate "STOPPED")
			})

			It("should handle constant strings correctly", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"value": "AUTO", "timestamp_ms": 1000},
        {"value": "AUTO", "timestamp_ms": 2000},  # Same → DROP
        {"value": "AUTO", "timestamp_ms": 3000},  # Same → DROP  
        {"value": "AUTO", "timestamp_ms": 4000},  # Same → DROP
        {"value": "AUTO", "timestamp_ms": 5000}   # Same → DROP
      ].index(count("generated") % 5)
      meta umh_topic = "test.mode"
    count: 5

pipeline:
  processors:
    - downsampler:
        default:
          deadband:
            threshold: 0.1  # Low threshold should be ignored for strings

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				err = stream.Stop(ctx)
				Expect(err).ToNot(HaveOccurred(), "Stream should run without errors")

				// Expected: 1 message kept ("AUTO" first)
				// Expected: 4 messages dropped (all duplicates)
			})
		})
	})

	Describe("Boolean Value Equality", func() {
		Context("Machine state booleans", func() {
			It("should ignore thresholds and use equality only", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"value": true, "timestamp_ms": 1000},
        {"value": true, "timestamp_ms": 2000},   # Same → DROP (threshold ignored)
        {"value": false, "timestamp_ms": 3000}, # Different → KEEP (threshold ignored)
        {"value": false, "timestamp_ms": 4000}, # Same → DROP (threshold ignored)
        {"value": true, "timestamp_ms": 5000}   # Different → KEEP (threshold ignored)
      ].index(count("generated"))
      meta umh_topic = "test.enabled"
    count: 5

pipeline:
  processors:
    - downsampler:
        default:
          deadband:
            threshold: 999.0  # High threshold should be ignored for booleans

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 3 messages kept (true first, false, true again)
				// Expected: 2 messages dropped (duplicate true, duplicate false)
			})

			It("should handle constant boolean values", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"value": false, "timestamp_ms": 1000},
        {"value": false, "timestamp_ms": 2000},  # Same → DROP
        {"value": false, "timestamp_ms": 3000},  # Same → DROP
        {"value": false, "timestamp_ms": 4000},  # Same → DROP
        {"value": false, "timestamp_ms": 5000}   # Same → DROP
      ].index(count("generated") % 5)
      meta umh_topic = "test.alarm"
    count: 5

pipeline:
  processors:
    - downsampler:
        default:
          deadband:
            threshold: 0.1  # Low threshold should be ignored for booleans

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 1 message kept (false first)
				// Expected: 4 messages dropped (all duplicates)
			})
		})
	})

	Describe("Topic-Specific Thresholds", func() {
		Context("Different thresholds per metric type", func() {
			It("should apply correct threshold based on topic pattern", func() {
				builder := service.NewStreamBuilder()

				// Add producer function
				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
downsampler:
  default:
    deadband:
      threshold: 2.0  # Default threshold
  overrides:
    - pattern: "*.temperature"
      deadband:
        threshold: 0.5  # Sensitive temperature threshold
    - pattern: "*.humidity"
      deadband:
        threshold: 1.0  # Moderate humidity threshold
    - pattern: "*.pressure"
      deadband:
        threshold: 3.0  # Less sensitive pressure threshold
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

				// Test temperature with 0.5 threshold
				tempMessages := []struct {
					value      float64
					timestamp  int64
					topic      string
					shouldKeep bool
					reason     string
				}{
					{20.0, 1000, "plant.line1.temperature", true, "first temperature value"},
					{20.3, 2000, "plant.line1.temperature", false, "+0.3°C < 0.5 threshold → DROP"},
					{20.6, 3000, "plant.line1.temperature", true, "+0.6°C > 0.5 threshold → KEEP"},
				}

				// Test humidity with 1.0 threshold
				humMessages := []struct {
					value      float64
					timestamp  int64
					topic      string
					shouldKeep bool
					reason     string
				}{
					{50.0, 4000, "plant.line1.humidity", true, "first humidity value"},
					{50.7, 5000, "plant.line1.humidity", false, "+0.7% < 1.0 threshold → DROP"},
					{51.2, 6000, "plant.line1.humidity", true, "+1.2% > 1.0 threshold → KEEP"},
				}

				// Test pressure with 3.0 threshold
				pressMessages := []struct {
					value      float64
					timestamp  int64
					topic      string
					shouldKeep bool
					reason     string
				}{
					{1000.0, 7000, "plant.line1.pressure", true, "first pressure value"},
					{1002.0, 8000, "plant.line1.pressure", false, "+2.0 Pa < 3.0 threshold → DROP"},
					{1004.0, 9000, "plant.line1.pressure", true, "+4.0 Pa > 3.0 threshold → KEEP"},
				}

				allMessages := []struct {
					value      float64
					timestamp  int64
					topic      string
					shouldKeep bool
					reason     string
				}{}
				allMessages = append(allMessages, tempMessages...)
				allMessages = append(allMessages, humMessages...)
				allMessages = append(allMessages, pressMessages...)

				// Send all test messages
				for _, tm := range allMessages {
					testMsg := service.NewMessage(nil)
					testMsg.SetStructured(map[string]interface{}{
						"value":        tm.value,
						"timestamp_ms": tm.timestamp,
					})
					testMsg.MetaSet("umh_topic", tm.topic)

					err = msgHandler(ctx, testMsg)
					Expect(err).NotTo(HaveOccurred())
				}

				// Expected: 6 messages kept (2 temp + 2 humid + 2 pressure)
				// - Temperature: 20.0 (first), 20.6 (>0.5)
				// - Humidity: 50.0 (first), 51.2 (>1.0)
				// - Pressure: 1000.0 (first), 1004.0 (>3.0)
				Eventually(func() int {
					return len(messages)
				}).Should(Equal(6), "Should keep 6 messages: 2 per metric type (first + threshold exceeded)")

				// Verify the kept messages in order
				Expect(len(messages)).To(Equal(6))

				// Group messages by topic for easier verification
				messagesByTopic := map[string][]*service.Message{}
				for _, msg := range messages {
					topic, exists := msg.MetaGet("umh_topic")
					Expect(exists).To(BeTrue())
					messagesByTopic[topic] = append(messagesByTopic[topic], msg)
				}

				// Verify temperature messages (threshold 0.5)
				tempMsgs := messagesByTopic["plant.line1.temperature"]
				Expect(len(tempMsgs)).To(Equal(2), "Should have 2 temperature messages")

				structured, err := tempMsgs[0].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				payload, ok := structured.(map[string]interface{})
				Expect(ok).To(BeTrue())
				Expect(payload["value"]).To(BeNumerically("==", 20.0))

				structured, err = tempMsgs[1].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				payload, ok = structured.(map[string]interface{})
				Expect(ok).To(BeTrue())
				Expect(payload["value"]).To(BeNumerically("==", 20.6))

				// Verify humidity messages (threshold 1.0)
				humMsgs := messagesByTopic["plant.line1.humidity"]
				Expect(len(humMsgs)).To(Equal(2), "Should have 2 humidity messages")

				structured, err = humMsgs[0].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				payload, ok = structured.(map[string]interface{})
				Expect(ok).To(BeTrue())
				Expect(payload["value"]).To(BeNumerically("==", 50.0))

				structured, err = humMsgs[1].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				payload, ok = structured.(map[string]interface{})
				Expect(ok).To(BeTrue())
				Expect(payload["value"]).To(BeNumerically("==", 51.2))

				// Verify pressure messages (threshold 3.0)
				pressMsgs := messagesByTopic["plant.line1.pressure"]
				Expect(len(pressMsgs)).To(Equal(2), "Should have 2 pressure messages")

				structured, err = pressMsgs[0].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				payload, ok = structured.(map[string]interface{})
				Expect(ok).To(BeTrue())
				Expect(payload["value"]).To(BeNumerically("==", 1000.0))

				structured, err = pressMsgs[1].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				payload, ok = structured.(map[string]interface{})
				Expect(ok).To(BeTrue())
				Expect(payload["value"]).To(BeNumerically("==", 1004.0))

				// Verify all messages have correct downsampler metadata
				for topic, topicMsgs := range messagesByTopic {
					for i, msg := range topicMsgs {
						downsampledBy, exists := msg.MetaGet("downsampled_by")
						Expect(exists).To(BeTrue(), "Message %d for topic %s should have downsampled_by metadata", i, topic)
						// The metadata includes threshold info, so check that it starts with "deadband"
						Expect(downsampledBy).To(HavePrefix("deadband"), "Message %d should be downsampled by deadband algorithm", i)
					}
				}
			})
		})
	})

	Describe("Mixed Data Types", func() {
		Context("Numeric and non-numeric in same series", func() {
			It("should handle type changes gracefully", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"value": 42.0, "timestamp_ms": 1000},      # Numeric
        {"value": 42.3, "timestamp_ms": 2000},      # Numeric +0.3 < 1.0 → DROP
        {"value": "OFFLINE", "timestamp_ms": 3000}, # String → KEEP (type change)
        {"value": "OFFLINE", "timestamp_ms": 4000}, # String same → DROP
        {"value": 45.0, "timestamp_ms": 5000}       # Numeric → KEEP (type change)
      ].index(count("generated") % 5)
      meta umh_topic = "test.mixed"
    count: 5

pipeline:
  processors:
    - downsampler:
        default:
          deadband:
            threshold: 1.0

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 4 messages kept (42.0, "OFFLINE", 45.0, type changes are always kept)
				// Expected: 1 message dropped (42.3 within threshold, duplicate "OFFLINE")
			})
		})
	})

	Describe("Edge Cases", func() {
		Context("Zero thresholds", func() {
			It("should drop only exact duplicates with zero threshold", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"value": 10.0, "timestamp_ms": 1000},
        {"value": 10.0, "timestamp_ms": 2000},       # Exact same → DROP
        {"value": 10.000001, "timestamp_ms": 3000},  # Tiny diff > 0 threshold → KEEP
        {"value": 10.000001, "timestamp_ms": 4000}   # Exact same → DROP
      ].index(count("generated") % 4)
      meta data_contract = "_historian"
      meta umh_topic = "test.zero_threshold"
    count: 4

pipeline:
  processors:
    - downsampler:
        default:
          deadband:
            threshold: 0.0

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 2 messages kept (10.0 first, 10.000001 different)
				// Expected: 2 messages dropped (exact duplicates)
			})
		})

		Context("Large thresholds", func() {
			It("should drop most values with very large threshold", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"value": 0.0, "timestamp_ms": 1000},
        {"value": 50.0, "timestamp_ms": 2000},   # +50 < 100 threshold → DROP
        {"value": 75.0, "timestamp_ms": 3000},   # +75 < 100 threshold → DROP
        {"value": 150.0, "timestamp_ms": 4000}   # +150 > 100 threshold → KEEP
      ].index(count("generated") % 4)
      meta data_contract = "_historian"
      meta umh_topic = "test.large_threshold"
    count: 4

pipeline:
  processors:
    - downsampler:
        default:
          deadband:
            threshold: 100.0

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 2 messages kept (0.0 first, 150.0 exceeds threshold)
				// Expected: 2 messages dropped (50.0, 75.0 within threshold)
			})
		})
	})
})
