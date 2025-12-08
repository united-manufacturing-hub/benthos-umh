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

package processor_test

import (
	"context"
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/processor"
)

// Processing Flow Test Suite
// Tests that systematically cover all routes in the processing_flow.mmd diagram
var _ = Describe("Processing Flow Coverage", func() {
	var (
		streamProcessor *processor.StreamProcessor
		resources       *service.Resources
		ctx             context.Context
		cancel          context.CancelFunc
		baseConfig      config.StreamProcessorConfig
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		resources = service.MockResources()

		// Base configuration for all tests
		baseConfig = config.StreamProcessorConfig{
			Mode:        "timeseries",
			OutputTopic: "umh.v1.corpA.plant-A.aawd",
			Model: config.ModelConfig{
				Name:    "pump",
				Version: "v1",
			},
			Sources: map[string]string{
				"press": "umh.v1.corpA.plant-A.aawd._raw.press",
				"temp":  "umh.v1.corpA.plant-A.aawd._raw.temp",
				"flow":  "umh.v1.corpA.plant-A.aawd._raw.flow",
			},
			Mapping: map[string]interface{}{
				// Static mappings (no source dependencies)
				"serialNumber": `"SN-P42-008"`,
				"deviceType":   `"pump"`,
				"version":      "42",
				"timestamp":    "Date.now()",

				// Dynamic mappings (depends on single variable)
				"pressure":    "press + 4.0",
				"temperature": "temp * 1.8 + 32",
				"flowRate":    "flow / 60",

				// Dynamic mappings (depends on multiple variables)
				"efficiency": "press * flow / 100",
				"powerRatio": "press / temp",
				"allThree":   "press + temp + flow",

				// Nested mappings
				"motor": map[string]interface{}{
					"rpm":    "press * 100",
					"status": `temp > 50 ? "hot" : "normal"`,
				},

				// Conditional mappings
				"alert": `press > 100 ? "HIGH_PRESSURE" : "NORMAL"`,
			},
		}
	})

	AfterEach(func() {
		if streamProcessor != nil {
			err := streamProcessor.Close(ctx)
			Expect(err).ToNot(HaveOccurred())
		}
		cancel()
	})

	// Helper function to create a valid timeseries message
	createValidMessage := func(topic string, value interface{}) *service.Message {
		payload := processor.TimeseriesMessage{
			Value:       value,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, err := json.Marshal(payload)
		Expect(err).ToNot(HaveOccurred())

		msg := service.NewMessage(payloadBytes)
		msg.MetaSet("umh_topic", topic)
		return msg
	}

	// Helper function to extract topics from output batch
	extractTopics := func(batch service.MessageBatch) map[string]interface{} {
		topics := make(map[string]interface{})
		for _, msg := range batch {
			topic, exists := msg.MetaGet("umh_topic")
			if !exists {
				continue
			}

			var payload processor.TimeseriesMessage
			payloadBytes, err := msg.AsBytes()
			Expect(err).ToNot(HaveOccurred())
			err = json.Unmarshal(payloadBytes, &payload)
			Expect(err).ToNot(HaveOccurred())

			topics[topic] = payload.Value
		}
		return topics
	}

	Describe("Flowchart Route 1: Missing umh_topic Metadata", func() {
		It("should skip message and return empty batch when umh_topic is missing", func() {
			// Route: Input Message → No umh_topic metadata → Skip Message → Return empty batch

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			// Create message without umh_topic metadata
			payload := processor.TimeseriesMessage{
				Value:       25.5,
				TimestampMs: time.Now().UnixMilli(),
			}
			payloadBytes, err := json.Marshal(payload)
			Expect(err).ToNot(HaveOccurred())

			msg := service.NewMessage(payloadBytes)
			// Intentionally NOT setting umh_topic metadata

			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).To(BeEmpty(), "Should return empty batch when umh_topic is missing")
		})
	})

	Describe("Flowchart Route 2: Invalid Timeseries Format", func() {
		It("should skip message with invalid JSON", func() {
			// Route: Input Message → Has umh_topic → Invalid timeseries format → Skip Message → Return empty batch

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			msg := service.NewMessage([]byte("invalid json"))
			msg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).To(BeEmpty(), "Should return empty batch for invalid JSON")
		})

		It("should skip message missing value field", func() {
			// Route: Input Message → Has umh_topic → Invalid timeseries format → Skip Message → Return empty batch

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			invalidPayload := map[string]interface{}{
				"timestamp_ms": time.Now().UnixMilli(),
				// Missing "value" field
			}
			payloadBytes, err := json.Marshal(invalidPayload)
			Expect(err).ToNot(HaveOccurred())

			msg := service.NewMessage(payloadBytes)
			msg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).To(BeEmpty(), "Should return empty batch for missing value field")
		})

		It("should skip message missing timestamp_ms field", func() {
			// Route: Input Message → Has umh_topic → Invalid timeseries format → Skip Message → Return empty batch

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			invalidPayload := map[string]interface{}{
				"value": 25.5,
				// Missing "timestamp_ms" field
			}
			payloadBytes, err := json.Marshal(invalidPayload)
			Expect(err).ToNot(HaveOccurred())

			msg := service.NewMessage(payloadBytes)
			msg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).To(BeEmpty(), "Should return empty batch for missing timestamp_ms field")
		})
	})

	Describe("Flowchart Route 3: Topic Doesn't Match Sources", func() {
		It("should skip message with unrecognized topic", func() {
			// Route: Input Message → Has umh_topic → Valid timeseries → Topic doesn't match sources → Skip Message → Return empty batch

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			msg := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.unknown", 25.5)

			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).To(BeEmpty(), "Should return empty batch for unrecognized topic")
		})

		It("should skip message with partially matching topic", func() {
			// Route: Input Message → Has umh_topic → Valid timeseries → Topic doesn't match sources → Skip Message → Return empty batch

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			msg := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.press.extra", 25.5)

			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).To(BeEmpty(), "Should return empty batch for partially matching topic")
		})
	})

	Describe("Flowchart Route 4: Static Mappings Only", func() {
		It("should process static mappings when no dynamic mappings depend on received variable", func() {
			// Route: Input Message → Valid → Topic matches → Extract variable → Store value →
			//        Find mappings → Add static mappings → No dependent mappings → Process static mappings only → Return batch

			// Create config with only static mappings
			staticOnlyConfig := baseConfig
			staticOnlyConfig.Mapping = map[string]interface{}{
				"serialNumber": `"SN-P42-008"`,
				"deviceType":   `"pump"`,
				"version":      "42",
				"timestamp":    "Date.now()",
			}

			var err error
			streamProcessor, err = processor.NewStreamProcessor(staticOnlyConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			msg := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.press", 25.5)

			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).ToNot(BeEmpty(), "Should return batch with static mappings")

			topics := extractTopics(batches[0])

			// Should have all static mappings
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"))
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.deviceType"))
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.version"))
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.timestamp"))

			// Verify values
			Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"]).To(Equal("SN-P42-008"))
			Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.deviceType"]).To(Equal("pump"))
			Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.version"]).To(Equal(42.0))
			Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.timestamp"]).To(BeNumerically(">", 0))
		})
	})

	Describe("Flowchart Route 5: Missing Dependencies", func() {
		It("should skip dynamic mappings when not all dependencies are available", func() {
			// Route: Input Message → Valid → Topic matches → Extract variable → Store value →
			//        Find mappings → Found dependent mappings → Missing dependencies → Skip mapping → Return batch

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			// Send only press (efficiency needs both press and flow)
			msg := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.press", 100.0)

			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).ToNot(BeEmpty(), "Should return batch with available mappings")

			topics := extractTopics(batches[0])

			// Should have static mappings
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"))

			// Should have single-variable mappings
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.pressure"))
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.motor.rpm"))
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.alert"))

			// Should NOT have multi-variable mappings (missing dependencies)
			Expect(topics).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.efficiency"))
			Expect(topics).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.powerRatio"))
			Expect(topics).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.allThree"))
		})

		It("should process mappings when some dependencies become available", func() {
			// Route: Multiple messages showing dependency resolution

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			// First message: press
			msg1 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.press", 50.0)
			batches1, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg1})
			Expect(err).ToNot(HaveOccurred())

			topics1 := extractTopics(batches1[0])
			Expect(topics1).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.pressure"))
			Expect(topics1).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.efficiency"))

			// Second message: flow (now efficiency should be available)
			msg2 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.flow", 120.0)
			batches2, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg2})
			Expect(err).ToNot(HaveOccurred())

			topics2 := extractTopics(batches2[0])
			Expect(topics2).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.flowRate"))
			Expect(topics2).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.efficiency"))

			// Verify efficiency calculation: press * flow / 100 = 50 * 120 / 100 = 60
			Expect(topics2["umh.v1.corpA.plant-A.aawd._pump_v1.efficiency"]).To(BeNumerically("~", 60.0, 0.01))
		})
	})

	Describe("Flowchart Route 6: JavaScript Execution Errors", func() {
		It("should continue processing when JavaScript execution fails", func() {
			// Route: Input Message → Valid → Topic matches → Extract variable → Store value →
			//        Find mappings → Found dependent mappings → All dependencies available →
			//        JS execution failed → Log error, continue → Return batch

			// Create config with intentionally failing JavaScript
			errorConfig := baseConfig
			errorConfig.Mapping = map[string]interface{}{
				"validMapping":   "press + 10",
				"errorMapping":   "press / 0",              // Division by zero
				"invalidMapping": "nonexistent_function()", // Undefined function
				"staticMapping":  `"this_works"`,
			}

			var err error
			streamProcessor, err = processor.NewStreamProcessor(errorConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			msg := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.press", 50.0)

			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).ToNot(BeEmpty(), "Should return batch even with JS errors")

			topics := extractTopics(batches[0])

			// Should have working mappings
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.validMapping"))
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.staticMapping"))

			// Should NOT have failing mappings
			Expect(topics).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.errorMapping"))
			Expect(topics).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.invalidMapping"))

			// Verify values
			Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.validMapping"]).To(Equal(60.0))
			Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.staticMapping"]).To(Equal("this_works"))
		})

		It("should handle JavaScript runtime errors gracefully", func() {
			// Test various JavaScript runtime errors that occur during execution, not configuration

			errorConfig := baseConfig
			errorConfig.Mapping = map[string]interface{}{
				"typeError":      "press.nonexistent.property",
				"referenceError": "undefined_var + 1",
				"validMapping":   "press * 2",
			}

			var err error
			streamProcessor, err = processor.NewStreamProcessor(errorConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			msg := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.press", 25.0)

			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).ToNot(BeEmpty())

			topics := extractTopics(batches[0])

			// Only the valid mapping should be present (runtime errors should be caught)
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.validMapping"))
			Expect(topics).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.typeError"))
			Expect(topics).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.referenceError"))

			Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.validMapping"]).To(Equal(50.0))
		})
	})

	Describe("Flowchart Route 7: Successful Processing", func() {
		It("should successfully process complete workflow", func() {
			// Route: Input Message → Valid → Topic matches → Extract variable → Store value →
			//        Find mappings → Found dependent mappings → All dependencies available →
			//        JS execution successful → Create output message → Return batch

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			// Send all variables to satisfy all dependencies
			msg1 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.press", 100.0)
			msg2 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.temp", 75.0)
			msg3 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.flow", 200.0)

			// Process messages sequentially
			_, err = streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg1})
			Expect(err).ToNot(HaveOccurred())

			_, err = streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg2})
			Expect(err).ToNot(HaveOccurred())

			batches3, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg3})
			Expect(err).ToNot(HaveOccurred())

			// Final batch should have all mappings
			topics := extractTopics(batches3[0])

			// Static mappings
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"))
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.deviceType"))
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.version"))

			// Single-variable mappings
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.flowRate"))

			// Multi-variable mappings that depend on flow
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.efficiency"))
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.allThree"))
			// powerRatio should NOT be generated because it depends on press/temp, not flow
			Expect(topics).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.powerRatio"))

			// Nested mappings that depend on flow
			// motor.rpm depends on press, not flow, so should NOT be generated
			Expect(topics).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.motor.rpm"))
			// motor.status depends on temp, not flow, so should NOT be generated
			Expect(topics).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.motor.status"))

			// Verify calculations for mappings that depend on flow
			Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.flowRate"]).To(BeNumerically("~", 200.0/60.0, 0.01))
			Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.efficiency"]).To(BeNumerically("~", 100.0*200.0/100.0, 0.01))
			Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.allThree"]).To(BeNumerically("~", 100.0+75.0+200.0, 0.01))
		})

		It("should only generate mappings that depend on the updated variable", func() {
			// Test that powerRatio is only generated when press or temp changes, not flow

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			// Send all variables first
			msg1 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.press", 100.0)
			msg2 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.temp", 75.0)
			msg3 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.flow", 200.0)

			// Process initial messages
			_, err = streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg1})
			Expect(err).ToNot(HaveOccurred())

			_, err = streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg2})
			Expect(err).ToNot(HaveOccurred())

			_, err = streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg3})
			Expect(err).ToNot(HaveOccurred())

			// Now update press - should generate powerRatio
			msg4 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.press", 120.0)
			batches4, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg4})
			Expect(err).ToNot(HaveOccurred())

			topics4 := extractTopics(batches4[0])

			// Should have static mappings
			Expect(topics4).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"))

			// Should have mappings that depend on press
			Expect(topics4).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.pressure"))
			Expect(topics4).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.powerRatio"))
			Expect(topics4).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.efficiency"))
			Expect(topics4).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.allThree"))
			Expect(topics4).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.motor.rpm"))
			Expect(topics4).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.alert"))

			// Should NOT have mappings that only depend on temp or flow
			Expect(topics4).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.temperature"))
			Expect(topics4).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.flowRate"))
			Expect(topics4).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.motor.status"))

			// Verify powerRatio calculation with new press value
			Expect(topics4["umh.v1.corpA.plant-A.aawd._pump_v1.powerRatio"]).To(BeNumerically("~", 120.0/75.0, 0.01))
		})

		It("should preserve message metadata and timestamps", func() {
			// Test that output messages preserve original metadata and timestamps

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			originalTimestamp := time.Now().UnixMilli()
			payload := processor.TimeseriesMessage{
				Value:       50.0,
				TimestampMs: originalTimestamp,
			}
			payloadBytes, err := json.Marshal(payload)
			Expect(err).ToNot(HaveOccurred())

			msg := service.NewMessage(payloadBytes)
			msg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")
			msg.MetaSet("correlation_id", "test-123")
			msg.MetaSet("source_system", "test-system")

			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).ToNot(BeEmpty())

			// Verify all output messages preserve metadata and timestamps
			for _, outputMsg := range batches[0] {
				// Check timestamp preservation
				var outputPayload processor.TimeseriesMessage
				payloadBytes, err := outputMsg.AsBytes()
				Expect(err).ToNot(HaveOccurred())
				err = json.Unmarshal(payloadBytes, &outputPayload)
				Expect(err).ToNot(HaveOccurred())

				Expect(outputPayload.TimestampMs).To(Equal(originalTimestamp))

				// Check metadata preservation
				correlationId, exists := outputMsg.MetaGet("correlation_id")
				Expect(exists).To(BeTrue())
				Expect(correlationId).To(Equal("test-123"))

				sourceSystem, exists := outputMsg.MetaGet("source_system")
				Expect(exists).To(BeTrue())
				Expect(sourceSystem).To(Equal("test-system"))

				// Check umh_topic is updated
				topic, exists := outputMsg.MetaGet("umh_topic")
				Expect(exists).To(BeTrue())
				Expect(topic).ToNot(Equal("umh.v1.corpA.plant-A.aawd._raw.press"))
				Expect(topic).To(ContainSubstring("umh.v1.corpA.plant-A.aawd._pump_v1."))
			}
		})
	})

	Describe("Edge Cases and Complex Scenarios", func() {
		It("should handle rapid successive messages correctly", func() {
			// Test that rapid processing doesn't cause race conditions

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			// Send rapid sequence of messages
			messages := []struct {
				topic string
				value float64
			}{
				{"umh.v1.corpA.plant-A.aawd._raw.press", 10.0},
				{"umh.v1.corpA.plant-A.aawd._raw.temp", 20.0},
				{"umh.v1.corpA.plant-A.aawd._raw.flow", 30.0},
				{"umh.v1.corpA.plant-A.aawd._raw.press", 15.0},
				{"umh.v1.corpA.plant-A.aawd._raw.temp", 25.0},
			}

			for _, msgData := range messages {
				msg := createValidMessage(msgData.topic, msgData.value)
				batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{msg})
				Expect(err).ToNot(HaveOccurred())
				Expect(batches).ToNot(BeEmpty())
			}

			// Final state should have latest values
			finalMsg := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.flow", 35.0)
			batches, err := streamProcessor.ProcessBatch(ctx, service.MessageBatch{finalMsg})
			Expect(err).ToNot(HaveOccurred())

			topics := extractTopics(batches[0])

			// Should reflect latest values: press=15, temp=25, flow=35
			Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.allThree"))
			Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.allThree"]).To(BeNumerically("~", 15.0+25.0+35.0, 0.01))
		})

		It("should handle batch processing correctly", func() {
			// Test processing multiple messages in a single batch

			var err error
			streamProcessor, err = processor.NewStreamProcessor(baseConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())

			// Create a batch with multiple messages
			msg1 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.press", 100.0)
			msg2 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.temp", 80.0)
			msg3 := createValidMessage("umh.v1.corpA.plant-A.aawd._raw.flow", 150.0)

			batch := service.MessageBatch{msg1, msg2, msg3}

			batches, err := streamProcessor.ProcessBatch(ctx, batch)
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).ToNot(BeEmpty())

			// Should have outputs from all messages
			allTopics := extractTopics(batches[0])

			// Should have outputs from all variables
			Expect(allTopics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.pressure"))
			Expect(allTopics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.temperature"))
			Expect(allTopics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.flowRate"))
			Expect(allTopics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.allThree"))
		})
	})
})
