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

package stream_processor_plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Integration Tests - Message Processing", func() {
	var (
		processor  *StreamProcessor
		resources  *service.Resources
		testConfig StreamProcessorConfig
		ctx        context.Context
		cancel     context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())

		// Create test resources
		resources = service.MockResources()

		// Create test configuration
		testConfig = StreamProcessorConfig{
			Mode:        "timeseries",
			OutputTopic: "umh.v1.corpA.plant-A.aawd",
			Model: ModelConfig{
				Name:    "pump",
				Version: "v1",
			},
			Sources: map[string]string{
				"press": "umh.v1.corpA.plant-A.aawd._raw.press",
				"tF":    "umh.v1.corpA.plant-A.aawd._raw.tempF",
				"run":   "umh.v1.corpA.plant-A.aawd._raw.run",
			},
			Mapping: map[string]interface{}{
				"pressure":    "press + 4.00001",
				"temperature": "tF * 9 / 5 + 32",
				"motor": map[string]interface{}{
					"rpm": "press / 4",
				},
				"serialNumber": `"SN-P42-008"`,
				"status":       `run ? "active" : "inactive"`,
			},
		}

		// Create processor
		var err error
		processor, err = newStreamProcessor(testConfig, resources.Logger(), resources.Metrics())
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		if processor != nil {
			processor.Close(ctx)
		}
		cancel()
	})

	Describe("End-to-End Message Processing", func() {
		Context("when processing a valid timeseries message", func() {
			It("should process press variable and generate outputs", func() {
				// Create input message
				inputPayload := TimeseriesMessage{
					Value:       25.5,
					TimestampMs: time.Now().UnixMilli(),
				}
				payloadBytes, err := json.Marshal(inputPayload)
				Expect(err).ToNot(HaveOccurred())

				inputMsg := service.NewMessage(payloadBytes)
				inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

				// Process the message
				batches, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
				Expect(err).ToNot(HaveOccurred())
				Expect(batches).ToNot(BeEmpty())

				outputBatch := batches[0]
				Expect(len(outputBatch)).To(BeNumerically(">=", 2)) // At least static mappings + pressure

				// Check that we have expected outputs
				topics := make(map[string]interface{})
				for _, msg := range outputBatch {
					topic, exists := msg.MetaGet("umh_topic")
					Expect(exists).To(BeTrue())

					var payload TimeseriesMessage
					payloadBytes, err := msg.AsBytes()
					Expect(err).ToNot(HaveOccurred())
					err = json.Unmarshal(payloadBytes, &payload)
					Expect(err).ToNot(HaveOccurred())

					topics[topic] = payload.Value
				}

				// Verify static mappings are present
				Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"))
				Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"]).To(Equal("SN-P42-008"))

				// Verify dynamic mappings are present
				Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.pressure"))
				Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.pressure"]).To(BeNumerically("~", 29.50001, 0.001))

				Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.motor.rpm"))
				Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.motor.rpm"]).To(BeNumerically("~", 6.375, 0.001))
			})

			It("should handle multiple variable updates correctly", func() {
				// First, send press value
				pressPayload := TimeseriesMessage{
					Value:       20.0,
					TimestampMs: time.Now().UnixMilli(),
				}
				pressBytes, err := json.Marshal(pressPayload)
				Expect(err).ToNot(HaveOccurred())

				pressMsg := service.NewMessage(pressBytes)
				pressMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

				// Then send temperature value
				tempPayload := TimeseriesMessage{
					Value:       25.0,
					TimestampMs: time.Now().UnixMilli(),
				}
				tempBytes, err := json.Marshal(tempPayload)
				Expect(err).ToNot(HaveOccurred())

				tempMsg := service.NewMessage(tempBytes)
				tempMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.tempF")

				// Finally send run status
				runPayload := TimeseriesMessage{
					Value:       true,
					TimestampMs: time.Now().UnixMilli(),
				}
				runBytes, err := json.Marshal(runPayload)
				Expect(err).ToNot(HaveOccurred())

				runMsg := service.NewMessage(runBytes)
				runMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.run")

				// Process all messages
				batches1, err := processor.ProcessBatch(ctx, service.MessageBatch{pressMsg})
				Expect(err).ToNot(HaveOccurred())

				batches2, err := processor.ProcessBatch(ctx, service.MessageBatch{tempMsg})
				Expect(err).ToNot(HaveOccurred())

				batches3, err := processor.ProcessBatch(ctx, service.MessageBatch{runMsg})
				Expect(err).ToNot(HaveOccurred())

				// Verify all batches contain outputs
				Expect(batches1).ToNot(BeEmpty())
				Expect(batches2).ToNot(BeEmpty())
				Expect(batches3).ToNot(BeEmpty())

				// Check the final batch for status mapping
				finalBatch := batches3[0]
				statusFound := false
				for _, msg := range finalBatch {
					topic, exists := msg.MetaGet("umh_topic")
					if exists && topic == "umh.v1.corpA.plant-A.aawd._pump_v1.status" {
						var payload TimeseriesMessage
						payloadBytes, err := msg.AsBytes()
						Expect(err).ToNot(HaveOccurred())
						err = json.Unmarshal(payloadBytes, &payload)
						Expect(err).ToNot(HaveOccurred())

						Expect(payload.Value).To(Equal("active"))
						statusFound = true
						break
					}
				}
				Expect(statusFound).To(BeTrue())
			})
		})

		Context("when processing invalid messages", func() {
			It("should skip messages without umh_topic metadata", func() {
				inputPayload := TimeseriesMessage{
					Value:       25.5,
					TimestampMs: time.Now().UnixMilli(),
				}
				payloadBytes, err := json.Marshal(inputPayload)
				Expect(err).ToNot(HaveOccurred())

				inputMsg := service.NewMessage(payloadBytes)
				// No umh_topic metadata set

				batches, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
				Expect(err).ToNot(HaveOccurred())
				Expect(batches).To(BeEmpty())
			})

			It("should skip messages with invalid JSON payload", func() {
				inputMsg := service.NewMessage([]byte("invalid json"))
				inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

				batches, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
				Expect(err).ToNot(HaveOccurred())
				Expect(batches).To(BeEmpty())
			})

			It("should skip messages with missing timestamp_ms", func() {
				invalidPayload := map[string]interface{}{
					"value": 25.5,
					// Missing timestamp_ms
				}
				payloadBytes, err := json.Marshal(invalidPayload)
				Expect(err).ToNot(HaveOccurred())

				inputMsg := service.NewMessage(payloadBytes)
				inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

				batches, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
				Expect(err).ToNot(HaveOccurred())
				Expect(batches).To(BeEmpty())
			})

			It("should skip messages with missing value", func() {
				invalidPayload := map[string]interface{}{
					"timestamp_ms": time.Now().UnixMilli(),
					// Missing value
				}
				payloadBytes, err := json.Marshal(invalidPayload)
				Expect(err).ToNot(HaveOccurred())

				inputMsg := service.NewMessage(payloadBytes)
				inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

				batches, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
				Expect(err).ToNot(HaveOccurred())
				Expect(batches).To(BeEmpty())
			})

			It("should skip messages with unrecognized topics", func() {
				inputPayload := TimeseriesMessage{
					Value:       25.5,
					TimestampMs: time.Now().UnixMilli(),
				}
				payloadBytes, err := json.Marshal(inputPayload)
				Expect(err).ToNot(HaveOccurred())

				inputMsg := service.NewMessage(payloadBytes)
				inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.unknown")

				batches, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
				Expect(err).ToNot(HaveOccurred())
				Expect(batches).To(BeEmpty())
			})
		})
	})

	Describe("JavaScript Expression Evaluation", func() {
		Context("when evaluating static expressions", func() {
			It("should handle string constants", func() {
				inputPayload := TimeseriesMessage{
					Value:       25.5,
					TimestampMs: time.Now().UnixMilli(),
				}
				payloadBytes, err := json.Marshal(inputPayload)
				Expect(err).ToNot(HaveOccurred())

				inputMsg := service.NewMessage(payloadBytes)
				inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

				batches, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
				Expect(err).ToNot(HaveOccurred())
				Expect(batches).ToNot(BeEmpty())

				// Find the serial number output
				found := false
				for _, msg := range batches[0] {
					if topic, exists := msg.MetaGet("umh_topic"); exists {
						if topic == "umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber" {
							var payload TimeseriesMessage
							payloadBytes, err := msg.AsBytes()
							Expect(err).ToNot(HaveOccurred())
							err = json.Unmarshal(payloadBytes, &payload)
							Expect(err).ToNot(HaveOccurred())

							Expect(payload.Value).To(Equal("SN-P42-008"))
							found = true
							break
						}
					}
				}
				Expect(found).To(BeTrue())
			})

			It("should handle complex mathematical expressions", func() {
				// Test Date.now() or Math operations by creating a config with them
				complexConfig := testConfig
				complexConfig.Mapping = map[string]interface{}{
					"mathTest":  "Math.PI * 2",
					"constTest": "42",
				}

				complexProcessor, err := newStreamProcessor(complexConfig, resources.Logger(), resources.Metrics())
				Expect(err).ToNot(HaveOccurred())
				defer complexProcessor.Close(ctx)

				inputPayload := TimeseriesMessage{
					Value:       25.5,
					TimestampMs: time.Now().UnixMilli(),
				}
				payloadBytes, err := json.Marshal(inputPayload)
				Expect(err).ToNot(HaveOccurred())

				inputMsg := service.NewMessage(payloadBytes)
				inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

				batches, err := complexProcessor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
				Expect(err).ToNot(HaveOccurred())
				Expect(batches).ToNot(BeEmpty())

				// Check outputs
				topics := make(map[string]interface{})
				for _, msg := range batches[0] {
					topic, exists := msg.MetaGet("umh_topic")
					Expect(exists).To(BeTrue())

					var payload TimeseriesMessage
					payloadBytes, err := msg.AsBytes()
					Expect(err).ToNot(HaveOccurred())
					err = json.Unmarshal(payloadBytes, &payload)
					Expect(err).ToNot(HaveOccurred())

					topics[topic] = payload.Value
				}

				Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.mathTest"))
				Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.mathTest"]).To(BeNumerically("~", 6.283185, 0.001))

				Expect(topics).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.constTest"))
				Expect(topics["umh.v1.corpA.plant-A.aawd._pump_v1.constTest"]).To(Equal(42.0))
			})
		})

		Context("when evaluating dynamic expressions", func() {
			It("should handle conditional expressions", func() {
				// Send run=true
				runPayload := TimeseriesMessage{
					Value:       true,
					TimestampMs: time.Now().UnixMilli(),
				}
				runBytes, err := json.Marshal(runPayload)
				Expect(err).ToNot(HaveOccurred())

				runMsg := service.NewMessage(runBytes)
				runMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.run")

				batches, err := processor.ProcessBatch(ctx, service.MessageBatch{runMsg})
				Expect(err).ToNot(HaveOccurred())
				Expect(batches).ToNot(BeEmpty())

				// Find the status output
				found := false
				for _, msg := range batches[0] {
					if topic, exists := msg.MetaGet("umh_topic"); exists {
						if topic == "umh.v1.corpA.plant-A.aawd._pump_v1.status" {
							var payload TimeseriesMessage
							payloadBytes, err := msg.AsBytes()
							Expect(err).ToNot(HaveOccurred())
							err = json.Unmarshal(payloadBytes, &payload)
							Expect(err).ToNot(HaveOccurred())

							Expect(payload.Value).To(Equal("active"))
							found = true
							break
						}
					}
				}
				Expect(found).To(BeTrue())
			})

			It("should skip mappings when dependencies are missing", func() {
				// Create config with a mapping that depends on multiple variables
				dependentConfig := testConfig
				dependentConfig.Mapping = map[string]interface{}{
					"efficiency": "press / tF * 100", // Depends on both press and tF
				}

				dependentProcessor, err := newStreamProcessor(dependentConfig, resources.Logger(), resources.Metrics())
				Expect(err).ToNot(HaveOccurred())
				defer dependentProcessor.Close(ctx)

				// Send only press, not tF
				pressPayload := TimeseriesMessage{
					Value:       20.0,
					TimestampMs: time.Now().UnixMilli(),
				}
				pressBytes, err := json.Marshal(pressPayload)
				Expect(err).ToNot(HaveOccurred())

				pressMsg := service.NewMessage(pressBytes)
				pressMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

				batches, err := dependentProcessor.ProcessBatch(ctx, service.MessageBatch{pressMsg})
				Expect(err).ToNot(HaveOccurred())

				// Should not have efficiency output yet
				if len(batches) > 0 {
					for _, msg := range batches[0] {
						if topic, exists := msg.MetaGet("umh_topic"); exists {
							Expect(topic).ToNot(Equal("umh.v1.corpA.plant-A.aawd._pump_v1.efficiency"))
						}
					}
				}
			})
		})
	})

	Describe("Output Topic Construction", func() {
		It("should construct proper output topics", func() {
			inputPayload := TimeseriesMessage{
				Value:       25.5,
				TimestampMs: time.Now().UnixMilli(),
			}
			payloadBytes, err := json.Marshal(inputPayload)
			Expect(err).ToNot(HaveOccurred())

			inputMsg := service.NewMessage(payloadBytes)
			inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			batches, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).ToNot(BeEmpty())

			// Verify topic format: <output_topic>.<data_contract>.<virtual_path>
			expectedTopics := map[string]bool{
				"umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber": false,
				"umh.v1.corpA.plant-A.aawd._pump_v1.pressure":     false,
				"umh.v1.corpA.plant-A.aawd._pump_v1.motor.rpm":    false,
			}

			for _, msg := range batches[0] {
				if topic, exists := msg.MetaGet("umh_topic"); exists {
					if _, expected := expectedTopics[topic]; expected {
						expectedTopics[topic] = true
					}
				}
			}

			// Check that all expected topics were found
			for topic, found := range expectedTopics {
				Expect(found).To(BeTrue(), fmt.Sprintf("Expected topic %s not found", topic))
			}
		})

		It("should preserve timestamp from input message", func() {
			originalTimestamp := time.Now().UnixMilli()
			inputPayload := TimeseriesMessage{
				Value:       25.5,
				TimestampMs: originalTimestamp,
			}
			payloadBytes, err := json.Marshal(inputPayload)
			Expect(err).ToNot(HaveOccurred())

			inputMsg := service.NewMessage(payloadBytes)
			inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			batches, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).ToNot(BeEmpty())

			// Check that all output messages have the same timestamp
			for _, msg := range batches[0] {
				var payload TimeseriesMessage
				payloadBytes, err := msg.AsBytes()
				Expect(err).ToNot(HaveOccurred())
				err = json.Unmarshal(payloadBytes, &payload)
				Expect(err).ToNot(HaveOccurred())

				Expect(payload.TimestampMs).To(Equal(originalTimestamp))
			}
		})

		It("should preserve metadata from input message", func() {
			inputPayload := TimeseriesMessage{
				Value:       25.5,
				TimestampMs: time.Now().UnixMilli(),
			}
			payloadBytes, err := json.Marshal(inputPayload)
			Expect(err).ToNot(HaveOccurred())

			inputMsg := service.NewMessage(payloadBytes)
			inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")
			inputMsg.MetaSet("correlation_id", "test-correlation-123")
			inputMsg.MetaSet("source_system", "test-system")
			inputMsg.MetaSet("priority", "high")

			batches, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).ToNot(BeEmpty())

			// Check that all output messages preserve the original metadata
			for _, msg := range batches[0] {
				// umh_topic should be different (overridden)
				topic, exists := msg.MetaGet("umh_topic")
				Expect(exists).To(BeTrue())
				Expect(topic).ToNot(Equal("umh.v1.corpA.plant-A.aawd._raw.press"))

				// Other metadata should be preserved
				correlationId, exists := msg.MetaGet("correlation_id")
				Expect(exists).To(BeTrue())
				Expect(correlationId).To(Equal("test-correlation-123"))

				sourceSystem, exists := msg.MetaGet("source_system")
				Expect(exists).To(BeTrue())
				Expect(sourceSystem).To(Equal("test-system"))

				priority, exists := msg.MetaGet("priority")
				Expect(exists).To(BeTrue())
				Expect(priority).To(Equal("high"))
			}
		})
	})

	Describe("Error Handling", func() {
		It("should handle JavaScript execution errors gracefully", func() {
			// Create config with invalid JavaScript
			errorConfig := testConfig
			errorConfig.Mapping = map[string]interface{}{
				"validMapping":   "press + 1",
				"invalidMapping": "nonexistent_var + 1", // This should fail
			}

			errorProcessor, err := newStreamProcessor(errorConfig, resources.Logger(), resources.Metrics())
			Expect(err).ToNot(HaveOccurred())
			defer errorProcessor.Close(ctx)

			inputPayload := TimeseriesMessage{
				Value:       25.5,
				TimestampMs: time.Now().UnixMilli(),
			}
			payloadBytes, err := json.Marshal(inputPayload)
			Expect(err).ToNot(HaveOccurred())

			inputMsg := service.NewMessage(payloadBytes)
			inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			// Should not fail completely, but should skip the invalid mapping
			batches, err := errorProcessor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).ToNot(BeEmpty())

			// Should have the valid mapping output
			validFound := false
			invalidFound := false
			for _, msg := range batches[0] {
				if topic, exists := msg.MetaGet("umh_topic"); exists {
					if topic == "umh.v1.corpA.plant-A.aawd._pump_v1.validMapping" {
						validFound = true
					}
					if topic == "umh.v1.corpA.plant-A.aawd._pump_v1.invalidMapping" {
						invalidFound = true
					}
				}
			}

			Expect(validFound).To(BeTrue())
			Expect(invalidFound).To(BeFalse())
		})

		It("should continue processing when individual messages fail", func() {
			// Create a batch with one valid and one invalid message
			validPayload := TimeseriesMessage{
				Value:       25.5,
				TimestampMs: time.Now().UnixMilli(),
			}
			validBytes, err := json.Marshal(validPayload)
			Expect(err).ToNot(HaveOccurred())

			validMsg := service.NewMessage(validBytes)
			validMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			invalidMsg := service.NewMessage([]byte("invalid json"))
			invalidMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			batch := service.MessageBatch{validMsg, invalidMsg}

			batches, err := processor.ProcessBatch(ctx, batch)
			Expect(err).ToNot(HaveOccurred())

			// Should have outputs from the valid message
			Expect(batches).ToNot(BeEmpty())
			Expect(len(batches[0])).To(BeNumerically(">", 0))
		})
	})
})
