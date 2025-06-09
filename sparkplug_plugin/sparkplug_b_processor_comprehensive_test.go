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

package sparkplug_plugin_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("Sparkplug B Processor - Comprehensive Tests", func() {
	var testData *SparkplugTestData

	BeforeEach(func() {
		testData = NewSparkplugTestData()
	})

	Describe("Configuration Options", func() {
		Context("drop_birth_messages option", func() {
			It("should drop BIRTH messages when enabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  drop_birth_messages: true
  data_messages_only: false
  auto_split_metrics: false
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Send NBIRTH message - should be dropped
				msg, err := CreateBenthosMessage(testData.NBirthPayload, testData.NBirthTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				// Wait a bit for processing
				time.Sleep(100 * time.Millisecond)

				// Should have no messages since BIRTH was dropped
				Expect(*messages).To(HaveLen(0))
			})

			It("should pass through BIRTH messages when disabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  drop_birth_messages: false
  data_messages_only: false
  auto_split_metrics: false
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Send NBIRTH message - should pass through
				msg, err := CreateBenthosMessage(testData.NBirthPayload, testData.NBirthTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(1))

				// Verify metadata
				ExpectMetadata((*messages)[0], map[string]string{
					"sparkplug_msg_type": "NBIRTH",
					"group_id":           "TestFactory",
					"edge_node_id":       "Line1",
				})
			})
		})

		Context("strict_topic_validation option", func() {
			It("should drop invalid topics when enabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  strict_topic_validation: true
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Send message with invalid topic
				msg, err := CreateBenthosMessage(testData.NDataPayload, "invalid/topic/format")
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				time.Sleep(100 * time.Millisecond)

				// Should have no messages due to strict validation
				Expect(*messages).To(HaveLen(0))
			})

			It("should pass through invalid topics when disabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  strict_topic_validation: false
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Send message with invalid topic
				msg, err := CreateBenthosMessage(testData.NDataPayload, "invalid/topic/format")
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(1))

				// Should pass through unchanged
				receivedMsg := (*messages)[0]
				topic, exists := receivedMsg.MetaGet("mqtt_topic")
				Expect(exists).To(BeTrue())
				Expect(topic).To(Equal("invalid/topic/format"))
			})
		})

		Context("auto_split_metrics option", func() {
			It("should split metrics when enabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  auto_split_metrics: true
  data_messages_only: false
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Send NDATA message with 2 metrics
				msg, err := CreateBenthosMessage(testData.NDataPayload, testData.NDataTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(2)) // Should split into 2 messages

				// Both messages should have tag_name metadata
				for _, msg := range *messages {
					_, exists := msg.MetaGet("tag_name")
					Expect(exists).To(BeTrue())
				}
			})

			It("should keep single message when disabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  auto_split_metrics: false
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Send NDATA message with 2 metrics
				msg, err := CreateBenthosMessage(testData.NDataPayload, testData.NDataTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(1)) // Should remain as single message

				// Verify it's the full payload
				structured, err := (*messages)[0].AsStructured()
				Expect(err).NotTo(HaveOccurred())

				payloadMap := structured.(map[string]interface{})
				metricsInterface, exists := payloadMap["metrics"]
				Expect(exists).To(BeTrue())

				metrics := metricsInterface.([]interface{})
				Expect(metrics).To(HaveLen(2)) // Original 2 metrics
			})
		})

		Context("data_messages_only option", func() {
			It("should only process DATA messages when enabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  data_messages_only: true
  auto_split_metrics: false
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Send NBIRTH message - should be dropped
				birthMsg, err := CreateBenthosMessage(testData.NBirthPayload, testData.NBirthTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, birthMsg)
				Expect(err).NotTo(HaveOccurred())

				// Send NDATA message - should be processed
				dataMsg, err := CreateBenthosMessage(testData.NDataPayload, testData.NDataTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, dataMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(1)) // Only DATA message processed

				ExpectMetadata((*messages)[0], map[string]string{
					"sparkplug_msg_type": "NDATA",
				})
			})

			It("should process all message types when disabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  data_messages_only: false
  auto_split_metrics: false
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Send both NBIRTH and NDATA messages
				birthMsg, err := CreateBenthosMessage(testData.NBirthPayload, testData.NBirthTopic)
				Expect(err).NotTo(HaveOccurred())

				dataMsg, err := CreateBenthosMessage(testData.NDataPayload, testData.NDataTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, birthMsg)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, dataMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(2)) // Both messages processed

				msgTypes := make([]string, 2)
				for i, msg := range *messages {
					msgType, exists := msg.MetaGet("sparkplug_msg_type")
					Expect(exists).To(BeTrue())
					msgTypes[i] = msgType
				}

				Expect(msgTypes).To(ContainElements("NBIRTH", "NDATA"))
			})
		})

		Context("auto_extract_values option", func() {
			It("should extract simple values when enabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  auto_extract_values: true
  auto_split_metrics: true
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				msg, err := CreateBenthosMessage(testData.NDataPayload, testData.NDataTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(2))

				// Check that values are extracted with quality metadata
				for _, msg := range *messages {
					structured, err := msg.AsStructured()
					Expect(err).NotTo(HaveOccurred())

					payloadMap := structured.(map[string]interface{})
					metricsInterface, exists := payloadMap["metrics"]
					Expect(exists).To(BeTrue())

					metrics := metricsInterface.([]interface{})
					Expect(metrics).To(HaveLen(2)) // Original metric + quality metric
				}
			})

			It("should preserve full metric structure when disabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  auto_extract_values: false
  auto_split_metrics: true
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				msg, err := CreateBenthosMessage(testData.NDataPayload, testData.NDataTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(2))

				// Check that full metric structure is preserved
				for _, msg := range *messages {
					structured, err := msg.AsStructured()
					Expect(err).NotTo(HaveOccurred())

					// Should have direct metric fields like alias, datatype, etc.
					metricMap := structured.(map[string]interface{})
					_, hasAlias := metricMap["alias"]
					_, hasDatatype := metricMap["datatype"]
					Expect(hasAlias || hasDatatype).To(BeTrue(), "Should preserve metric structure")
				}
			})
		})
	})

	Describe("Alias Resolution", func() {
		Context("BIRTH to DATA flow", func() {
			It("should cache aliases from BIRTH and resolve in DATA messages", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  drop_birth_messages: true
  data_messages_only: false
  auto_split_metrics: false
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// First send NBIRTH to cache aliases
				birthMsg, err := CreateBenthosMessage(testData.NBirthPayload, testData.NBirthTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, birthMsg)
				Expect(err).NotTo(HaveOccurred())

				// Then send NDATA with aliases - should be resolved
				dataMsg, err := CreateBenthosMessage(testData.NDataPayload, testData.NDataTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, dataMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(1)) // Only DATA message (BIRTH dropped)

				// Verify aliases were resolved
				structured, err := (*messages)[0].AsStructured()
				Expect(err).NotTo(HaveOccurred())

				payloadMap := structured.(map[string]interface{})
				metricsInterface, exists := payloadMap["metrics"]
				Expect(exists).To(BeTrue())

				metrics := metricsInterface.([]interface{})
				Expect(metrics).To(HaveLen(2))

				// Check that metrics now have names (resolved from aliases)
				metricNames := make([]string, 0)
				for _, metricInterface := range metrics {
					metric := metricInterface.(map[string]interface{})
					if name, exists := metric["name"]; exists && name != nil {
						metricNames = append(metricNames, name.(string))
					}
				}

				Expect(metricNames).To(ContainElements("Temperature", "Pressure"))
			})

			It("should handle device-level alias resolution", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  drop_birth_messages: true
  data_messages_only: false
  auto_split_metrics: false
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Send DBIRTH to cache device aliases
				birthMsg, err := CreateBenthosMessage(testData.DBirthPayload, testData.DBirthTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, birthMsg)
				Expect(err).NotTo(HaveOccurred())

				// Send DDATA with device aliases
				dataMsg, err := CreateBenthosMessage(testData.DDataPayload, testData.DDataTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, dataMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(1))

				// Verify device metadata
				ExpectMetadata((*messages)[0], map[string]string{
					"sparkplug_msg_type": "DDATA",
					"group_id":           "TestFactory",
					"edge_node_id":       "Line1",
					"device_id":          "Machine1",
				})

				// Verify alias resolution for device metric
				structured, err := (*messages)[0].AsStructured()
				Expect(err).NotTo(HaveOccurred())

				payloadMap := structured.(map[string]interface{})
				metricsInterface, exists := payloadMap["metrics"]
				Expect(exists).To(BeTrue())

				metrics := metricsInterface.([]interface{})
				Expect(metrics).To(HaveLen(1))

				metric := metrics[0].(map[string]interface{})
				name, exists := metric["name"]
				Expect(exists).To(BeTrue())
				Expect(name).To(Equal("Speed")) // Resolved from alias 200
			})
		})

		Context("Alias cache isolation", func() {
			It("should isolate aliases between different device keys", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  data_messages_only: false
  auto_split_metrics: false
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Send NBIRTH for node-level aliases
				nodeBirthMsg, err := CreateBenthosMessage(testData.NBirthPayload, testData.NBirthTopic)
				Expect(err).NotTo(HaveOccurred())

				// Send DBIRTH for device-level aliases
				deviceBirthMsg, err := CreateBenthosMessage(testData.DBirthPayload, testData.DBirthTopic)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, nodeBirthMsg)
				Expect(err).NotTo(HaveOccurred())

				err = msgHandler(ctx, deviceBirthMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(2))

				// Both should be processed independently
				msgTypes := make([]string, 2)
				deviceKeys := make([]string, 2)
				for i, msg := range *messages {
					msgType, _ := msg.MetaGet("sparkplug_msg_type")
					deviceKey, _ := msg.MetaGet("sparkplug_device_key")
					msgTypes[i] = msgType
					deviceKeys[i] = deviceKey
				}

				Expect(msgTypes).To(ContainElements("NBIRTH", "DBIRTH"))
				Expect(deviceKeys).To(ContainElements("TestFactory/Line1", "TestFactory/Line1/Machine1"))
			})
		})
	})

	Describe("Error Handling", func() {
		Context("Malformed payloads", func() {
			It("should drop malformed protobuf messages", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode: {}
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Create message with invalid protobuf data
				msg := service.NewMessage([]byte("invalid protobuf data"))
				msg.MetaSetMut("mqtt_topic", testData.NDataTopic)

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				time.Sleep(100 * time.Millisecond)

				// Should be dropped due to unmarshal error
				Expect(*messages).To(HaveLen(0))
			})
		})

		Context("Missing MQTT topic", func() {
			It("should pass through when strict validation disabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  strict_topic_validation: false
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Create message without mqtt_topic metadata
				payloadBytes, err := proto.Marshal(testData.NDataPayload)
				Expect(err).NotTo(HaveOccurred())

				msg := service.NewMessage(payloadBytes)
				// Don't set mqtt_topic metadata

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(*messages)
				}).Should(Equal(1))

				// Should pass through unchanged
				receivedMsg := (*messages)[0]
				_, exists := receivedMsg.MetaGet("sparkplug_msg_type")
				Expect(exists).To(BeFalse()) // No Sparkplug processing should have occurred
			})

			It("should drop when strict validation enabled", func() {
				builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  strict_topic_validation: true
`)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
				Expect(err).NotTo(HaveOccurred())
				defer cancel()

				// Create message without mqtt_topic metadata
				payloadBytes, err := proto.Marshal(testData.NDataPayload)
				Expect(err).NotTo(HaveOccurred())

				msg := service.NewMessage(payloadBytes)
				// Don't set mqtt_topic metadata

				err = msgHandler(ctx, msg)
				Expect(err).NotTo(HaveOccurred())

				time.Sleep(100 * time.Millisecond)

				// Should be dropped
				Expect(*messages).To(HaveLen(0))
			})
		})
	})

	Describe("Metadata Extraction", func() {
		It("should extract correct metadata for node-level messages", func() {
			builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  auto_split_metrics: false
`)
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
			Expect(err).NotTo(HaveOccurred())
			defer cancel()

			msg, err := CreateBenthosMessage(testData.NDataPayload, testData.NDataTopic)
			Expect(err).NotTo(HaveOccurred())

			err = msgHandler(ctx, msg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(*messages)
			}).Should(Equal(1))

			ExpectMetadata((*messages)[0], map[string]string{
				"sparkplug_msg_type":   "NDATA",
				"sparkplug_device_key": "TestFactory/Line1",
				"group_id":             "TestFactory",
				"edge_node_id":         "Line1",
			})

			// Should not have device_id for node-level messages
			_, exists := (*messages)[0].MetaGet("device_id")
			Expect(exists).To(BeFalse())
		})

		It("should extract correct metadata for device-level messages", func() {
			builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  auto_split_metrics: false
`)
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
			Expect(err).NotTo(HaveOccurred())
			defer cancel()

			msg, err := CreateBenthosMessage(testData.DDataPayload, testData.DDataTopic)
			Expect(err).NotTo(HaveOccurred())

			err = msgHandler(ctx, msg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(*messages)
			}).Should(Equal(1))

			ExpectMetadata((*messages)[0], map[string]string{
				"sparkplug_msg_type":   "DDATA",
				"sparkplug_device_key": "TestFactory/Line1/Machine1",
				"group_id":             "TestFactory",
				"edge_node_id":         "Line1",
				"device_id":            "Machine1",
			})
		})

		It("should set tag_name metadata when splitting metrics", func() {
			builder, msgHandler, messages, err := CreateProcessorPipeline(`
sparkplug_b_decode:
  auto_split_metrics: true
  drop_birth_messages: true
  data_messages_only: false
`)
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel, err := RunPipelineWithTimeout(builder, time.Second)
			Expect(err).NotTo(HaveOccurred())
			defer cancel()

			// First cache aliases
			birthMsg, err := CreateBenthosMessage(testData.NBirthPayload, testData.NBirthTopic)
			Expect(err).NotTo(HaveOccurred())

			err = msgHandler(ctx, birthMsg)
			Expect(err).NotTo(HaveOccurred())

			// Then send data with resolved aliases
			dataMsg, err := CreateBenthosMessage(testData.NDataPayload, testData.NDataTopic)
			Expect(err).NotTo(HaveOccurred())

			err = msgHandler(ctx, dataMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(*messages)
			}).Should(Equal(2))

			tagNames := make([]string, 2)
			for i, msg := range *messages {
				tagName, exists := msg.MetaGet("tag_name")
				Expect(exists).To(BeTrue())
				tagNames[i] = tagName
			}

			Expect(tagNames).To(ContainElements("Temperature", "Pressure"))
		})
	})
})
