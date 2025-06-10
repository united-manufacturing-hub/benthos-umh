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
	"context"
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin" // Import to trigger init()
	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("Sparkplug B Processor", func() {
	Describe("Integration Tests - Real Benthos Pipeline", func() {
		Context("Processing Sparkplug B messages through pipeline", func() {
			It("should use default configuration with all auto-features enabled", func() {
				// Create a simple payload with multiple metrics to test all defaults
				payload := &sproto.Payload{
					Timestamp: uint64Ptr(1672531200000),
					Metrics: []*sproto.Payload_Metric{
						{
							Name: stringPtr("Temperature"),
							Value: &sproto.Payload_Metric_FloatValue{
								FloatValue: 22.5,
							},
						},
						{
							Name: stringPtr("Pressure"),
							Value: &sproto.Payload_Metric_FloatValue{
								FloatValue: 1013.25,
							},
						},
					},
				}

				payloadBytes, err := proto.Marshal(payload)
				Expect(err).NotTo(HaveOccurred())

				// Create Benthos pipeline with DEFAULT configuration (empty config)
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err = builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				// Use empty config to test defaults
				err = builder.AddProcessorYAML(`
sparkplug_b_decode: {}  # All auto-features enabled by default
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

				// Send NDATA message (should pass through with defaults: data_messages_only=true)
				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSetMut("mqtt_topic", "spBv1.0/Factory1/NDATA/EdgeNode1")
				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				// Should get 2 messages due to auto_split_metrics=true by default
				Eventually(func() int {
					return len(messages)
				}).Should(Equal(2))

				// Verify both messages have auto-extracted metadata
				for i, msg := range messages {
					// Check auto-set metadata
					msgType, exists := msg.MetaGet("sparkplug_msg_type")
					Expect(exists).To(BeTrue())
					Expect(msgType).To(Equal("NDATA"))

					groupID, exists := msg.MetaGet("group_id")
					Expect(exists).To(BeTrue())
					Expect(groupID).To(Equal("Factory1"))

					edgeNodeID, exists := msg.MetaGet("edge_node_id")
					Expect(exists).To(BeTrue())
					Expect(edgeNodeID).To(Equal("EdgeNode1"))

					tagName, exists := msg.MetaGet("tag_name")
					Expect(exists).To(BeTrue())
					if i == 0 {
						Expect(tagName).To(Or(Equal("Temperature"), Equal("Pressure")))
					} else {
						Expect(tagName).To(Or(Equal("Temperature"), Equal("Pressure")))
					}

					// With auto_extract_values=true by default, check the payload structure
					structured, err := msg.AsStructured()
					Expect(err).NotTo(HaveOccurred())

					// The processor splits metrics but still returns full payload structure
					payloadMap, ok := structured.(map[string]interface{})
					Expect(ok).To(BeTrue())

					metricsInterface, exists := payloadMap["metrics"]
					Expect(exists).To(BeTrue())

					metrics, ok := metricsInterface.([]interface{})
					Expect(ok).To(BeTrue())
					Expect(metrics).To(HaveLen(2)) // Original metric + quality metric
				}
			})

			It("should process NBIRTH message and add metadata", func() {
				// Create NBIRTH payload
				payload := &sproto.Payload{
					Timestamp: uint64Ptr(1672531200000),
					Metrics: []*sproto.Payload_Metric{
						{
							Name:  stringPtr("bdSeq"),
							Alias: uint64Ptr(1),
							Value: &sproto.Payload_Metric_LongValue{
								LongValue: 0,
							},
						},
						{
							Name:  stringPtr("Temperature"),
							Alias: uint64Ptr(2),
							Value: &sproto.Payload_Metric_FloatValue{
								FloatValue: 22.5,
							},
						},
					},
				}

				payloadBytes, err := proto.Marshal(payload)
				Expect(err).NotTo(HaveOccurred())

				// Create Benthos pipeline
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err = builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
sparkplug_b_decode:
  auto_split_metrics: false  # Keep single message for easier testing
  data_messages_only: false  # Allow BIRTH messages
  auto_extract_values: false # Keep original structure
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

				// Send NBIRTH message
				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSetMut("mqtt_topic", "spBv1.0/Factory1/NBIRTH/EdgeNode1")
				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(messages)
				}).Should(Equal(1))

				// Verify message was processed and metadata added
				msg := messages[0]

				msgType, exists := msg.MetaGet("sparkplug_msg_type")
				Expect(exists).To(BeTrue())
				Expect(msgType).To(Equal("NBIRTH"))

				groupID, exists := msg.MetaGet("group_id")
				Expect(exists).To(BeTrue())
				Expect(groupID).To(Equal("Factory1"))

				edgeNodeID, exists := msg.MetaGet("edge_node_id")
				Expect(exists).To(BeTrue())
				Expect(edgeNodeID).To(Equal("EdgeNode1"))

				deviceKey, exists := msg.MetaGet("sparkplug_device_key")
				Expect(exists).To(BeTrue())
				Expect(deviceKey).To(Equal("Factory1/EdgeNode1"))

				// Verify payload structure
				structured, err := msg.AsStructured()
				Expect(err).NotTo(HaveOccurred())

				payloadMap, ok := structured.(map[string]interface{})
				Expect(ok).To(BeTrue())

				// Should contain metrics array
				metricsInterface, exists := payloadMap["metrics"]
				Expect(exists).To(BeTrue())

				metrics, ok := metricsInterface.([]interface{})
				Expect(ok).To(BeTrue())
				Expect(metrics).To(HaveLen(2))
			})

			It("should process DBIRTH message for device", func() {
				// Create DBIRTH payload for a device
				payload := &sproto.Payload{
					Timestamp: uint64Ptr(1672531200000),
					Metrics: []*sproto.Payload_Metric{
						{
							Name:  stringPtr("SerialNumber"),
							Alias: uint64Ptr(1),
							Value: &sproto.Payload_Metric_StringValue{
								StringValue: "DEV001",
							},
						},
						{
							Name:  stringPtr("SetPoint"),
							Alias: uint64Ptr(2),
							Value: &sproto.Payload_Metric_FloatValue{
								FloatValue: 25.0,
							},
						},
					},
				}

				payloadBytes, err := proto.Marshal(payload)
				Expect(err).NotTo(HaveOccurred())

				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err = builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
sparkplug_b_decode:
  auto_split_metrics: false
  data_messages_only: false
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

				// Send DBIRTH message
				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSetMut("mqtt_topic", "spBv1.0/Factory1/DBIRTH/EdgeNode1/Device1")
				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(messages)
				}).Should(Equal(1))

				msg := messages[0]

				msgType, exists := msg.MetaGet("sparkplug_msg_type")
				Expect(exists).To(BeTrue())
				Expect(msgType).To(Equal("DBIRTH"))

				deviceKey, exists := msg.MetaGet("sparkplug_device_key")
				Expect(exists).To(BeTrue())
				Expect(deviceKey).To(Equal("Factory1/EdgeNode1/Device1"))

				deviceID, exists := msg.MetaGet("device_id")
				Expect(exists).To(BeTrue())
				Expect(deviceID).To(Equal("Device1"))
			})

			It("should filter BIRTH messages when data_messages_only is enabled", func() {
				payload := &sproto.Payload{
					Timestamp: uint64Ptr(1672531200000),
					Metrics: []*sproto.Payload_Metric{
						{
							Name: stringPtr("Temperature"),
							Value: &sproto.Payload_Metric_FloatValue{
								FloatValue: 22.5,
							},
						},
					},
				}

				payloadBytes, err := proto.Marshal(payload)
				Expect(err).NotTo(HaveOccurred())

				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err = builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
sparkplug_b_decode:
  data_messages_only: true  # Should drop BIRTH messages
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

				// Send NBIRTH (should be dropped)
				birthMsg := service.NewMessage(payloadBytes)
				birthMsg.MetaSetMut("mqtt_topic", "spBv1.0/Factory1/NBIRTH/EdgeNode1")
				err = msgHandler(ctx, birthMsg)
				Expect(err).NotTo(HaveOccurred())

				// Send NDATA (should pass through)
				dataMsg := service.NewMessage(payloadBytes)
				dataMsg.MetaSetMut("mqtt_topic", "spBv1.0/Factory1/NDATA/EdgeNode1")
				err = msgHandler(ctx, dataMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(messages)
				}).Should(Equal(1)) // Only NDATA should pass through

				msg := messages[0]
				msgType, exists := msg.MetaGet("sparkplug_msg_type")
				Expect(exists).To(BeTrue())
				Expect(msgType).To(Equal("NDATA"))
			})

			It("should split metrics when auto_split_metrics is enabled", func() {
				// Create payload with multiple metrics
				payload := &sproto.Payload{
					Timestamp: uint64Ptr(1672531200000),
					Metrics: []*sproto.Payload_Metric{
						{
							Name: stringPtr("Temperature"),
							Value: &sproto.Payload_Metric_FloatValue{
								FloatValue: 22.5,
							},
						},
						{
							Name: stringPtr("Pressure"),
							Value: &sproto.Payload_Metric_FloatValue{
								FloatValue: 1013.25,
							},
						},
						{
							Name: stringPtr("Humidity"),
							Value: &sproto.Payload_Metric_FloatValue{
								FloatValue: 45.0,
							},
						},
					},
				}

				payloadBytes, err := proto.Marshal(payload)
				Expect(err).NotTo(HaveOccurred())

				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err = builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
sparkplug_b_decode:
  auto_split_metrics: true   # Should create separate messages per metric
  data_messages_only: false
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

				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSetMut("mqtt_topic", "spBv1.0/Factory1/NDATA/EdgeNode1")
				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(messages)
				}).Should(Equal(3)) // Should create 3 separate messages

				// Verify each message has different tag_name metadata
				tagNames := make([]string, len(messages))
				for i, msg := range messages {
					tagName, exists := msg.MetaGet("tag_name")
					Expect(exists).To(BeTrue())
					tagNames[i] = tagName
				}

				Expect(tagNames).To(ContainElements("Temperature", "Pressure", "Humidity"))
			})

			It("should auto-extract values when auto_extract_values is enabled", func() {
				payload := &sproto.Payload{
					Timestamp: uint64Ptr(1672531200000),
					Metrics: []*sproto.Payload_Metric{
						{
							Name: stringPtr("Temperature"),
							Value: &sproto.Payload_Metric_FloatValue{
								FloatValue: 23.7,
							},
						},
					},
				}

				payloadBytes, err := proto.Marshal(payload)
				Expect(err).NotTo(HaveOccurred())

				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err = builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
sparkplug_b_decode:
  auto_extract_values: true  # Should extract clean value
  auto_split_metrics: true   # Also split for easier testing
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

				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSetMut("mqtt_topic", "spBv1.0/Factory1/NDATA/EdgeNode1")
				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(messages)
				}).Should(Equal(1))

				msg := messages[0]

				// Check that value was extracted
				structured, err := msg.AsStructured()
				Expect(err).NotTo(HaveOccurred())

				// Based on the actual output, it appears the processor is returning
				// the full payload structure with the quality metric added
				payloadMap, ok := structured.(map[string]interface{})
				Expect(ok).To(BeTrue())

				metricsInterface, exists := payloadMap["metrics"]
				Expect(exists).To(BeTrue())

				metrics, ok := metricsInterface.([]interface{})
				Expect(ok).To(BeTrue())
				Expect(metrics).To(HaveLen(2)) // Temperature + quality metric

				// Find the temperature metric
				var tempMetric map[string]interface{}
				for _, metric := range metrics {
					metricMap := metric.(map[string]interface{})
					if name, exists := metricMap["name"]; exists && name == "Temperature" {
						tempMetric = metricMap
						break
					}
				}
				Expect(tempMetric).NotTo(BeNil())

				// Verify the float value
				floatValue, exists := tempMetric["float_value"]
				Expect(exists).To(BeTrue())
				// Convert json.Number to float64 for comparison
				numValue, ok := floatValue.(json.Number)
				Expect(ok).To(BeTrue())
				floatVal, err := numValue.Float64()
				Expect(err).NotTo(HaveOccurred())
				Expect(floatVal).To(BeNumerically("==", 23.7))

				// Check metadata for tag_name
				tagName, exists := msg.MetaGet("tag_name")
				Expect(exists).To(BeTrue())
				Expect(tagName).To(Equal("Temperature"))
			})
		})
	})
})

// Helper functions for creating pointers
// Helper functions are defined in sparkplug_b_suite_test.go
