//go:build !integration

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

package sparkplug_plugin

import (
	"encoding/json"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("FormatConverter", func() {
	var converter *FormatConverter

	BeforeEach(func() {
		converter = NewFormatConverter()
	})

	Describe("EncodeUMHToSparkplug", func() {
		Context("with valid UMH messages", func() {
			DescribeTable("should convert UMH messages to Sparkplug B format",
				func(setupMessage func() *service.Message, groupID, edgeNodeID string, expectations func(*SparkplugMessage)) {
					msg := setupMessage()
					result, err := converter.EncodeUMHToSparkplug(msg, groupID, edgeNodeID)

					Expect(err).NotTo(HaveOccurred())
					Expect(result).NotTo(BeNil())
					expectations(result)
				},
				Entry("Simple UMH message with basic metadata",
					func() *service.Message {
						msg := service.NewMessage([]byte(`{"value": 25.5, "timestamp_ms": 1640995200000}`))
						msg.MetaSet("location_path", "enterprise.site.area")
						msg.MetaSet("tag_name", "temperature")
						return msg
					},
					"FactoryA", "EdgeNode1",
					func(result *SparkplugMessage) {
						Expect(result.GroupID).To(Equal("FactoryA"))
						Expect(result.EdgeNodeID).To(Equal("EdgeNode1"))
						Expect(result.DeviceID).To(Equal("enterprise:site:area"))
						Expect(result.MetricName).To(Equal("temperature"))

						// JSON parsing preserves numbers as json.Number - convert for comparison
						if jsonNum, ok := result.Value.(json.Number); ok {
							floatVal, err := jsonNum.Float64()
							Expect(err).NotTo(HaveOccurred())
							Expect(floatVal).To(Equal(25.5))
						} else {
							Expect(result.Value).To(Equal(25.5))
						}

						Expect(result.DataType).To(Equal("double"))
					},
				),
				Entry("UMH message with virtual_path",
					func() *service.Message {
						msg := service.NewMessage([]byte(`{"value": true, "timestamp_ms": 1640995260000}`))
						msg.MetaSet("location_path", "factory.line1.station2")
						msg.MetaSet("virtual_path", "motor.diagnostics")
						msg.MetaSet("tag_name", "running")
						return msg
					},
					"PlantB", "Line1",
					func(result *SparkplugMessage) {
						Expect(result.GroupID).To(Equal("PlantB"))
						Expect(result.EdgeNodeID).To(Equal("Line1"))
						Expect(result.DeviceID).To(Equal("factory:line1:station2"))
						Expect(result.MetricName).To(Equal("motor:diagnostics:running"))
						Expect(result.Value).To(Equal(true))
						Expect(result.DataType).To(Equal("boolean"))
					},
				),
				Entry("UMH message with custom data contract",
					func() *service.Message {
						msg := service.NewMessage([]byte(`{"value": 1234, "timestamp_ms": 1640995320000}`))
						msg.MetaSet("location_path", "enterprise.plant.zone")
						msg.MetaSet("data_contract", "_analytics")
						msg.MetaSet("tag_name", "count")
						return msg
					},
					"Analytics", "Processor1",
					func(result *SparkplugMessage) {
						Expect(result.GroupID).To(Equal("Analytics"))
						Expect(result.EdgeNodeID).To(Equal("Processor1"))
						Expect(result.DeviceID).To(Equal("enterprise:plant:zone"))
						Expect(result.MetricName).To(Equal("count"))

						// JSON parsing preserves numbers as json.Number - convert for comparison
						if jsonNum, ok := result.Value.(json.Number); ok {
							intVal, err := jsonNum.Int64()
							Expect(err).NotTo(HaveOccurred())
							Expect(intVal).To(Equal(int64(1234)))
						} else {
							Expect(result.Value).To(Equal(1234))
						}

						Expect(result.DataType).To(Equal("int64"))
					},
				),
				Entry("UMH message with complex virtual path",
					func() *service.Message {
						msg := service.NewMessage([]byte(`{"value": "RUNNING", "timestamp_ms": 1640995380000}`))
						msg.MetaSet("location_path", "acme.berlin.assembly.line3")
						msg.MetaSet("virtual_path", "plc.tags.status.machine")
						msg.MetaSet("tag_name", "state")
						return msg
					},
					"ACME", "Berlin",
					func(result *SparkplugMessage) {
						Expect(result.GroupID).To(Equal("ACME"))
						Expect(result.EdgeNodeID).To(Equal("Berlin"))
						Expect(result.DeviceID).To(Equal("acme:berlin:assembly:line3"))
						Expect(result.MetricName).To(Equal("plc:tags:status:machine:state"))
						Expect(result.Value).To(Equal("RUNNING"))
						Expect(result.DataType).To(Equal("string"))
					},
				),
			)
		})

		Context("with invalid UMH messages", func() {
			It("should return error when location_path is missing", func() {
				msg := service.NewMessage([]byte(`{"value": 42, "timestamp_ms": 1640995440000}`))
				msg.MetaSet("tag_name", "temperature")

				result, err := converter.EncodeUMHToSparkplug(msg, "Test", "Node1")

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("location_path metadata is required"))
				Expect(result).To(BeNil())
			})

			It("should return error when tag_name is missing", func() {
				msg := service.NewMessage([]byte(`{"value": 42, "timestamp_ms": 1640995500000}`))
				msg.MetaSet("location_path", "enterprise.site")

				result, err := converter.EncodeUMHToSparkplug(msg, "Test", "Node1")

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("tag_name metadata is required"))
				Expect(result).To(BeNil())
			})
		})
	})

	Describe("DecodeSparkplugToUMH", func() {
		Context("with valid Sparkplug messages", func() {
			DescribeTable("should convert Sparkplug B messages to UMH format",
				func(sparkplugMsg *SparkplugMessage, dataContract string, expectations func(*UMHMessage)) {
					result, err := converter.DecodeSparkplugToUMH(sparkplugMsg, dataContract)

					Expect(err).NotTo(HaveOccurred())
					Expect(result).NotTo(BeNil())
					expectations(result)
				},
				Entry("Simple Sparkplug message",
					&SparkplugMessage{
						GroupID:    "FactoryA",
						EdgeNodeID: "EdgeNode1",
						DeviceID:   "enterprise:site:area",
						MetricName: "temperature",
						Value:      25.5,
						DataType:   "double",
						Timestamp:  time.Now(),
					},
					"_historian",
					func(result *UMHMessage) {
						Expect(result.Topic.String()).To(Equal("umh.v1.enterprise.site.area._historian.temperature"))
						Expect(result.TopicInfo.Level0).To(Equal("enterprise"))
						Expect(result.TopicInfo.LocationSublevels).To(Equal([]string{"site", "area"}))
						Expect(result.TopicInfo.DataContract).To(Equal("_historian"))
						Expect(result.TopicInfo.VirtualPath).To(BeNil())
						Expect(result.TopicInfo.Name).To(Equal("temperature"))
						Expect(result.Value).To(Equal(25.5))
					},
				),
				Entry("Sparkplug message with virtual path",
					&SparkplugMessage{
						GroupID:    "PlantB",
						EdgeNodeID: "Line1",
						DeviceID:   "factory:line1:station2",
						MetricName: "motor:diagnostics:running",
						Value:      true,
						DataType:   "boolean",
						Timestamp:  time.Now(),
					},
					"_raw",
					func(result *UMHMessage) {
						Expect(result.Topic.String()).To(Equal("umh.v1.factory.line1.station2._raw.motor.diagnostics.running"))
						Expect(result.TopicInfo.Level0).To(Equal("factory"))
						Expect(result.TopicInfo.LocationSublevels).To(Equal([]string{"line1", "station2"}))
						Expect(result.TopicInfo.DataContract).To(Equal("_raw"))
						Expect(result.TopicInfo.VirtualPath).NotTo(BeNil())
						Expect(*result.TopicInfo.VirtualPath).To(Equal("motor.diagnostics"))
						Expect(result.TopicInfo.Name).To(Equal("running"))
						Expect(result.Value).To(Equal(true))
					},
				),
				Entry("Sparkplug message with complex virtual path",
					&SparkplugMessage{
						GroupID:    "ACME",
						EdgeNodeID: "Berlin",
						DeviceID:   "acme:berlin:assembly:line3",
						MetricName: "plc:tags:status:machine:state",
						Value:      "RUNNING",
						DataType:   "string",
						Timestamp:  time.Now(),
					},
					"_analytics",
					func(result *UMHMessage) {
						Expect(result.Topic.String()).To(Equal("umh.v1.acme.berlin.assembly.line3._analytics.plc.tags.status.machine.state"))
						Expect(result.TopicInfo.Level0).To(Equal("acme"))
						Expect(result.TopicInfo.LocationSublevels).To(Equal([]string{"berlin", "assembly", "line3"}))
						Expect(result.TopicInfo.DataContract).To(Equal("_analytics"))
						Expect(result.TopicInfo.VirtualPath).NotTo(BeNil())
						Expect(*result.TopicInfo.VirtualPath).To(Equal("plc.tags.status.machine"))
						Expect(result.TopicInfo.Name).To(Equal("state"))
						Expect(result.Value).To(Equal("RUNNING"))
					},
				),
			)
		})

		Context("with invalid Sparkplug messages", func() {
			It("should return error when device ID is empty", func() {
				sparkplugMsg := &SparkplugMessage{
					GroupID:    "Test",
					EdgeNodeID: "Node1",
					DeviceID:   "",
					MetricName: "temperature",
					Value:      42.0,
					DataType:   "double",
					Timestamp:  time.Now(),
				}

				result, err := converter.DecodeSparkplugToUMH(sparkplugMsg, "_historian")

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("location_path is required"))
				Expect(result).To(BeNil())
			})

			It("should return error when metric name is empty", func() {
				sparkplugMsg := &SparkplugMessage{
					GroupID:    "Test",
					EdgeNodeID: "Node1",
					DeviceID:   "enterprise:site",
					MetricName: "",
					Value:      42.0,
					DataType:   "double",
					Timestamp:  time.Now(),
				}

				result, err := converter.DecodeSparkplugToUMH(sparkplugMsg, "_historian")

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("metric name cannot be empty"))
				Expect(result).To(BeNil())
			})
		})
	})

	Describe("Conversion Methods", func() {
		Context("convertLocationPathToDeviceID", func() {
			DescribeTable("should convert location paths to device IDs",
				func(level0 string, sublevels []string, expectedID string) {
					topicInfo := &proto.TopicInfo{
						Level0:            level0,
						LocationSublevels: sublevels,
					}
					result := converter.convertLocationPathToDeviceID(topicInfo)
					Expect(result).To(Equal(expectedID))
				},
				Entry("Single level", "enterprise", []string{}, "enterprise"),
				Entry("Multiple levels", "enterprise", []string{"site", "area", "line"}, "enterprise:site:area:line"),
				Entry("Empty level0", "", []string{"site", "area"}, ""),
			)
		})

		Context("convertDeviceIDToLocationPath", func() {
			DescribeTable("should convert device IDs to location paths",
				func(deviceID, expectedPath string) {
					result := converter.convertDeviceIDToLocationPath(deviceID)
					Expect(result).To(Equal(expectedPath))
				},
				Entry("Single level", "enterprise", "enterprise"),
				Entry("Multiple levels", "enterprise:site:area:line", "enterprise.site.area.line"),
				Entry("Empty device ID", "", ""),
			)
		})

		Context("parseSparkplugMetricName", func() {
			DescribeTable("should parse metric names correctly",
				func(metricName string, expectedVirtualPath *string, expectedTagName string, shouldError bool) {
					virtualPath, tagName, err := converter.parseSparkplugMetricName(metricName)

					if shouldError {
						Expect(err).To(HaveOccurred())
						return
					}

					Expect(err).NotTo(HaveOccurred())
					Expect(virtualPath).To(Equal(expectedVirtualPath))
					Expect(tagName).To(Equal(expectedTagName))
				},
				Entry("Simple metric name", "temperature", nil, "temperature", false),
				Entry("Metric name with virtual path", "motor:diagnostics:temperature", stringPtr("motor.diagnostics"), "temperature", false),
				Entry("Complex metric name", "plc:tags:status:machine:state", stringPtr("plc.tags.status.machine"), "state", false),
				Entry("Empty metric name", "", nil, "", true),
			)
		})
	})

	Describe("Helper Methods", func() {
		Context("ExtractMetricNameFromUMHMessage", func() {
			It("should extract metric names correctly", func() {
				msg := service.NewMessage([]byte(`{"value": 25.5, "timestamp_ms": 1640995560000}`))
				msg.MetaSet("location_path", "enterprise.site.area")
				msg.MetaSet("virtual_path", "motor.diagnostics")
				msg.MetaSet("tag_name", "temperature")

				metricName, err := converter.ExtractMetricNameFromUMHMessage(msg)

				Expect(err).NotTo(HaveOccurred())
				Expect(metricName).To(Equal("motor:diagnostics:temperature"))
			})
		})

		Context("ExtractDeviceIDFromUMHMessage", func() {
			It("should extract device IDs correctly", func() {
				msg := service.NewMessage([]byte(`{"value": 25.5, "timestamp_ms": 1640995620000}`))
				msg.MetaSet("location_path", "enterprise.site.area")
				msg.MetaSet("tag_name", "temperature")

				deviceID, err := converter.ExtractDeviceIDFromUMHMessage(msg)

				Expect(err).NotTo(HaveOccurred())
				Expect(deviceID).To(Equal("enterprise:site:area"))
			})
		})

		Context("ValidateUMHMessage", func() {
			It("should validate valid UMH messages", func() {
				validMsg := service.NewMessage([]byte(`{"value": 25.5, "timestamp_ms": 1640995680000}`))
				validMsg.MetaSet("location_path", "enterprise.site.area")
				validMsg.MetaSet("tag_name", "temperature")

				err := converter.ValidateUMHMessage(validMsg)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should return error for invalid UMH messages", func() {
				invalidMsg := service.NewMessage([]byte(`{"value": 25.5, "timestamp_ms": 1640995740000}`))
				invalidMsg.MetaSet("tag_name", "temperature")
				// Missing location_path

				err := converter.ValidateUMHMessage(invalidMsg)
				Expect(err).To(HaveOccurred())
			})
		})

		Context("ConvertSparkplugMetricsToUMHTopics", func() {
			It("should convert multiple metrics to UMH topics", func() {
				metrics := map[string]interface{}{
					"temperature":           25.5,
					"motor:diagnostics:rpm": 1500,
					"pressure":              10.2,
				}

				topics, err := converter.ConvertSparkplugMetricsToUMHTopics("enterprise:site:area", metrics, "_historian")

				Expect(err).NotTo(HaveOccurred())

				expectedTopics := map[string]string{
					"temperature":           "umh.v1.enterprise.site.area._historian.temperature",
					"motor:diagnostics:rpm": "umh.v1.enterprise.site.area._historian.motor.diagnostics.rpm",
					"pressure":              "umh.v1.enterprise.site.area._historian.pressure",
				}

				Expect(topics).To(Equal(expectedTopics))
			})
		})
	})

	Describe("Data Type Inference", func() {
		Context("TypeConverter direct testing", func() {
			var typeConverter *TypeConverter

			BeforeEach(func() {
				typeConverter = NewTypeConverter()
			})

			DescribeTable("should infer correct Sparkplug data types from Go values",
				func(value interface{}, expectedType string) {
					inferredType := typeConverter.InferMetricType(value)
					Expect(inferredType).To(Equal(expectedType))
				},
				Entry("boolean true", true, "boolean"),
				Entry("boolean false", false, "boolean"),
				Entry("int8", int8(127), "int32"),     // int8 maps to int32 in Sparkplug
				Entry("int16", int16(32767), "int32"), // int16 maps to int32 in Sparkplug
				Entry("int32", int32(2147483647), "int32"),
				Entry("int", int(42), "int32"), // int maps to int32 in Sparkplug
				Entry("int64", int64(9223372036854775807), "int64"),
				Entry("uint8", uint8(255), "uint32"),     // uint8 maps to uint32 in Sparkplug
				Entry("uint16", uint16(65535), "uint32"), // uint16 maps to uint32 in Sparkplug
				Entry("uint32", uint32(4294967295), "uint32"),
				Entry("uint64", uint64(18446744073709551615), "uint64"),
				Entry("float32", float32(3.14), "float"),
				Entry("float64", float64(3.14159), "double"),
				Entry("string", "hello", "string"),
				Entry("json.Number with decimal", json.Number("25.5"), "double"),
				Entry("json.Number integer", json.Number("42"), "int64"),
				Entry("unknown type", struct{}{}, "string"), // Unknown types default to string
			)
		})

		Context("FormatConverter with realistic JSON scenarios", func() {
			DescribeTable("should handle realistic UMH JSON data types",
				func(jsonPayload string, expectedType string) {
					msg := service.NewMessage([]byte(jsonPayload))
					msg.MetaSet("location_path", "enterprise.site")
					msg.MetaSet("tag_name", "test")

					sparkplugMsg, err := converter.EncodeUMHToSparkplug(msg, "TestGroup", "TestNode")

					Expect(err).NotTo(HaveOccurred())
					Expect(sparkplugMsg.DataType).To(Equal(expectedType))
				},
				// These are realistic JSON payloads that UMH systems would actually send
				Entry("boolean true", `{"value": true, "timestamp_ms": 1640995800000}`, "boolean"),
				Entry("boolean false", `{"value": false, "timestamp_ms": 1640995800000}`, "boolean"),
				Entry("integer from PLC", `{"value": 42, "timestamp_ms": 1640995800000}`, "int64"),
				Entry("decimal from sensor", `{"value": 25.5, "timestamp_ms": 1640995800000}`, "double"),
				Entry("large integer", `{"value": 9223372036854775807, "timestamp_ms": 1640995800000}`, "int64"),
				Entry("string status", `{"value": "RUNNING", "timestamp_ms": 1640995800000}`, "string"),
				Entry("zero value", `{"value": 0, "timestamp_ms": 1640995800000}`, "int64"),
				Entry("negative integer", `{"value": -100, "timestamp_ms": 1640995800000}`, "int64"),
				Entry("negative decimal", `{"value": -25.5, "timestamp_ms": 1640995800000}`, "double"),
				Entry("scientific notation", `{"value": 1.23e5, "timestamp_ms": 1640995800000}`, "double"),
			)
		})

		Context("Edge cases in JSON parsing", func() {
			It("should handle null values gracefully", func() {
				msg := service.NewMessage([]byte(`{"value": null, "timestamp_ms": 1640995800000}`))
				msg.MetaSet("location_path", "enterprise.site")
				msg.MetaSet("tag_name", "test")

				sparkplugMsg, err := converter.EncodeUMHToSparkplug(msg, "TestGroup", "TestNode")

				Expect(err).NotTo(HaveOccurred())
				// JSON parsing correctly extracts null as nil value
				Expect(sparkplugMsg.Value).To(BeNil())
				Expect(sparkplugMsg.DataType).To(Equal("string")) // Default type for unknown/null
			})

			It("should handle missing value field", func() {
				msg := service.NewMessage([]byte(`{"timestamp_ms": 1640995800000}`))
				msg.MetaSet("location_path", "enterprise.site")
				msg.MetaSet("tag_name", "test")

				sparkplugMsg, err := converter.EncodeUMHToSparkplug(msg, "TestGroup", "TestNode")

				Expect(err).NotTo(HaveOccurred())
				// Should use entire payload as value
				Expect(sparkplugMsg.Value).NotTo(BeNil())
				Expect(sparkplugMsg.DataType).To(Equal("string")) // Complex objects become strings
			})

			It("should handle alternative value field names", func() {
				// Test that converter tries different field names
				testCases := []struct {
					payload   string
					fieldName string
				}{
					{`{"val": 42.5, "timestamp_ms": 1640995800000}`, "val"},
					{`{"data": 100, "timestamp_ms": 1640995800000}`, "data"},
					{`{"measurement": true, "timestamp_ms": 1640995800000}`, "measurement"},
				}

				for _, tc := range testCases {
					msg := service.NewMessage([]byte(tc.payload))
					msg.MetaSet("location_path", "enterprise.site")
					msg.MetaSet("tag_name", "test")

					sparkplugMsg, err := converter.EncodeUMHToSparkplug(msg, "TestGroup", "TestNode")

					Expect(err).NotTo(HaveOccurred(), "Should handle field name: "+tc.fieldName)
					Expect(sparkplugMsg.Value).NotTo(BeNil(), "Should extract value from field: "+tc.fieldName)
				}
			})
		})
	})

	Describe("Round-trip Conversion", func() {
		It("should maintain data integrity through UMH → Sparkplug → UMH conversion", func() {
			// Create original UMH message
			originalMsg := service.NewMessage([]byte(`{"value": 25.5, "timestamp_ms": 1640995860000}`))
			originalMsg.MetaSet("location_path", "enterprise.site.area.line")
			originalMsg.MetaSet("virtual_path", "motor.diagnostics")
			originalMsg.MetaSet("tag_name", "temperature")
			originalMsg.MetaSet("data_contract", "_historian")

			// Convert to Sparkplug
			sparkplugMsg, err := converter.EncodeUMHToSparkplug(originalMsg, "TestGroup", "TestNode")
			Expect(err).NotTo(HaveOccurred())

			// Convert back to UMH
			umhMsg, err := converter.DecodeSparkplugToUMH(sparkplugMsg, "_historian")
			Expect(err).NotTo(HaveOccurred())

			// Verify round-trip conversion
			Expect(umhMsg.Topic.String()).To(Equal("umh.v1.enterprise.site.area.line._historian.motor.diagnostics.temperature"))
			Expect(umhMsg.TopicInfo.Level0).To(Equal("enterprise"))
			Expect(umhMsg.TopicInfo.LocationSublevels).To(Equal([]string{"site", "area", "line"}))
			Expect(umhMsg.TopicInfo.DataContract).To(Equal("_historian"))
			Expect(*umhMsg.TopicInfo.VirtualPath).To(Equal("motor.diagnostics"))
			Expect(umhMsg.TopicInfo.Name).To(Equal("temperature"))

			// Handle json.Number comparison properly
			if jsonNum, ok := umhMsg.Value.(json.Number); ok {
				floatVal, err := jsonNum.Float64()
				Expect(err).NotTo(HaveOccurred())
				Expect(floatVal).To(Equal(25.5))
			} else {
				Expect(umhMsg.Value).To(Equal(25.5))
			}
		})
	})
})

// Helper function to create string pointer
func stringPtr(s string) *string {
	return &s
}
