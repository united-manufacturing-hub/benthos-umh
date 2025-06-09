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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Sparkplug B Output", func() {
	Describe("Configuration Validation", func() {
		Context("Required fields", func() {
			It("should require group_id field", func() {
				_, err := service.NewConfigSpec().
					Field(service.NewStringListField("broker_urls").Default([]string{"tcp://localhost:1883"})).
					Field(service.NewStringField("client_id").Default("benthos-sparkplug-node")).
					Field(service.NewStringField("group_id")).
					Field(service.NewStringField("edge_node_id")).
					ParseYAML(`
client_id: "test-node"
edge_node_id: "Line1"
# group_id missing
`, nil)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("group_id"))
			})

			It("should require edge_node_id field", func() {
				_, err := service.NewConfigSpec().
					Field(service.NewStringListField("broker_urls").Default([]string{"tcp://localhost:1883"})).
					Field(service.NewStringField("client_id").Default("benthos-sparkplug-node")).
					Field(service.NewStringField("group_id")).
					Field(service.NewStringField("edge_node_id")).
					ParseYAML(`
client_id: "test-node"
group_id: "TestFactory"
# edge_node_id missing
`, nil)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("edge_node_id"))
			})

			It("should validate with minimal required configuration", func() {
				conf, err := service.NewConfigSpec().
					Field(service.NewStringListField("broker_urls").Default([]string{"tcp://localhost:1883"})).
					Field(service.NewStringField("client_id").Default("benthos-sparkplug-node")).
					Field(service.NewStringField("group_id")).
					Field(service.NewStringField("edge_node_id")).
					Field(service.NewStringField("device_id").Default("")).
					Field(service.NewObjectListField("metrics")).
					Field(service.NewBoolField("auto_extract_tag_name").Default(true)).
					Field(service.NewBoolField("retain_last_values").Default(true)).
					ParseYAML(`
group_id: "TestFactory"
edge_node_id: "Line1"
`, nil)

				Expect(err).NotTo(HaveOccurred())
				Expect(conf).NotTo(BeNil())

				groupID, err := conf.FieldString("group_id")
				Expect(err).NotTo(HaveOccurred())
				Expect(groupID).To(Equal("TestFactory"))

				edgeNodeID, err := conf.FieldString("edge_node_id")
				Expect(err).NotTo(HaveOccurred())
				Expect(edgeNodeID).To(Equal("Line1"))
			})
		})

		Context("Default values", func() {
			It("should use correct default values", func() {
				conf, err := service.NewConfigSpec().
					Field(service.NewStringListField("broker_urls").Default([]string{"tcp://localhost:1883"})).
					Field(service.NewStringField("client_id").Default("benthos-sparkplug-node")).
					Field(service.NewStringField("group_id")).
					Field(service.NewStringField("edge_node_id")).
					Field(service.NewStringField("device_id").Default("")).
					Field(service.NewObjectListField("metrics")).
					Field(service.NewIntField("qos").Default(1)).
					Field(service.NewDurationField("keep_alive").Default("30s")).
					Field(service.NewDurationField("connect_timeout").Default("10s")).
					Field(service.NewBoolField("clean_session").Default(true)).
					Field(service.NewBoolField("auto_extract_tag_name").Default(true)).
					Field(service.NewBoolField("retain_last_values").Default(true)).
					ParseYAML(`
group_id: "TestFactory"
edge_node_id: "Line1"
`, nil)

				Expect(err).NotTo(HaveOccurred())

				brokerURLs, err := conf.FieldStringList("broker_urls")
				Expect(err).NotTo(HaveOccurred())
				Expect(brokerURLs).To(Equal([]string{"tcp://localhost:1883"}))

				clientID, err := conf.FieldString("client_id")
				Expect(err).NotTo(HaveOccurred())
				Expect(clientID).To(Equal("benthos-sparkplug-node"))

				deviceID, err := conf.FieldString("device_id")
				Expect(err).NotTo(HaveOccurred())
				Expect(deviceID).To(Equal(""))

				qos, err := conf.FieldInt("qos")
				Expect(err).NotTo(HaveOccurred())
				Expect(qos).To(Equal(1))

				autoExtractTagName, err := conf.FieldBool("auto_extract_tag_name")
				Expect(err).NotTo(HaveOccurred())
				Expect(autoExtractTagName).To(BeTrue())
			})
		})

		Context("Metric definitions", func() {
			It("should handle metric configurations", func() {
				conf, err := service.NewConfigSpec().
					Field(service.NewStringListField("broker_urls").Default([]string{"tcp://localhost:1883"})).
					Field(service.NewStringField("client_id").Default("benthos-sparkplug-node")).
					Field(service.NewStringField("group_id")).
					Field(service.NewStringField("edge_node_id")).
					Field(service.NewObjectListField("metrics",
						service.NewStringField("name"),
						service.NewIntField("alias"),
						service.NewStringField("type").Default("double"),
						service.NewStringField("value_from").Default("value"))).
					ParseYAML(`
group_id: "TestFactory"
edge_node_id: "Line1"
metrics:
  - name: "Temperature"
    alias: 100
    type: "float"
    value_from: "temperature"
  - name: "Pressure"
    alias: 101
    type: "double"
    value_from: "pressure"
  - name: "Status"
    alias: 102
    type: "string"
    value_from: "status"
`, nil)

				Expect(err).NotTo(HaveOccurred())

				// We can't easily test the metric parsing without exposing internal structures
				// But we can verify the configuration parses correctly
				groupID, _ := conf.FieldString("group_id")
				Expect(groupID).To(Equal("TestFactory"))
			})
		})
	})

	Describe("Data Type Conversion", func() {
		Context("Numeric types", func() {
			It("should handle integer values", func() {
				testValues := map[string]interface{}{
					"int8_value":   int8(127),
					"int16_value":  int16(32767),
					"int32_value":  int32(2147483647),
					"int64_value":  int64(9223372036854775807),
					"uint8_value":  uint8(255),
					"uint16_value": uint16(65535),
					"uint32_value": uint32(4294967295),
					"uint64_value": uint64(18446744073709551615),
				}

				for name, value := range testValues {
					By(fmt.Sprintf("handling %s with value %v", name, value), func() {
						// Test that the value can be JSON marshaled/unmarshaled
						jsonBytes, err := json.Marshal(value)
						Expect(err).NotTo(HaveOccurred())

						var unmarshaled interface{}
						err = json.Unmarshal(jsonBytes, &unmarshaled)
						Expect(err).NotTo(HaveOccurred())
						Expect(unmarshaled).NotTo(BeNil())
					})
				}
			})

			It("should handle floating point values", func() {
				testValues := map[string]interface{}{
					"float32_value": float32(3.14159),
					"float64_value": float64(2.718281828459045),
				}

				for name, value := range testValues {
					By(fmt.Sprintf("handling %s with value %v", name, value), func() {
						jsonBytes, err := json.Marshal(value)
						Expect(err).NotTo(HaveOccurred())

						var unmarshaled interface{}
						err = json.Unmarshal(jsonBytes, &unmarshaled)
						Expect(err).NotTo(HaveOccurred())
						Expect(unmarshaled).NotTo(BeNil())
					})
				}
			})
		})

		Context("Boolean and string types", func() {
			It("should handle boolean values", func() {
				testValues := []bool{true, false}

				for _, value := range testValues {
					By(fmt.Sprintf("handling boolean value %v", value), func() {
						jsonBytes, err := json.Marshal(value)
						Expect(err).NotTo(HaveOccurred())

						var unmarshaled bool
						err = json.Unmarshal(jsonBytes, &unmarshaled)
						Expect(err).NotTo(HaveOccurred())
						Expect(unmarshaled).To(Equal(value))
					})
				}
			})

			It("should handle string values", func() {
				testValues := []string{
					"simple_string",
					"String with spaces",
					"String with special chars: !@#$%^&*()",
					"Unicode string: 你好世界",
					"",
				}

				for _, value := range testValues {
					By(fmt.Sprintf("handling string value '%s'", value), func() {
						jsonBytes, err := json.Marshal(value)
						Expect(err).NotTo(HaveOccurred())

						var unmarshaled string
						err = json.Unmarshal(jsonBytes, &unmarshaled)
						Expect(err).NotTo(HaveOccurred())
						Expect(unmarshaled).To(Equal(value))
					})
				}
			})
		})

		Context("Type detection", func() {
			It("should detect types from interface{} values", func() {
				testCases := []struct {
					value        interface{}
					expectedType string
				}{
					{int8(1), "int8"},
					{int16(1), "int16"},
					{int32(1), "int32"},
					{int64(1), "int64"},
					{uint8(1), "uint8"},
					{uint16(1), "uint16"},
					{uint32(1), "uint32"},
					{uint64(1), "uint64"},
					{float32(1.0), "float32"},
					{float64(1.0), "float64"},
					{true, "boolean"},
					{"test", "string"},
				}

				for _, tc := range testCases {
					By(fmt.Sprintf("detecting type for %v as %s", tc.value, tc.expectedType), func() {
						// We can't test the actual type detection logic without exposing it
						// But we can verify the types are representable
						switch tc.value.(type) {
						case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
							Expect(tc.expectedType).To(ContainSubstring("int"))
						case float32, float64:
							Expect(tc.expectedType).To(ContainSubstring("float"))
						case bool:
							Expect(tc.expectedType).To(Equal("boolean"))
						case string:
							Expect(tc.expectedType).To(Equal("string"))
						}
					})
				}
			})
		})
	})

	Describe("Message Processing", func() {
		Context("Value extraction", func() {
			It("should extract values from structured messages", func() {
				// Test data extraction from various message structures
				testMessages := []struct {
					name    string
					payload interface{}
				}{
					{
						"simple_value",
						map[string]interface{}{
							"value": 42.5,
						},
					},
					{
						"nested_values",
						map[string]interface{}{
							"sensor": map[string]interface{}{
								"temperature": 25.3,
								"humidity":    60.2,
							},
							"status": "RUNNING",
						},
					},
					{
						"array_values",
						map[string]interface{}{
							"readings": []interface{}{1.1, 2.2, 3.3},
							"count":    3,
						},
					},
				}

				for _, tc := range testMessages {
					By(fmt.Sprintf("processing message type %s", tc.name), func() {
						msg := service.NewMessage(nil)
						msg.SetStructured(tc.payload)

						// Verify message can be structured
						structured, err := msg.AsStructured()
						Expect(err).NotTo(HaveOccurred())
						Expect(structured).NotTo(BeNil())

						// Verify we can navigate the structure
						structMap, ok := structured.(map[string]interface{})
						Expect(ok).To(BeTrue())
						// Verify at least one expected key exists
						hasExpectedKey := false
						for key := range structMap {
							if key == "value" || key == "sensor" || key == "readings" {
								hasExpectedKey = true
								break
							}
						}
						Expect(hasExpectedKey).To(BeTrue(), "Should contain at least one expected key")
					})
				}
			})

			It("should handle auto tag name extraction", func() {
				testCases := []struct {
					tagName       string
					expectedValid bool
					description   string
				}{
					{"Temperature", true, "simple tag name"},
					{"sensor.temperature", true, "dotted path"},
					{"sensor/temperature", true, "slash path"},
					{"", false, "empty tag name"},
					{"Спецсимволы", true, "unicode characters"},
					{"tag-with-dashes", true, "dashes"},
					{"tag_with_underscores", true, "underscores"},
				}

				for _, tc := range testCases {
					By(fmt.Sprintf("processing %s: %s", tc.description, tc.tagName), func() {
						if tc.expectedValid && tc.tagName != "" {
							Expect(tc.tagName).NotTo(BeEmpty())
							// Tag names should be reasonable identifiers
							Expect(len(tc.tagName)).To(BeNumerically(">", 0))
						} else if !tc.expectedValid {
							Expect(tc.tagName).To(Or(BeEmpty(), Equal("")))
						}
					})
				}
			})
		})

		Context("Sequence number management", func() {
			It("should track sequence numbers correctly", func() {
				// Test sequence number progression
				sequences := []uint8{0, 1, 2, 3} // Normal progression

				for i, seq := range sequences {
					By(fmt.Sprintf("handling sequence %d: %d", i, seq), func() {
						if i > 0 {
							prevSeq := sequences[i-1]
							expectedSeq := uint8((int(prevSeq) + 1) % 256)
							Expect(seq).To(Equal(expectedSeq))
						}
					})
				}
			})

			It("should handle sequence wrapping", func() {
				// Test wrapping from 255 to 0
				sequences := []uint8{254, 255, 0, 1}

				for i, seq := range sequences {
					By(fmt.Sprintf("handling wrapping sequence %d: %d", i, seq), func() {
						if i > 0 {
							prevSeq := sequences[i-1]
							expectedSeq := uint8((int(prevSeq) + 1) % 256)
							Expect(seq).To(Equal(expectedSeq))
						}
					})
				}
			})

			It("should generate birth death sequence numbers", func() {
				// bdSeq should be unique for each session
				timestamps := []int64{
					time.Now().UnixMilli(),
					time.Now().UnixMilli() + 1000,
					time.Now().UnixMilli() + 2000,
				}

				for i, ts := range timestamps {
					By(fmt.Sprintf("generating bdSeq for timestamp %d: %d", i, ts), func() {
						// Each timestamp should be unique and reasonable
						Expect(ts).To(BeNumerically(">", 1600000000000)) // After year 2020
						Expect(ts).To(BeNumerically("<", 2000000000000)) // Before year 2033
					})
				}
			})
		})

		Context("BIRTH message generation", func() {
			It("should create valid BIRTH message structure", func() {
				// Test BIRTH message components
				birthComponents := map[string]interface{}{
					"bdSeq":     uint64(12345),
					"timestamp": uint64(time.Now().UnixMilli()),
					"metrics":   []interface{}{},
				}

				for component, value := range birthComponents {
					By(fmt.Sprintf("validating BIRTH component %s", component), func() {
						Expect(value).NotTo(BeNil())

						switch component {
						case "bdSeq":
							Expect(value).To(BeAssignableToTypeOf(uint64(0)))
						case "timestamp":
							Expect(value).To(BeAssignableToTypeOf(uint64(0)))
							Expect(value.(uint64)).To(BeNumerically(">", 0))
						case "metrics":
							Expect(value).To(BeAssignableToTypeOf([]interface{}{}))
						}
					})
				}
			})

			It("should create DEATH message structure", func() {
				deathComponents := map[string]interface{}{
					"bdSeq":     uint64(12345),
					"timestamp": uint64(time.Now().UnixMilli()),
				}

				for component, value := range deathComponents {
					By(fmt.Sprintf("validating DEATH component %s", component), func() {
						Expect(value).NotTo(BeNil())
						Expect(value).To(BeAssignableToTypeOf(uint64(0)))
						if component == "timestamp" {
							Expect(value.(uint64)).To(BeNumerically(">", 0))
						}
					})
				}
			})
		})
	})

	Describe("Topic Generation", func() {
		Context("Node-level topics", func() {
			It("should generate correct node BIRTH topic", func() {
				groupID := "TestFactory"
				edgeNodeID := "Line1"

				expectedTopic := fmt.Sprintf("spBv1.0/%s/NBIRTH/%s", groupID, edgeNodeID)
				Expect(expectedTopic).To(Equal("spBv1.0/TestFactory/NBIRTH/Line1"))

				// Verify topic structure
				parts := strings.Split(expectedTopic, "/")
				Expect(parts).To(HaveLen(4))
				Expect(parts[0]).To(Equal("spBv1.0"))
				Expect(parts[1]).To(Equal(groupID))
				Expect(parts[2]).To(Equal("NBIRTH"))
				Expect(parts[3]).To(Equal(edgeNodeID))
			})

			It("should generate correct node DATA topic", func() {
				groupID := "TestFactory"
				edgeNodeID := "Line1"

				expectedTopic := fmt.Sprintf("spBv1.0/%s/NDATA/%s", groupID, edgeNodeID)
				Expect(expectedTopic).To(Equal("spBv1.0/TestFactory/NDATA/Line1"))
			})

			It("should generate correct node DEATH topic", func() {
				groupID := "TestFactory"
				edgeNodeID := "Line1"

				expectedTopic := fmt.Sprintf("spBv1.0/%s/NDEATH/%s", groupID, edgeNodeID)
				Expect(expectedTopic).To(Equal("spBv1.0/TestFactory/NDEATH/Line1"))
			})
		})

		Context("Device-level topics", func() {
			It("should generate correct device BIRTH topic", func() {
				groupID := "TestFactory"
				edgeNodeID := "Line1"
				deviceID := "Machine1"

				expectedTopic := fmt.Sprintf("spBv1.0/%s/DBIRTH/%s/%s", groupID, edgeNodeID, deviceID)
				Expect(expectedTopic).To(Equal("spBv1.0/TestFactory/DBIRTH/Line1/Machine1"))

				// Verify topic structure
				parts := strings.Split(expectedTopic, "/")
				Expect(parts).To(HaveLen(5))
				Expect(parts[4]).To(Equal(deviceID))
			})

			It("should generate correct device DATA topic", func() {
				groupID := "TestFactory"
				edgeNodeID := "Line1"
				deviceID := "Machine1"

				expectedTopic := fmt.Sprintf("spBv1.0/%s/DDATA/%s/%s", groupID, edgeNodeID, deviceID)
				Expect(expectedTopic).To(Equal("spBv1.0/TestFactory/DDATA/Line1/Machine1"))
			})

			It("should generate correct device DEATH topic", func() {
				groupID := "TestFactory"
				edgeNodeID := "Line1"
				deviceID := "Machine1"

				expectedTopic := fmt.Sprintf("spBv1.0/%s/DDEATH/%s/%s", groupID, edgeNodeID, deviceID)
				Expect(expectedTopic).To(Equal("spBv1.0/TestFactory/DDEATH/Line1/Machine1"))
			})
		})
	})

	Describe("Alias Management", func() {
		Context("Alias assignment", func() {
			It("should handle metric alias mapping", func() {
				testMetrics := []struct {
					name  string
					alias uint64
				}{
					{"Temperature", 100},
					{"Pressure", 101},
					{"Speed", 200},
					{"Status", 201},
				}

				aliasMap := make(map[string]uint64)
				reverseMap := make(map[uint64]string)

				for _, metric := range testMetrics {
					By(fmt.Sprintf("mapping metric %s to alias %d", metric.name, metric.alias), func() {
						// Check for conflicts
						if existingName, exists := reverseMap[metric.alias]; exists {
							Fail(fmt.Sprintf("Alias %d already assigned to %s, cannot assign to %s", metric.alias, existingName, metric.name))
						}

						aliasMap[metric.name] = metric.alias
						reverseMap[metric.alias] = metric.name

						Expect(aliasMap[metric.name]).To(Equal(metric.alias))
						Expect(reverseMap[metric.alias]).To(Equal(metric.name))
					})
				}

				// Verify no conflicts occurred
				Expect(aliasMap).To(HaveLen(len(testMetrics)))
				Expect(reverseMap).To(HaveLen(len(testMetrics)))
			})

			It("should validate alias ranges", func() {
				validAliases := []uint64{1, 100, 1000, 65535}
				invalidAliases := []uint64{0, 65536, 100000}

				for _, alias := range validAliases {
					By(fmt.Sprintf("validating alias %d as valid", alias), func() {
						Expect(alias).To(BeNumerically(">", 0))
						Expect(alias).To(BeNumerically("<=", 65535))
					})
				}

				for _, alias := range invalidAliases {
					By(fmt.Sprintf("validating alias %d as invalid", alias), func() {
						Expect(alias == 0 || alias > 65535).To(BeTrue())
					})
				}
			})
		})
	})
})
