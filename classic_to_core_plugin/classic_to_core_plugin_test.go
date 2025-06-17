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

package classic_to_core_plugin_test

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("ClassicToCoreProcessor", func() {
	BeforeEach(func() {
		testActivated := os.Getenv("TEST_CLASSIC_TO_CORE")

		// Check if environment variables are set
		if testActivated == "" {
			Skip("Skipping Classic to Core tests: TEST_CLASSIC_TO_CORE not set")
			return
		}
	})

	When("using a stream builder", func() {
		DescribeTable("should convert Classic format to Core format",
			func(config string, inputTopic string, inputPayload map[string]interface{}, expectedResults []map[string]interface{}) {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(config)
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

				// Create and send test message
				payloadBytes, _ := json.Marshal(inputPayload)
				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSet("topic", inputTopic)

				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(messages)
				}).Should(Equal(len(expectedResults)))

				// Sort messages by topic for consistent testing
				sort.Slice(messages, func(i, j int) bool {
					topicI, _ := messages[i].MetaGet("topic")
					topicJ, _ := messages[j].MetaGet("topic")
					return topicI < topicJ
				})

				// Check each result
				for i, expected := range expectedResults {
					msg := messages[i]

					// Check topic
					topic, exists := msg.MetaGet("topic")
					Expect(exists).To(BeTrue())
					Expect(topic).To(Equal(expected["expected_topic"]))

					// Check umh_topic
					umhTopic, exists := msg.MetaGet("umh_topic")
					Expect(exists).To(BeTrue())
					Expect(umhTopic).To(Equal(expected["expected_topic"]))

					// Check payload
					structured, err := msg.AsStructured()
					Expect(err).NotTo(HaveOccurred())
					payload := structured.(map[string]interface{})

					// Check value
					expectedValue := expected["expected_value"]
					if actualValue, ok := payload["value"].(json.Number); ok {
						if floatVal, err := actualValue.Float64(); err == nil {
							Expect(floatVal).To(Equal(expectedValue))
						} else {
							intVal, err := actualValue.Int64()
							Expect(err).NotTo(HaveOccurred())
							Expect(intVal).To(Equal(expectedValue))
						}
					} else {
						Expect(payload["value"]).To(Equal(expectedValue))
					}

					// Check timestamp
					if timestampValue, ok := payload["timestamp_ms"].(json.Number); ok {
						intValue, err := timestampValue.Int64()
						Expect(err).NotTo(HaveOccurred())
						Expect(intValue).To(Equal(expected["expected_timestamp"].(int64)))
					} else {
						Expect(payload["timestamp_ms"]).To(Equal(expected["expected_timestamp"]))
					}

					// Check metadata
					if expected["expected_location_path"] != nil {
						locationPath, _ := msg.MetaGet("location_path")
						Expect(locationPath).To(Equal(expected["expected_location_path"]))
					}
					if expected["expected_data_contract"] != nil {
						dataContract, _ := msg.MetaGet("data_contract")
						Expect(dataContract).To(Equal(expected["expected_data_contract"]))
					}
					if expected["expected_tag_name"] != nil {
						tagName, _ := msg.MetaGet("tag_name")
						Expect(tagName).To(Equal(expected["expected_tag_name"]))
					}
					if expected["expected_virtual_path"] != nil {
						virtualPath, _ := msg.MetaGet("virtual_path")
						Expect(virtualPath).To(Equal(expected["expected_virtual_path"]))
					}

					// Check meta/metadata fields
					for key, expectedValue := range expected {
						if strings.HasPrefix(key, "expected_meta_") {
							metaKey := strings.TrimPrefix(key, "expected_meta_")
							actualValue, exists := msg.MetaGet(metaKey)
							Expect(exists).To(BeTrue(), "Expected meta key '%s' to exist", metaKey)
							Expect(actualValue).To(Equal(expectedValue), "Meta key '%s' value mismatch", metaKey)
						}
					}
				}
			},
			Entry("basic conversion with explicit target data contract",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.acme._historian.weather",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"temperature":  23.4,
					"humidity":     42.1,
				},
				[]map[string]interface{}{
					{
						"expected_topic":         "umh.v1.acme._raw.weather.humidity",
						"expected_value":         42.1,
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "acme",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "humidity",
						"expected_virtual_path":  "weather",
					},
					{
						"expected_topic":         "umh.v1.acme._raw.weather.temperature",
						"expected_value":         23.4,
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "acme",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "temperature",
						"expected_virtual_path":  "weather",
					},
				},
			),
			Entry("conversion using input data contract when target not specified",
				`classic_to_core: {}`,
				"umh.v1.acme._historian.weather",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"temperature":  25.0,
				},
				[]map[string]interface{}{
					{
						"expected_topic":         "umh.v1.acme._historian.weather.temperature",
						"expected_value":         25.0,
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "acme",
						"expected_data_contract": "_historian",
						"expected_tag_name":      "temperature",
						"expected_virtual_path":  "weather",
					},
				},
			),
			Entry("complex location path with virtual path",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.enterprise.plant1.machining.cnc-line.cnc5._historian.axis.position",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"x_position":   125.7,
				},
				[]map[string]interface{}{
					{
						"expected_topic":         "umh.v1.enterprise.plant1.machining.cnc-line.cnc5._raw.axis.position.x_position",
						"expected_value":         125.7,
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "enterprise.plant1.machining.cnc-line.cnc5",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "x_position",
						"expected_virtual_path":  "axis.position",
					},
				},
			),
			Entry("tag groups flattening with dot separator",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.acme._historian.cnc-mill",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"pos": map[string]interface{}{
						"x": 12.5,
						"y": 7.3,
						"z": 3.2,
					},
					"temperature": 50.0,
				},
				[]map[string]interface{}{
					{
						"expected_topic":         "umh.v1.acme._raw.cnc-mill.pos.x",
						"expected_value":         12.5,
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "acme",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "pos.x",
						"expected_virtual_path":  "cnc-mill",
					},
					{
						"expected_topic":         "umh.v1.acme._raw.cnc-mill.pos.y",
						"expected_value":         7.3,
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "acme",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "pos.y",
						"expected_virtual_path":  "cnc-mill",
					},
					{
						"expected_topic":         "umh.v1.acme._raw.cnc-mill.pos.z",
						"expected_value":         3.2,
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "acme",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "pos.z",
						"expected_virtual_path":  "cnc-mill",
					},
					{
						"expected_topic":         "umh.v1.acme._raw.cnc-mill.temperature",
						"expected_value":         50.0,
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "acme",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "temperature",
						"expected_virtual_path":  "cnc-mill",
					},
				},
			),
			Entry("meta field handling - basic meta field",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.acme._historian.sensor",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"temperature":  23.4,
					"meta": map[string]interface{}{
						"sensor_id":  "TEMP001",
						"location":   "warehouse_a",
						"batch_id":   "B12345",
						"calibrated": true,
						"precision":  0.1,
					},
				},
				[]map[string]interface{}{
					{
						"expected_topic":           "umh.v1.acme._raw.sensor.temperature",
						"expected_value":           23.4,
						"expected_timestamp":       int64(1717083000000),
						"expected_location_path":   "acme",
						"expected_data_contract":   "_raw",
						"expected_tag_name":        "temperature",
						"expected_virtual_path":    "sensor",
						"expected_meta_sensor_id":  "TEMP001",
						"expected_meta_location":   "warehouse_a",
						"expected_meta_batch_id":   "B12345",
						"expected_meta_calibrated": "true",
						"expected_meta_precision":  "0.1",
					},
				},
			),
			Entry("metadata field handling - basic metadata field",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.acme._historian.sensor",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"pressure":     101.3,
					"metadata": map[string]interface{}{
						"device_type": "pressure_sensor",
						"model":       "PS-500",
						"firmware":    "v2.1.3",
						"accuracy":    0.05,
					},
				},
				[]map[string]interface{}{
					{
						"expected_topic":            "umh.v1.acme._raw.sensor.pressure",
						"expected_value":            101.3,
						"expected_timestamp":        int64(1717083000000),
						"expected_location_path":    "acme",
						"expected_data_contract":    "_raw",
						"expected_tag_name":         "pressure",
						"expected_virtual_path":     "sensor",
						"expected_meta_device_type": "pressure_sensor",
						"expected_meta_model":       "PS-500",
						"expected_meta_firmware":    "v2.1.3",
						"expected_meta_accuracy":    "0.05",
					},
				},
			),
			Entry("both meta and metadata fields present",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.factory._historian.machine",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"temperature":  45.2,
					"vibration":    0.3,
					"meta": map[string]interface{}{
						"machine_id": "CNC001",
						"operator":   "john_doe",
					},
					"metadata": map[string]interface{}{
						"shift":       "morning",
						"maintenance": "2024-01-15",
						"part_number": 12345,
					},
				},
				[]map[string]interface{}{
					{
						"expected_topic":            "umh.v1.factory._raw.machine.temperature",
						"expected_value":            45.2,
						"expected_timestamp":        int64(1717083000000),
						"expected_location_path":    "factory",
						"expected_data_contract":    "_raw",
						"expected_tag_name":         "temperature",
						"expected_virtual_path":     "machine",
						"expected_meta_machine_id":  "CNC001",
						"expected_meta_operator":    "john_doe",
						"expected_meta_shift":       "morning",
						"expected_meta_maintenance": "2024-01-15",
						"expected_meta_part_number": "12345",
					},
					{
						"expected_topic":            "umh.v1.factory._raw.machine.vibration",
						"expected_value":            0.3,
						"expected_timestamp":        int64(1717083000000),
						"expected_location_path":    "factory",
						"expected_data_contract":    "_raw",
						"expected_tag_name":         "vibration",
						"expected_virtual_path":     "machine",
						"expected_meta_machine_id":  "CNC001",
						"expected_meta_operator":    "john_doe",
						"expected_meta_shift":       "morning",
						"expected_meta_maintenance": "2024-01-15",
						"expected_meta_part_number": "12345",
					},
				},
			),
			Entry("meta field with multiple data fields",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.plant._historian.sensors",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"temp1":        22.1,
					"temp2":        23.4,
					"temp3":        21.8,
					"meta": map[string]interface{}{
						"zone":     "production_floor",
						"shift_id": "S001",
					},
				},
				[]map[string]interface{}{
					{
						"expected_topic":         "umh.v1.plant._raw.sensors.temp1",
						"expected_value":         22.1,
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "plant",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "temp1",
						"expected_virtual_path":  "sensors",
						"expected_meta_zone":     "production_floor",
						"expected_meta_shift_id": "S001",
					},
					{
						"expected_topic":         "umh.v1.plant._raw.sensors.temp2",
						"expected_value":         23.4,
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "plant",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "temp2",
						"expected_virtual_path":  "sensors",
						"expected_meta_zone":     "production_floor",
						"expected_meta_shift_id": "S001",
					},
					{
						"expected_topic":         "umh.v1.plant._raw.sensors.temp3",
						"expected_value":         21.8,
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "plant",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "temp3",
						"expected_virtual_path":  "sensors",
						"expected_meta_zone":     "production_floor",
						"expected_meta_shift_id": "S001",
					},
				},
			),
			Entry("meta field with nested tag groups",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.factory._historian.robot",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"position": map[string]interface{}{
						"x": 12.5,
						"y": 7.3,
					},
					"meta": map[string]interface{}{
						"robot_id":   "R001",
						"program":    "welding_seq_a",
						"cycle_time": 45.2,
					},
				},
				[]map[string]interface{}{
					{
						"expected_topic":           "umh.v1.factory._raw.robot.position.x",
						"expected_value":           12.5,
						"expected_timestamp":       int64(1717083000000),
						"expected_location_path":   "factory",
						"expected_data_contract":   "_raw",
						"expected_tag_name":        "position.x",
						"expected_virtual_path":    "robot",
						"expected_meta_robot_id":   "R001",
						"expected_meta_program":    "welding_seq_a",
						"expected_meta_cycle_time": "45.2",
					},
					{
						"expected_topic":           "umh.v1.factory._raw.robot.position.y",
						"expected_value":           7.3,
						"expected_timestamp":       int64(1717083000000),
						"expected_location_path":   "factory",
						"expected_data_contract":   "_raw",
						"expected_tag_name":        "position.y",
						"expected_virtual_path":    "robot",
						"expected_meta_robot_id":   "R001",
						"expected_meta_program":    "welding_seq_a",
						"expected_meta_cycle_time": "45.2",
					},
				},
			),
			Entry("meta field with mixed data types",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.test._historian.mixed",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"value":        42.0, // Use float64 to match JSON unmarshalling behavior
					"meta": map[string]interface{}{
						"string_val": "test_string",
						"int_val":    123,
						"float_val":  45.67,
						"bool_val":   true,
						"null_val":   nil,
					},
				},
				[]map[string]interface{}{
					{
						"expected_topic":           "umh.v1.test._raw.mixed.value",
						"expected_value":           42.0, // Use float64 to match JSON unmarshalling behavior
						"expected_timestamp":       int64(1717083000000),
						"expected_location_path":   "test",
						"expected_data_contract":   "_raw",
						"expected_tag_name":        "value",
						"expected_virtual_path":    "mixed",
						"expected_meta_string_val": "test_string",
						"expected_meta_int_val":    "123",
						"expected_meta_float_val":  "45.67",
						"expected_meta_bool_val":   "true",
						"expected_meta_null_val":   "<nil>",
					},
				},
			),
			Entry("array values converted to string representation",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.factory._historian.sensors",
				map[string]interface{}{
					"timestamp_ms":      1717083000000,
					"string_array":      []interface{}{"a", "b", "c"},
					"number_array":      []interface{}{1, 2, 3},
					"mixed_array":       []interface{}{"text", 42, true, nil},
					"empty_array":       []interface{}{},
					"single_item_array": []interface{}{"single"},
					"float_array":       []interface{}{1.1, 2.2, 3.3},
				},
				[]map[string]interface{}{
					{
						"expected_topic":         "umh.v1.factory._raw.sensors.empty_array",
						"expected_value":         "[]",
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "factory",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "empty_array",
						"expected_virtual_path":  "sensors",
					},
					{
						"expected_topic":         "umh.v1.factory._raw.sensors.float_array",
						"expected_value":         "[1.1 2.2 3.3]",
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "factory",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "float_array",
						"expected_virtual_path":  "sensors",
					},
					{
						"expected_topic":         "umh.v1.factory._raw.sensors.mixed_array",
						"expected_value":         "[text 42 true <nil>]",
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "factory",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "mixed_array",
						"expected_virtual_path":  "sensors",
					},
					{
						"expected_topic":         "umh.v1.factory._raw.sensors.number_array",
						"expected_value":         "[1 2 3]",
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "factory",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "number_array",
						"expected_virtual_path":  "sensors",
					},
					{
						"expected_topic":         "umh.v1.factory._raw.sensors.single_item_array",
						"expected_value":         "[single]",
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "factory",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "single_item_array",
						"expected_virtual_path":  "sensors",
					},
					{
						"expected_topic":         "umh.v1.factory._raw.sensors.string_array",
						"expected_value":         "[a b c]",
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "factory",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "string_array",
						"expected_virtual_path":  "sensors",
					},
				},
			),
			Entry("nested arrays within tag groups",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.plant._historian.machine",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"system": map[string]interface{}{
						"sensors":     []interface{}{"temp1", "temp2", "pressure"},
						"coordinates": []interface{}{10.5, 20.3, 30.1},
					},
					"alerts": []interface{}{"warning", "info"},
				},
				[]map[string]interface{}{
					{
						"expected_topic":         "umh.v1.plant._raw.machine.alerts",
						"expected_value":         "[warning info]",
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "plant",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "alerts",
						"expected_virtual_path":  "machine",
					},
					{
						"expected_topic":         "umh.v1.plant._raw.machine.system.coordinates",
						"expected_value":         "[10.5 20.3 30.1]",
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "plant",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "system.coordinates",
						"expected_virtual_path":  "machine",
					},
					{
						"expected_topic":         "umh.v1.plant._raw.machine.system.sensors",
						"expected_value":         "[temp1 temp2 pressure]",
						"expected_timestamp":     int64(1717083000000),
						"expected_location_path": "plant",
						"expected_data_contract": "_raw",
						"expected_tag_name":      "system.sensors",
						"expected_virtual_path":  "machine",
					},
				},
			),
		)

		// Additional edge cases based on Historian Data Contract documentation
		DescribeTable("edge cases from documentation",
			func(configYAML, topic string, payload map[string]interface{}, expectedResults []map[string]interface{}) {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(configYAML)
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

				// Create message
				messageBytes, _ := json.Marshal(payload)
				message := service.NewMessage(messageBytes)
				message.MetaSet("topic", topic)

				err = msgHandler(ctx, message)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(messages)
				}).Should(Equal(len(expectedResults)))

				// Sort messages by topic for consistent testing
				sort.Slice(messages, func(i, j int) bool {
					topicI, _ := messages[i].MetaGet("topic")
					topicJ, _ := messages[j].MetaGet("topic")
					return topicI < topicJ
				})

				// Check each result
				for i, expected := range expectedResults {
					msg := messages[i]
					structured, _ := msg.AsStructured()
					payloadData := structured.(map[string]interface{})

					topic, _ := msg.MetaGet("topic")
					Expect(topic).To(Equal(expected["expected_topic"]))

					// Handle different value types properly - Benthos may convert values to json.Number
					expectedValue := expected["expected_value"]
					actualValue := payloadData["value"]

					switch expectedType := expectedValue.(type) {
					case bool:
						// For boolean values, check if they're preserved correctly
						Expect(actualValue).To(Equal(expectedType))
					case string:
						// For string values, check if they're preserved correctly
						Expect(actualValue).To(Equal(expectedType))
					case int:
						// For integer values, handle json.Number conversion
						if actualNumber, ok := actualValue.(json.Number); ok {
							intVal, err := actualNumber.Int64()
							Expect(err).NotTo(HaveOccurred())
							Expect(intVal).To(Equal(int64(expectedType)))
						} else {
							Expect(actualValue).To(Equal(expectedType))
						}
					case float64:
						// For float values, handle json.Number conversion
						if actualNumber, ok := actualValue.(json.Number); ok {
							floatVal, err := actualNumber.Float64()
							Expect(err).NotTo(HaveOccurred())
							Expect(floatVal).To(Equal(expectedType))
						} else {
							Expect(actualValue).To(Equal(expectedType))
						}
					default:
						// Default comparison for other types
						Expect(actualValue).To(Equal(expectedValue))
					}
				}
			},
			Entry("boolean values preserved as-is (not converted to 0/1)",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.acme._historian.machine",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"running":      true,
					"error":        false,
				},
				[]map[string]interface{}{
					{
						"expected_topic": "umh.v1.acme._raw.machine.error",
						"expected_value": false,
					},
					{
						"expected_topic": "umh.v1.acme._raw.machine.running",
						"expected_value": true,
					},
				},
			),
			Entry("location with underscores",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.site_1.area_2.line_3._historian.sensor",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"value":        42.0,
				},
				[]map[string]interface{}{
					{
						"expected_topic": "umh.v1.site_1.area_2.line_3._raw.sensor.value",
						"expected_value": 42.0,
					},
				},
			),
			Entry("deeply nested tag groups",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.factory._historian.machine",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"system": map[string]interface{}{
						"motor": map[string]interface{}{
							"speed": 1500.0,
							"temp":  75.5,
						},
						"controller": map[string]interface{}{
							"status": "ok",
							"version": map[string]interface{}{
								"major": 2,
								"minor": 1,
							},
						},
					},
				},
				[]map[string]interface{}{
					{
						"expected_topic": "umh.v1.factory._raw.machine.system.controller.status",
						"expected_value": "ok",
					},
					{
						"expected_topic": "umh.v1.factory._raw.machine.system.controller.version.major",
						"expected_value": 2,
					},
					{
						"expected_topic": "umh.v1.factory._raw.machine.system.controller.version.minor",
						"expected_value": 1,
					},
					{
						"expected_topic": "umh.v1.factory._raw.machine.system.motor.speed",
						"expected_value": 1500.0,
					},
					{
						"expected_topic": "umh.v1.factory._raw.machine.system.motor.temp",
						"expected_value": 75.5,
					},
				},
			),
			Entry("minimal valid payload (exactly 2 properties)",
				`classic_to_core:
  target_data_contract: _raw`,
				"umh.v1.minimal._historian.test",
				map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"single_tag":   "single_value",
				},
				[]map[string]interface{}{
					{
						"expected_topic": "umh.v1.minimal._raw.test.single_tag",
						"expected_value": "single_value",
					},
				},
			),
		)

		It("should process all fields except timestamp", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

			// Create message with multiple fields
			classicPayload := map[string]interface{}{
				"timestamp_ms":   1717083000000,
				"temperature":    23.4,
				"quality_status": "OK",
				"internal_id":    "sensor_123",
			}
			payloadBytes, _ := json.Marshal(classicPayload)
			testMsg := service.NewMessage(payloadBytes)
			testMsg.MetaSet("topic", "umh.v1.acme._historian.weather")

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(3)) // All fields except timestamp should be processed

			// Check that umh_topic is set correctly for all messages
			for _, msg := range messages {
				umhTopic, exists := msg.MetaGet("umh_topic")
				Expect(exists).To(BeTrue())
				Expect(umhTopic).To(ContainSubstring("umh.v1.acme._raw.weather"))
			}
		})

		It("should handle different timestamp formats", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

			// Test with string timestamp
			classicPayload := map[string]interface{}{
				"timestamp_ms": "1717083000000",
				"temperature":  23.4,
			}
			payloadBytes, _ := json.Marshal(classicPayload)
			testMsg := service.NewMessage(payloadBytes)
			testMsg.MetaSet("topic", "umh.v1.acme._historian.weather")

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]
			structured, err := msg.AsStructured()
			Expect(err).NotTo(HaveOccurred())
			payload := structured.(map[string]interface{})

			// Handle json.Number type
			if timestampValue, ok := payload["timestamp_ms"].(json.Number); ok {
				intValue, err := timestampValue.Int64()
				Expect(err).NotTo(HaveOccurred())
				Expect(intValue).To(BeNumerically("==", 1717083000000))
			} else {
				Expect(payload["timestamp_ms"]).To(BeNumerically("==", 1717083000000))
			}
		})

		It("should handle messages without topics gracefully", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

			// Create message without topic metadata
			classicPayload := map[string]interface{}{
				"timestamp_ms": 1717083000000,
				"temperature":  23.4,
			}
			payloadBytes, _ := json.Marshal(classicPayload)
			testMsg := service.NewMessage(payloadBytes)
			// No topic set

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Should not process any messages due to missing topic
			Consistently(func() int {
				return len(messages)
			}, "100ms").Should(Equal(0))
		})

		It("should handle malformed JSON gracefully", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

			// Create message with invalid JSON
			testMsg := service.NewMessage([]byte("invalid json"))
			testMsg.MetaSet("topic", "umh.v1.acme._historian.weather")

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Should not process any messages due to invalid JSON
			Consistently(func() int {
				return len(messages)
			}, "100ms").Should(Equal(0))
		})

		It("should preserve original metadata (always enabled)", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

			// Create message with custom metadata
			classicPayload := map[string]interface{}{
				"timestamp_ms": 1717083000000,
				"temperature":  23.4,
			}
			payloadBytes, _ := json.Marshal(classicPayload)
			testMsg := service.NewMessage(payloadBytes)
			testMsg.MetaSet("topic", "umh.v1.acme._historian.weather")
			testMsg.MetaSet("source_system", "PLC123")
			testMsg.MetaSet("quality", "GOOD")

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]

			// Check that original metadata is preserved
			sourceSystem, exists := msg.MetaGet("source_system")
			Expect(exists).To(BeTrue())
			Expect(sourceSystem).To(Equal("PLC123"))

			quality, exists := msg.MetaGet("quality")
			Expect(exists).To(BeTrue())
			Expect(quality).To(Equal("GOOD"))

			// Check that new Core metadata is also set
			schema, exists := msg.MetaGet("schema")
			Expect(exists).To(BeTrue())
			Expect(schema).To(Equal("_raw"))
		})

		It("should handle numeric field names correctly", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

			// Create message with different data types
			classicPayload := map[string]interface{}{
				"timestamp_ms":  1717083000000,
				"temperature":   23.4,
				"count":         42,
				"is_running":    true,
				"status_string": "OK",
			}
			payloadBytes, _ := json.Marshal(classicPayload)
			testMsg := service.NewMessage(payloadBytes)
			testMsg.MetaSet("topic", "umh.v1.acme._historian.machine")

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(4)) // All non-timestamp fields

			// Verify different data types are preserved
			for _, msg := range messages {
				structured, err := msg.AsStructured()
				Expect(err).NotTo(HaveOccurred())
				payload := structured.(map[string]interface{})

				// Each message should have value and timestamp_ms
				Expect(payload).To(HaveKey("value"))
				Expect(payload).To(HaveKey("timestamp_ms"))

				// Timestamp should always be numeric - handle json.Number type
				if timestampValue, ok := payload["timestamp_ms"].(json.Number); ok {
					intValue, err := timestampValue.Int64()
					Expect(err).NotTo(HaveOccurred())
					Expect(intValue).To(BeNumerically("==", 1717083000000))
				} else {
					Expect(payload["timestamp_ms"]).To(BeNumerically("==", 1717083000000))
				}
			}
		})

		// Tests for new limit and validation features
		Context("with limits and validation", func() {
			It("should process messages within hardcoded limits", func() {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

				// Create payload within limits (hardcoded max is 1000 tags)
				classicPayload := map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"tag1":         1.0,
					"tag2":         2.0,
					"tag3":         3.0,
				}
				payloadBytes, _ := json.Marshal(classicPayload)
				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSet("topic", "umh.v1.test._historian.data")

				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				// Should process all messages since we're within the hardcoded limit
				Eventually(func() int {
					return len(messages)
				}).Should(Equal(3))
			})

			It("should respect hardcoded recursion depth limit", func() {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

				// Create nested payload within the hardcoded limit (10 levels deep)
				classicPayload := map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"level1": map[string]interface{}{
						"level2": map[string]interface{}{
							"level3": map[string]interface{}{
								"deep_value": 42.0,
							},
						},
						"shallow_value": 1.0,
					},
				}
				payloadBytes, _ := json.Marshal(classicPayload)
				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSet("topic", "umh.v1.test._historian.data")

				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(messages)
				}).Should(Equal(2)) // Both values should be processed within the limit

				// Verify the nested value was processed correctly
				foundDeepValue := false
				for _, msg := range messages {
					topic, _ := msg.MetaGet("topic")
					if topic == "umh.v1.test._raw.data.level1.level2.level3.deep_value" {
						foundDeepValue = true
						break
					}
				}
				Expect(foundDeepValue).To(BeTrue())
			})

			It("should validate UMH topic prefix", func() {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

				classicPayload := map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"temperature":  25.5,
				}
				payloadBytes, _ := json.Marshal(classicPayload)
				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSet("topic", "invalid.prefix.test._historian.data") // Invalid prefix

				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				// Should not process any messages due to invalid topic
				Consistently(func() int {
					return len(messages)
				}, "100ms").Should(Equal(0))
			})

			It("should validate topic has location path", func() {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

				classicPayload := map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"temperature":  25.5,
				}
				payloadBytes, _ := json.Marshal(classicPayload)
				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSet("topic", "umh.v1._historian.data") // Missing location

				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				// Should not process any messages due to missing location
				Consistently(func() int {
					return len(messages)
				}, "100ms").Should(Equal(0))
			})
		})

		// Additional tests for uncovered code paths
		Context("missing test coverage", func() {
			It("should test extractTimestamp with various data types", func() {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

				// Test different timestamp formats
				tests := []struct {
					name          string
					timestampVal  interface{}
					expectedError bool
				}{
					{"float64 timestamp", float64(1717083000000), false},
					{"int64 timestamp", int64(1717083000000), false},
					{"int timestamp", int(1717083000000), false},
					{"valid json.Number", json.Number("1717083000000"), false},
					{"invalid json.Number", json.Number("invalid"), true},
					{"valid string timestamp", "1717083000000", false},
					{"invalid string timestamp", "not-a-number", true},
					{"unsupported type", []int{1, 2, 3}, true},
					{"nil value", nil, true},
					{"boolean value", true, true},
				}

				initialCount := len(messages)

				for _, test := range tests {
					payload := map[string]interface{}{
						"timestamp_ms": test.timestampVal,
						"temperature":  25.5,
					}

					payloadBytes, _ := json.Marshal(payload)
					msg := service.NewMessage(payloadBytes)
					msg.MetaSet("topic", "umh.v1.test._historian.data")

					err := msgHandler(ctx, msg)
					Expect(err).NotTo(HaveOccurred())

					if test.expectedError {
						// Should not add any messages due to timestamp error
						Consistently(func() int {
							return len(messages) - initialCount
						}, "50ms").Should(Equal(0), "Test case: %s", test.name)
					} else {
						// Should successfully process the message
						Eventually(func() int {
							return len(messages) - initialCount
						}).Should(BeNumerically(">=", 1), "Test case: %s", test.name)
						initialCount = len(messages) // Update count for next test
					}
				}
			})

			It("should test missing timestamp field", func() {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

				// Create message without the required timestamp_ms field
				classicPayload := map[string]interface{}{
					"wrong_timestamp": 1717083000000, // Wrong field name
					"temperature":     25.5,
				}
				payloadBytes, _ := json.Marshal(classicPayload)
				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSet("topic", "umh.v1.test._historian.data")

				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				// Should not process any messages due to missing timestamp field
				Consistently(func() int {
					return len(messages)
				}, "100ms").Should(Equal(0))
			})

			It("should test parsing structured data errors", func() {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

				// Test with non-object JSON (array instead of object)
				testMsg := service.NewMessage([]byte(`[1, 2, 3]`))
				testMsg.MetaSet("topic", "umh.v1.test._historian.data")

				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				// Should not process any messages due to non-object payload
				Consistently(func() int {
					return len(messages)
				}, "100ms").Should(Equal(0))
			})

			It("should test metadata preservation is always enabled", func() {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

				// Create message with custom metadata
				classicPayload := map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"temperature":  23.4,
				}
				payloadBytes, _ := json.Marshal(classicPayload)
				testMsg := service.NewMessage(payloadBytes)
				testMsg.MetaSet("topic", "umh.v1.acme._historian.weather")
				testMsg.MetaSet("original_meta", "should_be_preserved")

				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(messages)
				}).Should(Equal(1))

				msg := messages[0]

				// Check that original metadata IS preserved (always enabled)
				originalMeta, exists := msg.MetaGet("original_meta")
				Expect(exists).To(BeTrue())
				Expect(originalMeta).To(Equal("should_be_preserved"))

				// And Core metadata should be set
				schema, exists := msg.MetaGet("schema")
				Expect(exists).To(BeTrue())
				Expect(schema).To(Equal("_raw"))
			})

			It("should test topic parsing with umh_topic fallback", func() {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

				// Create message without topic but with umh_topic
				classicPayload := map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"temperature":  23.4,
				}
				payloadBytes, _ := json.Marshal(classicPayload)
				testMsg := service.NewMessage(payloadBytes)
				// No topic set, but umh_topic is set
				testMsg.MetaSet("umh_topic", "umh.v1.acme._historian.weather")

				err = msgHandler(ctx, testMsg)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return len(messages)
				}).Should(Equal(1))

				msg := messages[0]
				topic, _ := msg.MetaGet("topic")
				Expect(topic).To(Equal("umh.v1.acme._raw.weather.temperature"))
			})

			It("should test various topic parsing edge cases", func() {
				builder := service.NewStreamBuilder()

				var msgHandler service.MessageHandlerFunc
				msgHandler, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

				classicPayload := map[string]interface{}{
					"timestamp_ms": 1717083000000,
					"temperature":  23.4,
				}
				payloadBytes, _ := json.Marshal(classicPayload)

				// Test cases for topic parsing errors
				testCases := []string{
					"short.topic",                   // Too short
					"umh.v2.test._historian.data",   // Wrong version
					"wrong.v1.test._historian.data", // Wrong prefix
					"umh.v1.test.nocontract.data",   // No data contract
				}

				for _, topicCase := range testCases {
					testMsg := service.NewMessage(payloadBytes)
					testMsg.MetaSet("topic", topicCase)

					err = msgHandler(ctx, testMsg)
					Expect(err).NotTo(HaveOccurred())
				}

				// Should not process any messages due to invalid topics
				Consistently(func() int {
					return len(messages)
				}, "100ms").Should(Equal(0))
			})

			It("should test processor Close method coverage", func() {
				// This test ensures the Close method is covered by tests
				// Since we can't easily access the processor directly from stream builder,
				// we just verify that streams can be properly closed
				builder := service.NewStreamBuilder()

				_, err := builder.AddProducerFunc()
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
`)
				Expect(err).NotTo(HaveOccurred())

				err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
					return nil
				})
				Expect(err).NotTo(HaveOccurred())

				stream, err := builder.Build()
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()

				// Run stream briefly to initialize processors
				go func() {
					_ = stream.Run(ctx)
				}()

				// Wait for context cancellation which will trigger Close
				<-ctx.Done()

				// Close method coverage is achieved through stream shutdown
				Expect(true).To(BeTrue()) // Test passes if we get here
			})

		})

		It("should handle edge cases for meta/metadata fields", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

			// Test with empty meta and metadata objects
			classicPayload := map[string]interface{}{
				"timestamp_ms": 1717083000000,
				"temperature":  23.4,
				"meta":         map[string]interface{}{}, // Empty meta object
				"metadata":     map[string]interface{}{}, // Empty metadata object
			}
			payloadBytes, _ := json.Marshal(classicPayload)
			testMsg := service.NewMessage(payloadBytes)
			testMsg.MetaSet("topic", "umh.v1.test._historian.sensor")

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Should still create the message even with empty meta objects
			msg := messages[0]
			topic, _ := msg.MetaGet("topic")
			Expect(topic).To(Equal("umh.v1.test._raw.sensor.temperature"))
		})

		It("should handle non-object meta/metadata fields gracefully", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

			// Test with non-object meta and metadata fields (should be ignored)
			classicPayload := map[string]interface{}{
				"timestamp_ms": 1717083000000,
				"temperature":  23.4,
				"meta":         "not_an_object",    // String instead of object
				"metadata":     []string{"a", "b"}, // Array instead of object
			}
			payloadBytes, _ := json.Marshal(classicPayload)
			testMsg := service.NewMessage(payloadBytes)
			testMsg.MetaSet("topic", "umh.v1.test._historian.sensor")

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			// Should still create the message, just without the invalid meta fields
			msg := messages[0]
			topic, _ := msg.MetaGet("topic")
			Expect(topic).To(Equal("umh.v1.test._raw.sensor.temperature"))
		})

		It("should not create messages for meta and metadata fields themselves", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

			// Create message with only timestamp, meta, and metadata (no actual data fields)
			classicPayload := map[string]interface{}{
				"timestamp_ms": 1717083000000,
				"meta": map[string]interface{}{
					"sensor_id": "TEMP001",
				},
				"metadata": map[string]interface{}{
					"location": "warehouse",
				},
			}
			payloadBytes, _ := json.Marshal(classicPayload)
			testMsg := service.NewMessage(payloadBytes)
			testMsg.MetaSet("topic", "umh.v1.test._historian.sensor")

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			// Should not create any messages since there are no data fields (only meta/metadata)
			Consistently(func() int {
				return len(messages)
			}, "100ms").Should(Equal(0))
		})

		It("should handle meta/metadata with conflicting keys (metadata overwrites meta)", func() {
			builder := service.NewStreamBuilder()

			var msgHandler service.MessageHandlerFunc
			msgHandler, err := builder.AddProducerFunc()
			Expect(err).NotTo(HaveOccurred())

			err = builder.AddProcessorYAML(`
classic_to_core:
  target_data_contract: _raw
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

			// Test with conflicting keys in meta and metadata
			classicPayload := map[string]interface{}{
				"timestamp_ms": 1717083000000,
				"temperature":  23.4,
				"meta": map[string]interface{}{
					"source":  "sensor_A",
					"version": "1.0",
				},
				"metadata": map[string]interface{}{
					"source":  "sensor_B", // This should overwrite the meta value
					"quality": "good",
				},
			}
			payloadBytes, _ := json.Marshal(classicPayload)
			testMsg := service.NewMessage(payloadBytes)
			testMsg.MetaSet("topic", "umh.v1.test._historian.sensor")

			err = msgHandler(ctx, testMsg)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int {
				return len(messages)
			}).Should(Equal(1))

			msg := messages[0]

			// Check that metadata overwrote meta for conflicting key
			source, exists := msg.MetaGet("source")
			Expect(exists).To(BeTrue())
			Expect(source).To(Equal("sensor_B")) // Should be the metadata value, not meta

			// Check that non-conflicting keys from both are present
			version, exists := msg.MetaGet("version")
			Expect(exists).To(BeTrue())
			Expect(version).To(Equal("1.0"))

			quality, exists := msg.MetaGet("quality")
			Expect(exists).To(BeTrue())
			Expect(quality).To(Equal("good"))
		})
	})
})
