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

package downsampler_plugin

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Downsampler Processor - UMH Classic Format", func() {
	Describe("Per-key filtering functionality", func() {
		type testCase struct {
			name           string
			messages       []map[string]interface{}
			expectedOutput []map[string]interface{}
			description    string
		}

		DescribeTable("UMH Classic format processing",
			func(tc testCase) {
				// Create fresh processor for each test case to avoid state contamination
				config := DownsamplerConfig{
					Algorithm: "deadband",
					Threshold: 2.0, // Default threshold
					TopicThresholds: []TopicThreshold{
						{Pattern: "*.temperature", Threshold: 0.5},
						{Pattern: "*.humidity", Threshold: 1.0},
						{Pattern: "*.pressure", Threshold: 5.0},
					},
				}

				resources := service.MockResources()
				processor, err := newDownsamplerProcessor(config, resources.Logger(), resources.Metrics())
				Expect(err).NotTo(HaveOccurred())

				var results []map[string]interface{}

				for _, msgData := range tc.messages {
					// Create message with UMH classic _historian metadata
					msg := service.NewMessage(nil)
					msg.MetaSet("data_contract", "_historian")
					msg.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
					msg.SetStructured(msgData)

					// Process the message
					batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
					Expect(err).NotTo(HaveOccurred())

					// Extract results
					for _, batch := range batches {
						for _, processedMsg := range batch {
							data, err := processedMsg.AsStructured()
							Expect(err).NotTo(HaveOccurred())
							results = append(results, data.(map[string]interface{}))
						}
					}
				}

				// Verify results
				Expect(results).To(HaveLen(len(tc.expectedOutput)),
					"Expected %d messages, got %d for case: %s",
					len(tc.expectedOutput), len(results), tc.description)

				for i, expected := range tc.expectedOutput {
					if i < len(results) {
						Expect(results[i]).To(Equal(expected),
							"Message %d mismatch for case: %s", i, tc.description)
					}
				}
			},
			Entry("first message - all fields kept", testCase{
				name: "first_message_all_fields_kept",
				messages: []map[string]interface{}{
					{
						"temperature":  25.0,
						"humidity":     60.0,
						"pressure":     1000.0,
						"status":       "RUNNING",
						"timestamp_ms": int64(1000),
					},
				},
				expectedOutput: []map[string]interface{}{
					{
						"temperature":  25.0,
						"humidity":     60.0,
						"pressure":     1000.0,
						"status":       "RUNNING",
						"timestamp_ms": int64(1000),
					},
				},
				description: "First message should keep all fields",
			}),
			Entry("partial field filtering", testCase{
				name: "partial_field_filtering",
				messages: []map[string]interface{}{
					{
						"temperature":  25.0,
						"humidity":     60.0,
						"pressure":     1000.0,
						"status":       "RUNNING",
						"timestamp_ms": int64(1000),
					},
					{
						"temperature":  25.2,      // 0.2°C change < 0.5°C threshold - DROP
						"humidity":     61.5,      // 1.5% change > 1.0% threshold - KEEP
						"pressure":     1002.0,    // 2.0 Pa change < 5.0 Pa threshold - DROP
						"status":       "RUNNING", // String unchanged - DROP
						"timestamp_ms": int64(2000),
					},
				},
				expectedOutput: []map[string]interface{}{
					{
						"temperature":  25.0,
						"humidity":     60.0,
						"pressure":     1000.0,
						"status":       "RUNNING",
						"timestamp_ms": int64(1000),
					},
					{
						"humidity":     61.5,
						"timestamp_ms": int64(2000),
					},
				},
				description: "Second message should only keep humidity field",
			}),
			Entry("all fields filtered - message dropped", testCase{
				name: "all_fields_filtered_message_dropped",
				messages: []map[string]interface{}{
					{
						"temperature":  25.0,
						"humidity":     60.0,
						"pressure":     1000.0,
						"timestamp_ms": int64(1000),
					},
					{
						"temperature":  25.2,   // 0.2°C change < 0.5°C threshold - DROP
						"humidity":     60.3,   // 0.3% change < 1.0% threshold - DROP
						"pressure":     1003.0, // 3.0 Pa change < 5.0 Pa threshold - DROP
						"timestamp_ms": int64(2000),
					},
				},
				expectedOutput: []map[string]interface{}{
					{
						"temperature":  25.0,
						"humidity":     60.0,
						"pressure":     1000.0,
						"timestamp_ms": int64(1000),
					},
					// Second message completely dropped
				},
				description: "Message should be dropped if no fields meet thresholds",
			}),
			Entry("boolean and string handling", testCase{
				name: "boolean_and_string_handling",
				messages: []map[string]interface{}{
					{
						"machine_enabled": true,
						"status":          "RUNNING",
						"timestamp_ms":    int64(1000),
					},
					{
						"machine_enabled": false,     // Boolean change - KEEP
						"status":          "RUNNING", // String unchanged - DROP
						"timestamp_ms":    int64(2000),
					},
					{
						"machine_enabled": false,  // Boolean unchanged - DROP
						"status":          "IDLE", // String changed - KEEP
						"timestamp_ms":    int64(3000),
					},
				},
				expectedOutput: []map[string]interface{}{
					{
						"machine_enabled": true,
						"status":          "RUNNING",
						"timestamp_ms":    int64(1000),
					},
					{
						"machine_enabled": false,
						"timestamp_ms":    int64(2000),
					},
					{
						"status":       "IDLE",
						"timestamp_ms": int64(3000),
					},
				},
				description: "Boolean and string changes should be handled correctly",
			}),
			Entry("mixed types with thresholds", testCase{
				name: "mixed_types_with_thresholds",
				messages: []map[string]interface{}{
					{
						"temperature":  25.0,
						"count":        100,
						"active":       true,
						"mode":         "AUTO",
						"timestamp_ms": int64(1000),
					},
					{
						"temperature":  25.8,     // 0.8°C change > 0.5°C threshold - KEEP
						"count":        102,      // 2 change = default 2.0 threshold - KEEP
						"active":       true,     // Boolean unchanged - DROP
						"mode":         "MANUAL", // String changed - KEEP
						"timestamp_ms": int64(2000),
					},
				},
				expectedOutput: []map[string]interface{}{
					{
						"temperature":  25.0,
						"count":        100,
						"active":       true,
						"mode":         "AUTO",
						"timestamp_ms": int64(1000),
					},
					{
						"temperature":  25.8,
						"count":        102,
						"mode":         "MANUAL",
						"timestamp_ms": int64(2000),
					},
				},
				description: "Mixed data types should be handled with appropriate thresholds",
			}),
			Entry("only timestamp preserved on filtering", testCase{
				name: "only_timestamp_preserved_on_filtering",
				messages: []map[string]interface{}{
					{
						"value1":       10.0,
						"value2":       20.0,
						"timestamp_ms": int64(1000),
					},
					{
						"value1":       10.5, // 0.5 change < 2.0 default threshold - DROP
						"value2":       20.8, // 0.8 change < 2.0 default threshold - DROP
						"timestamp_ms": int64(2000),
					},
				},
				expectedOutput: []map[string]interface{}{
					{
						"value1":       10.0,
						"value2":       20.0,
						"timestamp_ms": int64(1000),
					},
					// Second message completely dropped (no measurement fields remain)
				},
				description: "Message dropped when only timestamp would remain",
			}),
		)
	})

	Describe("Format detection and processing", func() {
		var config DownsamplerConfig
		var processor *DownsamplerProcessor

		BeforeEach(func() {
			config = DownsamplerConfig{
				Algorithm: "deadband",
				Threshold: 1.0,
			}

			resources := service.MockResources()
			var err error
			processor, err = newDownsamplerProcessor(config, resources.Logger(), resources.Metrics())
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when processing UMH-core format", func() {
			It("should handle single value field correctly", func() {
				// UMH-core format with single "value" field
				msg := service.NewMessage(nil)
				msg.MetaSet("data_contract", "_historian")
				msg.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
				msg.SetStructured(map[string]interface{}{
					"value":        25.0,
					"timestamp_ms": int64(1000),
				})

				batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(HaveLen(1))
				Expect(batches[0]).To(HaveLen(1))

				data, err := batches[0][0].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				expected := map[string]interface{}{
					"value":        25.0,
					"timestamp_ms": int64(1000),
				}
				Expect(data).To(Equal(expected))
			})
		})

		Context("when processing UMH classic format", func() {
			It("should handle multiple fields correctly", func() {
				// UMH classic format without "value" field
				msg := service.NewMessage(nil)
				msg.MetaSet("data_contract", "_historian")
				msg.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
				msg.SetStructured(map[string]interface{}{
					"temperature":  25.0,
					"humidity":     60.0,
					"timestamp_ms": int64(1000),
				})

				batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(HaveLen(1))
				Expect(batches[0]).To(HaveLen(1))

				data, err := batches[0][0].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				expected := map[string]interface{}{
					"temperature":  25.0,
					"humidity":     60.0,
					"timestamp_ms": int64(1000),
				}
				Expect(data).To(Equal(expected))
			})
		})
	})

	Describe("Series state management", func() {
		var config DownsamplerConfig
		var processor *DownsamplerProcessor

		BeforeEach(func() {
			config = DownsamplerConfig{
				Algorithm: "deadband",
				Threshold: 1.0,
			}

			resources := service.MockResources()
			var err error
			processor, err = newDownsamplerProcessor(config, resources.Logger(), resources.Metrics())
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when processing multiple fields", func() {
			It("should maintain independent series state per key", func() {
				// Send first message to establish baseline
				msg1 := service.NewMessage(nil)
				msg1.MetaSet("data_contract", "_historian")
				msg1.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
				msg1.SetStructured(map[string]interface{}{
					"temp_sensor_1": 25.0,
					"temp_sensor_2": 30.0,
					"timestamp_ms":  int64(1000),
				})

				_, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1})
				Expect(err).NotTo(HaveOccurred())

				// Verify series states were created independently
				expectedSeriesIDs := []string{
					"umh.v1.plant1.line1._historian.temp_sensor_1",
					"umh.v1.plant1.line1._historian.temp_sensor_2",
				}

				processor.stateMutex.RLock()
				for _, seriesID := range expectedSeriesIDs {
					state, exists := processor.seriesState[seriesID]
					Expect(exists).To(BeTrue(), "Series state should exist for %s", seriesID)
					if exists {
						Expect(state.algorithm).NotTo(BeNil(), "Algorithm should be initialized for %s", seriesID)
					}
				}
				processor.stateMutex.RUnlock()

				// Send second message with different changes per sensor
				msg2 := service.NewMessage(nil)
				msg2.MetaSet("data_contract", "_historian")
				msg2.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
				msg2.SetStructured(map[string]interface{}{
					"temp_sensor_1": 25.5, // 0.5°C change < 1.0°C threshold - should be dropped
					"temp_sensor_2": 32.0, // 2.0°C change > 1.0°C threshold - should be kept
					"timestamp_ms":  int64(2000),
				})

				batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg2})
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(HaveLen(1))
				Expect(batches[0]).To(HaveLen(1))

				data, err := batches[0][0].AsStructured()
				Expect(err).NotTo(HaveOccurred())
				expected := map[string]interface{}{
					"temp_sensor_2": 32.0, // Only sensor 2 kept
					"timestamp_ms":  int64(2000),
				}
				Expect(data).To(Equal(expected))
			})
		})
	})

	Describe("Metadata annotation", func() {
		var config DownsamplerConfig
		var processor *DownsamplerProcessor

		BeforeEach(func() {
			config = DownsamplerConfig{
				Algorithm: "deadband",
				Threshold: 1.0,
			}

			resources := service.MockResources()
			var err error
			processor, err = newDownsamplerProcessor(config, resources.Logger(), resources.Metrics())
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when processing UMH-core format", func() {
			It("should add appropriate metadata annotation", func() {
				msg := service.NewMessage(nil)
				msg.MetaSet("data_contract", "_historian")
				msg.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
				msg.SetStructured(map[string]interface{}{
					"value":        25.0,
					"timestamp_ms": int64(1000),
				})

				batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(HaveLen(1))
				Expect(batches[0]).To(HaveLen(1))

				metadata, _ := batches[0][0].MetaGet("downsampled_by")
				Expect(metadata).To(ContainSubstring("deadband"))
				Expect(metadata).To(ContainSubstring("threshold"))
			})
		})

		Context("when processing UMH classic format", func() {
			It("should add filtering statistics in metadata", func() {
				msg := service.NewMessage(nil)
				msg.MetaSet("data_contract", "_historian")
				msg.MetaSet("umh_topic", "umh.v1.plant1.line1._historian")
				msg.SetStructured(map[string]interface{}{
					"temperature":  25.0,
					"humidity":     60.0,
					"pressure":     1000.0,
					"timestamp_ms": int64(1000),
				})

				batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(HaveLen(1))
				Expect(batches[0]).To(HaveLen(1))

				metadata, _ := batches[0][0].MetaGet("downsampled_by")
				Expect(metadata).To(ContainSubstring("deadband"))
				Expect(metadata).To(ContainSubstring("filtered"))
				Expect(metadata).To(ContainSubstring("_of_"))
				Expect(metadata).To(ContainSubstring("_keys"))
			})
		})
	})
})
