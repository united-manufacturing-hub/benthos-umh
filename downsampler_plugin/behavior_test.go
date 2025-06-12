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
	. "github.com/onsi/ginkgo/v2"
)

// Behavior Tests - End-to-End Behavioral Testing
// These tests verify the complete downsampler behavior through Benthos message processing.
// Set TEST_DOWNSAMPLER=1 to enable comprehensive end-to-end tests.

var _ = Describe("Downsampler Behavior", Ordered, func() {

	Describe("Basic Deadband Algorithm Behavior", func() {
		Context("Numeric Threshold Filtering", func() {
			DescribeTable("should filter messages based on numeric thresholds",
				RunStreamTestCase,

				Entry("Basic deadband filtering", StreamTestCase{
					Name:        "deadband_basic_filtering",
					Description: "Filters out changes below threshold",
					Config: `
downsampler:
  default:
    deadband:
      threshold: 0.5`,
					Input: []TestMessage{
						{Value: 10.0, TimestampMs: 1000, Topic: "sensor.temperature"},
						{Value: 10.3, TimestampMs: 2000, Topic: "sensor.temperature"}, // Below threshold of 0.5
						{Value: 11.0, TimestampMs: 3000, Topic: "sensor.temperature"}, // Above threshold of 0.5
						{Value: 10.9, TimestampMs: 4000, Topic: "sensor.temperature"}, // Below threshold of 0.5
						{Value: 9.0, TimestampMs: 5000, Topic: "sensor.temperature"},  // Above threshold of 0.5
					},
					ExpectedOutput: []ExpectedMessage{
						{Value: 10.0, TimestampMs: 1000, Topic: "sensor.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						{Value: 11.0, TimestampMs: 3000, Topic: "sensor.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						{Value: 9.0, TimestampMs: 5000, Topic: "sensor.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					},
					ExpectedMetrics: map[string]int{
						"messages_processed": 3,
						"messages_filtered":  2,
					},
				}),

				Entry("Temperature sensor with 0.5°C deadband threshold", StreamTestCase{
					Name:        "temperature_sensor_deadband",
					Description: "Industrial temperature sensor with precise deadband filtering",
					Config: `
downsampler:
  default:
    deadband:
      threshold: 0.5`,
					Input: []TestMessage{
						{Value: 20.0, TimestampMs: 1000, Topic: "umh.v1.plant.temperature"},
						{Value: 20.2, TimestampMs: 2000, Topic: "umh.v1.plant.temperature"}, // Filtered (diff=0.2 < 0.5)
						{Value: 20.6, TimestampMs: 3000, Topic: "umh.v1.plant.temperature"}, // Kept (diff=0.6 > 0.5)
						{Value: 20.9, TimestampMs: 4000, Topic: "umh.v1.plant.temperature"}, // Filtered (diff=0.3 < 0.5)
						{Value: 21.2, TimestampMs: 5000, Topic: "umh.v1.plant.temperature"}, // Kept (diff=0.6 > 0.5)
					},
					ExpectedOutput: []ExpectedMessage{
						{Value: 20.0, TimestampMs: 1000, Topic: "umh.v1.plant.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						{Value: 20.6, TimestampMs: 3000, Topic: "umh.v1.plant.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						{Value: 21.2, TimestampMs: 5000, Topic: "umh.v1.plant.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					},
				}),

				Entry("Integer production counter", StreamTestCase{
					Name:        "integer_counter_deadband",
					Description: "Production counter with integer values and threshold 5",
					Config: `
downsampler:
  default:
    deadband:
      threshold: 5`,
					Input: []TestMessage{
						{Value: 100, TimestampMs: 1000, Topic: "umh.v1.plant.counter"},
						{Value: 102, TimestampMs: 2000, Topic: "umh.v1.plant.counter"}, // Filtered (diff=2 < 5)
						{Value: 107, TimestampMs: 3000, Topic: "umh.v1.plant.counter"}, // Kept (diff=7 > 5)
						{Value: 110, TimestampMs: 4000, Topic: "umh.v1.plant.counter"}, // Filtered (diff=3 < 5)
					},
					ExpectedOutput: []ExpectedMessage{
						{Value: 100.0, TimestampMs: 1000, Topic: "umh.v1.plant.counter", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						{Value: 107.0, TimestampMs: 3000, Topic: "umh.v1.plant.counter", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					},
				}),
			)
		})

		Context("Heartbeat Behavior", func() {
			DescribeTable("should enforce max_time heartbeat intervals",
				RunStreamTestCase,

				Entry("Deadband with max_time heartbeat", StreamTestCase{
					Name:        "deadband_with_heartbeat",
					Description: "Pressure sensor with 1.0 Pa threshold and 3-second heartbeat",
					Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0
      max_time: 3s`,
					Input: []TestMessage{
						{Value: 100.0, TimestampMs: 1000, Topic: "umh.v1.plant.pressure"},
						{Value: 100.2, TimestampMs: 2000, Topic: "umh.v1.plant.pressure"}, // Filtered (diff=0.2 < 1.0)
						{Value: 100.3, TimestampMs: 4500, Topic: "umh.v1.plant.pressure"}, // Kept (time > max_time)
						{Value: 100.4, TimestampMs: 5000, Topic: "umh.v1.plant.pressure"}, // Filtered (diff=0.1 < 1.0)
					},
					ExpectedOutput: []ExpectedMessage{
						{Value: 100.0, TimestampMs: 1000, Topic: "umh.v1.plant.pressure", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						{Value: 100.3, TimestampMs: 4500, Topic: "umh.v1.plant.pressure", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					},
				}),
			)
		})
	})

	Describe("Data Type Handling", func() {
		DescribeTable("should handle different data types correctly",
			RunStreamTestCase,

			Entry("Boolean change detection", StreamTestCase{
				Name:        "boolean_change_detection",
				Description: "Motor status with boolean values (change-based logic)",
				Config: `
downsampler:
  default:
    deadband:
      threshold: 0.5`,
				Input: []TestMessage{
					{Value: true, TimestampMs: 1000, Topic: "umh.v1.plant.motor_on"},
					{Value: true, TimestampMs: 2000, Topic: "umh.v1.plant.motor_on"},  // Filtered (no change)
					{Value: false, TimestampMs: 3000, Topic: "umh.v1.plant.motor_on"}, // Kept (changed)
					{Value: false, TimestampMs: 4000, Topic: "umh.v1.plant.motor_on"}, // Filtered (no change)
					{Value: true, TimestampMs: 5000, Topic: "umh.v1.plant.motor_on"},  // Kept (changed)
				},
				ExpectedOutput: []ExpectedMessage{
					{Value: true, TimestampMs: 1000, Topic: "umh.v1.plant.motor_on", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: false, TimestampMs: 3000, Topic: "umh.v1.plant.motor_on", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: true, TimestampMs: 5000, Topic: "umh.v1.plant.motor_on", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				},
				ExpectedMetrics: map[string]int{
					"messages_processed": 3,
					"messages_filtered":  2,
				},
			}),

			Entry("String change detection", StreamTestCase{
				Name:        "string_change_detection",
				Description: "Process state with string values (change-based logic)",
				Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0`,
				Input: []TestMessage{
					{Value: "RUNNING", TimestampMs: 1000, Topic: "umh.v1.plant.state"},
					{Value: "RUNNING", TimestampMs: 2000, Topic: "umh.v1.plant.state"}, // Filtered (no change)
					{Value: "STOPPED", TimestampMs: 3000, Topic: "umh.v1.plant.state"}, // Kept (changed)
					{Value: "ERROR", TimestampMs: 4000, Topic: "umh.v1.plant.state"},   // Kept (changed)
					{Value: "ERROR", TimestampMs: 5000, Topic: "umh.v1.plant.state"},   // Filtered (no change)
				},
				ExpectedOutput: []ExpectedMessage{
					{Value: "RUNNING", TimestampMs: 1000, Topic: "umh.v1.plant.state", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: "STOPPED", TimestampMs: 3000, Topic: "umh.v1.plant.state", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: "ERROR", TimestampMs: 4000, Topic: "umh.v1.plant.state", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				},
				ExpectedMetrics: map[string]int{
					"messages_processed": 3,
					"messages_filtered":  2,
				},
			}),

			Entry("Mixed data types", StreamTestCase{
				Name:        "mixed_data_types",
				Description: "Handles different data types in same stream",
				Config: `
downsampler:
  default:
    deadband:
      threshold: 0.5`,
				Input: []TestMessage{
					{Value: 10.0, TimestampMs: 1000, Topic: "sensor.mixed"},
					{Value: 10.3, TimestampMs: 2000, Topic: "sensor.mixed"},   // Numeric below threshold
					{Value: true, TimestampMs: 3000, Topic: "sensor.mixed"},   // Boolean change
					{Value: "test", TimestampMs: 4000, Topic: "sensor.mixed"}, // String change
					{Value: 15.0, TimestampMs: 5000, Topic: "sensor.mixed"},   // Numeric above threshold
				},
				ExpectedOutput: []ExpectedMessage{
					{Value: 10.0, TimestampMs: 1000, Topic: "sensor.mixed", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: true, TimestampMs: 3000, Topic: "sensor.mixed", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: "test", TimestampMs: 4000, Topic: "sensor.mixed", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 15.0, TimestampMs: 5000, Topic: "sensor.mixed", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				},
				ExpectedMetrics: map[string]int{
					"messages_processed": 4,
					"messages_filtered":  1,
				},
			}),
		)
	})

	Describe("Late Arrival Policy", func() {
		DescribeTable("should handle out-of-order messages according to policy",
			RunStreamTestCase,

			Entry("Late arrival passthrough policy", StreamTestCase{
				Name:        "late_arrival_passthrough",
				Description: "Passes through out-of-order messages unchanged",
				Config: `
downsampler:
  default:
    deadband:
      threshold: 0.5
    late_policy:
      late_policy: passthrough`,
				Input: []TestMessage{
					{Value: 10.0, TimestampMs: 3000, Topic: "sensor.temp"}, // In order
					{Value: 15.0, TimestampMs: 4000, Topic: "sensor.temp"}, // In order
					{Value: 15.0, TimestampMs: 2000, Topic: "sensor.temp"}, // Out of order - should passthrough even though it is below the threshold of 0.5
					{Value: 20.0, TimestampMs: 5000, Topic: "sensor.temp"}, // In order
				},
				ExpectedOutput: []ExpectedMessage{
					{Value: 10.0, TimestampMs: 3000, Topic: "sensor.temp", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 15.0, TimestampMs: 4000, Topic: "sensor.temp", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 15.0, TimestampMs: 2000, Topic: "sensor.temp", HasMetadata: map[string]string{"downsampled_by": "deadband"}}, // Passthrough
					{Value: 20.0, TimestampMs: 5000, Topic: "sensor.temp", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				},
				ExpectedMetrics: map[string]int{
					"messages_processed": 4,
					"messages_filtered":  0,
				},
			}),

			Entry("Late arrival drop policy", StreamTestCase{
				Name:        "late_arrival_drop",
				Description: "Drops out-of-order messages",
				Config: `
downsampler:
  default:
    deadband:
      threshold: 0.5
    late_policy:
      late_policy: drop`,
				Input: []TestMessage{
					{Value: 10.0, TimestampMs: 3000, Topic: "sensor.temp"}, // In order
					{Value: 15.0, TimestampMs: 4000, Topic: "sensor.temp"}, // In order
					{Value: 15.0, TimestampMs: 2000, Topic: "sensor.temp"}, // Out of order - should drop
					{Value: 20.0, TimestampMs: 5000, Topic: "sensor.temp"}, // In order
				},
				ExpectedOutput: []ExpectedMessage{
					{Value: 10.0, TimestampMs: 3000, Topic: "sensor.temp", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 15.0, TimestampMs: 4000, Topic: "sensor.temp", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 20.0, TimestampMs: 5000, Topic: "sensor.temp", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				},
				ExpectedMetrics: map[string]int{
					"messages_processed": 3,
					"messages_filtered":  1, // Out-of-order message dropped
				},
			}),
		)
	})

	Describe("Configuration Overrides", func() {
		DescribeTable("should apply topic-specific configurations",
			RunStreamTestCase,

			Entry("Topic-specific override", StreamTestCase{
				Name:        "topic_specific_override",
				Description: "Applies different thresholds to different topics",
				Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0
  overrides:
    - topic: "sensor.precise"
      deadband:
        threshold: 0.1`,
				Input: []TestMessage{
					{Value: 10.0, TimestampMs: 1000, Topic: "sensor.normal"},
					{Value: 10.5, TimestampMs: 2000, Topic: "sensor.normal"}, // Below default threshold
					{Value: 10.0, TimestampMs: 3000, Topic: "sensor.precise"},
					{Value: 10.05, TimestampMs: 4000, Topic: "sensor.precise"}, // Below override threshold
					{Value: 10.2, TimestampMs: 5000, Topic: "sensor.precise"},  // Above override threshold
				},
				ExpectedOutput: []ExpectedMessage{
					{Value: 10.0, TimestampMs: 1000, Topic: "sensor.normal", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 10.0, TimestampMs: 3000, Topic: "sensor.precise", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 10.2, TimestampMs: 5000, Topic: "sensor.precise", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				},
				ExpectedMetrics: map[string]int{
					"messages_processed": 3,
					"messages_filtered":  2,
				},
			}),

			Entry("Pattern-based override", StreamTestCase{
				Name:        "pattern_based_override",
				Description: "Applies overrides based on topic patterns",
				Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0
  overrides:
    - pattern: "*_temperature"
      deadband:
        threshold: 0.5`,
				Input: []TestMessage{
					{Value: 20.0, TimestampMs: 1000, Topic: "room_temperature"},
					{Value: 20.3, TimestampMs: 2000, Topic: "room_temperature"}, // Below pattern threshold
					{Value: 20.8, TimestampMs: 3000, Topic: "room_temperature"}, // Above pattern threshold
					{Value: 50.0, TimestampMs: 4000, Topic: "pressure"},
					{Value: 50.5, TimestampMs: 5000, Topic: "pressure"}, // Below default threshold
				},
				ExpectedOutput: []ExpectedMessage{
					{Value: 20.0, TimestampMs: 1000, Topic: "room_temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 20.8, TimestampMs: 3000, Topic: "room_temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 50.0, TimestampMs: 4000, Topic: "pressure", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				},
				ExpectedMetrics: map[string]int{
					"messages_processed": 3,
					"messages_filtered":  2,
				},
			}),

			Entry("UMH pattern-based overrides", StreamTestCase{
				Name:        "umh_pattern_overrides",
				Description: "Different thresholds for temperature vs pressure sensors in UMH format",
				Config: `
downsampler:
  default:
    deadband:
      threshold: 5.0
  overrides:
    - pattern: "*.temperature"
      deadband:
        threshold: 0.1
    - pattern: "*.pressure"
      deadband:
        threshold: 0.5`,
				Input: []TestMessage{
					// Temperature with 0.1 threshold
					{Value: 25.0, TimestampMs: 1000, Topic: "umh.v1.plant._historian.temperature"},
					{Value: 25.05, TimestampMs: 2000, Topic: "umh.v1.plant._historian.temperature"}, // Filtered (diff=0.05 < 0.1)
					{Value: 25.15, TimestampMs: 3000, Topic: "umh.v1.plant._historian.temperature"}, // Kept (diff=0.15 > 0.1)

					// Pressure with 0.5 threshold
					{Value: 1000.0, TimestampMs: 1000, Topic: "umh.v1.plant._historian.pressure"},
					{Value: 1000.3, TimestampMs: 2000, Topic: "umh.v1.plant._historian.pressure"}, // Filtered (diff=0.3 < 0.5)
					{Value: 1000.8, TimestampMs: 3000, Topic: "umh.v1.plant._historian.pressure"}, // Kept (diff=0.8 > 0.5)

					// Other topic uses default 5.0 threshold
					{Value: 50.0, TimestampMs: 1000, Topic: "umh.v1.plant._historian.flow"},
					{Value: 52.0, TimestampMs: 2000, Topic: "umh.v1.plant._historian.flow"}, // Filtered (diff=2.0 < 5.0)
					{Value: 56.0, TimestampMs: 3000, Topic: "umh.v1.plant._historian.flow"}, // Kept (diff=6.0 > 5.0)
				},
				ExpectedOutput: []ExpectedMessage{
					{Value: 25.0, TimestampMs: 1000, Topic: "umh.v1.plant._historian.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 25.15, TimestampMs: 3000, Topic: "umh.v1.plant._historian.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 1000.0, TimestampMs: 1000, Topic: "umh.v1.plant._historian.pressure", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 1000.8, TimestampMs: 3000, Topic: "umh.v1.plant._historian.pressure", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 50.0, TimestampMs: 1000, Topic: "umh.v1.plant._historian.flow", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
					{Value: 56.0, TimestampMs: 3000, Topic: "umh.v1.plant._historian.flow", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				},
			}),
		)
	})

	Describe("Edge Cases and Advanced Features", func() {
		Describe("Idle Timer Behavior", func() {
			Context("Max Time Idle Flush", func() {
				DescribeTable("should flush idle buffered messages after max_time",
					RunStreamTestCase,

					Entry("Max time forces emission of unchanged value", StreamTestCase{
						Name:        "max_time_idle_flush",
						Description: "Unchanged values are emitted after max_time to prevent infinite buffering",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 5.0
      max_time: 2s`,
						Input: []TestMessage{
							{Value: 100.0, TimestampMs: 1000, Topic: "sensor.idle"},
							{Value: 101.0, TimestampMs: 3500, Topic: "sensor.idle"}, // emitted! 2.5s later, max_time exceeded, even though the value is udner the threshold
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 100.0, TimestampMs: 1000, Topic: "sensor.idle", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 101.0, TimestampMs: 3500, Topic: "sensor.idle", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						},
						ExpectedMetrics: map[string]int{
							"messages_processed": 2,
							"messages_filtered":  0,
						},
					}),

					Entry("Multiple series with different max_time", StreamTestCase{
						Name:        "multi_series_max_time",
						Description: "Different series have different max_time configurations",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0
      max_time: 5s
  overrides:
    - topic: "fast.sensor"
      deadband:
        threshold: 1.0
        max_time: 1s`,
						Input: []TestMessage{
							// Fast sensor should flush after 1s
							{Value: 10.0, TimestampMs: 1000, Topic: "fast.sensor"},
							{Value: 10.5, TimestampMs: 2500, Topic: "fast.sensor"}, // 1.5s later, > 1s max_time

							// Normal sensor should flush after 5s
							{Value: 20.0, TimestampMs: 1000, Topic: "normal.sensor"},
							{Value: 20.5, TimestampMs: 6500, Topic: "normal.sensor"}, // 5.5s later, > 5s max_time
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 10.0, TimestampMs: 1000, Topic: "fast.sensor", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 10.5, TimestampMs: 2500, Topic: "fast.sensor", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 20.0, TimestampMs: 1000, Topic: "normal.sensor", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 20.5, TimestampMs: 6500, Topic: "normal.sensor", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						},
					}),
				)
			})
		})

		Describe("ACK Buffering and Data Safety", func() {
			Context("Message Buffering for Emit-Previous Algorithms", func() {
				DescribeTable("should handle ACK buffering correctly",
					RunStreamTestCase,

					Entry("Stale buffer flush", StreamTestCase{
						Name:        "stale_buffer_flush",
						Description: "Stale buffered messages are flushed based on max_time",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 5.0
      max_time: 3s`,
						Input: []TestMessage{
							{Value: 100.0, TimestampMs: 1000, Topic: "sensor.stale"},
							{Value: 101.0, TimestampMs: 4500, Topic: "sensor.stale"}, // 3.5s later, should flush due to max_time
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 100.0, TimestampMs: 1000, Topic: "sensor.stale", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 101.0, TimestampMs: 4500, Topic: "sensor.stale", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						},
					}),
				)
			})
		})

		Describe("Performance and Memory Edge Cases", func() {
			Context("High-Throughput Scenarios", func() {
				DescribeTable("should handle high-throughput data efficiently",
					RunStreamTestCase,

					Entry("Rapid-fire identical values", StreamTestCase{
						Name:        "rapid_fire_identical",
						Description: "Many identical values in rapid succession should be efficiently filtered",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 0.1`,
						Input: []TestMessage{
							{Value: 50.0, TimestampMs: 1000, Topic: "sensor.highfreq"},
							{Value: 50.0, TimestampMs: 1100, Topic: "sensor.highfreq"},
							{Value: 50.0, TimestampMs: 1200, Topic: "sensor.highfreq"},
							{Value: 50.0, TimestampMs: 1300, Topic: "sensor.highfreq"},
							{Value: 50.0, TimestampMs: 1400, Topic: "sensor.highfreq"},
							{Value: 50.2, TimestampMs: 1500, Topic: "sensor.highfreq"}, // Above threshold
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 50.0, TimestampMs: 1000, Topic: "sensor.highfreq", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 50.2, TimestampMs: 1500, Topic: "sensor.highfreq", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						},
						ExpectedMetrics: map[string]int{
							"messages_processed": 2,
							"messages_filtered":  4,
						},
					}),

					Entry("Noisy sensor data compression", StreamTestCase{
						Name:        "noisy_sensor_compression",
						Description: "Noisy sensor data should be effectively compressed",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0`,
						Input: []TestMessage{
							{Value: 100.0, TimestampMs: 1000, Topic: "sensor.noisy"},
							{Value: 100.3, TimestampMs: 1500, Topic: "sensor.noisy"}, // Noise
							{Value: 99.8, TimestampMs: 2000, Topic: "sensor.noisy"},  // Noise
							{Value: 100.2, TimestampMs: 2500, Topic: "sensor.noisy"}, // Noise
							{Value: 101.5, TimestampMs: 3000, Topic: "sensor.noisy"}, // Signal
							{Value: 101.8, TimestampMs: 3500, Topic: "sensor.noisy"}, // Noise
							{Value: 102.8, TimestampMs: 4000, Topic: "sensor.noisy"}, // Signal
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 100.0, TimestampMs: 1000, Topic: "sensor.noisy", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 101.5, TimestampMs: 3000, Topic: "sensor.noisy", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 102.8, TimestampMs: 4000, Topic: "sensor.noisy", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						},
						ExpectedMetrics: map[string]int{
							"messages_processed": 3,
							"messages_filtered":  4,
						},
					}),
				)
			})
		})

		Describe("Extreme Value Handling", func() {
			Context("Boundary Value Testing", func() {
				DescribeTable("should handle extreme and boundary values",
					RunStreamTestCase,

					Entry("Very large numbers", StreamTestCase{
						Name:        "very_large_numbers",
						Description: "Algorithm should handle very large floating point numbers",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 1000000.0`,
						Input: []TestMessage{
							{Value: 1e15, TimestampMs: 1000, Topic: "sensor.large"},
							{Value: 1e15 + 500000, TimestampMs: 2000, Topic: "sensor.large"},  // Below threshold
							{Value: 1e15 + 2000000, TimestampMs: 3000, Topic: "sensor.large"}, // Above threshold
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 1e15, TimestampMs: 1000, Topic: "sensor.large", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 1e15 + 2000000, TimestampMs: 3000, Topic: "sensor.large", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						},
					}),

					Entry("Very small numbers", StreamTestCase{
						Name:        "very_small_numbers",
						Description: "Algorithm should handle very small floating point numbers",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 1e-10`,
						Input: []TestMessage{
							{Value: 1e-15, TimestampMs: 1000, Topic: "sensor.small"},
							{Value: 1e-15 + 5e-11, TimestampMs: 2000, Topic: "sensor.small"}, // Below threshold
							{Value: 1e-15 + 2e-10, TimestampMs: 3000, Topic: "sensor.small"}, // Above threshold
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 1e-15, TimestampMs: 1000, Topic: "sensor.small", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 1e-15 + 2e-10, TimestampMs: 3000, Topic: "sensor.small", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						},
					}),

					Entry("Zero threshold edge case", StreamTestCase{
						Name:        "zero_threshold",
						Description: "Zero threshold should drop exact repeats but keep any change",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 0.0`,
						Input: []TestMessage{
							{Value: 10.0, TimestampMs: 1000, Topic: "sensor.zero"},
							{Value: 10.0, TimestampMs: 2000, Topic: "sensor.zero"},    // Exact repeat - drop
							{Value: 10.0001, TimestampMs: 3000, Topic: "sensor.zero"}, // Tiny change - keep
							{Value: 10.0001, TimestampMs: 4000, Topic: "sensor.zero"}, // Exact repeat - drop
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 10.0, TimestampMs: 1000, Topic: "sensor.zero", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 10.0001, TimestampMs: 3000, Topic: "sensor.zero", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						},
						ExpectedMetrics: map[string]int{
							"messages_processed": 2,
							"messages_filtered":  2,
						},
					}),
				)
			})
		})

		Describe("Concurrent Series Handling", func() {
			Context("Multiple Independent Series", func() {
				DescribeTable("should handle multiple independent time series correctly",
					RunStreamTestCase,

					Entry("Independent series filtering", StreamTestCase{
						Name:        "independent_series",
						Description: "Multiple series should be filtered independently",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0`,
						Input: []TestMessage{
							// Series A
							{Value: 10.0, TimestampMs: 1000, Topic: "plant.a.temperature"},
							{Value: 10.5, TimestampMs: 2000, Topic: "plant.a.temperature"}, // Below threshold
							{Value: 11.5, TimestampMs: 3000, Topic: "plant.a.temperature"}, // Above threshold

							// Series B (independent state)
							{Value: 20.0, TimestampMs: 1000, Topic: "plant.b.temperature"},
							{Value: 20.8, TimestampMs: 2000, Topic: "plant.b.temperature"}, // Below threshold
							{Value: 21.2, TimestampMs: 3000, Topic: "plant.b.temperature"}, // Above threshold

							// Series A continues
							{Value: 11.8, TimestampMs: 4000, Topic: "plant.a.temperature"}, // Below threshold from 11.5
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 10.0, TimestampMs: 1000, Topic: "plant.a.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 11.5, TimestampMs: 3000, Topic: "plant.a.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 20.0, TimestampMs: 1000, Topic: "plant.b.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 21.2, TimestampMs: 3000, Topic: "plant.b.temperature", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						},
						ExpectedMetrics: map[string]int{
							"messages_processed": 4,
							"messages_filtered":  3,
						},
					}),

					Entry("Interleaved series processing", StreamTestCase{
						Name:        "interleaved_series",
						Description: "Interleaved messages from different series should be handled correctly",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 0.5`,
						Input: []TestMessage{
							{Value: 100.0, TimestampMs: 1000, Topic: "sensor.x"},
							{Value: 200.0, TimestampMs: 1100, Topic: "sensor.y"},
							{Value: 100.3, TimestampMs: 1200, Topic: "sensor.x"}, // Below threshold for x
							{Value: 200.8, TimestampMs: 1300, Topic: "sensor.y"}, // Above threshold for y
							{Value: 100.6, TimestampMs: 1400, Topic: "sensor.x"}, // Above threshold for x
							{Value: 200.9, TimestampMs: 1500, Topic: "sensor.y"}, // Below threshold for y
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 100.0, TimestampMs: 1000, Topic: "sensor.x", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 200.0, TimestampMs: 1100, Topic: "sensor.y", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 200.8, TimestampMs: 1300, Topic: "sensor.y", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 100.6, TimestampMs: 1400, Topic: "sensor.x", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						},
						ExpectedMetrics: map[string]int{
							"messages_processed": 4,
							"messages_filtered":  2,
						},
					}),
				)
			})
		})

		Describe("Time Handling Edge Cases", func() {
			Context("Timestamp Edge Cases", func() {
				DescribeTable("should handle timestamp edge cases correctly",
					RunStreamTestCase,

					Entry("Messages with same timestamp", StreamTestCase{
						Name:        "same_timestamp",
						Description: "Messages with identical timestamps should be handled gracefully",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0`,
						Input: []TestMessage{
							{Value: 10.0, TimestampMs: 1000, Topic: "sensor.time"},
							{Value: 11.5, TimestampMs: 1000, Topic: "sensor.time"}, // Same timestamp, above threshold
							{Value: 12.0, TimestampMs: 1000, Topic: "sensor.time"}, // Same timestamp, below threshold from 11.5
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 10.0, TimestampMs: 1000, Topic: "sensor.time", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							// further messages are dropped because they have the same timestamp
						},
					}),

					Entry("Very long time gaps", StreamTestCase{
						Name:        "long_time_gaps",
						Description: "Very long time gaps should not cause issues",
						Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0
      max_time: 1h`,
						Input: []TestMessage{
							{Value: 10.0, TimestampMs: 1000, Topic: "sensor.gap"},
							{Value: 10.5, TimestampMs: 3600000 + 1000, Topic: "sensor.gap"}, // 1 hour + 1s later, should be kept due to max_time
						},
						ExpectedOutput: []ExpectedMessage{
							{Value: 10.0, TimestampMs: 1000, Topic: "sensor.gap", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
							{Value: 10.5, TimestampMs: 3600000 + 1000, Topic: "sensor.gap", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						},
					}),
				)
			})
		})
	})
})
