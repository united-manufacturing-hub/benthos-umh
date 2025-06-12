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
	"os"

	. "github.com/onsi/ginkgo/v2"
)

// Behavior Tests - End-to-End Behavioral Testing
// These tests verify the complete downsampler behavior through Benthos message processing.
// Set TEST_DOWNSAMPLER=1 to enable comprehensive end-to-end tests.

var _ = Describe("Downsampler Behavior", Ordered, func() {
	BeforeAll(func() {
		// Skip comprehensive E2E tests unless explicitly enabled
		if os.Getenv("TEST_DOWNSAMPLER") != "1" {
			Skip("Comprehensive E2E tests require TEST_DOWNSAMPLER=1 environment variable")
		}
	})

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
						{Value: 100, TimestampMs: 1000, Topic: "umh.v1.plant.counter", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						{Value: 107, TimestampMs: 3000, Topic: "umh.v1.plant.counter", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
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
					{Value: 12.0, TimestampMs: 2000, Topic: "sensor.temp"}, // Out of order - should drop
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
})
