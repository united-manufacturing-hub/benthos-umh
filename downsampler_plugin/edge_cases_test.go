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

// Edge Cases Tests - Advanced Edge Scenarios and Boundary Conditions
// These tests verify edge cases, performance scenarios, and boundary conditions.
// Set TEST_DOWNSAMPLER=1 to enable comprehensive edge case testing.

var _ = Describe("Downsampler Edge Cases", Ordered, func() {

	Describe("Idle Timer Behavior", func() {
		Context("Algorithm Max Time Behavior (New Message Triggers Flush)", func() {
			DescribeTable("should flush previous message when new message exceeds max_time",
				RunStreamTestCase,

				Entry("Algorithm max_time heartbeat - new message triggers old message flush", StreamTestCase{
					Name:        "algorithm_max_time_heartbeat",
					Description: "When a new data point comes in after max_time, it sends the old one for a heartbeat",
					Config: `
downsampler:
  default:
    deadband:
      threshold: 5.0
      max_time: 2s`,
					Input: []TestMessage{
						{Value: 100.0, TimestampMs: 1000, Topic: "sensor.heartbeat"},
						{Value: 101.0, TimestampMs: 3500, Topic: "sensor.heartbeat"}, // 2.5s later, > max_time, triggers flush of previous value
					},
					ExpectedOutput: []ExpectedMessage{
						{Value: 100.0, TimestampMs: 1000, Topic: "sensor.heartbeat", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
						{Value: 101.0, TimestampMs: 3500, Topic: "sensor.heartbeat", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
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
