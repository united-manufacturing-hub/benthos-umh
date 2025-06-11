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

var _ = Describe("Downsampler Behavior", func() {
	DescribeTable("Deadband algorithm behavior",
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
		Entry("Boolean change detection", StreamTestCase{
			Name:        "boolean_change_detection",
			Description: "Keeps only when boolean values change",
			Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0`,
			Input: []TestMessage{
				{Value: true, TimestampMs: 1000, Topic: "sensor.state"},
				{Value: true, TimestampMs: 2000, Topic: "sensor.state"},  // Same value
				{Value: false, TimestampMs: 3000, Topic: "sensor.state"}, // Changed
				{Value: false, TimestampMs: 4000, Topic: "sensor.state"}, // Same value
				{Value: true, TimestampMs: 5000, Topic: "sensor.state"},  // Changed
			},
			ExpectedOutput: []ExpectedMessage{
				{Value: true, TimestampMs: 1000, Topic: "sensor.state", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				{Value: false, TimestampMs: 3000, Topic: "sensor.state", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				{Value: true, TimestampMs: 5000, Topic: "sensor.state", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
			},
			ExpectedMetrics: map[string]int{
				"messages_processed": 3,
				"messages_filtered":  2,
			},
		}),
		Entry("String change detection", StreamTestCase{
			Name:        "string_change_detection",
			Description: "Keeps only when string values change",
			Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0`,
			Input: []TestMessage{
				{Value: "running", TimestampMs: 1000, Topic: "sensor.status"},
				{Value: "running", TimestampMs: 2000, Topic: "sensor.status"}, // Same value
				{Value: "stopped", TimestampMs: 3000, Topic: "sensor.status"}, // Changed
				{Value: "stopped", TimestampMs: 4000, Topic: "sensor.status"}, // Same value
				{Value: "error", TimestampMs: 5000, Topic: "sensor.status"},   // Changed
			},
			ExpectedOutput: []ExpectedMessage{
				{Value: "running", TimestampMs: 1000, Topic: "sensor.status", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				{Value: "stopped", TimestampMs: 3000, Topic: "sensor.status", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
				{Value: "error", TimestampMs: 5000, Topic: "sensor.status", HasMetadata: map[string]string{"downsampled_by": "deadband"}},
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

	DescribeTable("Late arrival policy behavior",
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

	// TODO: check if it is correct
	DescribeTable("Swinging Door algorithm behavior",
		RunStreamTestCase,
		Entry("Basic swinging door compression", StreamTestCase{
			Name:        "swinging_door_basic",
			Description: "Compresses linear trends effectively",
			Config: `
downsampler:
  default:
    swinging_door:
      threshold: 1.0`,
			Input: []TestMessage{
				{Value: 10.0, TimestampMs: 1000, Topic: "sensor.trend"},
				{Value: 11.0, TimestampMs: 2000, Topic: "sensor.trend"}, // Linear trend
				{Value: 12.0, TimestampMs: 3000, Topic: "sensor.trend"}, // Linear trend
				{Value: 15.0, TimestampMs: 4000, Topic: "sensor.trend"}, // Break from trend
				{Value: 16.0, TimestampMs: 5000, Topic: "sensor.trend"}, // New trend
			},
			ExpectedOutput: []ExpectedMessage{
				{Value: 10.0, TimestampMs: 1000, Topic: "sensor.trend", HasMetadata: map[string]string{"downsampled_by": "swinging_door"}},
				{Value: 15.0, TimestampMs: 4000, Topic: "sensor.trend", HasMetadata: map[string]string{"downsampled_by": "swinging_door"}},
				{Value: 16.0, TimestampMs: 5000, Topic: "sensor.trend", HasMetadata: map[string]string{"downsampled_by": "swinging_door"}},
			},
			ExpectedMetrics: map[string]int{
				"messages_processed": 3,
				"messages_filtered":  2,
			},
		}),
	)

	DescribeTable("Configuration override behavior",
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
	)
})
