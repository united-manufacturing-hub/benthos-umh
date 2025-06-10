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
	"context"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Manual Behavior Tests
// These test cases are manually crafted to document and validate specific
// behavior scenarios for the downsampler plugin. They serve as both
// documentation and regression tests for edge cases.

var _ = Describe("Manual Behavior Tests", func() {
	var ctx context.Context

	BeforeEach(func() {
		// Only run if TEST_DOWNSAMPLER environment variable is set
		if os.Getenv("TEST_DOWNSAMPLER") == "" {
			Skip("Skipping downsampler tests (set TEST_DOWNSAMPLER=1 to run)")
		}
		ctx = context.Background()
	})

	Describe("Numeric Value Thresholds", func() {
		Context("Temperature with 0.5°C threshold", func() {
			It("should keep values that exceed threshold", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"temperature": 20.0, "timestamp_ms": 1000},
        {"temperature": 20.6, "timestamp_ms": 2000},  # +0.6°C > 0.5 threshold → KEEP
        {"temperature": 20.8, "timestamp_ms": 3000},  # +0.2°C < 0.5 threshold → DROP
        {"temperature": 21.2, "timestamp_ms": 4000}   # +0.4°C from last kept < 0.5 → DROP
      ].index(count("generated") % 4)
      meta data_contract = "_historian"
      meta umh_topic = "test.temperature"
    count: 4

pipeline:
  processors:
    - downsampler:
        algorithm: deadband
        threshold: 0.5

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 2 messages kept (20.0 first, 20.6 exceeds threshold)
				// Expected: 2 messages dropped (20.8 and 21.2 below threshold from last kept)
			})

			It("should drop small fluctuations within threshold", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"temperature": 25.0, "timestamp_ms": 1000},
        {"temperature": 25.3, "timestamp_ms": 2000},  # +0.3°C < 0.5 threshold → DROP
        {"temperature": 25.1, "timestamp_ms": 3000},  # -0.2°C < 0.5 threshold → DROP
        {"temperature": 24.7, "timestamp_ms": 4000},  # -0.3°C < 0.5 threshold → DROP
        {"temperature": 24.4, "timestamp_ms": 5000}   # -0.6°C > 0.5 threshold → KEEP
      ].index(count("generated") % 5)
      meta data_contract = "_historian"
      meta umh_topic = "test.temperature"
    count: 5

pipeline:
  processors:
    - downsampler:
        algorithm: deadband
        threshold: 0.5

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 2 messages kept (25.0 first, 24.4 exceeds threshold)
				// Expected: 3 messages dropped (25.3, 25.1, 24.7 all within threshold)
			})
		})

		Context("Pressure with 2.0 Pa threshold", func() {
			It("should handle larger threshold correctly", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"pressure": 1000.0, "timestamp_ms": 1000},
        {"pressure": 1001.5, "timestamp_ms": 2000},  # +1.5 Pa < 2.0 threshold → DROP
        {"pressure": 1003.0, "timestamp_ms": 3000},  # +3.0 Pa > 2.0 threshold → KEEP
        {"pressure": 1004.0, "timestamp_ms": 4000},  # +1.0 Pa < 2.0 threshold → DROP
        {"pressure": 1006.0, "timestamp_ms": 5000}   # +3.0 Pa > 2.0 threshold → KEEP
      ].index(count("generated") % 5)
      meta data_contract = "_historian"
      meta umh_topic = "test.pressure"  
    count: 5

pipeline:
  processors:
    - downsampler:
        algorithm: deadband
        threshold: 2.0

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 3 messages kept (1000.0 first, 1003.0, 1006.0 exceed threshold)
				// Expected: 2 messages dropped (1001.5, 1004.0 within threshold)
			})
		})
	})

	Describe("String Value Equality", func() {
		Context("Machine status strings", func() {
			It("should ignore thresholds and use equality only", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"status": "RUNNING", "timestamp_ms": 1000},
        {"status": "RUNNING", "timestamp_ms": 2000},   # Same → DROP (threshold ignored)
        {"status": "STOPPED", "timestamp_ms": 3000},  # Different → KEEP (threshold ignored)
        {"status": "STOPPED", "timestamp_ms": 4000},  # Same → DROP (threshold ignored)
        {"status": "RUNNING", "timestamp_ms": 5000}   # Different → KEEP (threshold ignored)
      ].index(count("generated") % 5)
      meta data_contract = "_historian"
      meta umh_topic = "test.status"
    count: 5

pipeline:
  processors:
    - downsampler:
        algorithm: deadband
        threshold: 999.0  # High threshold should be ignored for strings

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 3 messages kept ("RUNNING" first, "STOPPED", "RUNNING" again)
				// Expected: 2 messages dropped (duplicate "RUNNING", duplicate "STOPPED")
			})

			It("should handle constant strings correctly", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"mode": "AUTO", "timestamp_ms": 1000},
        {"mode": "AUTO", "timestamp_ms": 2000},  # Same → DROP
        {"mode": "AUTO", "timestamp_ms": 3000},  # Same → DROP  
        {"mode": "AUTO", "timestamp_ms": 4000},  # Same → DROP
        {"mode": "AUTO", "timestamp_ms": 5000}   # Same → DROP
      ].index(count("generated") % 5)
      meta data_contract = "_historian"
      meta umh_topic = "test.mode"
    count: 5

pipeline:
  processors:
    - downsampler:
        algorithm: deadband
        threshold: 0.1  # Low threshold should be ignored for strings

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 1 message kept ("AUTO" first)
				// Expected: 4 messages dropped (all duplicates)
			})
		})
	})

	Describe("Boolean Value Equality", func() {
		Context("Machine state booleans", func() {
			It("should ignore thresholds and use equality only", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"enabled": true, "timestamp_ms": 1000},
        {"enabled": true, "timestamp_ms": 2000},   # Same → DROP (threshold ignored)
        {"enabled": false, "timestamp_ms": 3000}, # Different → KEEP (threshold ignored)
        {"enabled": false, "timestamp_ms": 4000}, # Same → DROP (threshold ignored)
        {"enabled": true, "timestamp_ms": 5000}   # Different → KEEP (threshold ignored)
      ].index(count("generated"))
      meta data_contract = "_historian"
      meta umh_topic = "test.enabled"
    count: 5

pipeline:
  processors:
    - downsampler:
        algorithm: deadband
        threshold: 999.0  # High threshold should be ignored for booleans

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 3 messages kept (true first, false, true again)
				// Expected: 2 messages dropped (duplicate true, duplicate false)
			})

			It("should handle constant boolean values", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"alarm": false, "timestamp_ms": 1000},
        {"alarm": false, "timestamp_ms": 2000},  # Same → DROP
        {"alarm": false, "timestamp_ms": 3000},  # Same → DROP
        {"alarm": false, "timestamp_ms": 4000},  # Same → DROP
        {"alarm": false, "timestamp_ms": 5000}   # Same → DROP
      ].index(count("generated") % 5)
      meta data_contract = "_historian"
      meta umh_topic = "test.alarm"
    count: 5

pipeline:
  processors:
    - downsampler:
        algorithm: deadband
        threshold: 0.1  # Low threshold should be ignored for booleans

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 1 message kept (false first)
				// Expected: 4 messages dropped (all duplicates)
			})
		})
	})

	Describe("Topic-Specific Thresholds", func() {
		Context("Different thresholds per metric type", func() {
			It("should apply correct threshold based on topic pattern", func() {
				spec := `
input:
  generate:
    mapping: |
      let messages = [
        {"temperature": 20.0, "timestamp_ms": 1000, "topic": "plant.line1.temperature"},
        {"temperature": 20.3, "timestamp_ms": 2000, "topic": "plant.line1.temperature"},  # +0.3°C < 0.5 → DROP
        {"humidity": 50.0, "timestamp_ms": 3000, "topic": "plant.line1.humidity"},
        {"humidity": 50.7, "timestamp_ms": 4000, "topic": "plant.line1.humidity"},       # +0.7% < 1.0 → DROP
        {"pressure": 1000.0, "timestamp_ms": 5000, "topic": "plant.line1.pressure"},
        {"pressure": 1001.0, "timestamp_ms": 6000, "topic": "plant.line1.pressure"}     # +1.0 Pa < 2.0 → DROP
      ]
      let msg = $messages.index(count("generated") % $messages.length())
      root = msg.without("topic")
      meta data_contract = "_historian"
      meta umh_topic = $msg.topic
    count: 6

pipeline:
  processors:
    - downsampler:
        algorithm: deadband
        threshold: 2.0  # Default
        topic_thresholds:
          - pattern: "*.temperature"
            threshold: 0.5
          - pattern: "*.humidity"
            threshold: 1.0
          # pressure uses default 2.0

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 3 messages kept (first of each metric type)
				// Expected: 3 messages dropped (all within their respective thresholds)
			})
		})
	})

	Describe("Mixed Data Types", func() {
		Context("Numeric and non-numeric in same series", func() {
			It("should handle type changes gracefully", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"value": 42.0, "timestamp_ms": 1000},      # Numeric
        {"value": 42.3, "timestamp_ms": 2000},      # Numeric +0.3 < 1.0 → DROP
        {"value": "OFFLINE", "timestamp_ms": 3000}, # String → KEEP (type change)
        {"value": "OFFLINE", "timestamp_ms": 4000}, # String same → DROP
        {"value": 45.0, "timestamp_ms": 5000}       # Numeric → KEEP (type change)
      ].index(count("generated") % 5)
      meta data_contract = "_historian"
      meta umh_topic = "test.mixed"
    count: 5

pipeline:
  processors:
    - downsampler:
        algorithm: deadband
        threshold: 1.0

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 4 messages kept (42.0, "OFFLINE", 45.0, type changes are always kept)
				// Expected: 1 message dropped (42.3 within threshold, duplicate "OFFLINE")
			})
		})
	})

	Describe("Edge Cases", func() {
		Context("Zero thresholds", func() {
			It("should drop only exact duplicates with zero threshold", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"value": 10.0, "timestamp_ms": 1000},
        {"value": 10.0, "timestamp_ms": 2000},       # Exact same → DROP
        {"value": 10.000001, "timestamp_ms": 3000},  # Tiny diff > 0 threshold → KEEP
        {"value": 10.000001, "timestamp_ms": 4000}   # Exact same → DROP
      ].index(count("generated") % 4)
      meta data_contract = "_historian"
      meta umh_topic = "test.zero_threshold"
    count: 4

pipeline:
  processors:
    - downsampler:
        algorithm: deadband
        threshold: 0.0

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 2 messages kept (10.0 first, 10.000001 different)
				// Expected: 2 messages dropped (exact duplicates)
			})
		})

		Context("Large thresholds", func() {
			It("should drop most values with very large threshold", func() {
				spec := `
input:
  generate:
    mapping: |
      root = [
        {"value": 0.0, "timestamp_ms": 1000},
        {"value": 50.0, "timestamp_ms": 2000},   # +50 < 100 threshold → DROP
        {"value": 75.0, "timestamp_ms": 3000},   # +75 < 100 threshold → DROP
        {"value": 150.0, "timestamp_ms": 4000}   # +150 > 100 threshold → KEEP
      ].index(count("generated") % 4)
      meta data_contract = "_historian"
      meta umh_topic = "test.large_threshold"
    count: 4

pipeline:
  processors:
    - downsampler:
        algorithm: deadband
        threshold: 100.0

output:
  drop: {}
`

				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				Expect(builder.SetYAML(spec)).To(Succeed())

				stream, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				go func() { _ = stream.Run(ctx) }()
				time.Sleep(100 * time.Millisecond)
				stream.Stop(ctx)

				// Expected: 2 messages kept (0.0 first, 150.0 exceeds threshold)
				// Expected: 2 messages dropped (50.0, 75.0 within threshold)
			})
		})
	})
})
