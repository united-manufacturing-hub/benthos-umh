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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	downsampler "github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin"
)

// Unit Tests for Configuration Logic
// These tests focus on configuration parsing, topic matching, and algorithm selection
// without requiring Benthos streams or environment variables.

var _ = Describe("Configuration Logic", func() {
	Describe("getConfigForTopic function", func() {
		var config downsampler.DownsamplerConfig

		BeforeEach(func() {
			// Reset config for each test
			config = downsampler.DownsamplerConfig{}
		})

		Context("with default configuration only", func() {
			BeforeEach(func() {
				config.Default.Deadband.Threshold = 1.5
				config.Default.Deadband.MaxTime = 30 * time.Second
			})

			It("should return default values for any topic", func() {
				algorithm, configMap := config.GetConfigForTopic("any.topic.here")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 1.5))
				Expect(configMap).To(HaveKeyWithValue("max_time", 30*time.Second))
			})
		})

		Context("with pattern matching overrides", func() {
			BeforeEach(func() {
				config.Default.Deadband.Threshold = 2.0
				config.Overrides = []downsampler.OverrideConfig{
					{
						Pattern:  "*.temperature",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.1},
					},
					{
						Pattern:  "*.pressure",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.5},
					},
				}
			})

			It("should match wildcard patterns correctly", func() {
				// Test temperature pattern match
				algorithm, configMap := config.GetConfigForTopic("umh.v1.acme._historian.temperature")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.1))
			})

			It("should match different patterns", func() {
				// Test pressure pattern match
				algorithm, configMap := config.GetConfigForTopic("umh.v1.factory.line1._historian.pressure")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.5))
			})

			It("should fall back to default for non-matching topics", func() {
				// Test non-matching topic
				algorithm, configMap := config.GetConfigForTopic("umh.v1.acme._historian.humidity")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 2.0))
			})
		})

		Context("with exact topic matching overrides", func() {
			BeforeEach(func() {
				config.Default.Deadband.Threshold = 2.0
				config.Overrides = []downsampler.OverrideConfig{
					{
						Topic:    "umh.v1.acme._historian.critical_temp",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.01},
					},
				}
			})

			It("should match exact topics correctly", func() {
				algorithm, configMap := config.GetConfigForTopic("umh.v1.acme._historian.critical_temp")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.01))
			})

			It("should fall back to default for non-exact topics", func() {
				algorithm, configMap := config.GetConfigForTopic("umh.v1.acme._historian.critical_temp_2")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 2.0))
			})
		})

		Context("with mixed pattern and exact topic overrides", func() {
			BeforeEach(func() {
				config.Default.Deadband.Threshold = 10.0
				config.Overrides = []downsampler.OverrideConfig{
					{
						Pattern:  "*.temperature",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.5},
					},
					{
						Topic:    "umh.v1.acme._historian.critical_temperature",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.01},
					},
				}
			})

			It("should apply first matching rule (exact topic before pattern)", func() {
				// This should match the exact topic rule first
				algorithm, configMap := config.GetConfigForTopic("umh.v1.acme._historian.critical_temperature")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.01))
			})

			It("should apply pattern rule when exact doesn't match", func() {
				// This should match the pattern rule
				algorithm, configMap := config.GetConfigForTopic("umh.v1.factory._historian.temperature")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.5))
			})

			It("should use default values when no overrides match", func() {
				// This topic should not match any pattern or exact topic override
				algorithm, configMap := config.GetConfigForTopic("umh.v1.plant._historian.flow_rate")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 10.0)) // Default threshold
			})
		})

		Context("with complex wildcard patterns", func() {
			BeforeEach(func() {
				config.Default.Deadband.Threshold = 5.0
				config.Overrides = []downsampler.OverrideConfig{
					{
						Pattern:  "umh.v1.*.temperature",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.2},
					},
					{
						Pattern:  "umh.v1.factory.*",
						Deadband: &downsampler.DeadbandConfig{Threshold: 1.0},
					},
				}
			})

			It("should match specific wildcard patterns", func() {
				// Should match umh.v1.*.temperature
				algorithm, configMap := config.GetConfigForTopic("umh.v1.plant1.temperature")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.2))
			})

			It("should match broader wildcard patterns", func() {
				// Should match umh.v1.factory.*
				algorithm, configMap := config.GetConfigForTopic("umh.v1.factory.pressure")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 1.0))
			})

			It("should use default values when no complex patterns match", func() {
				// This topic should not match any of the complex wildcard patterns
				algorithm, configMap := config.GetConfigForTopic("umh.v2.plant1.humidity")

				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 5.0)) // Default threshold
			})
		})

		Context("with swinging door algorithm", func() {
			BeforeEach(func() {
				config.Default.SwingingDoor.Threshold = 0.3
				config.Default.SwingingDoor.MinTime = 5 * time.Second
				config.Default.SwingingDoor.MaxTime = 1 * time.Hour
				config.Overrides = []downsampler.OverrideConfig{
					{
						Pattern: "*.vibration",
						SwingingDoor: &downsampler.SwingingDoorConfig{
							Threshold: 0.01,
							MinTime:   1 * time.Second,
						},
					},
				}
			})

			It("should return swinging door algorithm for default", func() {
				algorithm, configMap := config.GetConfigForTopic("any.topic")

				Expect(algorithm).To(Equal("swinging_door"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.3))
				Expect(configMap).To(HaveKeyWithValue("min_time", 5*time.Second))
				Expect(configMap).To(HaveKeyWithValue("max_time", 1*time.Hour))
			})

			It("should apply swinging door overrides", func() {
				algorithm, configMap := config.GetConfigForTopic("umh.v1.machine._historian.vibration")

				Expect(algorithm).To(Equal("swinging_door"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.01))
				Expect(configMap).To(HaveKeyWithValue("min_time", 1*time.Second))
			})

			It("should use default swinging door values when no overrides match", func() {
				// This topic should not match the vibration pattern
				algorithm, configMap := config.GetConfigForTopic("umh.v1.machine._historian.temperature")

				Expect(algorithm).To(Equal("swinging_door"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.3))          // Default threshold
				Expect(configMap).To(HaveKeyWithValue("min_time", 5*time.Second)) // Default min_time
				Expect(configMap).To(HaveKeyWithValue("max_time", 1*time.Hour))   // Default max_time
			})
		})
	})
})
