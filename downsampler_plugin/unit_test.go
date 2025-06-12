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

// Unit Tests - Configuration and Component Logic
// These tests focus on individual components and configuration logic
// without requiring Benthos streams or environment variables.

var _ = Describe("Unit Tests", func() {
	Describe("Configuration Logic", func() {
		Describe("GetConfigForTopic function", func() {
			var config downsampler.DownsamplerConfig

			BeforeEach(func() {
				// Reset config for each test
				config = downsampler.DownsamplerConfig{}
			})

			Context("with default deadband configuration", func() {
				BeforeEach(func() {
					config.Default.Deadband.Threshold = 1.5
					config.Default.Deadband.MaxTime = 30 * time.Second
				})

				It("should return default deadband values for any topic", func() {
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
					algorithm, configMap := config.GetConfigForTopic("umh.v1.plant._historian.temperature")
					Expect(algorithm).To(Equal("deadband"))
					Expect(configMap).To(HaveKeyWithValue("threshold", 0.1))
				})

				It("should fall back to default for non-matching topics", func() {
					algorithm, configMap := config.GetConfigForTopic("umh.v1.plant._historian.humidity")
					Expect(algorithm).To(Equal("deadband"))
					Expect(configMap).To(HaveKeyWithValue("threshold", 2.0))
				})
			})

			Context("with exact topic matching", func() {
				BeforeEach(func() {
					config.Default.Deadband.Threshold = 2.0
					config.Overrides = []downsampler.OverrideConfig{
						{
							Topic:    "umh.v1.plant._historian.critical_temp",
							Deadband: &downsampler.DeadbandConfig{Threshold: 0.01},
						},
					}
				})

				It("should match exact topics correctly", func() {
					algorithm, configMap := config.GetConfigForTopic("umh.v1.plant._historian.critical_temp")
					Expect(algorithm).To(Equal("deadband"))
					Expect(configMap).To(HaveKeyWithValue("threshold", 0.01))
				})

				It("should fall back to default for non-exact topics", func() {
					algorithm, configMap := config.GetConfigForTopic("umh.v1.plant._historian.critical_temp_2")
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

			Context("with late policy configuration", func() {
				BeforeEach(func() {
					config.Default.Deadband.Threshold = 1.0
					config.Default.LatePolicy.LatePolicy = "drop"
					config.Overrides = []downsampler.OverrideConfig{
						{
							Pattern:    "*.critical",
							LatePolicy: &downsampler.LatePolicyConfig{LatePolicy: "passthrough"},
						},
					}
				})

				It("should apply default late policy", func() {
					algorithm, configMap := config.GetConfigForTopic("umh.v1.plant.temperature")

					Expect(algorithm).To(Equal("deadband"))
					Expect(configMap).To(HaveKeyWithValue("late_policy", "drop"))
				})

				It("should apply late policy overrides", func() {
					algorithm, configMap := config.GetConfigForTopic("umh.v1.plant.critical")

					Expect(algorithm).To(Equal("deadband"))
					Expect(configMap).To(HaveKeyWithValue("late_policy", "passthrough"))
				})
			})
		})
	})
})
