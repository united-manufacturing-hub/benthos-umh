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

var _ = Describe("Configuration Logic", func() {
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
				algorithm, configMap, err := config.GetConfigForTopic("any.topic.here")

				Expect(err).NotTo(HaveOccurred())
				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 1.5))
				Expect(configMap).To(HaveKeyWithValue("max_time", "30s"))
			})
		})

		Context("with default swinging_door min_time configuration", func() {
			BeforeEach(func() {
				config.Default.SwingingDoor.Threshold = 1.5
				config.Default.SwingingDoor.MinTime = 15 * time.Second
			})

			It("should return min_time as a string in the config map", func() {
				algorithm, configMap, err := config.GetConfigForTopic("any.topic.here")

				Expect(err).NotTo(HaveOccurred())
				Expect(algorithm).To(Equal("swinging_door"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 1.5))
				Expect(configMap).To(HaveKeyWithValue("min_time", "15s"))
			})
		})

		Context("with pattern matching overrides", func() {
			BeforeEach(func() {
				config.Default.Deadband.Threshold = 2.0
				config.Overrides = []downsampler.OverrideConfig{
					{
						Pattern:  "*temperature",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.1},
					},
					{
						Pattern:  "*pressure",
						Deadband: &downsampler.DeadbandConfig{Threshold: 0.5},
					},
				}
			})

			It("should match wildcard patterns across different field levels", func() {
				// This should match *temperature since 'critical_temperature' ends with 'temperature'
				algorithm, configMap, err := config.GetConfigForTopic("umh.v1.acme._historian.critical_temperature")
				Expect(err).NotTo(HaveOccurred())
				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.1))

				// This should match *temperature since it ends with 'temperature'
				algorithm, configMap, err = config.GetConfigForTopic("umh.v1.factory._historian.temperature")
				Expect(err).NotTo(HaveOccurred())
				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.1))

				// This should use default since it doesn't match any pattern
				algorithm, configMap, err = config.GetConfigForTopic("umh.v1.plant._historian.flow_rate")
				Expect(err).NotTo(HaveOccurred())
				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 2.0))
			})

			It("should match final segment fallback patterns", func() {
				algorithm, configMap, err := config.GetConfigForTopic("umh.v1.plant1.temperature")
				Expect(err).NotTo(HaveOccurred())
				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.1))

				algorithm, configMap, err = config.GetConfigForTopic("umh.v1.factory.pressure")
				Expect(err).NotTo(HaveOccurred())
				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.5))

				algorithm, configMap, err = config.GetConfigForTopic("umh.v2.plant1.humidity")
				Expect(err).NotTo(HaveOccurred())
				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 2.0))
			})

			It("should return algorithm and config matching the override", func() {
				algorithm, configMap, err := config.GetConfigForTopic("umh.v1.plant.temperature")
				Expect(err).NotTo(HaveOccurred())
				Expect(algorithm).To(Equal("deadband"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.1))

				// For this test to work, we need to add a swinging_door override
				config.Overrides = append(config.Overrides, downsampler.OverrideConfig{
					Pattern:      "*critical",
					SwingingDoor: &downsampler.SwingingDoorConfig{Threshold: 0.1},
				})

				algorithm, configMap, err = config.GetConfigForTopic("umh.v1.plant.critical")
				Expect(err).NotTo(HaveOccurred())
				Expect(algorithm).To(Equal("swinging_door"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.1))
			})
		})

		Context("algorithm precedence behavior", func() {
			It("should prioritize swinging_door when both algorithms are specified", func() {
				// Test that swinging_door takes precedence over deadband when both are configured
				conflictingOverride := downsampler.OverrideConfig{
					Pattern:      "*temperature",
					Deadband:     &downsampler.DeadbandConfig{Threshold: 0.1, MaxTime: time.Minute},
					SwingingDoor: &downsampler.SwingingDoorConfig{Threshold: 0.2, MaxTime: time.Hour, MinTime: time.Second * 5},
				}

				config.Overrides = []downsampler.OverrideConfig{conflictingOverride}

				algorithm, configMap, err := config.GetConfigForTopic("test.temperature")
				Expect(err).NotTo(HaveOccurred())
				Expect(algorithm).To(Equal("swinging_door"))
				Expect(configMap).To(HaveKeyWithValue("threshold", 0.2))
				Expect(configMap).To(HaveKeyWithValue("max_time", "1h0m0s"))
				Expect(configMap).To(HaveKeyWithValue("min_time", "5s"))
			})
		})
	})
})
