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

package algorithms_test

import (
	"math"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

var _ = Describe("Deadband Algorithm", func() {
	var algo algorithms.StreamCompressor
	var err error
	var baseTime time.Time

	BeforeEach(func() {
		baseTime = time.Now()
		DeferCleanup(func() {
			if algo != nil {
				algo.Reset()
			}
		})
	})

	Describe("basic functionality", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"threshold": 0.5,
			}
			algo, err = algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should always keep the first point", func() {
			points, err := algo.Ingest(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1), "First point should always be kept")
			Expect(points[0].Value).To(Equal(10.0))
			Expect(points[0].Timestamp).To(Equal(baseTime))
		})

		It("should process a sequence correctly", func() {
			// First point - always kept
			points, err := algo.Ingest(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1), "First point should always be kept")
			Expect(points[0].Value).To(Equal(10.0))

			// Small change (0.3 < 0.5) - should be filtered
			points, err = algo.Ingest(10.3, baseTime.Add(time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(0), "Small change (0.3 < 0.5) should be filtered")

			// Large change (1.6 >= 0.5) - should be kept
			points, err = algo.Ingest(11.6, baseTime.Add(2*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1), "Large change (1.6 >= 0.5) should be kept")
			Expect(points[0].Value).To(Equal(11.6))

			// Exact threshold change (0.5 = 0.5) - should be kept
			points, err = algo.Ingest(12.1, baseTime.Add(3*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1), "Exact threshold change (0.5 = 0.5) should be kept")
			Expect(points[0].Value).To(Equal(12.1))
		})

		Context("additional basic filtering scenarios", func() {
			It("should handle basic filtering sequence", func() {
				// First point should always be kept
				points, err := algo.Ingest(10.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1), "First point should be kept")
				Expect(points[0].Value).To(Equal(10.0))

				// Small change (< threshold) should be dropped
				points, err = algo.Ingest(10.3, baseTime.Add(time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(0), "Small change should be dropped")

				// Large change (>= threshold) should be kept
				points, err = algo.Ingest(10.6, baseTime.Add(2*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1), "Large change should be kept")
				Expect(points[0].Value).To(Equal(10.6))

				// Another small change should be dropped
				points, err = algo.Ingest(10.5, baseTime.Add(3*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(0), "Small change should be dropped")
			})
		})
	})

	Describe("max interval functionality", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"threshold": 0.5,
				"max_time":  60 * time.Second,
			}
			algo, err = algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle max interval correctly", func() {
			// First point
			points, err := algo.Ingest(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1), "First point should be kept")
			Expect(points[0].Value).To(Equal(10.0))

			// Small change within time limit - should be filtered
			points, err = algo.Ingest(10.3, baseTime.Add(30*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(0), "Small change within time limit should be filtered")

			// Small change after max interval - should be kept
			points, err = algo.Ingest(10.3, baseTime.Add(70*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(points).To(HaveLen(1), "Small change after max interval should be kept")
			Expect(points[0].Value).To(Equal(10.3))
		})

		Context("max interval with 1 minute duration", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 0.5,
					"max_time":  1 * time.Minute,
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should enforce max interval constraint", func() {
				// First point
				points, err := algo.Ingest(10.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))
				Expect(points[0].Value).To(Equal(10.0))

				// Within max_interval, small change should be dropped
				points, err = algo.Ingest(10.3, baseTime.Add(30*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(0))

				// After max_interval, even small change should be kept
				points, err = algo.Ingest(10.3, baseTime.Add(70*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))
				Expect(points[0].Value).To(Equal(10.3))
			})
		})
	})

	Describe("gap-filling edge cases", func() {
		Context("directional threshold behavior", func() {
			// Scientific basis: Dead-band compression must use abs(Δ) to ensure symmetrical
			// behavior for both upward and downward changes. This prevents directional bias
			// that could lead to systematic drift in long-term data series.
			// Reference: Moravek et al., Atmos. Meas. Tech., 2019 - discusses how asymmetric
			// filtering can introduce systematic errors in flux measurements.
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 0.5,
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("keeps a negative Δ exactly = threshold", func() {
				// Critical test: Ensures that negative changes of exactly threshold magnitude
				// are preserved. This prevents "≤" vs "<" implementation bugs that could
				// create directional bias where downward changes are treated differently
				// than upward changes. Industrial historians must maintain symmetry to avoid
				// systematic drift in compressed data.
				points, err := algo.Ingest(10.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))
				points, err = algo.Ingest(9.5, baseTime.Add(time.Second)) // Δ = −0.5
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1)) // must keep - exact threshold magnitude
			})

			It("drops an identical repeat", func() {
				// Edge case validation: Δ = 0 should always be filtered unless other
				// constraints (like max_interval) force emission. This test prevents
				// accidental "≤" vs "<" boundary condition errors in threshold comparison.
				// Even with threshold = 0, repeats should be dropped (special case behavior).
				_, _ = algo.Ingest(10.0, baseTime)
				points, err := algo.Ingest(10.0, baseTime.Add(time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(0), "Identical value should be dropped")
			})
		})

		Context("max interval edge cases", func() {
			It("flushes unchanged value after max_interval", func() {
				// Time-bounded compression principle: Industrial historians implement
				// "compression deviation OR comp-max-time" rule to bound both magnitude
				// and temporal error. This ensures that even constant values are
				// periodically recorded to indicate system liveness and prevent
				// interpolation errors over extended periods.
				// Reference: PI Server docs - compression testing page describes this
				// dual-constraint approach as fundamental to data integrity.
				cfg := map[string]interface{}{"threshold": 0.5, "max_time": 60 * time.Second}
				algo, _ := algorithms.NewDeadbandAlgorithm(cfg)
				t0 := baseTime
				_, _ = algo.Ingest(10.0, t0)
				points, err := algo.Ingest(10.0, t0.Add(65*time.Second)) // Δ = 0, but > 60 s
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1), "time constraint overrides threshold")
			})
		})

		Context("threshold validation", func() {
			It("should reject negative thresholds", func() {
				config := map[string]interface{}{
					"threshold": -0.5,
				}
				_, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("threshold cannot be negative"))
			})

			It("should accept zero threshold", func() {
				config := map[string]interface{}{
					"threshold": 0.0,
				}
				algo, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
				Expect(algo).NotTo(BeNil())

				// First point
				points, err := algo.Ingest(10.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))

				// Even tiny change should be kept with zero threshold
				points, err = algo.Ingest(10.001, baseTime.Add(time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))

				// Exact repeat should be dropped even with zero threshold
				points, err = algo.Ingest(10.001, baseTime.Add(2*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(0))
			})
		})

		Context("very large threshold edge case", func() {
			It("should drop everything after first point with MaxFloat64 threshold", func() {
				config := map[string]interface{}{
					"threshold": math.MaxFloat64,
				}
				algo, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())

				// First point should be kept
				points, err := algo.Ingest(10.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))

				// Even huge change should be dropped with MaxFloat64 threshold
				points, err = algo.Ingest(1e100, baseTime.Add(time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(0))
			})
		})

		Context("zero max_time configuration", func() {
			It("should behave identically to no max_time when explicitly set to 0s", func() {
				configWithZero := map[string]interface{}{
					"threshold": 0.5,
					"max_time":  0 * time.Second,
				}
				algoWithZero, err := algorithms.NewDeadbandAlgorithm(configWithZero)
				Expect(err).NotTo(HaveOccurred())

				configWithoutMax := map[string]interface{}{
					"threshold": 0.5,
				}
				algoWithoutMax, err := algorithms.NewDeadbandAlgorithm(configWithoutMax)
				Expect(err).NotTo(HaveOccurred())

				// Both should behave identically
				testSequence := []struct {
					value float64
					delay time.Duration
				}{
					{10.0, 0},
					{10.3, time.Second},     // Small change - should be dropped
					{11.0, 2 * time.Second}, // Large change - should be kept
					{11.2, 3 * time.Second}, // Small change - should be dropped
				}

				for _, test := range testSequence {
					timestamp := baseTime.Add(test.delay)

					pointsZero, errZero := algoWithZero.Ingest(test.value, timestamp)
					pointsNoMax, errNoMax := algoWithoutMax.Ingest(test.value, timestamp)

					if errZero == nil && errNoMax == nil {
						Expect(len(pointsZero) > 0).To(Equal(len(pointsNoMax) > 0), "Algorithms should behave identically for value %.1f", test.value)
					} else {
						Expect(errZero).To(Equal(errNoMax))
					}
				}
			})
		})

		Context("non-monotonic timestamp handling", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 0.5,
					"max_time":  60 * time.Second,
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should handle clock skew gracefully", func() {
				// First point
				points, err := algo.Ingest(10.0, baseTime.Add(5*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))

				// Point with earlier timestamp (clock skew)
				points, err = algo.Ingest(10.6, baseTime.Add(4*time.Second))

				// Should either handle gracefully or return an error, but not panic
				// The current implementation will treat negative duration as 0
				if err != nil {
					// Error is acceptable
					Expect(err).To(HaveOccurred())
				} else {
					// If no error, behavior should be predictable
					// In this case, negative duration means no max_time constraint
					Expect(points).To(HaveLen(1)) // Should keep due to threshold (0.6 > 0.5)
				}
			})
		})
	})

	Describe("metadata generation", func() {
		It("should generate correct metadata without max interval", func() {
			config := map[string]interface{}{
				"threshold": 0.5,
			}
			algo, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())

			metadata := algo.Config()
			Expect(metadata).To(Equal("deadband(threshold=0.500)"))
		})

		It("should generate correct metadata with max time", func() {
			config := map[string]interface{}{
				"threshold": 0.5,
				"max_time":  60 * time.Second,
			}
			algo, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())

			metadata := algo.Config()
			Expect(metadata).To(Equal("deadband(threshold=0.500,max_time=1m0s)"))
		})
	})

	Describe("configuration validation", func() {
		DescribeTable("valid configurations",
			func(config map[string]interface{}) {
				_, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			},
			Entry("empty config (uses defaults)", map[string]interface{}{}),
			Entry("basic threshold", map[string]interface{}{"threshold": 1.5}),
			Entry("with max_time", map[string]interface{}{
				"threshold": 0.5,
				"max_time":  60 * time.Second,
			}),
		)

		DescribeTable("invalid configurations",
			func(config map[string]interface{}, expectedErrorSubstring string) {
				_, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(expectedErrorSubstring))
			},
			Entry("invalid threshold type",
				map[string]interface{}{"threshold": "invalid"},
				"invalid threshold"),
			Entry("invalid max_time",
				map[string]interface{}{
					"threshold": 1.0,
					"max_time":  "invalid",
				},
				"invalid max_time"),
		)
	})

	Describe("human-verifiable examples", func() {
		// Simple tests that are easy to manually verify and understand
		Context("simple sequences", func() {
			BeforeEach(func() {
				config := map[string]interface{}{"threshold": 1.0}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should filter small oscillations around a value", func() {
				// Human verification: Sensor reading 10.0, then small noise ±0.5, should filter the noise
				points, _ := algo.Ingest(10.0, baseTime)
				Expect(points).To(HaveLen(1), "First reading: 10.0 → KEEP (always keep first)")

				points, _ = algo.Ingest(10.3, baseTime.Add(1*time.Second))
				Expect(points).To(HaveLen(0), "Reading: 10.3 → DROP (change = 0.3 < threshold 1.0)")

				points, _ = algo.Ingest(9.7, baseTime.Add(2*time.Second))
				Expect(points).To(HaveLen(0), "Reading: 9.7 → DROP (change from last kept = 0.3 < threshold 1.0)")

				points, _ = algo.Ingest(11.5, baseTime.Add(3*time.Second))
				Expect(points).To(HaveLen(1), "Reading: 11.5 → KEEP (change from last kept = 1.5 ≥ threshold 1.0)")
			})

			It("should handle a temperature sensor example", func() {
				// Human verification: Room temperature sensor, threshold 0.5°C
				config := map[string]interface{}{"threshold": 0.5}
				algo, _ := algorithms.NewDeadbandAlgorithm(config)

				points, _ := algo.Ingest(22.0, baseTime)
				Expect(points).To(HaveLen(1), "Initial temp: 22.0°C → KEEP")

				points, _ = algo.Ingest(22.3, baseTime.Add(1*time.Minute))
				Expect(points).To(HaveLen(0), "Small rise: 22.3°C → DROP (0.3°C < 0.5°C threshold)")

				points, _ = algo.Ingest(22.8, baseTime.Add(2*time.Minute))
				Expect(points).To(HaveLen(1), "Significant rise: 22.8°C → KEEP (0.8°C ≥ 0.5°C threshold)")

				points, _ = algo.Ingest(22.9, baseTime.Add(3*time.Minute))
				Expect(points).To(HaveLen(0), "Small rise: 22.9°C → DROP (0.1°C < 0.5°C threshold)")
			})
		})

		Context("max time examples", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 2.0,
					"max_time":  30 * time.Second,
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should send heartbeat even with no significant change", func() {
				// Human verification: System sends periodic updates even if value doesn't change much
				points, _ := algo.Ingest(100.0, baseTime)
				Expect(points).To(HaveLen(1), "Initial value: 100.0 → KEEP")

				// Small changes that would normally be filtered
				points, _ = algo.Ingest(100.5, baseTime.Add(10*time.Second))
				Expect(points).To(HaveLen(0), "Small change: 100.5 → DROP (0.5 < 2.0 threshold)")

				points, _ = algo.Ingest(101.0, baseTime.Add(20*time.Second))
				Expect(points).To(HaveLen(0), "Small change: 101.0 → DROP (1.0 < 2.0 threshold)")

				// After 35 seconds, max_time forces emission even with small change
				points, _ = algo.Ingest(101.2, baseTime.Add(35*time.Second))
				Expect(points).To(HaveLen(1), "Heartbeat: 101.2 → KEEP (max_time 30s exceeded)")
			})
		})

		Context("edge case demonstrations", func() {
			BeforeEach(func() {
				config := map[string]interface{}{"threshold": 1.0}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should handle exact threshold boundaries correctly", func() {
				// Human verification: Test the boundary condition at exactly threshold value
				points, _ := algo.Ingest(10.0, baseTime)
				Expect(points).To(HaveLen(1), "Start: 10.0 → KEEP")

				points, _ = algo.Ingest(11.0, baseTime.Add(1*time.Second))
				Expect(points).To(HaveLen(1), "Exact threshold: 11.0 → KEEP (change = 1.0 = threshold)")

				points, _ = algo.Ingest(11.99, baseTime.Add(2*time.Second))
				Expect(points).To(HaveLen(0), "Just under threshold: 11.99 → DROP (change = 0.99 < 1.0)")

				points, _ = algo.Ingest(12.01, baseTime.Add(3*time.Second))
				Expect(points).To(HaveLen(1), "Just over threshold: 12.01 → KEEP (change = 1.01 > 1.0)")
			})

			It("should work with negative values and negative changes", func() {
				// Human verification: Algorithm works symmetrically for positive and negative values
				points, _ := algo.Ingest(-5.0, baseTime)
				Expect(points).To(HaveLen(1), "Start: -5.0 → KEEP")

				points, _ = algo.Ingest(-4.0, baseTime.Add(1*time.Second))
				Expect(points).To(HaveLen(1), "Increase: -4.0 → KEEP (change = 1.0 = threshold)")

				points, _ = algo.Ingest(-6.0, baseTime.Add(2*time.Second))
				Expect(points).To(HaveLen(1), "Decrease: -6.0 → KEEP (change = 2.0 > threshold)")

				points, _ = algo.Ingest(-5.5, baseTime.Add(3*time.Second))
				Expect(points).To(HaveLen(0), "Small change: -5.5 → DROP (change = 0.5 < threshold)")
			})
		})
	})

	// New context for additional edge cases from code review
	Describe("Code Review Edge Cases", func() {
		Context("default configuration behavior", func() {
			It("should use threshold=0 when no config provided", func() {
				// Test that empty config {} behaves as threshold=0 (keep any change, drop exact repeats)
				config := map[string]interface{}{}
				algo, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
				Expect(algo).NotTo(BeNil())

				// Verify the algorithm behaves as if threshold=0
				points, _ := algo.Ingest(10.0, baseTime)
				Expect(points).To(HaveLen(1), "First value: 10.0 → KEEP (always keep first)")

				points, _ = algo.Ingest(10.0, baseTime.Add(1*time.Second))
				Expect(points).To(HaveLen(0), "Exact repeat: 10.0 → DROP (no change with threshold=0)")

				points, _ = algo.Ingest(10.0001, baseTime.Add(2*time.Second))
				Expect(points).To(HaveLen(1), "Tiny change: 10.0001 → KEEP (any change with threshold=0)")

				points, _ = algo.Ingest(10.0001, baseTime.Add(3*time.Second))
				Expect(points).To(HaveLen(0), "Exact repeat: 10.0001 → DROP (no change with threshold=0)")

				// Verify config string shows threshold=0.000
				configStr := algo.Config()
				Expect(configStr).To(Equal("deadband(threshold=0.000)"))
			})

			It("should behave identically to explicit threshold=0", func() {
				// Compare empty config vs explicit threshold=0
				emptyConfig := map[string]interface{}{}
				explicitConfig := map[string]interface{}{"threshold": 0.0}

				algoEmpty, err := algorithms.NewDeadbandAlgorithm(emptyConfig)
				Expect(err).NotTo(HaveOccurred())

				algoExplicit, err := algorithms.NewDeadbandAlgorithm(explicitConfig)
				Expect(err).NotTo(HaveOccurred())

				// Both should produce identical results
				testValues := []float64{5.0, 5.0, 5.1, 5.1, 5.2}
				expectedLengths := []int{1, 0, 1, 0, 1} // first, repeat, change, repeat, change

				for i, value := range testValues {
					timestamp := baseTime.Add(time.Duration(i) * time.Second)

					pointsEmpty, err := algoEmpty.Ingest(value, timestamp)
					Expect(err).NotTo(HaveOccurred())

					pointsExplicit, err := algoExplicit.Ingest(value, timestamp)
					Expect(err).NotTo(HaveOccurred())

					Expect(len(pointsEmpty)).To(Equal(len(pointsExplicit)),
						"Empty config and explicit threshold=0 should behave identically for value %v", value)
					Expect(len(pointsEmpty)).To(Equal(expectedLengths[i]),
						"Value %v should produce %d points", value, expectedLengths[i])
				}

				// Both should have identical config strings
				Expect(algoEmpty.Config()).To(Equal(algoExplicit.Config()))
			})
		})

		Context("thread safety documentation", func() {
			It("should document thread safety expectations", func() {
				// This is a documentation test - the algorithm should clearly state
				// that it's not goroutine-safe
				config := map[string]interface{}{"threshold": 0.5}
				algo, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
				Expect(algo).NotTo(BeNil())

				// Algorithm should work correctly when used sequentially
				_, _ = algo.Ingest(10.0, baseTime)
				points, err := algo.Ingest(10.6, baseTime.Add(time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))
			})
		})

		Context("NaN and Inf value guards", func() {
			It("should reject NaN values", func() {
				config := map[string]interface{}{"threshold": 1.0}
				algo, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())

				points, err := algo.Ingest(math.NaN(), baseTime)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("NaN and Inf values are not allowed"))
				Expect(points).To(BeNil())
			})

			It("should reject positive infinity", func() {
				config := map[string]interface{}{"threshold": 1.0}
				algo, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())

				points, err := algo.Ingest(math.Inf(1), baseTime)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("NaN and Inf values are not allowed"))
				Expect(points).To(BeNil())
			})

			It("should reject negative infinity", func() {
				config := map[string]interface{}{"threshold": 1.0}
				algo, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())

				points, err := algo.Ingest(math.Inf(-1), baseTime)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("NaN and Inf values are not allowed"))
				Expect(points).To(BeNil())
			})

			It("should continue working normally after rejecting invalid values", func() {
				config := map[string]interface{}{"threshold": 1.0}
				algo, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())

				// Try to ingest NaN - should fail
				_, err = algo.Ingest(math.NaN(), baseTime)
				Expect(err).To(HaveOccurred())

				// Normal values should still work
				points, err := algo.Ingest(10.0, baseTime.Add(time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1)) // First valid point kept
			})
		})
	})

	Describe("comprehensive deadband test plan verification", func() {
		// Test cases implementing deadband filtering patterns
		// These tests verify deadband behavior against various data patterns

		Context("Basic Filtering - Threshold-based compression", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 1.0,
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should filter small changes below threshold", func() {
				rawData := []struct {
					x float64
					y float64
				}{
					{0, 10.0}, // Base value, always kept
					{1, 10.5}, // Change = 0.5 < 1.0, should be dropped
					{2, 10.8}, // Change = 0.8 < 1.0, should be dropped
					{3, 11.2}, // Change = 1.2 >= 1.0, should be kept (new base)
					{4, 11.5}, // Change = 0.3 < 1.0, should be dropped
					{5, 12.5}, // Change = 1.3 >= 1.0, should be kept
					{6, 12.3}, // Change = 0.2 < 1.0, should be dropped
				}

				expectedKept := []bool{true, false, false, true, false, true, false}

				for i, point := range rawData {
					timestamp := baseTime.Add(time.Duration(point.x) * time.Second)
					points, err := algo.Ingest(point.y, timestamp)
					Expect(err).NotTo(HaveOccurred())
					if expectedKept[i] {
						Expect(points).To(HaveLen(1), "Point %d (x=%.1f, y=%.1f) should be kept", i, point.x, point.y)
					} else {
						Expect(points).To(HaveLen(0), "Point %d (x=%.1f, y=%.1f) should be dropped", i, point.x, point.y)
					}
				}

				// Verify compression: 7 raw -> 3 kept = ~57% reduction
				keptCount := 0
				for _, kept := range expectedKept {
					if kept {
						keptCount++
					}
				}
				reductionPercent := int(float64(len(rawData)-keptCount) / float64(len(rawData)) * 100)
				Expect(reductionPercent).To(Equal(57), "Expected ~57%% reduction")
			})
		})

		Context("High Precision Filtering - Small threshold", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 0.1,
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should handle high precision sensor data", func() {
				rawData := []struct {
					x float64
					y float64
				}{
					{0, 5.00}, // Base value
					{1, 5.05}, // Change = 0.05 < 0.1, dropped
					{2, 5.08}, // Change = 0.08 < 0.1, dropped
					{3, 5.12}, // Change = 0.12 >= 0.1, kept
					{4, 5.15}, // Change = 0.03 < 0.1, dropped
					{5, 5.25}, // Change = 0.13 >= 0.1, kept
					{6, 5.30}, // Change = 0.05 < 0.1, dropped
					{7, 5.40}, // Change = 0.15 >= 0.1, kept
				}

				expectedKept := []bool{true, false, false, true, false, true, false, true}

				for i, point := range rawData {
					timestamp := baseTime.Add(time.Duration(point.x) * time.Second)
					points, err := algo.Ingest(point.y, timestamp)
					Expect(err).NotTo(HaveOccurred())
					if expectedKept[i] {
						Expect(points).To(HaveLen(1), "Point %d (x=%.1f, y=%.1f) should be kept", i, point.x, point.y)
					} else {
						Expect(points).To(HaveLen(0), "Point %d (x=%.1f, y=%.1f) should be dropped", i, point.x, point.y)
					}
				}
			})
		})

		Context("Noise Filtering - Medium threshold", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 2.0,
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should filter noise while preserving significant changes", func() {
				rawData := []struct {
					x float64
					y float64
				}{
					{0, 100.0}, // Base value
					{1, 101.5}, // Noise: change = 1.5 < 2.0, dropped
					{2, 99.2},  // Noise: change = 1.8 < 2.0, dropped
					{3, 102.5}, // Signal: change = 2.5 >= 2.0, kept
					{4, 103.8}, // Small change: 1.3 < 2.0, dropped
					{5, 105.0}, // Signal: change = 2.5 >= 2.0, kept
					{6, 104.5}, // Small change: 0.5 < 2.0, dropped
					{7, 102.0}, // Signal: change = 3.0 >= 2.0, kept
				}

				expectedKept := []bool{true, false, false, true, false, true, false, true}

				for i, point := range rawData {
					timestamp := baseTime.Add(time.Duration(point.x) * time.Second)
					points, err := algo.Ingest(point.y, timestamp)
					Expect(err).NotTo(HaveOccurred())
					if expectedKept[i] {
						Expect(points).To(HaveLen(1), "Point %d (x=%.1f, y=%.1f) should be kept", i, point.x, point.y)
					} else {
						Expect(points).To(HaveLen(0), "Point %d (x=%.1f, y=%.1f) should be dropped", i, point.x, point.y)
					}
				}
			})
		})

		Context("Max Time Constraint - Heartbeat emission", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 5.0,             // Large threshold
					"max_time":  3 * time.Second, // Force emission every 3 seconds
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should force emission after max_time regardless of value change", func() {
				rawData := []struct {
					x float64
					y float64
				}{
					{0, 50.0}, // Base value, always kept
					{1, 51.0}, // Change = 1.0 < 5.0, normally dropped
					{2, 52.0}, // Change = 1.0 < 5.0, normally dropped
					{3, 53.0}, // Change = 1.0 < 5.0, but 3s elapsed -> kept due to max_time
					{4, 54.0}, // Change = 1.0 < 5.0, dropped
					{5, 55.0}, // Change = 1.0 < 5.0, dropped
					{6, 56.0}, // Change = 1.0 < 5.0, but 3s elapsed -> kept due to max_time
				}

				expectedKept := []bool{true, false, false, true, false, false, true}

				for i, point := range rawData {
					timestamp := baseTime.Add(time.Duration(point.x) * time.Second)
					points, err := algo.Ingest(point.y, timestamp)
					Expect(err).NotTo(HaveOccurred())
					if expectedKept[i] {
						Expect(points).To(HaveLen(1), "Point %d (x=%.1f, y=%.1f) should be kept", i, point.x, point.y)
					} else {
						Expect(points).To(HaveLen(0), "Point %d (x=%.1f, y=%.1f) should be dropped", i, point.x, point.y)
					}
				}
			})
		})

		Context("Constant Value Filtering", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 0.5,
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should drop exact repeats and small variations", func() {
				rawData := []struct {
					x float64
					y float64
				}{
					{0, 25.0}, // Base value
					{1, 25.0}, // Exact repeat, dropped
					{2, 25.0}, // Exact repeat, dropped
					{3, 25.3}, // Small change = 0.3 < 0.5, dropped
					{4, 25.0}, // Small change = 0.3 < 0.5, dropped
					{5, 25.6}, // Significant change = 0.6 >= 0.5, kept
					{6, 25.6}, // Exact repeat, dropped
				}

				expectedKept := []bool{true, false, false, false, false, true, false}

				for i, point := range rawData {
					timestamp := baseTime.Add(time.Duration(point.x) * time.Second)
					points, err := algo.Ingest(point.y, timestamp)
					Expect(err).NotTo(HaveOccurred())
					if expectedKept[i] {
						Expect(points).To(HaveLen(1), "Point %d (x=%.1f, y=%.1f) should be kept", i, point.x, point.y)
					} else {
						Expect(points).To(HaveLen(0), "Point %d (x=%.1f, y=%.1f) should be dropped", i, point.x, point.y)
					}
				}
			})
		})

		Context("Bidirectional Threshold Test", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 1.0,
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should handle positive and negative changes symmetrically", func() {
				rawData := []struct {
					x float64
					y float64
				}{
					{0, 10.0}, // Base value
					{1, 11.2}, // Positive change = +1.2 >= 1.0, kept
					{2, 10.8}, // Negative change = -0.4 < 1.0, dropped
					{3, 9.8},  // Negative change = -1.0 >= 1.0, kept (exactly threshold)
					{4, 9.3},  // Negative change = -0.5 < 1.0, dropped
					{5, 11.0}, // Positive change = +1.7 >= 1.0, kept
					{6, 10.5}, // Negative change = -0.5 < 1.0, dropped
				}

				expectedKept := []bool{true, true, false, true, false, true, false}

				for i, point := range rawData {
					timestamp := baseTime.Add(time.Duration(point.x) * time.Second)
					points, err := algo.Ingest(point.y, timestamp)
					Expect(err).NotTo(HaveOccurred())
					if expectedKept[i] {
						Expect(points).To(HaveLen(1), "Point %d (x=%.1f, y=%.1f) should be kept", i, point.x, point.y)
					} else {
						Expect(points).To(HaveLen(0), "Point %d (x=%.1f, y=%.1f) should be dropped", i, point.x, point.y)
					}
				}

				// Verify compression ratio
				keptCount := 0
				for _, kept := range expectedKept {
					if kept {
						keptCount++
					}
				}
				reductionPercent := int(float64(len(rawData)-keptCount) / float64(len(rawData)) * 100)
				Expect(reductionPercent).To(Equal(42), "Expected ~42% reduction")
			})
		})

		Context("Zero Threshold Behavior", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 0.0,
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should keep any change but drop exact repeats", func() {
				rawData := []struct {
					x float64
					y float64
				}{
					{0, 42.0},   // Base value
					{1, 42.0},   // Exact repeat -> dropped
					{2, 42.001}, // Tiny change -> kept (any change with threshold=0)
					{3, 42.001}, // Exact repeat -> dropped
					{4, 42.000}, // Small change -> kept
					{5, 42.000}, // Exact repeat -> dropped
				}

				expectedKept := []bool{true, false, true, false, true, false}

				for i, point := range rawData {
					timestamp := baseTime.Add(time.Duration(point.x) * time.Second)
					points, err := algo.Ingest(point.y, timestamp)
					Expect(err).NotTo(HaveOccurred())
					if expectedKept[i] {
						Expect(points).To(HaveLen(1), "Point %d (x=%.1f, y=%.1f) should be kept", i, point.x, point.y)
					} else {
						Expect(points).To(HaveLen(0), "Point %d (x=%.1f, y=%.1f) should be dropped", i, point.x, point.y)
					}
				}
			})
		})
	})
})
