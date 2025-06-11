package algorithms_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

var _ = Describe("Deadband Algorithm", func() {
	var algo algorithms.DownsampleAlgorithm
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
			keep, err := algo.ProcessPoint(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "First point should always be kept")
		})

		It("should process a sequence correctly", func() {
			// First point - always kept
			keep, err := algo.ProcessPoint(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "First point should always be kept")

			// Small change (0.3 < 0.5) - should be filtered
			keep, err = algo.ProcessPoint(10.3, baseTime.Add(time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeFalse(), "Small change (0.3 < 0.5) should be filtered")

			// Large change (1.6 >= 0.5) - should be kept
			keep, err = algo.ProcessPoint(11.6, baseTime.Add(2*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Large change (1.6 >= 0.5) should be kept")

			// Exact threshold change (0.5 = 0.5) - should be kept
			keep, err = algo.ProcessPoint(12.1, baseTime.Add(3*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Exact threshold change (0.5 = 0.5) should be kept")
		})

		Context("additional basic filtering scenarios", func() {
			It("should handle basic filtering sequence", func() {
				// First point should always be kept
				keep, err := algo.ProcessPoint(10.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeTrue(), "First point should be kept")

				// Small change (< threshold) should be dropped
				keep, err = algo.ProcessPoint(10.3, baseTime.Add(time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeFalse(), "Small change should be dropped")

				// Large change (>= threshold) should be kept
				keep, err = algo.ProcessPoint(10.6, baseTime.Add(2*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeTrue(), "Large change should be kept")

				// Another small change should be dropped
				keep, err = algo.ProcessPoint(10.5, baseTime.Add(3*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeFalse(), "Small change should be dropped")
			})
		})
	})

	Describe("max interval functionality", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"threshold": 0.5,
				"max_time":  "60s",
			}
			algo, err = algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle max interval correctly", func() {
			// First point
			keep, err := algo.ProcessPoint(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "First point should be kept")

			// Small change within time limit - should be filtered
			keep, err = algo.ProcessPoint(10.3, baseTime.Add(30*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeFalse(), "Small change within time limit should be filtered")

			// Small change after max interval - should be kept
			keep, err = algo.ProcessPoint(10.3, baseTime.Add(70*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Small change after max interval should be kept")
		})

		Context("max interval with 1 minute duration", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 0.5,
					"max_time":  "1m",
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should enforce max interval constraint", func() {
				// First point
				keep, err := algo.ProcessPoint(10.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeTrue())

				// Within max_interval, small change should be dropped
				keep, err = algo.ProcessPoint(10.3, baseTime.Add(30*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeFalse())

				// After max_interval, even small change should be kept
				keep, err = algo.ProcessPoint(10.3, baseTime.Add(70*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeTrue())
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
				keep, _ := algo.ProcessPoint(10.0, baseTime)
				Expect(keep).Should(BeTrue())
				keep, _ = algo.ProcessPoint(9.5, baseTime.Add(time.Second)) // Δ = −0.5
				Expect(keep).Should(BeTrue())                               // must keep - exact threshold magnitude
			})

			It("drops an identical repeat", func() {
				// Edge case validation: Δ = 0 should always be filtered unless other
				// constraints (like max_interval) force emission. This test prevents
				// accidental "≤" vs "<" boundary condition errors in threshold comparison.
				// Even with threshold = 0, repeats should be dropped (special case behavior).
				_, _ = algo.ProcessPoint(10.0, baseTime)
				keep, _ := algo.ProcessPoint(10.0, baseTime.Add(time.Second))
				Expect(keep).Should(BeFalse())
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
				cfg := map[string]interface{}{"threshold": 0.5, "max_time": "60s"}
				algo, _ := algorithms.NewDeadbandAlgorithm(cfg)
				t0 := baseTime
				_, _ = algo.ProcessPoint(10.0, t0)
				keep, _ := algo.ProcessPoint(10.0, t0.Add(65*time.Second)) // Δ = 0, but > 60 s
				Expect(keep).Should(BeTrue())                              // time constraint overrides threshold
			})
		})

		Context("threshold validation", func() {
			It("rejects a negative threshold", func() {
				// Mathematical validation: Negative thresholds are meaningless since
				// abs(Δ) ≥ negative_value is always true, effectively disabling filtering.
				// Proper input validation prevents configuration errors that would
				// compromise data compression effectiveness.
				_, err := algorithms.NewDeadbandAlgorithm(map[string]interface{}{"threshold": -0.1})
				Expect(err).Should(HaveOccurred())
			})

			It("accepts threshold 0 (drop exact repeats)", func() {
				// Special case: threshold = 0 creates a "drop exact repeats only" mode,
				// which is useful for systems that produce many duplicate readings.
				// This is a valid edge case used in practice for Boolean or discrete sensors.
				_, err := algorithms.NewDeadbandAlgorithm(map[string]interface{}{"threshold": 0})
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("string handling", func() {
			It("rejects non-numeric strings", func() {
				// Data type integrity: Deadband algorithms are designed for numeric compression.
				// While some systems might accept numeric strings, maintaining strict type
				// boundaries prevents unexpected behavior and maintains algorithm clarity.
				// String data should be handled by appropriate string-based algorithms.
				// Reference: Clean separation of numeric vs string processing is industrial best practice.
				cfg := map[string]interface{}{"threshold": 0.5}
				algo, _ := algorithms.NewDeadbandAlgorithm(cfg)
				_, err := algo.ProcessPoint("10.0", baseTime)
				Expect(err).Should(HaveOccurred()) // strings should be rejected
			})
		})
	})

	Describe("data type handling", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"threshold": 1.0,
			}
			algo, err = algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject non-numeric data types", func() {
			// Boolean values should be rejected - handled by plugin-level equality
			_, err := algo.ProcessPoint(true, baseTime)
			Expect(err).To(HaveOccurred(), "Boolean values should be rejected")

			// String value - should error for non-convertible strings
			_, err = algo.ProcessPoint("stopped", baseTime)
			Expect(err).To(HaveOccurred(), "Non-numeric strings should cause error")

			// Reset and test with numeric values
			algo.Reset()

			// First numeric point
			keep, err := algo.ProcessPoint(1.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "First numeric point should be kept")

			// Integer value (converts to float)
			keep, err = algo.ProcessPoint(11, baseTime.Add(2*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Integer change (1.0->11 = 10.0) should be kept")
		})

		It("should handle string sequence", func() {
			// Reset for string test
			config := map[string]interface{}{
				"threshold": 1.0,
			}
			algo, err = algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())

			// String values should cause errors since deadband needs numeric values
			_, err = algo.ProcessPoint("running", baseTime)
			Expect(err).To(HaveOccurred(), "Strings should cause error in deadband")
		})

		Context("type conversion scenarios", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 0.5,
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should reject boolean values and handle integer conversion", func() {
				// Boolean values should be rejected - they're handled by plugin-level equality
				_, err := algo.ProcessPoint(true, baseTime)
				Expect(err).To(HaveOccurred())

				// Reset for integer test
				algo.Reset()

				// Test int conversion - first int
				keep, err := algo.ProcessPoint(1, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeTrue())

				// Test another int conversion
				keep, err = algo.ProcessPoint(11, baseTime.Add(2*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeTrue()) // |11 - 1| = 10 > 0.5

				// Test string that can't be parsed should cause error
				_, err = algo.ProcessPoint("stopped", baseTime.Add(3*time.Second))
				Expect(err).To(HaveOccurred())
			})

			It("should handle reset and string error scenarios", func() {
				// Reset algorithm for string test
				algo.Reset()
				_, err = algo.ProcessPoint("running", baseTime.Add(4*time.Second))
				Expect(err).To(HaveOccurred())
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

			metadata := algo.GetMetadata()
			Expect(metadata).To(Equal("deadband(threshold=0.500)"))
		})

		It("should generate correct metadata with max time", func() {
			config := map[string]interface{}{
				"threshold": 0.5,
				"max_time":  "60s",
			}
			algo, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())

			metadata := algo.GetMetadata()
			Expect(metadata).To(Equal("deadband(threshold=0.500,max_time=1m0s)"))
		})
	})

	Describe("configuration validation", func() {
		DescribeTable("valid configurations",
			func(config map[string]interface{}) {
				_, err := algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			},
			Entry("basic threshold", map[string]interface{}{"threshold": 1.5}),
			Entry("with max_time", map[string]interface{}{
				"threshold": 0.5,
				"max_time":  "60s",
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
				keep, _ := algo.ProcessPoint(10.0, baseTime)
				Expect(keep).To(BeTrue(), "First reading: 10.0 → KEEP (always keep first)")

				keep, _ = algo.ProcessPoint(10.3, baseTime.Add(1*time.Second))
				Expect(keep).To(BeFalse(), "Reading: 10.3 → DROP (change = 0.3 < threshold 1.0)")

				keep, _ = algo.ProcessPoint(9.7, baseTime.Add(2*time.Second))
				Expect(keep).To(BeFalse(), "Reading: 9.7 → DROP (change from last kept = 0.3 < threshold 1.0)")

				keep, _ = algo.ProcessPoint(11.5, baseTime.Add(3*time.Second))
				Expect(keep).To(BeTrue(), "Reading: 11.5 → KEEP (change from last kept = 1.5 ≥ threshold 1.0)")
			})

			It("should handle a temperature sensor example", func() {
				// Human verification: Room temperature sensor, threshold 0.5°C
				config := map[string]interface{}{"threshold": 0.5}
				algo, _ := algorithms.NewDeadbandAlgorithm(config)

				keep, _ := algo.ProcessPoint(22.0, baseTime)
				Expect(keep).To(BeTrue(), "Initial temp: 22.0°C → KEEP")

				keep, _ = algo.ProcessPoint(22.3, baseTime.Add(1*time.Minute))
				Expect(keep).To(BeFalse(), "Small rise: 22.3°C → DROP (0.3°C < 0.5°C threshold)")

				keep, _ = algo.ProcessPoint(22.8, baseTime.Add(2*time.Minute))
				Expect(keep).To(BeTrue(), "Significant rise: 22.8°C → KEEP (0.8°C ≥ 0.5°C threshold)")

				keep, _ = algo.ProcessPoint(22.9, baseTime.Add(3*time.Minute))
				Expect(keep).To(BeFalse(), "Small rise: 22.9°C → DROP (0.1°C < 0.5°C threshold)")
			})
		})

		Context("max time examples", func() {
			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 2.0,
					"max_time":  "30s",
				}
				algo, err = algorithms.NewDeadbandAlgorithm(config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should send heartbeat even with no significant change", func() {
				// Human verification: System sends periodic updates even if value doesn't change much
				keep, _ := algo.ProcessPoint(100.0, baseTime)
				Expect(keep).To(BeTrue(), "Initial value: 100.0 → KEEP")

				// Small changes that would normally be filtered
				keep, _ = algo.ProcessPoint(100.5, baseTime.Add(10*time.Second))
				Expect(keep).To(BeFalse(), "Small change: 100.5 → DROP (0.5 < 2.0 threshold)")

				keep, _ = algo.ProcessPoint(101.0, baseTime.Add(20*time.Second))
				Expect(keep).To(BeFalse(), "Small change: 101.0 → DROP (1.0 < 2.0 threshold)")

				// After 35 seconds, max_time forces emission even with small change
				keep, _ = algo.ProcessPoint(101.2, baseTime.Add(35*time.Second))
				Expect(keep).To(BeTrue(), "Heartbeat: 101.2 → KEEP (max_time 30s exceeded)")
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
				keep, _ := algo.ProcessPoint(10.0, baseTime)
				Expect(keep).To(BeTrue(), "Start: 10.0 → KEEP")

				keep, _ = algo.ProcessPoint(11.0, baseTime.Add(1*time.Second))
				Expect(keep).To(BeTrue(), "Exact threshold: 11.0 → KEEP (change = 1.0 = threshold)")

				keep, _ = algo.ProcessPoint(11.99, baseTime.Add(2*time.Second))
				Expect(keep).To(BeFalse(), "Just under threshold: 11.99 → DROP (change = 0.99 < 1.0)")

				keep, _ = algo.ProcessPoint(12.01, baseTime.Add(3*time.Second))
				Expect(keep).To(BeTrue(), "Just over threshold: 12.01 → KEEP (change = 1.01 > 1.0)")
			})

			It("should work with negative values and negative changes", func() {
				// Human verification: Algorithm works symmetrically for positive and negative values
				keep, _ := algo.ProcessPoint(-5.0, baseTime)
				Expect(keep).To(BeTrue(), "Start: -5.0 → KEEP")

				keep, _ = algo.ProcessPoint(-4.0, baseTime.Add(1*time.Second))
				Expect(keep).To(BeTrue(), "Increase: -4.0 → KEEP (change = 1.0 = threshold)")

				keep, _ = algo.ProcessPoint(-6.0, baseTime.Add(2*time.Second))
				Expect(keep).To(BeTrue(), "Decrease: -6.0 → KEEP (change = 2.0 > threshold)")

				keep, _ = algo.ProcessPoint(-5.5, baseTime.Add(3*time.Second))
				Expect(keep).To(BeFalse(), "Small change: -5.5 → DROP (change = 0.5 < threshold)")
			})
		})
	})
})
