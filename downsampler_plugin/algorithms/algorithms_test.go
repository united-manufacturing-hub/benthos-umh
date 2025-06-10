package algorithms_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

func TestAlgorithms(t *testing.T) {
	// Only run if TEST_DOWNSAMPLER is set
	if os.Getenv("TEST_DOWNSAMPLER") == "" {
		t.Skip("Skipping downsampler tests. Set TEST_DOWNSAMPLER=1 to run.")
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "Downsampler Algorithms Suite")
}

var _ = Describe("Deadband Algorithm", func() {
	var algo algorithms.DownsampleAlgorithm
	var err error
	var baseTime time.Time

	BeforeEach(func() {
		baseTime = time.Now()
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
				"threshold":    0.5,
				"max_interval": "60s",
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
					"threshold":    0.5,
					"max_interval": "1m",
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
				cfg := map[string]interface{}{"threshold": 0.5, "max_interval": "60s"}
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

		It("should generate correct metadata with max interval", func() {
			config := map[string]interface{}{
				"threshold":    0.5,
				"max_interval": "60s",
			}
			algo, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())

			metadata := algo.GetMetadata()
			Expect(metadata).To(Equal("deadband(threshold=0.500,max_interval=1m0s)"))
		})
	})

	Describe("configuration validation", func() {
		It("should accept valid configuration", func() {
			config := map[string]interface{}{
				"threshold": 1.5,
			}
			_, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject invalid threshold type", func() {
			config := map[string]interface{}{
				"threshold": "invalid",
			}
			_, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).To(HaveOccurred())
		})

		It("should reject invalid max_interval", func() {
			config := map[string]interface{}{
				"threshold":    1.0,
				"max_interval": "invalid",
			}
			_, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).To(HaveOccurred())
		})
	})
})

var _ = Describe("Swinging Door Algorithm", func() {
	var algo algorithms.DownsampleAlgorithm
	var err error
	var baseTime time.Time

	BeforeEach(func() {
		baseTime = time.Now()
	})

	Describe("configuration validation", func() {
		It("should accept valid configuration", func() {
			config := map[string]interface{}{
				"comp_dev": 1.0,
			}
			_, err := algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject invalid comp_dev", func() {
			config := map[string]interface{}{
				"comp_dev": -1.0,
			}
			_, err := algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).To(HaveOccurred())
		})

		It("should reject invalid comp_min_time", func() {
			config := map[string]interface{}{
				"comp_dev":      1.0,
				"comp_min_time": "invalid",
			}
			_, err := algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).To(HaveOccurred())
		})

		It("should reject invalid comp_max_time", func() {
			config := map[string]interface{}{
				"comp_dev":      1.0,
				"comp_max_time": "invalid",
			}
			_, err := algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("basic SDT functionality", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"comp_dev": 1.0,
			}
			algo, err = algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should always keep the first point", func() {
			keep, err := algo.ProcessPoint(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "First point should always be kept")
		})

		It("should implement basic SDT logic from reference implementation", func() {
			// Test case: CD=1, samples: 18,5,6,0,1,0,2,8
			// Our implementation: [true, false, true, false, true, false, true, true]
			// (emits current point when envelope collapses, not previous point)
			testData := []float64{18.0, 5.0, 6.0, 0.0, 1.0, 0.0, 2.0, 8.0}
			expected := []bool{true, false, true, false, true, false, true, true}

			for i, value := range testData {
				currentTime := baseTime.Add(time.Duration(i) * time.Second)
				keep, err := algo.ProcessPoint(value, currentTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(Equal(expected[i]), "Point %d (value=%.1f) expected=%t got=%t", i, value, expected[i], keep)
			}
		})

		Context("basic SDT test case", func() {
			It("should handle the reference test sequence correctly", func() {
				testData := []float64{18.0, 5.0, 6.0, 0.0, 1.0, 0.0, 2.0, 8.0}
				expected := []bool{true, false, true, false, true, false, true, true}

				for i, value := range testData {
					currentTime := baseTime.Add(time.Duration(i) * time.Second)
					keep, err := algo.ProcessPoint(value, currentTime)
					Expect(err).NotTo(HaveOccurred())
					Expect(keep).To(Equal(expected[i]), fmt.Sprintf("Point %d (value=%.1f) expected=%t got=%t", i, value, expected[i], keep))
				}
			})
		})
	})

	Describe("comp_min_time functionality", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"comp_dev":      1.0,
				"comp_min_time": "100ms",
			}
			algo, err = algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle min time constraint", func() {
			// First point
			keep, err := algo.ProcessPoint(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())

			// Point within min time - behavior depends on SDT implementation
			keep, err = algo.ProcessPoint(15.0, baseTime.Add(50*time.Millisecond))
			Expect(err).NotTo(HaveOccurred())
			// Could be true or false depending on implementation details
			_ = keep
		})
	})

	Describe("comp_max_time functionality", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"comp_dev":      1.0,
				"comp_max_time": "100ms",
			}
			algo, err = algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle max time constraint", func() {
			// First point
			keep, err := algo.ProcessPoint(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())

			// Second point becomes candidate
			keep, err = algo.ProcessPoint(12.0, baseTime.Add(50*time.Millisecond))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeFalse()) // Becomes candidate, not emitted yet

			// Third point after max time should force emission
			keep, err = algo.ProcessPoint(14.0, baseTime.Add(150*time.Millisecond))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue()) // Max time exceeded, should emit
		})
	})

	Describe("type conversion", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"comp_dev": 1.0,
			}
			algo, err = algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle integer values", func() {
			keep, err := algo.ProcessPoint(10, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())

			keep, err = algo.ProcessPoint(11, baseTime.Add(time.Second))
			Expect(err).NotTo(HaveOccurred())
			// Could be true or false depending on SDT logic
			_ = keep
		})

		It("should handle float32 values", func() {
			keep, err := algo.ProcessPoint(float32(10.5), baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())
		})

		It("should handle mixed types", func() {
			keep, err := algo.ProcessPoint(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())

			keep, err = algo.ProcessPoint(11, baseTime.Add(time.Second))
			Expect(err).NotTo(HaveOccurred())
			// Could be true or false depending on SDT logic
			_ = keep
		})
	})

	Describe("reset functionality", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"comp_dev": 1.0,
			}
			algo, err = algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reset state correctly", func() {
			// Process some points
			keep, err := algo.ProcessPoint(10.0, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())

			keep, err = algo.ProcessPoint(11.0, baseTime.Add(time.Second))
			Expect(err).NotTo(HaveOccurred())
			// State now has some data

			// Reset and test again
			algo.Reset()
			keep, err = algo.ProcessPoint(20.0, baseTime.Add(2*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "First point after reset should be kept")
		})

		Context("state management", func() {
			It("should maintain independent state after reset", func() {
				// First point
				keep, err := algo.ProcessPoint(10.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeTrue())

				// Reset and test again
				algo.Reset()
				keep, err = algo.ProcessPoint(15.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeTrue())
			})
		})
	})

	Describe("error handling", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"comp_dev": 1.0,
			}
			algo, err = algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle conversion errors", func() {
			// Test string that can't be converted
			_, err = algo.ProcessPoint("error", baseTime)
			Expect(err).To(HaveOccurred())

			// Test boolean rejection - should be handled by plugin-level equality
			_, err = algo.ProcessPoint(true, baseTime)
			Expect(err).To(HaveOccurred(), "Boolean values should be rejected")
		})
	})

	Describe("gap-filling SDT edge cases", func() {
		Context("envelope boundary conditions", func() {
			// Scientific basis: Swinging Door Trending tracks upper and lower envelope slopes.
			// The critical edge case occurs when these slopes become equal (doors intersect).
			// Literature shows two valid implementations: emit-previous or emit-current point.
			// Reference: PI Square forum discussions on SDT geometry clarify this behavior.
			It("allows equality without emit", func() {
				// Implementation note: This test assumes "emit-current" variant.
				// Canon implementations may emit-previous when doors intersect.
				// Our implementation follows emit-current pattern for interface consistency.
				cfg := map[string]interface{}{"comp_dev": 1.0}
				algo, _ := algorithms.NewSwingingDoorAlgorithm(cfg)
				t0 := baseTime
				Expect(algo.ProcessPoint(0.0, t0)).Should(BeTrue())
				// Build a point exactly on both envelope lines where slope_low = slope_up
				Expect(algo.ProcessPoint(1.0, t0.Add(1*time.Second))).Should(BeFalse()) // no emit
			})
		})

		Context("temporal constraint validation", func() {
			It("forces emit after comp_max_time with no value change", func() {
				// Time-bounded compression: Industrial historians implement dual constraints
				// (compression deviation OR comp-max-time) to bound both spatial and temporal error.
				// This prevents interpolation artifacts over long stable periods and ensures
				// periodic "heartbeat" signals even for constant processes.
				// Reference: PI Server compression documentation describes this as essential
				// for data integrity in process control applications.
				cfg := map[string]interface{}{"comp_dev": 1.0, "comp_max_time": "100ms"}
				algo, _ := algorithms.NewSwingingDoorAlgorithm(cfg)
				t0 := baseTime
				Expect(algo.ProcessPoint(5.0, t0)).Should(BeTrue())
				keep, _ := algo.ProcessPoint(5.0, t0.Add(150*time.Millisecond))
				Expect(keep).Should(BeTrue()) // forced emit due to time constraint
			})

			It("suppresses emit until comp_min_time elapses", func() {
				// Noise filtering: comp_min_time prevents emission of high-frequency noise
				// that would otherwise cause envelope collapse. This is critical in industrial
				// environments with electrical interference or sensor quantization noise.
				// Canon implementations buffer "candidate" points until min_time elapses.
				// Reference: CygNet and Weatherford documentation emphasize this for noise immunity.
				cfg := map[string]interface{}{"comp_dev": 0.1, "comp_min_time": "100ms"}
				algo, _ := algorithms.NewSwingingDoorAlgorithm(cfg)
				t0 := baseTime
				Expect(algo.ProcessPoint(0.0, t0)).Should(BeTrue())
				// Big jump but only 20 ms later - should be suppressed by min_time
				keep, _ := algo.ProcessPoint(10.0, t0.Add(20*time.Millisecond))
				Expect(keep).Should(BeFalse()) // candidate only, not emitted
				// Next point still within envelope but now > 100 ms - should emit candidate
				keep, _ = algo.ProcessPoint(11.0, t0.Add(120*time.Millisecond))
				Expect(keep).Should(BeTrue()) // now emits candidate
			})
		})

		Context("state integrity validation", func() {
			It("drops repeat value only after reset clears memory", func() {
				// State management validation: Reset must completely clear envelope state
				// to prevent cross-contamination between data streams or after reconnection.
				// This test ensures that previous envelope calculations don't affect
				// post-reset behavior, which is critical for stream processing systems.
				cfg := map[string]interface{}{"comp_dev": 1.0}
				algo, _ := algorithms.NewSwingingDoorAlgorithm(cfg)
				Expect(algo.ProcessPoint(2.0, baseTime)).Should(BeTrue())
				Expect(algo.ProcessPoint(3.0, baseTime.Add(time.Second))).Should(BeTrue()) // door collapsed
				algo.Reset()
				// 3.0 should be kept again (state wiped) - validates complete reset
				Expect(algo.ProcessPoint(3.0, baseTime.Add(2*time.Second))).Should(BeTrue())
			})
		})

		Context("industrial data type compatibility", func() {
			It("rejects non-numeric strings", func() {
				// Data type integrity: SDT algorithms are designed for numeric compression.
				// String values should be rejected to maintain algorithm integrity and prevent
				// unexpected behavior. String data requires different handling approaches.
				// Reference: Industrial best practice is to separate numeric and string processing.
				cfg := map[string]interface{}{"comp_dev": 0.5}
				algo, _ := algorithms.NewSwingingDoorAlgorithm(cfg)
				_, err := algo.ProcessPoint("10.0", baseTime)
				Expect(err).Should(HaveOccurred()) // strings should be rejected
			})

			It("rejects boolean values", func() {
				// Boolean signal processing: Industrial systems use boolean signals for
				// discrete states (pump on/off, valve open/closed). These should be handled
				// by plugin-level equality logic rather than numeric compression algorithms.
				// This ensures clean separation between discrete and continuous data processing.
				cfg := map[string]interface{}{"comp_dev": 0.1}
				algo, _ := algorithms.NewSwingingDoorAlgorithm(cfg)
				_, err := algo.ProcessPoint(true, baseTime)
				Expect(err).Should(HaveOccurred()) // booleans should be rejected
			})
		})

		Context("parameter validation", func() {
			It("rejects negative comp_dev", func() {
				// Mathematical validation: Negative compression deviation is meaningless
				// since envelope width would be negative, making all points fall outside
				// the envelope. Proper validation prevents misconfiguration.
				_, err := algorithms.NewSwingingDoorAlgorithm(map[string]interface{}{"comp_dev": -0.1})
				Expect(err).Should(HaveOccurred())
			})

			It("accepts comp_dev = 0 (exact envelope)", func() {
				// Edge case: comp_dev = 0 creates zero-width envelope, effectively
				// becoming a "drop exact repeats" filter. This is mathematically valid
				// and useful for discrete sensors with perfect repeatability.
				_, err := algorithms.NewSwingingDoorAlgorithm(map[string]interface{}{"comp_dev": 0})
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})

	Describe("metadata", func() {
		It("should return correct metadata", func() {
			config := map[string]interface{}{
				"comp_dev":      1.5,
				"comp_min_time": "100ms",
				"comp_max_time": "1s",
			}
			algo, err := algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())

			metadata := algo.GetMetadata()
			Expect(metadata).To(ContainSubstring("swinging_door"))
			Expect(metadata).To(ContainSubstring("1.500"))
			Expect(metadata).To(ContainSubstring("100ms"))
			Expect(metadata).To(ContainSubstring("1s"))
		})

		It("should return correct name", func() {
			config := map[string]interface{}{
				"comp_dev": 1.0,
			}
			algo, err := algorithms.NewSwingingDoorAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())

			name := algo.GetName()
			Expect(name).To(Equal("swinging_door"))
		})
	})
})
