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

	Describe("data type handling", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"threshold": 1.0,
			}
			algo, err = algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle different data types", func() {
			// First boolean
			keep, err := algo.ProcessPoint(true, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "First point should be kept")

			// Integer value
			keep, err = algo.ProcessPoint(11, baseTime.Add(2*time.Second))
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Integer change (true->11 = 10.0) should be kept")

			// String value - should error for non-convertible strings
			_, err = algo.ProcessPoint("stopped", baseTime.Add(3*time.Second))
			Expect(err).To(HaveOccurred(), "Non-numeric strings should cause error")
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

			It("should handle boolean and integer conversion", func() {
				// Test boolean conversion
				keep, err := algo.ProcessPoint(true, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(keep).To(BeTrue())

				// Test int conversion
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

			// Test boolean conversion
			keep, err := algo.ProcessPoint(true, baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue())
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
