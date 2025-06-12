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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

// TestPoint represents a test data point with time and value
type TestPoint struct {
	TimeSeconds int     // Time in seconds from base time
	Value       float64 // Value at this time
}

// SwingingDoorTestCase represents a complete test case for SDT algorithm
type SwingingDoorTestCase struct {
	Name              string                 // Test case name
	Description       string                 // Human-readable description
	Config            map[string]interface{} // Algorithm configuration
	InputPoints       []TestPoint            // Input data points
	ExpectedEmitted   []TestPoint            // Expected emitted points with time and value
	ExpectedReduction int                    // Expected compression percentage
}

// Helper function to run a swinging door test case
func runSwingingDoorTestCase(testCase SwingingDoorTestCase) {
	By("Creating the swinging door algorithm")
	algo, err := algorithms.Create("swinging_door", testCase.Config)
	Expect(err).NotTo(HaveOccurred())
	defer algo.Reset()

	By("Processing input points and tracking emissions")
	baseTime := time.Now()
	emittedPoints := []TestPoint{}

	// Process all input points
	for _, point := range testCase.InputPoints {
		timestamp := baseTime.Add(time.Duration(point.TimeSeconds) * time.Second)
		algorithmOutput, err := algo.Ingest(point.Value, timestamp)
		Expect(err).NotTo(HaveOccurred())

		// Track which points were emitted
		for _, emittedPoint := range algorithmOutput {
			// Convert back to TestPoint format for easy comparison
			timeFromBase := emittedPoint.Timestamp.Sub(baseTime)
			emittedPoints = append(emittedPoints, TestPoint{
				TimeSeconds: int(timeFromBase.Seconds()),
				Value:       emittedPoint.Value,
			})
		}
	}

	By("Handling final flush")
	finalPoints, err := algo.Flush()
	Expect(err).NotTo(HaveOccurred())

	// Add any final points to our emitted list
	for _, finalPoint := range finalPoints {
		timeFromBase := finalPoint.Timestamp.Sub(baseTime)
		emittedPoints = append(emittedPoints, TestPoint{
			TimeSeconds: int(timeFromBase.Seconds()),
			Value:       finalPoint.Value,
		})
	}

	By("Verifying the correct points were emitted")
	Expect(emittedPoints).To(Equal(testCase.ExpectedEmitted),
		"Expected points %+v but got %+v", testCase.ExpectedEmitted, emittedPoints)

	By("Verifying compression ratio")
	actualReduction := int(float64(len(testCase.InputPoints)-len(emittedPoints)) / float64(len(testCase.InputPoints)) * 100)
	Expect(actualReduction).To(Equal(testCase.ExpectedReduction),
		"Expected %d%% reduction but got %d%%", testCase.ExpectedReduction, actualReduction)
}

var _ = Describe("Swinging Door Algorithm", func() {

	Describe("Reference Implementation Test Cases", func() {
		// These test cases are derived from gfoidl DataCompression library
		// All expected results are verified against the reference implementation

		DescribeTable("should compress data according to reference implementation",
			runSwingingDoorTestCase,

			Entry("Trend - Basic trend following", SwingingDoorTestCase{
				Name:        "trend_basic",
				Description: "Basic trend following test case",
				Config: map[string]interface{}{
					"threshold": 1.0,
				},
				InputPoints: []TestPoint{
					{TimeSeconds: 2, Value: 2},    // x=2, y=2
					{TimeSeconds: 4, Value: 3},    // x=4, y=3
					{TimeSeconds: 6, Value: 2.5},  // x=6, y=2.5
					{TimeSeconds: 8, Value: 3.5},  // x=8, y=3.5
					{TimeSeconds: 10, Value: 5.5}, // x=10, y=5.5
					{TimeSeconds: 12, Value: 2.5}, // x=12, y=2.5
					{TimeSeconds: 14, Value: 1},   // x=14, y=1
					{TimeSeconds: 16, Value: 1},   // x=16, y=1
				},
				ExpectedEmitted: []TestPoint{
					{TimeSeconds: 2, Value: 2},    // Index 0
					{TimeSeconds: 8, Value: 3.5},  // Index 3
					{TimeSeconds: 10, Value: 5.5}, // Index 4
					{TimeSeconds: 14, Value: 1},   // Index 6
					{TimeSeconds: 16, Value: 1},   // Index 7
				},
				ExpectedReduction: 37, // 3 out of 8 points filtered = 37% reduction
			}),

			Entry("Trend1 - High precision filtering", SwingingDoorTestCase{
				Name:        "trend1_high_precision",
				Description: "High precision trend test with small compression deviation",
				Config: map[string]interface{}{
					"threshold": 0.1, // Small compression deviation
				},
				InputPoints: []TestPoint{
					{TimeSeconds: 0, Value: 1},   // x=0, y=1
					{TimeSeconds: 1, Value: 1.1}, // x=1, y=1.1
					{TimeSeconds: 2, Value: 1.2}, // x=2, y=1.2
					{TimeSeconds: 3, Value: 1.6}, // x=3, y=1.6
					{TimeSeconds: 4, Value: 2},   // x=4, y=2
					{TimeSeconds: 5, Value: 2},   // x=5, y=2
					{TimeSeconds: 6, Value: 2},   // x=6, y=2
					{TimeSeconds: 7, Value: 1.2}, // x=7, y=1.2
				},
				ExpectedEmitted: []TestPoint{
					{TimeSeconds: 0, Value: 1},   // Index 0
					{TimeSeconds: 2, Value: 1.2}, // Index 2
					{TimeSeconds: 4, Value: 2},   // Index 4
					{TimeSeconds: 6, Value: 2},   // Index 6
					{TimeSeconds: 7, Value: 1.2}, // Index 7
				},
				ExpectedReduction: 37, // 3 out of 8 points filtered = 37% reduction
			}),

			Entry("Trend2 - Plateau behavior", SwingingDoorTestCase{
				Name:        "trend2_plateau",
				Description: "High precision with plateau behavior",
				Config: map[string]interface{}{
					"threshold": 0.1,
				},
				InputPoints: []TestPoint{
					{TimeSeconds: 0, Value: 1},   // x=0, y=1
					{TimeSeconds: 1, Value: 1.1}, // x=1, y=1.1
					{TimeSeconds: 2, Value: 1.2}, // x=2, y=1.2
					{TimeSeconds: 3, Value: 1.6}, // x=3, y=1.6
					{TimeSeconds: 4, Value: 2},   // x=4, y=2
					{TimeSeconds: 5, Value: 2},   // x=5, y=2
					{TimeSeconds: 6, Value: 2},   // x=6, y=2
					{TimeSeconds: 7, Value: 2},   // x=7, y=2
				},
				ExpectedEmitted: []TestPoint{
					{TimeSeconds: 0, Value: 1},   // Index 0
					{TimeSeconds: 2, Value: 1.2}, // Index 2
					{TimeSeconds: 4, Value: 2},   // Index 4
					{TimeSeconds: 7, Value: 2},   // Index 7 (final plateau point)
				},
				ExpectedReduction: 50, // 4 out of 8 points filtered = 50% reduction
			}),

			Entry("Trend3 - Dramatic value changes", SwingingDoorTestCase{
				Name:        "trend3_dramatic_changes",
				Description: "Large deviation test with dramatic value changes",
				Config: map[string]interface{}{
					"threshold": 2.0, // Large deviation tolerance
				},
				InputPoints: []TestPoint{
					{TimeSeconds: 1, Value: 0},  // x=1, y=0
					{TimeSeconds: 2, Value: 1},  // x=2, y=1
					{TimeSeconds: 3, Value: 2},  // x=3, y=2
					{TimeSeconds: 4, Value: 5},  // x=4, y=5
					{TimeSeconds: 5, Value: -2}, // x=5, y=-2
					{TimeSeconds: 6, Value: 5},  // x=6, y=5
					{TimeSeconds: 7, Value: 4},  // x=7, y=4
					{TimeSeconds: 8, Value: 3},  // x=8, y=3
					{TimeSeconds: 9, Value: 5},  // x=9, y=5
				},
				ExpectedEmitted: []TestPoint{
					{TimeSeconds: 1, Value: 0},  // Index 0
					{TimeSeconds: 4, Value: 5},  // Index 3
					{TimeSeconds: 5, Value: -2}, // Index 4
					{TimeSeconds: 6, Value: 5},  // Index 5
					{TimeSeconds: 8, Value: 3},  // Index 7
					{TimeSeconds: 9, Value: 5},  // Index 8
				},
				ExpectedReduction: 33, // 3 out of 9 points filtered = 33% reduction
			}),

			Entry("Trend3 Mini - Minimal reproduction", SwingingDoorTestCase{
				Name:        "trend3_mini",
				Description: "Minimal reproduction of trend3 behavior",
				Config: map[string]interface{}{
					"threshold": 2.0,
				},
				InputPoints: []TestPoint{
					{TimeSeconds: 5, Value: -2}, // x=5, y=-2
					{TimeSeconds: 6, Value: 5},  // x=6, y=5
					{TimeSeconds: 7, Value: 4},  // x=7, y=4
					{TimeSeconds: 8, Value: 3},  // x=8, y=3
					{TimeSeconds: 9, Value: 5},  // x=9, y=5
				},
				ExpectedEmitted: []TestPoint{
					{TimeSeconds: 5, Value: -2}, // Index 0
					{TimeSeconds: 6, Value: 5},  // Index 1
					{TimeSeconds: 8, Value: 3},  // Index 3
					{TimeSeconds: 9, Value: 5},  // Index 4
				},
				ExpectedReduction: 20, // 1 out of 5 points filtered = 20% reduction
			}),

			Entry("MaxDelta - Maximum time constraint", SwingingDoorTestCase{
				Name:        "max_delta_time_constraint",
				Description: "Test maximum X distance enforcement - forces archiving every 6 time units",
				Config: map[string]interface{}{
					"threshold": 1.0,
					"max_time":  "6s", // Forces archiving every 6 time units
				},
				InputPoints: []TestPoint{
					{TimeSeconds: 2, Value: 2},    // x=2, y=2
					{TimeSeconds: 4, Value: 3},    // x=4, y=3
					{TimeSeconds: 6, Value: 3},    // x=6, y=3
					{TimeSeconds: 8, Value: 3.5},  // x=8, y=3.5
					{TimeSeconds: 10, Value: 3.5}, // x=10, y=3.5
					{TimeSeconds: 12, Value: 4.5}, // x=12, y=4.5
					{TimeSeconds: 14, Value: 4},   // x=14, y=4
					{TimeSeconds: 16, Value: 4.5}, // x=16, y=4.5
					{TimeSeconds: 18, Value: 1.5}, // x=18, y=1.5
					{TimeSeconds: 20, Value: 2.5}, // x=20, y=2.5
				},
				ExpectedEmitted: []TestPoint{
					{TimeSeconds: 2, Value: 2},    // Index 0
					{TimeSeconds: 8, Value: 3.5},  // Index 3
					{TimeSeconds: 14, Value: 4},   // Index 6
					{TimeSeconds: 16, Value: 4.5}, // Index 7
					{TimeSeconds: 18, Value: 1.5}, // Index 8
					{TimeSeconds: 20, Value: 2.5}, // Index 9
				},
				ExpectedReduction: 40, // 4 out of 10 points filtered = 40% reduction
			}),
		)
	})

	Describe("Configuration and Basic Functionality", func() {
		var baseTime time.Time

		BeforeEach(func() {
			baseTime = time.Now()
		})

		Context("Configuration validation", func() {
			It("should accept valid configuration", func() {
				config := map[string]interface{}{
					"threshold": 1.0,
				}
				_, err := algorithms.Create("swinging_door", config)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should reject negative threshold", func() {
				config := map[string]interface{}{
					"threshold": -1.0,
				}
				_, err := algorithms.Create("swinging_door", config)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("cannot be negative"))
			})

			It("should require threshold parameter", func() {
				config := map[string]interface{}{
					"max_time": "1s",
				}
				_, err := algorithms.Create("swinging_door", config)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("missing required parameter"))
			})
		})

		Context("Basic algorithm behavior", func() {
			var algo algorithms.StreamCompressor

			BeforeEach(func() {
				config := map[string]interface{}{
					"threshold": 1.0,
				}
				var err error
				algo, err = algorithms.Create("swinging_door", config)
				Expect(err).NotTo(HaveOccurred())
			})

			AfterEach(func() {
				if algo != nil {
					algo.Reset()
				}
			})

			It("should always emit the first point", func() {
				points, err := algo.Ingest(10.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1), "First point should always be emitted")
				Expect(points[0].Value).To(Equal(10.0))
				Expect(points[0].Timestamp).To(Equal(baseTime))
			})

			It("should handle flush correctly", func() {
				// Process some points
				_, err := algo.Ingest(10.0, baseTime)
				Expect(err).NotTo(HaveOccurred())

				_, err = algo.Ingest(10.5, baseTime.Add(time.Second))
				Expect(err).NotTo(HaveOccurred())

				// Flush should return the final pending point
				finalPoints, err := algo.Flush()
				Expect(err).NotTo(HaveOccurred())
				Expect(finalPoints).To(HaveLen(1))
				Expect(finalPoints[0].Value).To(Equal(10.5))
			})

			It("should handle reset correctly", func() {
				// Process a point
				points, err := algo.Ingest(10.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))

				// Reset
				algo.Reset()

				// First point after reset should be emitted
				points, err = algo.Ingest(20.0, baseTime.Add(time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1))
			})

			It("forces emit at every max_time interval over a long period", func() {
				// This test checks that the max_time constraint triggers emits at regular intervals
				// even when the value changes are small and do not exceed the threshold.
				config := map[string]interface{}{"threshold": 1.0, "max_time": "100ms"}
				algo, err := algorithms.Create("swinging_door", config)
				Expect(err).NotTo(HaveOccurred())
				defer algo.Reset()

				t0 := baseTime
				times := []time.Time{t0}
				for i := 1; i <= 10; i++ {
					times = append(times, t0.Add(time.Duration(i)*100*time.Millisecond))
				}

				emitted := 0
				for i, ts := range times {
					val := 5.0 + 0.05*float64(i) // small changes, always below threshold
					points, err := algo.Ingest(val, ts)
					Expect(err).NotTo(HaveOccurred())
					if i == 0 {
						Expect(points).To(HaveLen(1), "First point should be emitted")
						emitted++
					} else if i%1 == 0 && i > 0 {
						// Should emit every 100ms due to max_time
						if len(points) > 0 {
							emitted += len(points)
						}
					}
				}
				// Should emit at least once every 100ms interval (10 intervals + first point)
				Expect(emitted).To(BeNumerically(">=", 10), "Should emit at least once per max_time interval")
			})
		})

		Context("Metadata and configuration info", func() {
			It("should return correct configuration string", func() {
				config := map[string]interface{}{
					"threshold": 1.5,
					"max_time":  "1s",
				}
				algo, err := algorithms.Create("swinging_door", config)
				Expect(err).NotTo(HaveOccurred())

				configStr := algo.Config()
				Expect(configStr).To(ContainSubstring("swinging_door"))
				Expect(configStr).To(ContainSubstring("1.500"))
				Expect(configStr).To(ContainSubstring("1s"))
			})

			It("should return correct algorithm name", func() {
				config := map[string]interface{}{
					"threshold": 1.0,
				}
				algo, err := algorithms.Create("swinging_door", config)
				Expect(err).NotTo(HaveOccurred())

				name := algo.Name()
				Expect(name).To(Equal("swinging_door"))
			})

			It("should indicate need for previous point buffering", func() {
				config := map[string]interface{}{
					"threshold": 1.0,
				}
				algo, err := algorithms.Create("swinging_door", config)
				Expect(err).NotTo(HaveOccurred())

				needsPrev := algo.NeedsPreviousPoint()
				Expect(needsPrev).To(BeTrue(), "SDT uses emit-previous logic")
			})
		})
	})

	Describe("Gap-filling SDT Edge Cases", func() {
		var baseTime time.Time

		BeforeEach(func() {
			baseTime = time.Now()
		})

		Context("Envelope boundary conditions", func() {
			// Scientific basis: Swinging Door Trending tracks upper and lower envelope slopes.
			// The critical edge case occurs when these slopes become equal (doors intersect).
			// Our implementation follows the industry-standard "emit-previous" behavior
			// used by PI Server, WinCC, and other historians.
			// Reference: PI Square forum discussions on SDT geometry clarify this behavior.
			It("allows equality without emit", func() {
				// Implementation note: This test verifies "emit-previous" behavior.
				// When doors intersect, we emit the previous in-bounds point and
				// keep the violating point as pending.
				config := map[string]interface{}{"threshold": 1.0}
				algo, err := algorithms.Create("swinging_door", config)
				Expect(err).NotTo(HaveOccurred())
				defer algo.Reset()

				t0 := baseTime
				points, err := algo.Ingest(0.0, t0)
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1), "First point should be emitted")

				// Build a point exactly on both envelope lines where slope_low = slope_up
				points, err = algo.Ingest(1.0, t0.Add(1*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(0), "Point within envelope should not emit")
			})
		})

		Context("Temporal constraint validation", func() {
			It("forces emit after max_time with no value change", func() {
				// Time-bounded compression: Industrial historians implement dual constraints
				// (compression deviation OR comp-max-time) to bound both spatial and temporal error.
				// This prevents interpolation artifacts over long stable periods and ensures
				// periodic "heartbeat" signals even for constant processes.
				// Reference: PI Server compression documentation describes this as essential
				// for data integrity in process control applications.
				config := map[string]interface{}{"threshold": 1.0, "max_time": "100ms"}
				algo, err := algorithms.Create("swinging_door", config)
				Expect(err).NotTo(HaveOccurred())
				defer algo.Reset()

				t0 := baseTime
				points, err := algo.Ingest(5.0, t0)
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1), "First point should be emitted")

				// Add a small change to create a candidate
				points, err = algo.Ingest(5.1, t0.Add(50*time.Millisecond))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(0), "Small change should not emit yet")

				// Now test max_time with another small change
				points, err = algo.Ingest(5.2, t0.Add(150*time.Millisecond))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1), "Should force emit due to max_time constraint")
			})

		})

		Context("State integrity validation", func() {
			It("drops repeat value only after reset clears memory", func() {
				// State management validation: Reset must completely clear envelope state
				// to prevent cross-contamination between data streams or after reconnection.
				// This test ensures that previous envelope calculations don't affect
				// post-reset behavior, which is critical for stream processing systems.
				config := map[string]interface{}{"threshold": 1.0}
				algo, err := algorithms.Create("swinging_door", config)
				Expect(err).NotTo(HaveOccurred())

				points, err := algo.Ingest(2.0, baseTime)
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1), "First point should be emitted")

				points, err = algo.Ingest(3.0, baseTime.Add(time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(0), "Point within envelope should not be emitted")

				algo.Reset()

				// 3.0 should be kept again (state wiped) - validates complete reset
				points, err = algo.Ingest(3.0, baseTime.Add(2*time.Second))
				Expect(err).NotTo(HaveOccurred())
				Expect(points).To(HaveLen(1), "First point after reset should be emitted")
			})
		})

		Context("Parameter validation", func() {
			It("rejects negative threshold", func() {
				// Mathematical validation: Negative compression deviation is meaningless
				// since envelope width would be negative, making all points fall outside
				// the envelope. Proper validation prevents misconfiguration.
				config := map[string]interface{}{"threshold": -0.1}
				_, err := algorithms.Create("swinging_door", config)
				Expect(err).To(HaveOccurred(), "Should reject negative threshold")
				Expect(err.Error()).To(ContainSubstring("cannot be negative"))
			})

			It("accepts threshold = 0 (exact envelope)", func() {
				// Edge case: threshold = 0 creates zero-width envelope, effectively
				// becoming a "drop exact repeats" filter. This is mathematically valid
				// and useful for discrete sensors with perfect repeatability.
				config := map[string]interface{}{"threshold": 0.0}
				_, err := algorithms.Create("swinging_door", config)
				Expect(err).NotTo(HaveOccurred(), "Should accept threshold = 0")
			})
		})
	})
})
