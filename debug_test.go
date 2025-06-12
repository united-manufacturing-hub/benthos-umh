package main

import (
	"fmt"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

func main() {
	// Test the Trend case: Raw data from test plan
	config := map[string]interface{}{
		"threshold": 1.0,
	}
	algo, _ := algorithms.NewSwingingDoorAlgorithm(config)

	rawData := []struct {
		x float64
		y float64
	}{
		{2, 2},
		{4, 3},
		{6, 2.5},
		{8, 3.5},
		{10, 5.5},
		{12, 2.5},
		{14, 1},
		{16, 1},
	}

	baseTime := time.Now()
	emittedPoints := []int{}

	fmt.Println("Processing points:")
	for i, point := range rawData {
		timestamp := baseTime.Add(time.Duration(point.x) * time.Second)
		keep, _ := algo.ProcessPoint(point.y, timestamp)
		fmt.Printf("Point %d: x=%.0f, y=%.1f -> keep=%t\n", i, point.x, point.y, keep)

		if keep {
			emittedPoints = append(emittedPoints, i)
		}
	}

	fmt.Printf("Immediate emissions: %v (count: %d)\n", emittedPoints, len(emittedPoints))

	// Check flush
	finalPoint, _ := algo.Flush()
	if finalPoint != nil {
		// Find the index of the final point
		for i, point := range rawData {
			if point.y == finalPoint.Value && baseTime.Add(time.Duration(point.x)*time.Second).Equal(finalPoint.Timestamp) {
				emittedPoints = append(emittedPoints, i)
				fmt.Printf("Flush returned: Point %d (x=%.0f, y=%.1f)\n", i, point.x, point.y)
				break
			}
		}
	} else {
		fmt.Println("Flush returned: nil")
	}

	fmt.Printf("Total emissions: %v (count: %d)\n", emittedPoints, len(emittedPoints))
	fmt.Printf("Expected: [0, 3, 4, 6, 7] (count: 5)\n")
	fmt.Printf("Actual:   %v (count: %d)\n", emittedPoints, len(emittedPoints))
}
