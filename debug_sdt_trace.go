package main

import (
	"fmt"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

func main() {
	// Create SDT algorithm
	algo, err := algorithms.NewSwingingDoorAlgorithm(map[string]interface{}{
		"comp_dev": 1.0,
	})
	if err != nil {
		panic(err)
	}

	baseTime := time.Now()
	testData := []float64{18.0, 5.0, 6.0, 0.0, 1.0, 0.0, 2.0, 8.0}
	expected := []bool{true, true, true, true, false, false, true, true}

	fmt.Println("=== SDT Algorithm Debug Trace ===")
	fmt.Printf("CompDev: 1.0\n")
	fmt.Printf("Data: %v\n", testData)
	fmt.Printf("Expected: %v\n", expected)
	fmt.Println()

	for i, value := range testData {
		currentTime := baseTime.Add(time.Duration(i) * time.Second)
		keep, err := algo.ProcessPoint(value, currentTime)
		if err != nil {
			fmt.Printf("ERROR at point %d: %v\n", i, err)
			continue
		}

		fmt.Printf("Point %d: value=%.1f, keep=%t (expected=%t) %s\n",
			i, value, keep, expected[i], func() string {
				if keep == expected[i] {
					return "✓"
				}
				return "✗"
			}())
	}
}
