package main

import (
	"fmt"
	"math"
)

func swingingDoorTrace(data []float64, compDev float64) []bool {
	n := len(data)
	if n == 0 {
		return nil
	}

	// Track which points should be kept
	kept := make([]bool, n)

	// Initial base point (start of current segment)
	baseIndex := 0
	baseTime := 0
	baseVal := data[0]

	// First point is always kept
	kept[0] = true
	fmt.Printf("Point (t=%d, value=%.1f): start new segment (initial point) -> KEEP\n", baseIndex, baseVal)

	// Initialize envelope slope bounds for current segment
	var maxLowerSlope float64 = math.Inf(-1) // initially -∞
	var minUpperSlope float64 = math.Inf(1)  // initially +∞

	// Iterate over subsequent points
	for i := 1; i < n; i++ {
		currentTime := i
		currentVal := data[i]
		fmt.Printf("\nPoint (t=%d, value=%.1f):\n", currentTime, currentVal)

		// Compute candidate slopes from base to current point
		dt := float64(currentTime - baseTime)
		if dt <= 0 {
			continue
		}
		lowerSlope := (currentVal - compDev - baseVal) / dt
		upperSlope := (currentVal + compDev - baseVal) / dt
		fmt.Printf("  Calculated lowerSlope = %.2f, upperSlope = %.2f (from base t=%d, value=%.1f)\n",
			lowerSlope, upperSlope, baseIndex, baseVal)

		// Update envelope bounds with these slopes
		if lowerSlope > maxLowerSlope {
			maxLowerSlope = lowerSlope
		}
		if upperSlope < minUpperSlope {
			minUpperSlope = upperSlope
		}
		fmt.Printf("  Envelope slopes after including this point: maxLowerSlope = %.2f, minUpperSlope = %.2f\n",
			maxLowerSlope, minUpperSlope)

		// Check if envelope is still valid or has collapsed
		if maxLowerSlope > minUpperSlope {
			// The doors have intersected – threshold exceeded
			fmt.Printf("  ** Slopes intersect (maxLowerSlope > minUpperSlope) -> compression threshold exceeded!\n")
			// Emit the previous point as end of the current segment
			prevIndex := i - 1
			prevVal := data[prevIndex]
			kept[prevIndex] = true
			fmt.Printf("  -> Emitting point (t=%d, value=%.1f) as end of segment -> KEEP\n", prevIndex, prevVal)
			// Start a new segment from the emitted point
			baseIndex = prevIndex
			baseTime = baseIndex
			baseVal = prevVal
			fmt.Printf("  -> Starting new segment from (t=%d, value=%.1f)\n", baseIndex, baseVal)
			// Reset the envelope for the new segment
			maxLowerSlope = math.Inf(-1)
			minUpperSlope = math.Inf(1)
			// Recompute slopes for the current point with the new base
			dt = float64(currentTime - baseTime)
			lowerSlope = (currentVal - compDev - baseVal) / dt
			upperSlope = (currentVal + compDev - baseVal) / dt
			maxLowerSlope = lowerSlope
			minUpperSlope = upperSlope
			fmt.Printf("  Recomputed slopes with new base: lowerSlope = %.2f, upperSlope = %.2f\n",
				lowerSlope, upperSlope)
			fmt.Printf("  New envelope: maxLowerSlope = %.2f, minUpperSlope = %.2f\n",
				maxLowerSlope, minUpperSlope)
			// The current point is now the first point of the new segment (not emitted now)
			fmt.Printf("  -> Point (t=%d) is kept as part of new segment (not emitted) -> DROP\n", currentTime)
		} else {
			// Point fits in the current envelope, no emission needed
			fmt.Printf("  Doors intersect? No, within envelope (point continues in segment)\n")
			fmt.Printf("  -> Point (t=%d) dropped (represented by current trend line) -> DROP\n", currentTime)
			// (No change to base; segment continues)
		}
	}

	// After processing all points, emit the last point to close the final segment
	lastIndex := n - 1
	if lastIndex != baseIndex {
		// Last point was not yet emitted in a segment break, so output it now
		kept[lastIndex] = true
		fmt.Printf("\nEnd of data reached. Emitting final point (t=%d, value=%.1f) to close last segment -> KEEP\n",
			lastIndex, data[lastIndex])
	} else {
		// If baseIndex == lastIndex, the last point was just emitted as a segment end in the loop
		fmt.Printf("\nEnd of data reached. Final point (t=%d, value=%.1f) was already emitted as segment end.\n",
			lastIndex, data[lastIndex])
	}

	return kept
}

func main() {
	// Example test case
	data := []float64{18.0, 5.0, 6.0, 0.0, 1.0, 0.0, 2.0, 8.0}
	compDev := 1.0
	fmt.Printf("Input data: %.1f", data[0])
	for i := 1; i < len(data); i++ {
		fmt.Printf(", %.1f", data[i])
	}
	fmt.Printf("  (CompDev = %.1f)\n\n", compDev)

	// Run the SDT trace
	kept := swingingDoorTrace(data, compDev)

	fmt.Printf("\nSummary:\n")
	for i, k := range kept {
		fmt.Printf("Point %d (%.1f): %t\n", i, data[i], k)
	}
}
