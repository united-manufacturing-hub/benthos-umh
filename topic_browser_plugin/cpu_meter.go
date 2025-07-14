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

package topic_browser_plugin

import (
	"math"
	"runtime"
	"sync"
	"time"
)

// CPUMeter tracks CPU usage using platform-specific implementations with EMA smoothing.
// Uses exponential moving average to provide stable CPU load measurements
// that don't oscillate rapidly.
type CPUMeter struct {
	lastUsage time.Duration // Previous total CPU time
	lastCheck time.Time     // When we last measured
	smoothed  float64       // EMA-smoothed CPU percentage (0-100)
	alpha     float64       // EMA smoothing factor (0.2 = fast response, 0.05 = stable)
	mu        sync.Mutex
}

// NewCPUMeter creates a new CPU meter with the specified EMA smoothing factor.
// alpha=0.2 provides good balance between responsiveness and stability.
func NewCPUMeter(alpha float64) *CPUMeter {
	return &CPUMeter{
		lastCheck: time.Now(),
		alpha:     alpha,
		smoothed:  0.0,
	}
}

// GetCPUPercent returns the current CPU usage percentage (0-100) with EMA smoothing.
// Normalized across all CPU cores (e.g., 100% of one core on a 4-core system = 25%).
// This method samples CPU usage and applies exponential moving average to prevent
// rapid oscillations that could cause unstable adaptive behavior.
func (c *CPUMeter) GetCPUPercent() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get platform-specific total CPU time
	totalCPUTime, err := c.getTotalCPUTime()
	if err != nil {
		// If we can't get CPU usage, return the last known smoothed value
		// This prevents errors from disrupting the adaptive algorithm
		return c.smoothed
	}

	now := time.Now()
	wallTime := now.Sub(c.lastCheck)

	// Calculate CPU percentage over the elapsed wall time
	var cpuPercent float64
	if wallTime > 0 && c.lastCheck != (time.Time{}) {
		cpuUsage := totalCPUTime - c.lastUsage
		cpuPercent = float64(cpuUsage) / float64(wallTime) * 100

		// Normalize to 0-100% by dividing by GOMAXPROCS
		// (100% of one core on a 4-core system = 25% with GOMAXPROCS=4)
		// (100% of one core on a 4-core system = 100% with GOMAXPROCS=1)
		cpuPercent = cpuPercent / float64(runtime.GOMAXPROCS(0))

		// Clamp to reasonable bounds (0-100%)
		cpuPercent = math.Max(0, math.Min(cpuPercent, 100))
	}

	// Apply EMA smoothing: new_value = alpha * current + (1-alpha) * previous
	c.smoothed = c.alpha*cpuPercent + (1-c.alpha)*c.smoothed

	// Update tracking variables for next measurement
	c.lastUsage = totalCPUTime
	c.lastCheck = now

	return c.smoothed
}
