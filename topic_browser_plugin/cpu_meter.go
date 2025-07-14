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
//
// Platform-specific implementation is provided in cpu_meter_unix.go and cpu_meter_windows.go.
func (c *CPUMeter) GetCPUPercent() float64 {
	return c.getCPUPercent()
}
