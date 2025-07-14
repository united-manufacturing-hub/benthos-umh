//go:build windows

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
	"time"

	"golang.org/x/sys/windows"
)

// getCPUPercent implements the Windows-specific CPU usage measurement using Windows API.
// Returns the current CPU usage percentage (0-100) with EMA smoothing.
// Normalized across all CPU cores (e.g., 100% of one core on a 4-core system = 25%).
func (c *CPUMeter) getCPUPercent() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get current process handle
	handle := windows.CurrentProcess()

	// Get process CPU times
	var creationTime, exitTime, kernelTime, userTime windows.Filetime
	err := windows.GetProcessTimes(handle, &creationTime, &exitTime, &kernelTime, &userTime)
	if err != nil {
		// If we can't get CPU usage, return the last known smoothed value
		// This prevents errors from disrupting the adaptive algorithm
		return c.smoothed
	}

	now := time.Now()
	wallTime := now.Sub(c.lastCheck)

	// Convert Windows FILETIME to time.Duration
	// FILETIME is 64-bit value representing the number of 100-nanosecond intervals since January 1, 1601 (UTC)
	totalCPUTime := filetimeToDuration(kernelTime) + filetimeToDuration(userTime)

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

// filetimeToDuration converts Windows FILETIME to time.Duration
// FILETIME represents the number of 100-nanosecond intervals since January 1, 1601 (UTC)
func filetimeToDuration(ft windows.Filetime) time.Duration {
	// Convert FILETIME to 64-bit integer (100-nanosecond intervals)
	nsec := int64(ft.HighDateTime)<<32 + int64(ft.LowDateTime)
	// Convert to nanoseconds and then to time.Duration
	return time.Duration(nsec * 100)
}
