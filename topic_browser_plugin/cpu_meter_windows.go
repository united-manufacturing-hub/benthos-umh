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
	"time"

	"golang.org/x/sys/windows"
)

// getTotalCPUTime returns the total CPU time (user + system) using Windows API.
func (c *CPUMeter) getTotalCPUTime() (time.Duration, error) {
	// Get current process handle
	handle := windows.CurrentProcess()

	// Get process CPU times
	var creationTime, exitTime, kernelTime, userTime windows.Filetime
	err := windows.GetProcessTimes(handle, &creationTime, &exitTime, &kernelTime, &userTime)
	if err != nil {
		return 0, err
	}

	// Convert Windows FILETIME to time.Duration
	// FILETIME is 64-bit value representing the number of 100-nanosecond intervals since January 1, 1601 (UTC)
	totalCPUTime := filetimeToDuration(kernelTime) + filetimeToDuration(userTime)

	return totalCPUTime, nil
}

// filetimeToDuration converts Windows FILETIME to time.Duration
// FILETIME represents the number of 100-nanosecond intervals since January 1, 1601 (UTC)
func filetimeToDuration(ft windows.Filetime) time.Duration {
	// Convert FILETIME to 64-bit integer (100-nanosecond intervals)
	nsec := int64(ft.HighDateTime)<<32 + int64(ft.LowDateTime)
	// Convert to nanoseconds and then to time.Duration
	return time.Duration(nsec * 100)
}
