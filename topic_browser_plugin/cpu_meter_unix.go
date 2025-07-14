//go:build unix

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
	"syscall"
	"time"
)

// getTotalCPUTime returns the total CPU time (user + system) using Unix syscall.Getrusage.
func (c *CPUMeter) getTotalCPUTime() (time.Duration, error) {
	var rusage syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	if err != nil {
		return 0, err
	}

	// Convert rusage timeval to time.Duration
	totalCPUTime := time.Duration(rusage.Utime.Sec+rusage.Stime.Sec)*time.Second +
		time.Duration(rusage.Utime.Usec+rusage.Stime.Usec)*time.Microsecond

	return totalCPUTime, nil
}
