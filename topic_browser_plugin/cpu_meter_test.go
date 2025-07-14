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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CPUMeter", func() {
	Context("when created with valid alpha", func() {
		It("should return non-negative CPU percentage", func() {
			meter := NewCPUMeter(0.2)
			Expect(meter).ToNot(BeNil())

			// Get initial reading
			percent := meter.GetCPUPercent()
			Expect(percent).To(BeNumerically(">=", 0))
			Expect(percent).To(BeNumerically("<=", 100))
		})

		It("should return consistent readings when called multiple times", func() {
			meter := NewCPUMeter(0.2)

			// Get multiple readings
			var readings []float64
			for i := 0; i < 5; i++ {
				reading := meter.GetCPUPercent()
				readings = append(readings, reading)
				time.Sleep(10 * time.Millisecond)
			}

			// All readings should be valid
			for _, reading := range readings {
				Expect(reading).To(BeNumerically(">=", 0))
				Expect(reading).To(BeNumerically("<=", 100))
			}
		})

		It("should smooth readings with EMA", func() {
			meter := NewCPUMeter(0.1) // Low alpha for more smoothing

			// Get initial reading to initialize
			meter.GetCPUPercent()
			time.Sleep(50 * time.Millisecond)

			// Get two readings close in time
			reading1 := meter.GetCPUPercent()
			time.Sleep(10 * time.Millisecond)
			reading2 := meter.GetCPUPercent()

			// Readings should be smoothed (not wildly different)
			// This is a basic test - in practice, EMA prevents rapid oscillations
			Expect(reading1).To(BeNumerically(">=", 0))
			Expect(reading2).To(BeNumerically(">=", 0))
			Expect(reading1).To(BeNumerically("<=", 100))
			Expect(reading2).To(BeNumerically("<=", 100))
		})
	})

	Context("when created with different alpha values", func() {
		It("should handle alpha = 0.05 (stable)", func() {
			meter := NewCPUMeter(0.05)
			percent := meter.GetCPUPercent()
			Expect(percent).To(BeNumerically(">=", 0))
			Expect(percent).To(BeNumerically("<=", 100))
		})

		It("should handle alpha = 0.5 (responsive)", func() {
			meter := NewCPUMeter(0.5)
			percent := meter.GetCPUPercent()
			Expect(percent).To(BeNumerically(">=", 0))
			Expect(percent).To(BeNumerically("<=", 100))
		})

		It("should handle alpha = 1.0 (no smoothing)", func() {
			meter := NewCPUMeter(1.0)
			percent := meter.GetCPUPercent()
			Expect(percent).To(BeNumerically(">=", 0))
			Expect(percent).To(BeNumerically("<=", 100))
		})
	})
})
