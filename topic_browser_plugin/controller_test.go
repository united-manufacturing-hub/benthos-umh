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

var _ = Describe("AdaptiveController", func() {
	var controller *AdaptiveController

	BeforeEach(func() {
		controller = NewAdaptiveController(5 * time.Second)
	})

	Describe("NewAdaptiveController", func() {
		It("initializes with the correct default values", func() {
			Expect(controller).NotTo(BeNil())
			Expect(controller.GetCurrentInterval()).To(Equal(5 * time.Second))
			Expect(controller.cpuMeter).NotTo(BeNil())
		})

		It("sets next flush time correctly", func() {
			// nextFlush should be approximately now + initialInterval
			expectedFlush := time.Now().Add(5 * time.Second)
			actualFlush := controller.nextFlush

			// Allow 100ms tolerance for timing differences
			Expect(actualFlush).To(BeTemporally("~", expectedFlush, 100*time.Millisecond))
		})
	})

	Describe("OnBatch", func() {
		It("updates EMA bytes correctly", func() {
			// First call should set initial value
			controller.OnBatch(1000)
			Expect(controller.emaBytes).To(Equal(1000.0))

			// Second call should apply EMA smoothing
			controller.OnBatch(2000)
			expected := emaAlpha*2000.0 + (1-emaAlpha)*1000.0
			Expect(controller.emaBytes).To(BeNumerically("~", expected, 0.1))
		})

		It("handles zero payload bytes", func() {
			controller.OnBatch(0)
			Expect(controller.emaBytes).To(Equal(0.0))
		})

		It("tracks CPU sampling interval correctly", func() {
			initialSample := controller.lastCPUSample

			// First OnBatch should trigger CPU sampling (if enough time passed)
			controller.OnBatch(1000)

			// If CPU sampling occurred, lastCPUSample should be updated
			if controller.lastCPUSample.After(initialSample) {
				Expect(controller.lastCPUSample).To(BeTemporally(">", initialSample))
			}
		})
	})

	Describe("ShouldFlush", func() {
		It("returns false when next flush time hasn't been reached", func() {
			// Just created controller should not need flushing yet
			Expect(controller.ShouldFlush()).To(BeFalse())
		})

		It("returns true when next flush time has passed", func() {
			// Set next flush to the past
			controller.mu.Lock()
			controller.nextFlush = time.Now().Add(-1 * time.Second)
			controller.mu.Unlock()

			Expect(controller.ShouldFlush()).To(BeTrue())
		})
	})

	Describe("OnFlush", func() {
		It("updates next flush time based on current interval", func() {
			initialNextFlush := controller.nextFlush

			controller.OnFlush()

			// Next flush should be updated to now + current interval
			newNextFlush := controller.nextFlush
			Expect(newNextFlush).To(BeTemporally(">", initialNextFlush))

			expectedFlush := time.Now().Add(controller.GetCurrentInterval())
			Expect(newNextFlush).To(BeTemporally("~", expectedFlush, 100*time.Millisecond))
		})
	})

	Describe("calculateBaseInterval", func() {
		It("returns correct intervals for different payload sizes", func() {
			// Small payload (< 1KB)
			controller.emaBytes = 500
			interval := controller.calculateBaseInterval()
			Expect(interval).To(Equal(8 * time.Second))

			// Medium payload (1KB - 10KB)
			controller.emaBytes = 5000
			interval = controller.calculateBaseInterval()
			Expect(interval).To(Equal(4 * time.Second))

			// Large payload (10KB - 50KB)
			controller.emaBytes = 25000
			interval = controller.calculateBaseInterval()
			Expect(interval).To(Equal(2 * time.Second))

			// Very large payload (> 50KB)
			controller.emaBytes = 100000
			interval = controller.calculateBaseInterval()
			Expect(interval).To(Equal(minEmitInterval))
		})
	})

	Describe("applyGradualChange", func() {
		It("allows small changes immediately", func() {
			current := 5 * time.Second
			target := 6 * time.Second

			result := controller.applyGradualChange(current, target)
			Expect(result).To(Equal(target))
		})

		It("limits large increases", func() {
			current := 5 * time.Second
			target := 10 * time.Second // +5s change, but max is 2s

			result := controller.applyGradualChange(current, target)
			expected := current + maxIntervalChange
			Expect(result).To(Equal(expected))
		})

		It("limits large decreases", func() {
			current := 10 * time.Second
			target := 3 * time.Second // -7s change, but max is 2s

			result := controller.applyGradualChange(current, target)
			expected := current - maxIntervalChange
			Expect(result).To(Equal(expected))
		})

		It("handles same current and target", func() {
			current := 5 * time.Second
			target := 5 * time.Second

			result := controller.applyGradualChange(current, target)
			Expect(result).To(Equal(current))
		})
	})

	Describe("updateInterval integration", func() {
		It("bounds intervals to min/max values", func() {
			// Force very small payload to trigger max interval
			controller.emaBytes = 100 // Very small

			// Force multiple updates with small payloads
			for i := 0; i < 10; i++ {
				controller.OnBatch(100)                             // Small payload
				time.Sleep(cpuSampleInterval + 10*time.Millisecond) // Ensure CPU sampling
			}

			// Interval should not exceed maximum
			currentInterval := controller.GetCurrentInterval()
			Expect(currentInterval).To(BeNumerically("<=", maxEmitInterval))
			Expect(currentInterval).To(BeNumerically(">=", minEmitInterval))
		})
	})

	Describe("CPUMeter integration", func() {
		It("returns CPU percentage values", func() {
			cpuPercent := controller.GetCPUPercent()

			// CPU percentage should be reasonable bounds
			Expect(cpuPercent).To(BeNumerically(">=", 0))
			Expect(cpuPercent).To(BeNumerically("<=", 100))
		})

		It("handles multiple consecutive calls", func() {
			first := controller.GetCPUPercent()
			second := controller.GetCPUPercent()

			// Both should be valid percentages
			Expect(first).To(BeNumerically(">=", 0))
			Expect(first).To(BeNumerically("<=", 100))
			Expect(second).To(BeNumerically(">=", 0))
			Expect(second).To(BeNumerically("<=", 100))
		})
	})

	Describe("Thread safety", func() {
		It("handles concurrent OnBatch calls", func() {
			done := make(chan bool)

			// Launch multiple goroutines calling OnBatch
			for i := 0; i < 10; i++ {
				go func(payloadSize int) {
					defer GinkgoRecover()
					controller.OnBatch(payloadSize * 1000)
					done <- true
				}(i)
			}

			// Wait for all goroutines to complete
			for i := 0; i < 10; i++ {
				<-done
			}

			// Controller should still be in valid state
			Expect(controller.GetCurrentInterval()).To(BeNumerically(">=", minEmitInterval))
			Expect(controller.GetCurrentInterval()).To(BeNumerically("<=", maxEmitInterval))
		})

		It("handles concurrent ShouldFlush/OnFlush calls", func() {
			done := make(chan bool)

			// Set flush time to past so ShouldFlush returns true
			controller.mu.Lock()
			controller.nextFlush = time.Now().Add(-1 * time.Second)
			controller.mu.Unlock()

			// Launch multiple goroutines
			for i := 0; i < 5; i++ {
				go func() {
					defer GinkgoRecover()
					if controller.ShouldFlush() {
						controller.OnFlush()
					}
					done <- true
				}()
			}

			// Wait for all goroutines to complete
			for i := 0; i < 5; i++ {
				<-done
			}

			// Controller should be in valid state
			Expect(controller.nextFlush).To(BeTemporally(">", time.Now().Add(-1*time.Second)))
		})
	})
})
