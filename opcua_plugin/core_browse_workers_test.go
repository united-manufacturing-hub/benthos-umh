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

package opcua_plugin

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewServerMetrics", func() {
	Context("when profile has MaxWorkers less than InitialWorkers", func() {
		It("should clamp currentWorkers to MaxWorkers", func() {
			// ARRANGE: Create profile with MaxWorkers=5 (less than InitialWorkers=10)
			profile := ServerProfile{
				Name:       "Auto",
				MinWorkers: 2,
				MaxWorkers: 5, // Less than InitialWorkers (10)
			}

			// ACT: Create metrics with this profile
			metrics := NewServerMetrics(profile)

			// ASSERT: currentWorkers should be clamped to MaxWorkers, not InitialWorkers
			Expect(metrics.currentWorkers).To(Equal(5),
				"currentWorkers should be clamped to profile.MaxWorkers (5), not InitialWorkers (10)")
			Expect(metrics.minWorkers).To(Equal(2))
			Expect(metrics.maxWorkers).To(Equal(5))
		})
	})

	Context("when profile has MinWorkers greater than InitialWorkers", func() {
		It("should clamp currentWorkers to MinWorkers", func() {
			// ARRANGE: Create profile with MinWorkers=15 (greater than InitialWorkers=10)
			profile := ServerProfile{
				Name:       "Aggressive",
				MinWorkers: 15, // Greater than InitialWorkers (10)
				MaxWorkers: 100,
			}

			// ACT: Create metrics with this profile
			metrics := NewServerMetrics(profile)

			// ASSERT: currentWorkers should be clamped to MinWorkers, not InitialWorkers
			Expect(metrics.currentWorkers).To(Equal(15),
				"currentWorkers should be clamped to profile.MinWorkers (15), not InitialWorkers (10)")
			Expect(metrics.minWorkers).To(Equal(15))
			Expect(metrics.maxWorkers).To(Equal(100))
		})
	})

	Context("when InitialWorkers falls within profile bounds", func() {
		It("should use InitialWorkers", func() {
			// ARRANGE: Create profile with bounds that include InitialWorkers
			profile := ServerProfile{
				Name:       "Default",
				MinWorkers: 5,
				MaxWorkers: 20, // InitialWorkers=10 falls within [5, 20]
			}

			// ACT: Create metrics with this profile
			metrics := NewServerMetrics(profile)

			// ASSERT: currentWorkers should use InitialWorkers (10)
			Expect(metrics.currentWorkers).To(Equal(10),
				"currentWorkers should use InitialWorkers (10) when within profile bounds")
			Expect(metrics.minWorkers).To(Equal(5))
			Expect(metrics.maxWorkers).To(Equal(20))
		})
	})

	Context("when profile has zero MaxWorkers", func() {
		It("should use InitialWorkers without clamping", func() {
			// ARRANGE: Create profile with zero MaxWorkers (disabled/unlimited)
			profile := ServerProfile{
				Name:       "Unknown",
				MinWorkers: 0,
				MaxWorkers: 0, // Zero means no maximum limit
			}

			// ACT: Create metrics with this profile
			metrics := NewServerMetrics(profile)

			// ASSERT: currentWorkers should use InitialWorkers without clamping
			Expect(metrics.currentWorkers).To(Equal(10),
				"currentWorkers should use InitialWorkers when MaxWorkers is zero (unlimited)")
			Expect(metrics.minWorkers).To(Equal(0))
			Expect(metrics.maxWorkers).To(Equal(0))
		})
	})

	Context("when profile has MinWorkers greater than MaxWorkers (invalid)", func() {
		It("should clamp to MaxWorkers (hardware limit takes priority)", func() {
			// ARRANGE: Create profile with invalid configuration (MinWorkers > MaxWorkers)
			profile := ServerProfile{
				Name:       "InvalidProfile",
				MinWorkers: 20, // Invalid: Min > Max
				MaxWorkers: 10,
			}

			// ACT: Create metrics with this profile
			metrics := NewServerMetrics(profile)

			// ASSERT: currentWorkers should respect MaxWorkers even when MinWorkers is higher (invalid profile)
			Expect(metrics.currentWorkers).To(Equal(10),
				"currentWorkers should respect MaxWorkers even when MinWorkers is higher (invalid profile)")
			Expect(metrics.minWorkers).To(Equal(20))
			Expect(metrics.maxWorkers).To(Equal(10))
		})
	})
})

var _ = Describe("adjustWorkers with latency-based scaling", func() {
	var (
		metrics *ServerMetrics
		logger  *testLogger
	)

	BeforeEach(func() {
		logger = &testLogger{}
	})

	Context("when sample size is insufficient (< 5)", func() {
		It("should return (0, 0) with no scaling", func() {
			// ARRANGE: Create metrics with only 3 samples
			profile := ServerProfile{
				Name:       "Ignition",
				MinWorkers: 5,
				MaxWorkers: 20,
			}
			metrics = NewServerMetrics(profile)
			metrics.recordResponseTime(100 * time.Millisecond)
			metrics.recordResponseTime(150 * time.Millisecond)
			metrics.recordResponseTime(200 * time.Millisecond)

			// ACT: Call adjustWorkers
			toAdd, toRemove := metrics.adjustWorkers(logger)

			// ASSERT: Should return (0, 0) due to insufficient sample size
			Expect(toAdd).To(Equal(0), "toAdd should be 0 when sample size < 5")
			Expect(toRemove).To(Equal(0), "toRemove should be 0 when sample size < 5")
		})
	})

	Context("when avgResponse > targetLatency (250ms)", func() {
		It("should reduce workers by 1, respecting MinWorkers bound", func() {
			// ARRANGE: Create metrics with high response times (400ms avg)
			profile := ServerProfile{
				Name:       "Ignition",
				MinWorkers: 5,
				MaxWorkers: 20,
			}
			metrics = NewServerMetrics(profile)
			// Record 5 high-latency samples (total 2000ms, avg 400ms > 250ms target)
			for i := 0; i < 5; i++ {
				metrics.recordResponseTime(400 * time.Millisecond)
			}

			// ACT: Call adjustWorkers
			toAdd, toRemove := metrics.adjustWorkers(logger)

			// ASSERT: Should reduce workers by 1 (from 10 to 9)
			Expect(toAdd).To(Equal(0), "toAdd should be 0 when reducing workers")
			Expect(toRemove).To(Equal(1), "toRemove should be 1 (reducing by 1 worker)")
			Expect(metrics.currentWorkers).To(Equal(9), "currentWorkers should be 9 (10 - 1)")
		})

		It("should not reduce below MinWorkers", func() {
			// ARRANGE: Create metrics already at MinWorkers
			profile := ServerProfile{
				Name:       "Auto",
				MinWorkers: 2,
				MaxWorkers: 5,
			}
			metrics = NewServerMetrics(profile)
			// Force currentWorkers to MinWorkers
			metrics.currentWorkers = 2
			// Record high-latency samples
			for i := 0; i < 5; i++ {
				metrics.recordResponseTime(500 * time.Millisecond)
			}

			// ACT: Call adjustWorkers
			toAdd, toRemove := metrics.adjustWorkers(logger)

			// ASSERT: Should not reduce further
			Expect(toAdd).To(Equal(0))
			Expect(toRemove).To(Equal(0))
			Expect(metrics.currentWorkers).To(Equal(2), "currentWorkers should stay at MinWorkers")
		})
	})

	Context("when avgResponse < targetLatency (250ms)", func() {
		It("should increase workers by 1, respecting MaxWorkers bound", func() {
			// ARRANGE: Create metrics with low response times (100ms avg)
			profile := ServerProfile{
				Name:       "Ignition",
				MinWorkers: 5,
				MaxWorkers: 20,
			}
			metrics = NewServerMetrics(profile)
			// Start with initial workers (10)
			// Record 5 low-latency samples (total 500ms, avg 100ms < 250ms target)
			for i := 0; i < 5; i++ {
				metrics.recordResponseTime(100 * time.Millisecond)
			}

			// ACT: Call adjustWorkers
			toAdd, toRemove := metrics.adjustWorkers(logger)

			// ASSERT: Should increase workers by 1 (from 10 to 11)
			Expect(toAdd).To(Equal(1), "toAdd should be 1 when increasing workers")
			Expect(toRemove).To(Equal(0), "toRemove should be 0 when adding workers")
			Expect(metrics.currentWorkers).To(Equal(11), "currentWorkers should be 11 (10 + 1)")
		})

		It("should not increase above MaxWorkers", func() {
			// ARRANGE: Create metrics already at MaxWorkers
			profile := ServerProfile{
				Name:       "Auto",
				MinWorkers: 2,
				MaxWorkers: 5,
			}
			metrics = NewServerMetrics(profile)
			// Force currentWorkers to MaxWorkers
			metrics.currentWorkers = 5
			// Record low-latency samples
			for i := 0; i < 5; i++ {
				metrics.recordResponseTime(100 * time.Millisecond)
			}

			// ACT: Call adjustWorkers
			toAdd, toRemove := metrics.adjustWorkers(logger)

			// ASSERT: Should not increase further
			Expect(toAdd).To(Equal(0))
			Expect(toRemove).To(Equal(0))
			Expect(metrics.currentWorkers).To(Equal(5), "currentWorkers should stay at MaxWorkers")
		})
	})

	Context("when avgResponse == targetLatency (edge case)", func() {
		It("should not adjust workers", func() {
			// ARRANGE: Create metrics with exact target latency (250ms avg)
			profile := ServerProfile{
				Name:       "Ignition",
				MinWorkers: 5,
				MaxWorkers: 20,
			}
			metrics = NewServerMetrics(profile)
			// Record 5 samples at exactly 250ms
			for i := 0; i < 5; i++ {
				metrics.recordResponseTime(250 * time.Millisecond)
			}

			// ACT: Call adjustWorkers
			toAdd, toRemove := metrics.adjustWorkers(logger)

			// ASSERT: Should not adjust workers (250ms is NOT > 250ms, NOT < 250ms)
			Expect(toAdd).To(Equal(0))
			Expect(toRemove).To(Equal(0))
			Expect(metrics.currentWorkers).To(Equal(10), "currentWorkers should remain unchanged")
		})
	})

	Context("responseTimes buffer clearing", func() {
		It("should clear responseTimes after adjusting workers", func() {
			// ARRANGE: Create metrics with samples
			profile := ServerProfile{
				Name:       "Ignition",
				MinWorkers: 5,
				MaxWorkers: 20,
			}
			metrics = NewServerMetrics(profile)
			for i := 0; i < 5; i++ {
				metrics.recordResponseTime(300 * time.Millisecond)
			}

			// ACT: Call adjustWorkers
			metrics.adjustWorkers(logger)

			// ASSERT: responseTimes should be cleared
			Expect(len(metrics.responseTimes)).To(Equal(0), "responseTimes should be cleared after adjusting")
		})
	})
})

// testLogger is a simple Logger implementation for testing
type testLogger struct{}

func (t *testLogger) Errorf(format string, v ...any)   {}
func (t *testLogger) Warnf(format string, v ...any)    {}
func (t *testLogger) Infof(format string, v ...any)    {}
func (t *testLogger) Debugf(format string, v ...any)   {}
func (t *testLogger) Tracef(format string, v ...any)   {}
func (t *testLogger) Fatal(v ...any)                   {}
func (t *testLogger) With(keyValues ...any) Logger { return t }
