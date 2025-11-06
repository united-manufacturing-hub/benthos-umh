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
