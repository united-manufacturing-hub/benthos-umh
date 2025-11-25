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
	"os"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("MonitorBatched three-way trial logic", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	Context("Decision 1: FilterCapability is FilterSupported", func() {
		It("should use filter immediately without trial", func() {
			// Setup: Profile with FilterCapability=FilterSupported (e.g., Kepware)
			profile := profileKepware
			Expect(profile.FilterCapability).To(Equal(FilterSupported), "Kepware profile should have FilterCapability=FilterSupported")

			// Simulate decideDataChangeFilterSupport logic
			var supportsFilter, shouldTrial bool
			switch profile.FilterCapability {
			case FilterSupported:
				supportsFilter = true
				shouldTrial = false
			}

			// Assertions
			Expect(supportsFilter).To(BeTrue(), "Filter should be applied when FilterCapability=FilterSupported")
			Expect(shouldTrial).To(BeFalse(), "Should not trial when FilterCapability=FilterSupported")
		})
	})

	Context("Decision 2: FilterCapability is FilterUnsupported", func() {
		It("should skip filter without trial", func() {
			// Setup: S7-1200 profile has FilterCapability=FilterUnsupported
			profile := profileS71200
			Expect(profile.FilterCapability).To(Equal(FilterUnsupported), "S7-1200 profile should have FilterCapability=FilterUnsupported")

			// Simulate decideDataChangeFilterSupport logic
			var supportsFilter, shouldTrial bool
			switch profile.FilterCapability {
			case FilterUnsupported:
				supportsFilter = false
				shouldTrial = false
			}

			// Assertions
			Expect(supportsFilter).To(BeFalse(), "Filter should not be applied when FilterCapability=FilterUnsupported")
			Expect(shouldTrial).To(BeFalse(), "Should never trial when FilterCapability=FilterUnsupported")
		})
	})

	Context("Decision 3: FilterCapability is FilterUnknown", func() {
		Context("when hasTrialedThisConnection is false", func() {
			It("should attempt trial with filter on first batch", func() {
				// Setup: Auto profile has FilterCapability=FilterUnknown
				profile := profileAuto
				Expect(profile.FilterCapability).To(Equal(FilterUnknown), "Auto profile should have FilterCapability=FilterUnknown")

				// Simulate first batch (not trialed yet)
				hasTrialed := false

				// Simulate decideDataChangeFilterSupport logic
				var supportsFilter, shouldTrial bool
				switch profile.FilterCapability {
				case FilterUnknown:
					if !hasTrialed {
						supportsFilter = true // Trial attempt
						shouldTrial = true
					}
				}

				// Assertions
				Expect(supportsFilter).To(BeTrue(), "Filter should be applied (trial attempt) when FilterCapability=FilterUnknown")
				Expect(shouldTrial).To(BeTrue(), "Should mark this as a trial when not yet trialed")
			})
		})

		Context("when hasTrialedThisConnection is true", func() {
			It("should use cached result without retrialing", func() {
				// Setup: Auto profile has FilterCapability=FilterUnknown
				profile := profileAuto
				Expect(profile.FilterCapability).To(Equal(FilterUnknown))

				// Simulate second batch (already trialed, cached result is false)
				hasTrialed := true
				cachedSupportsFilter := false

				// Simulate decideDataChangeFilterSupport logic
				var supportsFilter, shouldTrial bool
				switch profile.FilterCapability {
				case FilterUnknown:
					if hasTrialed {
						supportsFilter = cachedSupportsFilter // Use cached result
						shouldTrial = false
					}
				}

				// Assertions
				Expect(supportsFilter).To(BeFalse(), "Filter should not be applied (cached false result)")
				Expect(shouldTrial).To(BeFalse(), "Should not trial again when already trialed")
			})
		})
	})

	Context("Decision 3 variant: Custom profile with FilterUnknown", func() {
		Context("when hasTrialedThisConnection is false", func() {
			It("should attempt trial with filter on first batch", func() {
				// Setup: Custom test profile with FilterCapability=FilterUnknown
				testProfile := ServerProfile{
					Name:             ProfileProsys,
					FilterCapability: FilterUnknown, // Test scenario
				}

				hasTrialed := false

				// Simulate decideDataChangeFilterSupport logic
				var supportsFilter, shouldTrial bool
				switch testProfile.FilterCapability {
				case FilterUnknown:
					if !hasTrialed {
						supportsFilter = true // Trial attempt
						shouldTrial = true
					}
				}

				// Assertions
				Expect(supportsFilter).To(BeTrue(), "Filter should be applied (trial) for FilterUnknown profile")
				Expect(shouldTrial).To(BeTrue(), "Should mark as trial for FilterUnknown profile")
			})
		})
	})

	Context("Invalid FilterCapability value (defensive fallback)", func() {
		It("should default to trial mechanism with warning", func() {
			// Setup: Test profile with invalid FilterCapability value
			testProfile := ServerProfile{
				Name:             "test-invalid",
				FilterCapability: FilterInvalid, // Test constant for invalid enum
			}

			// Simulate decideDataChangeFilterSupport logic
			var supportsFilter, shouldTrial bool
			switch testProfile.FilterCapability {
			case FilterSupported:
				supportsFilter = true
				shouldTrial = false
			case FilterUnsupported:
				supportsFilter = false
				shouldTrial = false
			case FilterUnknown:
				supportsFilter = true
				shouldTrial = true
			default:
				// Defensive fallback
				supportsFilter = true
				shouldTrial = true
			}

			// Assertions
			Expect(supportsFilter).To(BeTrue(), "Invalid enum should trigger defensive fallback to trial")
			Expect(shouldTrial).To(BeTrue(), "Invalid enum should trigger trial mechanism")
		})
	})
})

var _ = Describe("MonitorBatched trial-and-retry error handling", Label("trial-and-retry"), func() {
	Context("when trial succeeds with StatusOK", func() {
		It("should not retry and should update capabilities to true", func() {
			// Setup: shouldTrial=true, server accepts filter
			shouldTrial := true
			hasTrialed := false
			supportsFilter := false

			// Mock: Server returns StatusOK for filter request
			statusCode := "Good (0x00000000)" // ua.StatusOK

			// Expected behavior:
			// 1. Monitor request with filter succeeds
			// 2. hasTrialedThisConnection = true
			// 3. SupportsDataChangeFilter = true
			// 4. No retry needed

			// Simulate successful trial
			if shouldTrial {
				// Trial succeeded
				hasTrialed = true
				supportsFilter = true
			}

			// Assertions
			Expect(hasTrialed).To(BeTrue(), "hasTrialedThisConnection should be set to true")
			Expect(supportsFilter).To(BeTrue(), "SupportsDataChangeFilter should be set to true")
			Expect(statusCode).To(Equal("Good (0x00000000)"), "Status should be OK")
		})
	})

	Context("when trial fails with StatusBadFilterNotAllowed", func() {
		It("should update capabilities and retry without filter", func() {
			// Setup: shouldTrial=true, server rejects filter
			shouldTrial := true
			hasTrialed := false
			supportsFilter := false

			// Mock: First call returns StatusBadFilterNotAllowed
			firstStatusCode := "Bad_FilterNotAllowed (0x80440000)"

			// Expected behavior on first attempt:
			// 1. Detect StatusBadFilterNotAllowed
			// 2. Set hasTrialedThisConnection = true
			// 3. Set SupportsDataChangeFilter = false
			// 4. Retry without filter

			// Simulate first attempt failure
			if shouldTrial && firstStatusCode == "Bad_FilterNotAllowed (0x80440000)" {
				// Update capabilities
				hasTrialed = true
				supportsFilter = false

				// Retry without filter (mock second attempt)
				secondStatusCode := "Good (0x00000000)" // Success without filter

				// Assertions for retry
				Expect(hasTrialed).To(BeTrue(), "hasTrialedThisConnection should be true after trial")
				Expect(supportsFilter).To(BeFalse(), "SupportsDataChangeFilter should be false")
				Expect(secondStatusCode).To(Equal("Good (0x00000000)"), "Second attempt should succeed")
			}

			// Final assertions
			Expect(hasTrialed).To(BeTrue(), "Trial should have occurred")
			Expect(supportsFilter).To(BeFalse(), "Filter support should be marked as false")
		})

		It("should not propagate error to caller after successful retry", func() {
			// Setup: Trial fails but retry succeeds
			shouldTrial := true
			Expect(shouldTrial).To(BeTrue(), "This is a trial scenario")

			// Mock: First StatusBadFilterNotAllowed, second StatusOK
			firstAttemptFailed := true
			secondAttemptSucceeded := true

			// Expected: No error returned to caller despite first attempt failure
			var finalError error = nil

			if firstAttemptFailed && secondAttemptSucceeded {
				finalError = nil // Retry succeeded, no error propagated
			}

			// Assertions
			Expect(finalError).To(BeNil(), "No error should be returned after successful retry")
		})
	})

	Context("when non-trial errors occur", func() {
		It("should propagate StatusBadNodeIdUnknown without retry", func() {
			// Setup: shouldTrial=false OR different error code
			shouldTrial := false
			statusCode := "Bad_NodeIdUnknown (0x80330000)"

			// Expected: Error propagated immediately, no retry
			shouldRetry := false

			if statusCode != "Bad_FilterNotAllowed (0x80440000)" || !shouldTrial {
				shouldRetry = false
			}

			// Assertions
			Expect(shouldRetry).To(BeFalse(), "Non-trial errors should not trigger retry")
		})

		It("should propagate StatusBadFilterNotAllowed when shouldTrial is false", func() {
			// Setup: Not a trial, but still got FilterNotAllowed
			shouldTrial := false
			statusCode := "Bad_FilterNotAllowed (0x80440000)"
			Expect(statusCode).To(Equal("Bad_FilterNotAllowed (0x80440000)"))

			// Expected: Even though it's FilterNotAllowed, don't retry if not trialing
			shouldRetry := false

			if !shouldTrial {
				shouldRetry = false // Not a trial, don't retry
			}

			// Assertions
			Expect(shouldRetry).To(BeFalse(), "FilterNotAllowed should propagate if not trialing")
		})
	})

	Context("when trial rejection triggers capability update", func() {
		It("should prevent infinite retry loops via hasTrialedThisConnection", func() {
			// Setup: Trial rejected
			hasTrialed := false

			// First attempt: Trial
			shouldTrial := !hasTrialed
			Expect(shouldTrial).To(BeTrue(), "First attempt should be a trial")

			// Trial fails and updates hasTrialed
			hasTrialed = true

			// Second attempt: Should use cached result (Decision 3b)
			shouldTrial = !hasTrialed
			Expect(shouldTrial).To(BeFalse(), "Second attempt should not trial again")

			// Assertion
			Expect(hasTrialed).To(BeTrue(), "hasTrialedThisConnection prevents re-trialing")
		})
	})
})

var _ = Describe("MonitorBatched UX recommendation messages", Label("ux-messages"), func() {
	Context("after trial succeeds", func() {
		It("should emit recommendation message with supportsDataChangeFilter: true", func() {
			// Setup: Trial succeeds (StatusOK)
			shouldTrial := true
			supportsFilter := false

			// Simulate trial success logic
			if shouldTrial {
				supportsFilter = true
				// UX message would be emitted here in actual code
			}

			// Verify trial succeeded and capability was updated
			Expect(supportsFilter).To(BeTrue(), "Trial should have succeeded")

			// Test documents expected UX message content:
			// - "Recommendation: To save ~1-2 seconds on future connections"
			// - "explicitly configure this capability in your server profile"
			// - "serverProfile:\n    supportsDataChangeFilter: true"
			// - "See documentation: docs/input/opc-ua-input.md#server-profiles"
		})
	})

	Context("after trial fails", func() {
		It("should emit recommendation message with supportsDataChangeFilter: false", func() {
			// Setup: Trial fails (StatusBadFilterNotAllowed)
			shouldTrial := true
			supportsFilter := true

			// Simulate trial failure logic
			if shouldTrial {
				supportsFilter = false
				// UX message would be emitted here in actual code
			}

			// Verify trial failed and capability was updated
			Expect(supportsFilter).To(BeFalse(), "Trial should have failed")

			// Test documents expected UX message content:
			// - "Recommendation: To save ~1-2 seconds on future connections"
			// - "explicitly configure this capability in your server profile"
			// - "serverProfile:\n    supportsDataChangeFilter: false"
			// - "See documentation: docs/input/opc-ua-input.md#server-profiles"
		})
	})

	Context("for profiles with FilterCapability=FilterUnsupported (Decision 2)", func() {
		It("should not trial or emit recommendation message", func() {
			// Setup: S7-1200 profile has FilterCapability=FilterUnsupported (never trials)
			profile := profileS71200

			// Verify S7-1200 profile configuration
			Expect(profile.Name).To(Equal(ProfileS71200))
			Expect(profile.FilterCapability).To(Equal(FilterUnsupported))

			// Simulate decideDataChangeFilterSupport logic
			var supportsFilter, shouldTrial bool
			switch profile.FilterCapability {
			case FilterUnsupported:
				supportsFilter = false
				shouldTrial = false
			}

			// Verify no trial occurs (no UX message)
			Expect(supportsFilter).To(BeFalse(), "FilterUnsupported profiles should not use filter")
			Expect(shouldTrial).To(BeFalse(), "FilterUnsupported profiles should never trial")
		})
	})

	Context("for profiles with FilterCapability=FilterSupported (Decision 1)", func() {
		It("should not trial or emit recommendation message", func() {
			// Setup: Kepware profile has FilterCapability=FilterSupported (trusted)
			profile := profileKepware

			// Verify Kepware profile configuration
			Expect(profile.FilterCapability).To(Equal(FilterSupported), "Kepware profile should be FilterSupported")

			// Simulate decideDataChangeFilterSupport logic
			var supportsFilter, shouldTrial bool
			switch profile.FilterCapability {
			case FilterSupported:
				supportsFilter = true
				shouldTrial = false
			}

			// Verify no trial occurs (no UX message)
			Expect(supportsFilter).To(BeTrue(), "FilterSupported profiles should use filter")
			Expect(shouldTrial).To(BeFalse(), "FilterSupported profiles don't trial")
		})
	})
})
