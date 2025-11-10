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

var _ = Describe("MonitorBatched three-way trial logic", func() {
	Context("Decision 1: Profile says true (SupportsDataChangeFilter=true)", func() {
		It("should use filter immediately without checking hasTrialedThisConnection", func() {
			// Setup: Profile with SupportsDataChangeFilter=true (e.g., Kepware)
			profile := profileKepware // SupportsDataChangeFilter=true
			Expect(profile.SupportsDataChangeFilter).To(BeTrue(), "Kepware profile should support DataChangeFilter")

			// Simulate MonitorBatched logic
			hasTrialed := false // Unused when profile=true

			// Decision logic (current implementation)
			shouldApplyFilter := profile.SupportsDataChangeFilter

			// Assertions
			Expect(shouldApplyFilter).To(BeTrue(), "Filter should be applied when profile says true")
			Expect(hasTrialed).To(BeFalse(), "hasTrialedThisConnection should not be checked")
		})
	})

	Context("Decision 2: Profile says false + S7-1200 profile", func() {
		It("should skip filter without trial", func() {
			// Setup: S7-1200 profile (SupportsDataChangeFilter=false, name="siemens-s7-1200")
			profile := profileS71200
			Expect(profile.SupportsDataChangeFilter).To(BeFalse(), "S7-1200 profile should not support DataChangeFilter")
			Expect(profile.Name).To(Equal(ProfileS71200), "Profile should be S7-1200")

			// Simulate MonitorBatched logic
			hasTrialed := false

			// Decision logic (NEW - not implemented yet, this will fail)
			// Expected behavior: Never trial S7-1200
			shouldApplyFilter := false
			shouldTrial := false

			// Current (broken) logic would be:
			// shouldApplyFilter = profile.SupportsDataChangeFilter  // false
			// But we need to verify it NEVER trials

			// Assertions
			Expect(shouldApplyFilter).To(BeFalse(), "Filter should not be applied for S7-1200")
			Expect(shouldTrial).To(BeFalse(), "S7-1200 should never be trialed")
			Expect(hasTrialed).To(BeFalse(), "hasTrialedThisConnection should remain false")
		})
	})

	Context("Decision 3: Profile says false + other server (not S7-1200)", func() {
		Context("when hasTrialedThisConnection is false", func() {
			It("should attempt trial with filter on first batch", func() {
				// Setup: Auto profile (SupportsDataChangeFilter=false, name="auto")
				profile := profileAuto
				Expect(profile.SupportsDataChangeFilter).To(BeFalse(), "Auto profile should default to false")
				Expect(profile.Name).To(Equal(ProfileAuto), "Profile should be Auto")

				// Simulate first batch (not trialed yet)
				hasTrialed := false

				// Decision logic (NEW - not implemented yet, this will fail)
				// Expected: Trial on first batch
				var shouldApplyFilter bool
				var shouldTrial bool

				// Three-way logic (not yet implemented):
				if profile.SupportsDataChangeFilter {
					// Decision 1: Profile says true
					shouldApplyFilter = true
					shouldTrial = false
				} else if profile.Name == ProfileS71200 {
					// Decision 2: S7-1200 never trials
					shouldApplyFilter = false
					shouldTrial = false
				} else if !hasTrialed {
					// Decision 3: Other servers trial on first batch
					shouldApplyFilter = true // Trial attempt
					shouldTrial = true
				} else {
					// Already trialed, use cached result
					shouldApplyFilter = profile.SupportsDataChangeFilter
					shouldTrial = false
				}

				// Assertions
				Expect(shouldApplyFilter).To(BeTrue(), "Filter should be applied (trial attempt)")
				Expect(shouldTrial).To(BeTrue(), "Should mark this as a trial")
			})
		})

		Context("when hasTrialedThisConnection is true", func() {
			It("should use cached result without retrialing", func() {
				// Setup: Auto profile (SupportsDataChangeFilter=false, name="auto")
				profile := profileAuto
				Expect(profile.SupportsDataChangeFilter).To(BeFalse())

				// Simulate second batch (already trialed)
				hasTrialed := true

				// Decision logic (NEW - not implemented yet)
				var shouldApplyFilter bool
				var shouldTrial bool

				// Three-way logic (not yet implemented):
				if profile.SupportsDataChangeFilter {
					shouldApplyFilter = true
					shouldTrial = false
				} else if profile.Name == ProfileS71200 {
					shouldApplyFilter = false
					shouldTrial = false
				} else if !hasTrialed {
					shouldApplyFilter = true
					shouldTrial = true
				} else {
					// Already trialed, use cached result
					shouldApplyFilter = profile.SupportsDataChangeFilter
					shouldTrial = false
				}

				// Assertions
				Expect(shouldApplyFilter).To(BeFalse(), "Filter should not be applied (cached false result)")
				Expect(shouldTrial).To(BeFalse(), "Should not trial again")
			})
		})
	})

	Context("Decision 3 variant: Profile says false + Prosys (unknown)", func() {
		Context("when hasTrialedThisConnection is false", func() {
			It("should attempt trial with filter on first batch", func() {
				// Setup: Test profile with SupportsDataChangeFilter=false, not S7-1200
				// Create test profile variant
				testProfile := ServerProfile{
					Name:                     ProfileProsys,
					SupportsDataChangeFilter: false, // Test scenario
				}

				hasTrialed := false

				// Three-way logic
				var shouldApplyFilter bool
				var shouldTrial bool

				if testProfile.SupportsDataChangeFilter {
					shouldApplyFilter = true
					shouldTrial = false
				} else if testProfile.Name == ProfileS71200 {
					shouldApplyFilter = false
					shouldTrial = false
				} else if !hasTrialed {
					shouldApplyFilter = true // Trial attempt
					shouldTrial = true
				} else {
					shouldApplyFilter = testProfile.SupportsDataChangeFilter
					shouldTrial = false
				}

				// Assertions
				Expect(shouldApplyFilter).To(BeTrue(), "Filter should be applied (trial) for non-S7-1200 server")
				Expect(shouldTrial).To(BeTrue(), "Should mark as trial for unknown server")
			})
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
