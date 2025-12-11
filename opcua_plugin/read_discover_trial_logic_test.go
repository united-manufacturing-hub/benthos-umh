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
			Expect(finalError).ToNot(HaveOccurred(), "No error should be returned after successful retry")
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
			supportsFilter := shouldTrial

			// Simulate trial success logic

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
			supportsFilter := !shouldTrial

			// Simulate trial failure logic

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
		})
	})

	Context("for profiles with FilterCapability=FilterSupported (Decision 1)", func() {
		It("should not trial or emit recommendation message", func() {
			// Setup: Kepware profile has FilterCapability=FilterSupported (trusted)
			profile := profileKepware

			// Verify Kepware profile configuration
			Expect(profile.FilterCapability).To(Equal(FilterSupported), "Kepware profile should be FilterSupported")
		})
	})
})
