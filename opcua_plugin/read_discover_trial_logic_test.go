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

// This file previously re-implemented decideDataChangeFilterSupport's switch
// inside the test body and asserted against that copy, so it could not fail
// when production changed. It now calls the real method,
// (*OPCUAInput).decideDataChangeFilterSupport (read_discover.go), for every
// capability state the old file intended to cover.
//
// Specs from the old file that described MonitorBatched's response-handling
// loop (trial retry on StatusBadFilterNotAllowed, error propagation for
// non-trial failures, UX recommendation messages) asserted only against their
// own local variables and cannot reach MonitorBatched in a unit test
// (g.Subscription is the concrete gopcua *opcua.Subscription, which performs a
// real network round-trip; there is no seam). They were dropped; the states
// they were really about (cached trial results) are covered by the table
// below. See CLEANUP-REPORT.md.

var _ = Describe("decideDataChangeFilterSupport three-way trial logic", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	type decisionCase struct {
		profile        ServerProfile
		caps           *ServerCapabilities
		expectSupports bool
		expectTrial    bool
	}

	DescribeTable("returns the filter decision for each capability state",
		func(c decisionCase) {
			// OPCUAConnection is set so the embedded pointer is non-nil; the
			// nil *service.Logger is safe to log through (nil-receiver check).
			g := &OPCUAInput{
				OPCUAConnection:    &OPCUAConnection{},
				ServerProfile:      c.profile,
				ServerCapabilities: c.caps,
			}

			supportsFilter, shouldTrial := g.decideDataChangeFilterSupport()

			Expect(supportsFilter).To(Equal(c.expectSupports),
				"unexpected supportsFilter for FilterCapability=%d", c.profile.FilterCapability)
			Expect(shouldTrial).To(Equal(c.expectTrial),
				"unexpected shouldTrial for FilterCapability=%d", c.profile.FilterCapability)
		},

		// Decision 1: profile declares support -> trust immediately
		Entry("FilterSupported (Kepware): filter without trial",
			decisionCase{profile: profileKepware, expectSupports: true, expectTrial: false}),

		// Decision 2: profile declares no support -> never filter, never trial
		Entry("FilterUnsupported (S7-1200): no filter, no trial",
			decisionCase{profile: profileS71200, expectSupports: false, expectTrial: false}),

		// Decision 3, no runtime capabilities: fall back to profile default
		Entry("FilterUnknown with nil ServerCapabilities: profile default true, no trial",
			decisionCase{
				profile:        ServerProfile{Name: "test-unknown-true", FilterCapability: FilterUnknown, SupportsDataChangeFilter: true},
				expectSupports: true,
				expectTrial:    false,
			}),
		Entry("FilterUnknown with nil ServerCapabilities: profile default false, no trial",
			decisionCase{
				profile:        ServerProfile{Name: "test-unknown-false", FilterCapability: FilterUnknown, SupportsDataChangeFilter: false},
				expectSupports: false,
				expectTrial:    false,
			}),

		// Decision 3a: unknown support, first batch on this connection -> trial
		Entry("FilterUnknown, not yet trialed this connection: trial with filter",
			decisionCase{
				profile:        ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown},
				caps:           &ServerCapabilities{hasTrialedThisConnection: false, SupportsDataChangeFilter: false},
				expectSupports: true,
				expectTrial:    true,
			}),

		// Decision 3b: trial completed, cached result used, no re-trial
		Entry("FilterUnknown, trialed with cached success: filter, no re-trial",
			decisionCase{
				profile:        ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown},
				caps:           &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: true},
				expectSupports: true,
				expectTrial:    false,
			}),
		Entry("FilterUnknown, trialed with cached failure: no filter, no re-trial (prevents retry loops)",
			decisionCase{
				profile:        ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown},
				caps:           &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: false},
				expectSupports: false,
				expectTrial:    false,
			}),

		// Defensive fallback for an invalid enum value: the real default branch,
		// previously only asserted against the test's own switch.
		Entry("FilterInvalid: defensive fallback to trial mechanism",
			decisionCase{
				profile:        ServerProfile{Name: "test-invalid", FilterCapability: FilterInvalid},
				expectSupports: true,
				expectTrial:    true,
			}),
	)
})
