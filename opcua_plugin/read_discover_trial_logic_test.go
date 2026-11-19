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

// Which servers get a DataChangeFilter and which ones are tried with one
// first, as (*OPCUAInput).decideDataChangeFilterSupport (read_discover.go)
// decides it. A trial means MonitorBatched sends the filter once and falls back
// if the server rejects it; the trial's outcome is cached on ServerCapabilities
// for the rest of the connection.

var _ = Describe("decideDataChangeFilterSupport", func() {
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

	DescribeTable("returns (supportsFilter, shouldTrial) for every capability state",
		func(c decisionCase) {
			// The embedded OPCUAConnection only has to be non-nil: the branches
			// that log do so through its nil *service.Logger, which is safe.
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

		Entry("FilterSupported (Kepware): filter, no trial",
			decisionCase{profile: profileKepware, expectSupports: true, expectTrial: false}),

		Entry("FilterUnsupported (S7-1200): no filter, no trial",
			decisionCase{profile: profileS71200, expectSupports: false, expectTrial: false}),

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

		Entry("FilterUnknown, not yet trialed on this connection: filter, and trial it",
			decisionCase{
				profile:        ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown},
				caps:           &ServerCapabilities{hasTrialedThisConnection: false, SupportsDataChangeFilter: false},
				expectSupports: true,
				expectTrial:    true,
			}),

		Entry("FilterUnknown, trial succeeded: filter, no re-trial",
			decisionCase{
				profile:        ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown},
				caps:           &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: true},
				expectSupports: true,
				expectTrial:    false,
			}),
		Entry("FilterUnknown, trial failed: no filter, no re-trial (this is what stops retry loops)",
			decisionCase{
				profile:        ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown},
				caps:           &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: false},
				expectSupports: false,
				expectTrial:    false,
			}),

		// FilterInvalid (server_profiles.go) exists only to reach the switch's
		// default branch, which treats an unknown enum value like FilterUnknown.
		Entry("FilterInvalid: filter, and trial it",
			decisionCase{
				profile:        ServerProfile{Name: "test-invalid", FilterCapability: FilterInvalid},
				expectSupports: true,
				expectTrial:    true,
			}),
	)
})
