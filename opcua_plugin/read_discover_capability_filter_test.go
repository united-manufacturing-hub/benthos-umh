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

	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This file previously held a copy of MonitorBatched's capability-resolution
// and filter-creation block ("EXACT logic ... line 336-347") and asserted
// against that copy, so it could not fail when production changed.
//
// MonitorBatched itself has no unit-test seam: g.Subscription is the concrete
// gopcua *opcua.Subscription and its Monitor() performs a real network
// round-trip. What this file can do is call the three real production
// functions MonitorBatched composes for each node (read_discover.go):
//
//	supportsFilter, shouldTrial := g.decideDataChangeFilterSupport()
//	...
//	if supportsFilter && isNumericDataType(nodeDef.DataTypeID) {
//	    filter = createDataChangeFilter(g.DeadbandType, g.DeadbandValue)
//	}
//
// and assert on their real outputs. The two pending specs at the bottom
// preserve expectations the old mirror asserted that the real functions do
// NOT reproduce - see CLEANUP-REPORT.md.

var _ = Describe("MonitorBatched Filter Creation Logic", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	type filterCase struct {
		profile        ServerProfile
		caps           *ServerCapabilities
		dataTypeID     ua.TypeID
		deadbandType   string
		deadbandValue  float64
		expectSupports bool
		expectFilter   bool
	}

	DescribeTable("applies capability check before creating filter",
		func(c filterCase) {
			g := &OPCUAInput{
				OPCUAConnection:    &OPCUAConnection{},
				DeadbandType:       c.deadbandType,
				DeadbandValue:      c.deadbandValue,
				QueueSize:          1,
				SamplingInterval:  1000.0,
				ServerProfile:      c.profile,
				ServerCapabilities: c.caps,
			}

			nodeDef := NodeDef{
				NodeID:     ua.NewNumericNodeID(2, 1001),
				Path:       "Test.Node",
				DataTypeID: c.dataTypeID,
			}

			// Real production calls, in MonitorBatched's exact composition.
			supportsFilter, _ := g.decideDataChangeFilterSupport()
			Expect(supportsFilter).To(Equal(c.expectSupports),
				"unexpected supportsFilter decision")

			var filter *ua.ExtensionObject
			if supportsFilter && isNumericDataType(nodeDef.DataTypeID) {
				filter = createDataChangeFilter(g.DeadbandType, g.DeadbandValue)
			}

			if c.expectFilter {
				Expect(filter).NotTo(BeNil(),
					"Filter should be created (profile=%s, type=%v, deadband=%s)",
					c.profile.Name, c.dataTypeID, c.deadbandType)
			} else {
				Expect(filter).To(BeNil(),
					"Filter should be omitted (profile=%s, type=%v, deadband=%s)",
					c.profile.Name, c.dataTypeID, c.deadbandType)
			}
		},

		// Scenario 1: S7-1200 (FilterUnsupported, no runtime detection)
		Entry("S7-1200: numeric + deadband, but profile doesn't support filters → NO filter",
			filterCase{profile: profileS71200, dataTypeID: ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: false, expectFilter: false}),

		// Scenario 2: Kepware (FilterSupported, no runtime detection)
		Entry("Kepware: numeric + deadband, profile supports filters → CREATE filter",
			filterCase{profile: profileKepware, dataTypeID: ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: true, expectFilter: true}),

		// Scenario 3: runtime (cached trial) result overrides unknown-profile default
		Entry("Unknown profile, cached runtime result true: numeric + deadband → CREATE filter",
			filterCase{
				profile:        ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown, SupportsDataChangeFilter: false},
				caps:           &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: true},
				dataTypeID:     ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: true, expectFilter: true}),

		// Scenario 4: runtime (cached trial) result overrides unknown-profile default
		Entry("Unknown profile, cached runtime result false: numeric + deadband → NO filter",
			filterCase{
				profile:        ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown, SupportsDataChangeFilter: true},
				caps:           &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: false},
				dataTypeID:     ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: false, expectFilter: false}),

		// Scenario 5: non-numeric node (even if profile supports)
		Entry("Non-numeric: profile supports, but node is String → NO filter",
			filterCase{profile: profileKepware, dataTypeID: ua.TypeIDString, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: true, expectFilter: false}),

		// Scenario 6: deadband disabled (production's nil comes from
		// createDataChangeFilter itself, there is no separate "none" check in
		// MonitorBatched)
		Entry("Deadband disabled: profile supports, numeric, but deadband=none → NO filter",
			filterCase{profile: profileKepware, dataTypeID: ua.TypeIDDouble, deadbandType: "none", deadbandValue: 0.0,
				expectSupports: true, expectFilter: false}),

		// Scenario 7: percent deadband
		Entry("Percent deadband: profile supports, numeric, percent deadband → CREATE filter",
			filterCase{profile: profileKepware, dataTypeID: ua.TypeIDFloat, deadbandType: "percent", deadbandValue: 2.0,
				expectSupports: true, expectFilter: true}),

		// Scenario 8: runtime confirms profile default (both no)
		Entry("Runtime confirms NO: profile default no, cached no, numeric + deadband → NO filter",
			filterCase{
				profile:        ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown, SupportsDataChangeFilter: false},
				caps:           &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: false},
				dataTypeID:     ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: false, expectFilter: false}),

		// Scenario 9: runtime confirms profile default (both yes)
		Entry("Runtime confirms YES: profile default yes, cached yes, numeric + deadband → CREATE filter",
			filterCase{
				profile:        ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown, SupportsDataChangeFilter: true},
				caps:           &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: true},
				dataTypeID:     ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: true, expectFilter: true}),
	)

	Context("two-tier detection priority", func() {
		It("should prioritize the cached runtime result over the profile default", func() {
			// Profile default says NO, cached trial result says YES → runtime wins
			g := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{},
				ServerProfile: ServerProfile{
					Name:                     "test-unknown",
					FilterCapability:         FilterUnknown,
					SupportsDataChangeFilter: false,
				},
				ServerCapabilities: &ServerCapabilities{
					hasTrialedThisConnection: true,
					SupportsDataChangeFilter: true,
				},
			}

			supportsFilter, shouldTrial := g.decideDataChangeFilterSupport()
			Expect(supportsFilter).To(BeTrue(), "cached runtime result should override profile default")
			Expect(shouldTrial).To(BeFalse(), "already trialed this connection")
		})

		It("should use the profile default when ServerCapabilities is nil", func() {
			g := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{},
				ServerProfile: ServerProfile{
					Name:                     "test-unknown",
					FilterCapability:         FilterUnknown,
					SupportsDataChangeFilter: true,
				},
				ServerCapabilities: nil,
			}

			supportsFilter, shouldTrial := g.decideDataChangeFilterSupport()
			Expect(supportsFilter).To(BeTrue(), "should use profile default when runtime not available")
			Expect(shouldTrial).To(BeFalse(), "no trial without ServerCapabilities")
		})
	})

	Context("CONVERTED FINDINGS: old mirror claims that production does not implement", func() {
		// The two specs below preserve expectations the deleted mirror asserted
		// against its own copy of the capability logic. Pointed at the real
		// decideDataChangeFilterSupport they go RED, so they are left pending.
		// Evidence and analysis in CLEANUP-REPORT.md - a human decides whether
		// the old expectation or production behavior is the intended one.

		PIt("old claim: untrialed runtime detection (caps=false) suppresses the filter", func() {
			// Old mirror scenario 4 asserted: profileSupport=true, runtime=false
			// → NO filter. The mirror read ServerCapabilities.SupportsDataChangeFilter
			// directly, so it expected the filter to be suppressed.
			//
			// This is exactly the state right after Connect(): ServerCapabilities
			// is set by queryOperationLimits with the safe default
			// SupportsDataChangeFilter=false (read.go) and
			// hasTrialedThisConnection=false.
			//
			// The real decideDataChangeFilterSupport ignores the cached value
			// until a trial has happened and returns (true, true) for this
			// state (read_discover.go: "if !g.ServerCapabilities.hasTrialedThisConnection
			// { return true, true }"), so MonitorBatched sends the filter on the
			// first batch. DISAGREEMENT with the old expectation.
			g := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{},
				ServerProfile: ServerProfile{
					Name:                     "test-unknown",
					FilterCapability:         FilterUnknown,
					SupportsDataChangeFilter: true,
				},
				ServerCapabilities: &ServerCapabilities{
					hasTrialedThisConnection: false,
					SupportsDataChangeFilter: false,
				},
			}

			supportsFilter, _ := g.decideDataChangeFilterSupport()

			// The old mirror's expectation, kept verbatim:
			Expect(supportsFilter).To(BeFalse(),
				"old mirror expected runtime caps=false to suppress the filter; production returns (true, true) pre-trial")
		})

		PIt("old claim: runtime detection overrides a FilterUnsupported profile", func() {
			// Old mirror scenario 3 asserted: profileSupport=false (S7-1200),
			// runtime=true → CREATE filter, i.e. runtime caps always override
			// the profile.
			//
			// The real decideDataChangeFilterSupport switches on
			// FilterCapability first: FilterUnsupported (profileS71200)
			// short-circuits to (false, false) and never consults
			// ServerCapabilities, so no filter is created. DISAGREEMENT with
			// the old expectation.
			g := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{},
				ServerProfile:   profileS71200,
				ServerCapabilities: &ServerCapabilities{
					hasTrialedThisConnection: true,
					SupportsDataChangeFilter: true,
				},
			}

			supportsFilter, _ := g.decideDataChangeFilterSupport()

			// The old mirror's expectation, kept verbatim:
			Expect(supportsFilter).To(BeTrue(),
				"old mirror expected runtime caps=true to override the profile; FilterUnsupported never consults caps")
		})
	})
})
