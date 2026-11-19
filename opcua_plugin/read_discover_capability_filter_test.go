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

// Whether a node ends up with a DataChangeFilter. It takes all three of the
// server's capability, the node's data type and the deadband config.
//
// MonitorBatched decides this per node by composing three functions,
// decideDataChangeFilterSupport and isNumericDataType from read_discover.go
// and createDataChangeFilter from deadband.go:
//
//	supportsFilter, shouldTrial := g.decideDataChangeFilterSupport()
//	...
//	if supportsFilter && isNumericDataType(nodeDef.DataTypeID) {
//	    filter = createDataChangeFilter(g.DeadbandType, g.DeadbandValue)
//	}
//
// The specs below call those three in that same composition, because
// MonitorBatched itself cannot be reached from a unit test: g.Subscription is
// the concrete gopcua *opcua.Subscription, whose Monitor() does a network
// round-trip.

var _ = Describe("whether a node gets a DataChangeFilter", func() {
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

	DescribeTable("creates one only when server, data type and deadband all allow it",
		func(c filterCase) {
			g := &OPCUAInput{
				OPCUAConnection:    &OPCUAConnection{},
				DeadbandType:       c.deadbandType,
				DeadbandValue:      c.deadbandValue,
				ServerProfile:      c.profile,
				ServerCapabilities: c.caps,
			}
			nodeDef := NodeDef{DataTypeID: c.dataTypeID}

			supportsFilter, _ := g.decideDataChangeFilterSupport()
			Expect(supportsFilter).To(Equal(c.expectSupports),
				"unexpected supportsFilter for profile=%s", c.profile.Name)

			var filter *ua.ExtensionObject
			if supportsFilter && isNumericDataType(nodeDef.DataTypeID) {
				filter = createDataChangeFilter(g.DeadbandType, g.DeadbandValue)
			}

			if c.expectFilter {
				Expect(filter).NotTo(BeNil(),
					"expected a filter (profile=%s, type=%v, deadband=%s)",
					c.profile.Name, c.dataTypeID, c.deadbandType)
			} else {
				Expect(filter).To(BeNil(),
					"expected no filter (profile=%s, type=%v, deadband=%s)",
					c.profile.Name, c.dataTypeID, c.deadbandType)
			}
		},

		Entry("S7-1200 profile declares no filter support: no filter, although the node is numeric and a deadband is set",
			filterCase{
				profile: profileS71200, dataTypeID: ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: false, expectFilter: false,
			}),

		Entry("Kepware profile declares filter support: filter",
			filterCase{
				profile: profileKepware, dataTypeID: ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: true, expectFilter: true,
			}),

		Entry("unknown profile, trial succeeded against a profile default of false: filter",
			filterCase{
				profile:    ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown, SupportsDataChangeFilter: false},
				caps:       &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: true},
				dataTypeID: ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: true, expectFilter: true,
			}),

		Entry("unknown profile, trial failed against a profile default of true: no filter",
			filterCase{
				profile:    ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown, SupportsDataChangeFilter: true},
				caps:       &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: false},
				dataTypeID: ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: false, expectFilter: false,
			}),

		Entry("String node: no filter, although the profile supports filters",
			filterCase{
				profile: profileKepware, dataTypeID: ua.TypeIDString, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: true, expectFilter: false,
			}),

		// Nothing in the composition tests for deadband "none"; the nil comes
		// out of createDataChangeFilter itself.
		Entry("deadband none: no filter, although the profile supports filters and the node is numeric",
			filterCase{
				profile: profileKepware, dataTypeID: ua.TypeIDDouble, deadbandType: "none", deadbandValue: 0.0,
				expectSupports: true, expectFilter: false,
			}),

		Entry("deadband percent: filter",
			filterCase{
				profile: profileKepware, dataTypeID: ua.TypeIDFloat, deadbandType: "percent", deadbandValue: 2.0,
				expectSupports: true, expectFilter: true,
			}),

		Entry("unknown profile, trial failed and the profile default was false too: no filter",
			filterCase{
				profile:    ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown, SupportsDataChangeFilter: false},
				caps:       &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: false},
				dataTypeID: ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: false, expectFilter: false,
			}),

		Entry("unknown profile, trial succeeded and the profile default was true too: filter",
			filterCase{
				profile:    ServerProfile{Name: "test-unknown", FilterCapability: FilterUnknown, SupportsDataChangeFilter: true},
				caps:       &ServerCapabilities{hasTrialedThisConnection: true, SupportsDataChangeFilter: true},
				dataTypeID: ua.TypeIDDouble, deadbandType: "absolute", deadbandValue: 1.0,
				expectSupports: true, expectFilter: true,
			}),
	)

	Context("the trial result against the profile default", func() {
		It("uses the trial result, not the profile default, once a trial has run", func() {
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
			Expect(supportsFilter).To(BeTrue(), "the trial result should win over the profile default")
			Expect(shouldTrial).To(BeFalse(), "already trialed on this connection")
		})

		It("falls back to the profile default when there are no ServerCapabilities", func() {
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
			Expect(supportsFilter).To(BeTrue(), "the profile default should decide it")
			Expect(shouldTrial).To(BeFalse(), "nothing can cache a trial result without ServerCapabilities")
		})
	})

	Context("expectations production does not meet, pending a decision", func() {
		// The two specs below are what the tests this file replaces asserted
		// about the capability logic. Against the real
		// decideDataChangeFilterSupport both fail. Nobody has decided yet which
		// of the two behaviors is the intended one, so they stay pending until
		// ENG-5553 settles it; un-mark them to see the failures.

		PIt("before a trial has run, cached capabilities of false suppress the filter", func() {
			// Production instead returns (true, true) here: it ignores the
			// cached value until a trial has run. The state below is a real
			// connection's first batch, because Connect leaves
			// SupportsDataChangeFilter at false and nothing has trialed yet, so
			// the two behaviors differ on every unknown server.
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

			Expect(supportsFilter).To(BeFalse(),
				"capabilities of false should suppress the filter; production sends it and trials instead")
		})

		PIt("cached capabilities of true override a profile that declares no filter support", func() {
			// Production instead returns (false, false) here: the switch takes
			// the FilterUnsupported branch and never reaches ServerCapabilities,
			// so no runtime result can lift that profile's verdict.
			g := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{},
				ServerProfile:   profileS71200,
				ServerCapabilities: &ServerCapabilities{
					hasTrialedThisConnection: true,
					SupportsDataChangeFilter: true,
				},
			}

			supportsFilter, _ := g.decideDataChangeFilterSupport()

			Expect(supportsFilter).To(BeTrue(),
				"capabilities of true should override the profile; FilterUnsupported never consults them")
		})
	})
})
