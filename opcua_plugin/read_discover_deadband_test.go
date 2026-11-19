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

// This file previously re-built the MonitoredItemCreateRequest by hand ("EXACT
// code pattern from MonitorBatched() line 238-252") and asserted on the struct
// it had just written. MonitorBatched has no unit-test seam (g.Subscription is
// the concrete gopcua *opcua.Subscription doing a real network round-trip), but
// the only production behavior these specs were about is what
// createDataChangeFilter returns - so they now call that real function
// directly. Field-level assertions on the filter contents already live in
// deadband_test.go (createDataChangeFilter is fully covered there; the overlap
// was left in place deliberately).

var _ = Describe("MonitoredItemRequest Creation", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	DescribeTable("creates correct request with filter based on deadband",
		func(deadbandType string, deadbandValue float64, expectFilter bool) {
			// MonitorBatched passes g.DeadbandType/g.DeadbandValue straight
			// into createDataChangeFilter (read_discover.go); this is the real
			// production call, not a re-implementation.
			filter := createDataChangeFilter(deadbandType, deadbandValue)

			if expectFilter {
				// Filter should be present when deadband is enabled
				Expect(filter).NotTo(BeNil(),
					"expected Filter to be set based on deadband config")
			} else {
				// Filter should be nil when deadband is disabled
				Expect(filter).To(BeNil(),
					"expected Filter to be nil when deadband disabled")
			}
		},
		Entry("disabled deadband - filter should be nil",
			"none", 0.0, false),
		Entry("absolute deadband - filter should be present",
			"absolute", 0.5, true),
		Entry("percent deadband - filter should be present",
			"percent", 2.0, true),
	)
})

// TestDeadbandTypeChecking verifies filters only applied to numeric types.
// This table calls the real isNumericDataType and stays as-is.
var _ = Describe("Deadband Type Checking", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	DescribeTable("validates numeric data types correctly",
		func(nodeDataType ua.TypeID, shouldFilter bool) {
			result := isNumericDataType(nodeDataType)
			Expect(result).To(Equal(shouldFilter),
				"isNumericDataType(%v) should return %v", nodeDataType, shouldFilter)
		},
		Entry("ByteString node - no filter",
			ua.TypeIDByteString, false),
		Entry("String node - no filter",
			ua.TypeIDString, false),
		Entry("DateTime node - no filter",
			ua.TypeIDDateTime, false),
		Entry("Double node - apply filter",
			ua.TypeIDDouble, true),
		Entry("Float node - apply filter",
			ua.TypeIDFloat, true),
		Entry("Int32 node - apply filter",
			ua.TypeIDInt32, true),
		Entry("UInt32 node - apply filter",
			ua.TypeIDUint32, true),
	)
})
