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

// Whether a monitored node gets a DataChangeFilter at all, as
// createDataChangeFilter (deadband.go) and isNumericDataType (read_discover.go)
// decide it. What MonitorBatched actually sends is asserted in
// subscription_test.go ("requests a DataChangeFilter ...").

// The contents of the filter are asserted in deadband_test.go
// ("DataChangeFilter Creation"); this table only asks whether there is one.
var _ = Describe("createDataChangeFilter: filter or no filter", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	DescribeTable("returns a filter only for a configured deadband",
		func(deadbandType string, deadbandValue float64, expectFilter bool) {
			filter := createDataChangeFilter(deadbandType, deadbandValue)

			if expectFilter {
				Expect(filter).NotTo(BeNil(),
					"expected a filter for deadband %q", deadbandType)
			} else {
				Expect(filter).To(BeNil(),
					"expected no filter for deadband %q", deadbandType)
			}
		},
		Entry("deadband none: no filter",
			"none", 0.0, false),
		Entry("deadband absolute: filter",
			"absolute", 0.5, true),
		Entry("deadband percent: filter",
			"percent", 2.0, true),
	)
})

var _ = Describe("isNumericDataType", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	DescribeTable("reports which data types can carry a deadband",
		func(nodeDataType ua.TypeID, isNumeric bool) {
			Expect(isNumericDataType(nodeDataType)).To(Equal(isNumeric),
				"isNumericDataType(%v) should return %v", nodeDataType, isNumeric)
		},
		Entry("ByteString: not numeric, no filter",
			ua.TypeIDByteString, false),
		Entry("String: not numeric, no filter",
			ua.TypeIDString, false),
		Entry("DateTime: not numeric, no filter",
			ua.TypeIDDateTime, false),
		Entry("Double: numeric, filter allowed",
			ua.TypeIDDouble, true),
		Entry("Float: numeric, filter allowed",
			ua.TypeIDFloat, true),
		Entry("Int32: numeric, filter allowed",
			ua.TypeIDInt32, true),
		Entry("UInt32: numeric, filter allowed",
			ua.TypeIDUint32, true),
	)
})
