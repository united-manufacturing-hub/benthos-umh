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

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

// TestCreateDataChangeFilter verifies that data change filters are created
// correctly based on deadband configuration, including the critical edge case
// of deadbandValue=0.0 for duplicate suppression
var _ = Describe("DataChangeFilter Creation", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	DescribeTable("creates correct filter based on deadband configuration",
		func(deadbandType string, deadbandValue float64, expectNil bool, expectType uint32, expectValue float64) {
			result := createDataChangeFilter(deadbandType, deadbandValue)

			if expectNil {
				Expect(result).To(BeNil(), "filter should be nil when deadband is disabled")
				return
			}

			// Verify ExtensionObject structure
			Expect(result).NotTo(BeNil(), "filter should not be nil when deadband is enabled")

			// Verify TypeID
			expectedNodeID := ua.NewNumericNodeID(0, id.DataChangeFilter_Encoding_DefaultBinary)
			Expect(result.TypeID).NotTo(BeNil(), "TypeID should be set")
			Expect(result.TypeID.NodeID).NotTo(BeNil(), "TypeID.NodeID should be set")

			// Compare NodeID fields directly
			Expect(result.TypeID.NodeID.Namespace()).To(Equal(expectedNodeID.Namespace()),
				"TypeID.NodeID namespace should match")
			Expect(result.TypeID.NodeID.IntID()).To(Equal(expectedNodeID.IntID()),
				"TypeID.NodeID IntID should match")

			// Verify EncodingMask
			Expect(result.EncodingMask).To(BeEquivalentTo(ua.ExtensionObjectBinary),
				"EncodingMask should be ExtensionObjectBinary")

			// Unwrap and verify DataChangeFilter fields
			filter, ok := result.Value.(*ua.DataChangeFilter)
			Expect(ok).To(BeTrue(), "Value should be *ua.DataChangeFilter")

			// Verify Trigger
			Expect(filter.Trigger).To(Equal(ua.DataChangeTriggerStatusValue),
				"Trigger should be DataChangeTriggerStatusValue")

			// Verify DeadbandType
			Expect(filter.DeadbandType).To(Equal(expectType),
				"DeadbandType should match expected type")

			// Verify DeadbandValue
			Expect(filter.DeadbandValue).To(Equal(expectValue),
				"DeadbandValue should match expected value")
		},
		Entry("disabled deadband - type none",
			"none", 0.5, true, uint32(0), 0.0),
		Entry("duplicate suppression - value zero",
			"absolute", 0.0, false, uint32(ua.DeadbandTypeAbsolute), 0.0),
		Entry("absolute deadband",
			"absolute", 0.5, false, uint32(ua.DeadbandTypeAbsolute), 0.5),
		Entry("percent deadband",
			"percent", 2.0, false, uint32(ua.DeadbandTypePercent), 2.0),
	)
})
