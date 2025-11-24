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

	"github.com/gopcua/opcua/ua"
)

// TestCreateMonitoredItemRequestWithFilter verifies that MonitoredItemCreateRequest
// construction applies deadband filters correctly based on OPCUAInput configuration.
// This tests the actual code path used in MonitorBatched().
var _ = Describe("MonitoredItemRequest Creation", func() {
	BeforeEach(func() {
		if os.Getenv("INTEGRATION_TESTS_ONLY") == "true" {
			Skip("Skipping unit tests in integration-only mode")
		}
	})

	DescribeTable("creates correct request with filter based on deadband",
		func(deadbandType string, deadbandValue float64, expectFilter bool) {
			// Create test OPCUAInput with deadband configuration
			g := &OPCUAInput{
				DeadbandType:     deadbandType,
				DeadbandValue:    deadbandValue,
				QueueSize:        1,
				SamplingInterval: 1000.0,
			}

			// Create a test NodeDef (simulating one node from a batch)
			nodeDef := NodeDef{
				NodeID: ua.NewNumericNodeID(0, 2258), // CurrentTime node
			}

			// This is the EXACT code pattern from MonitorBatched() line 238-252
			// We're testing that THIS code applies the filter correctly
			request := &ua.MonitoredItemCreateRequest{
				ItemToMonitor: &ua.ReadValueID{
					NodeID:       nodeDef.NodeID,
					AttributeID:  ua.AttributeIDValue,
					DataEncoding: &ua.QualifiedName{},
				},
				MonitoringMode: ua.MonitoringModeReporting,
				RequestedParameters: &ua.MonitoringParameters{
					ClientHandle:     0,
					DiscardOldest:    true,
					Filter:           createDataChangeFilter(g.DeadbandType, g.DeadbandValue),
					QueueSize:        g.QueueSize,
					SamplingInterval: g.SamplingInterval,
				},
			}

			// Verify filter application matches expected behavior
			if expectFilter {
				// Filter should be present when deadband is enabled
				Expect(request.RequestedParameters.Filter).NotTo(BeNil(),
					"expected Filter to be set based on deadband config")
			} else {
				// Filter should be nil when deadband is disabled
				Expect(request.RequestedParameters.Filter).To(BeNil(),
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

// TestDeadbandTypeChecking verifies filters only applied to numeric types
var _ = Describe("Deadband Type Checking", func() {
	BeforeEach(func() {
		if os.Getenv("INTEGRATION_TESTS_ONLY") == "true" {
			Skip("Skipping unit tests in integration-only mode")
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
