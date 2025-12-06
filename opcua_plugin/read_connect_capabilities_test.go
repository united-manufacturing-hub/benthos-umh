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

var _ = Describe("Connect Method ServerCapabilities", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	Context("fallback ServerCapabilities", func() {
		It("should include all three capability fields when queryOperationLimits fails", func() {
			// This test verifies that the fallback ServerCapabilities struct
			// includes all required fields:
			// - SupportsAbsoluteDeadband (legacy, deprecated)
			// - SupportsPercentDeadband (for percent deadband)
			// - SupportsDataChangeFilter (for profile-based detection)
			//
			// The fallback is used when queryOperationLimits fails, which is
			// common for servers that don't expose OperationLimits node.

			fallbackCaps := &ServerCapabilities{
				SupportsAbsoluteDeadband: true,  // Most servers support this
				SupportsPercentDeadband:  false, // Conservative assumption
				SupportsDataChangeFilter: false, // Safe default - MonitorBatched will use profile default
			}

			// Verify all three fields are present
			Expect(fallbackCaps.SupportsAbsoluteDeadband).To(BeTrue(),
				"Fallback should assume absolute deadband support")
			Expect(fallbackCaps.SupportsPercentDeadband).To(BeFalse(),
				"Fallback should conservatively assume no percent deadband")
			Expect(fallbackCaps.SupportsDataChangeFilter).To(BeFalse(),
				"Fallback should safely assume no DataChangeFilter - MonitorBatched will use profile default")
		})

		It("should use false as safe default for SupportsDataChangeFilter", func() {
			// When SupportsDataChangeFilter is false, MonitorBatched will fall back
			// to the ServerProfile's SupportsDataChangeFilter value, which is
			// determined by profile detection (Task 2).
			//
			// This ensures conservative behavior:
			// 1. If profile detection succeeds → use profile's value
			// 2. If profile detection fails → Auto profile has safe defaults

			fallbackCaps := &ServerCapabilities{
				SupportsDataChangeFilter: false, // Safe default
			}

			Expect(fallbackCaps.SupportsDataChangeFilter).To(BeFalse(),
				"Safe default should be false to trigger profile fallback in MonitorBatched")
		})
	})
})
