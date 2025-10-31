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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Server Capability Detection verifies graceful fallback when
// server doesn't support requested deadband type
var _ = Describe("Server Capability Detection", func() {
	DescribeTable("verifies graceful fallback",
		func(requestedType string, serverSupportsPercent bool, expectedType string) {
			// Mock server capabilities
			// Most servers support absolute deadband (basic OPC UA feature)
			caps := &ServerCapabilities{
				SupportsPercentDeadband:  serverSupportsPercent,
				SupportsAbsoluteDeadband: true, // Absolute is widely supported
			}

			// Adjust deadband type based on capabilities
			resultType := adjustDeadbandType(requestedType, caps)

			Expect(resultType).To(Equal(expectedType))
		},
		Entry("percent requested, server supports it", "percent", true, "percent"),
		Entry("percent requested, server doesn't support - fallback to absolute", "percent", false, "absolute"),
		Entry("absolute requested - always works", "absolute", false, "absolute"),
		Entry("none requested - no capability check needed", "none", false, "none"),
	)
})
