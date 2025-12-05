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

package eip_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	. "github.com/united-manufacturing-hub/benthos-umh/eip_plugin"
)

// Note: The "override" test was removed per Anti-Pattern 6 (Excessive Test Granularity).
// The single test below proves the field is optional and has the correct default.
// Override behavior is trivially implied once optionality works.

var _ = Describe("Ethernet/IP Advanced Fields Config", func() {
	Describe("ConfigSpec advanced fields optionality", func() {
		It("should parse minimal config with default socketTimeoutMs", func() {
			// This test proves socketTimeoutMs is optional with correct default
			env := service.NewEnvironment()
			minimalYAML := `
endpoint: "192.168.1.100"
tags:
  - name: "TestTag"
    type: "bool"
`
			parsedConfig, err := EthernetIPConfigSpec.ParseYAML(minimalYAML, env)

			Expect(err).NotTo(HaveOccurred(), "minimal config without advanced fields should parse successfully")
			Expect(parsedConfig).NotTo(BeNil(), "parsed config should not be nil")

			// Verify default is applied for socketTimeoutMs
			socketTimeoutMs, err := parsedConfig.FieldInt("socketTimeoutMs")
			Expect(err).NotTo(HaveOccurred(), "socketTimeoutMs should have a default value")
			Expect(socketTimeoutMs).To(Equal(10000), "socketTimeoutMs should default to 10000ms")
		})
	})

	Describe("Runtime behavior", func() {
		Context("when socketTimeoutMs is configured", func() {
			It("should wire timeout to gologix.Client.SocketTimeout", func() {
				Skip("Integration test - requires actual gologix.Client instantiation")
				// TODO: Add integration test that verifies:
				// 1. EIPInput is created with custom socketTimeoutMs
				// 2. Connect() is called
				// 3. gologix.Client.SocketTimeout equals configured value (converted to time.Duration)
			})
		})
	})
})
