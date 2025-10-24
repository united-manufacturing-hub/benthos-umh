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

var _ = Describe("Ethernet/IP Advanced Fields Config", func() {
	Describe("ConfigSpec advanced fields optionality", func() {
		Context("when parsing minimal config", func() {
			It("should parse minimal config with only endpoint", func() {
				// Test that 3 advanced fields are truly optional:
				// connectionTimeoutMs, requestTimeoutMs, connectionPath
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

				// Verify defaults are applied for advanced fields
				connectionTimeoutMs, err := parsedConfig.FieldInt("connectionTimeoutMs")
				Expect(err).NotTo(HaveOccurred(), "connectionTimeoutMs should have a default value")
				Expect(connectionTimeoutMs).To(Equal(10000), "connectionTimeoutMs should default to 10000ms")

				requestTimeoutMs, err := parsedConfig.FieldInt("requestTimeoutMs")
				Expect(err).NotTo(HaveOccurred(), "requestTimeoutMs should have a default value")
				Expect(requestTimeoutMs).To(Equal(10000), "requestTimeoutMs should default to 10000ms")

				connectionPath, err := parsedConfig.FieldString("connectionPath")
				Expect(err).NotTo(HaveOccurred(), "connectionPath should have a default value")
				Expect(connectionPath).To(Equal("1,0"), "connectionPath should default to '1,0'")
			})

			It("should allow overriding advanced fields", func() {
				// Verify advanced fields can be explicitly set
				env := service.NewEnvironment()
				configWithAdvanced := `
endpoint: "192.168.1.100"
connectionTimeoutMs: 5000
requestTimeoutMs: 3000
connectionPath: "2,1"
tags:
  - name: "TestTag"
    type: "bool"
`
				parsedConfig, err := EthernetIPConfigSpec.ParseYAML(configWithAdvanced, env)

				Expect(err).NotTo(HaveOccurred())
				Expect(parsedConfig).NotTo(BeNil())

				connectionTimeoutMs, err := parsedConfig.FieldInt("connectionTimeoutMs")
				Expect(err).NotTo(HaveOccurred())
				Expect(connectionTimeoutMs).To(Equal(5000))

				requestTimeoutMs, err := parsedConfig.FieldInt("requestTimeoutMs")
				Expect(err).NotTo(HaveOccurred())
				Expect(requestTimeoutMs).To(Equal(3000))

				connectionPath, err := parsedConfig.FieldString("connectionPath")
				Expect(err).NotTo(HaveOccurred())
				Expect(connectionPath).To(Equal("2,1"))
			})
		})
	})
})
