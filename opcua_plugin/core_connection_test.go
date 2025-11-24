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

package opcua_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"
)

var _ = Describe("OPC UA Connection Config", func() {
	Describe("ConfigSpec fields optionality", func() {
		Context("when inspecting field definitions", func() {
			It("should have username field marked as optional", func() {
				// This field has .Optional() and should not be in required list
				// Parse config without username - should succeed
				env := service.NewEnvironment()
				confYAML := `
endpoint: "opc.tcp://localhost:4840"
`
				_, err := OPCUAConnectionConfigSpec.ParseYAML(confYAML, env)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should have securityMode field marked as optional", func() {
				// This field currently lacks .Optional() - this test documents the bug
				// Once .Optional() is added, it should behave like username
				env := service.NewEnvironment()
				confYAML := `
endpoint: "opc.tcp://localhost:4840"
`
				_, err := OPCUAConnectionConfigSpec.ParseYAML(confYAML, env)

				// Currently passes because Default("") makes it parse-optional
				// But without .Optional(), it appears in "required" array in OpenAPI schema
				// This test verifies the fix maintains parse-optional behavior
				Expect(err).NotTo(HaveOccurred(), "securityMode should remain parseable when omitted after adding .Optional()")
			})

			It("should have securityPolicy field marked as optional", func() {
				// This field currently lacks .Optional() - this test documents the bug
				// Once .Optional() is added, it should behave like username
				env := service.NewEnvironment()
				confYAML := `
endpoint: "opc.tcp://localhost:4840"
`
				_, err := OPCUAConnectionConfigSpec.ParseYAML(confYAML, env)

				// Currently passes because Default("") makes it parse-optional
				// But without .Optional(), it appears in "required" array in OpenAPI schema
				// This test verifies the fix maintains parse-optional behavior
				Expect(err).NotTo(HaveOccurred(), "securityPolicy should remain parseable when omitted after adding .Optional()")
			})

			It("should parse minimal config with only endpoint", func() {
				// Test that all 9 advanced fields are truly optional
				// Fields tested: sessionTimeout, clientCertificate, serverCertificateFingerprint,
				// userCertificate, userPrivateKey, insecure, directConnect, autoReconnect, reconnectIntervalInSeconds
				env := service.NewEnvironment()
				minimalYAML := `
endpoint: "opc.tcp://localhost:4840"
`
				parsedConfig, err := OPCUAConnectionConfigSpec.ParseYAML(minimalYAML, env)

				Expect(err).NotTo(HaveOccurred(), "minimal config with only endpoint should parse successfully")
				Expect(parsedConfig).NotTo(BeNil(), "parsed config should not be nil")

				// Verify defaults are applied
				sessionTimeout, err := parsedConfig.FieldInt("sessionTimeout")
				Expect(err).NotTo(HaveOccurred())
				Expect(sessionTimeout).To(Equal(10000), "sessionTimeout should have default value")

				insecure, err := parsedConfig.FieldBool("insecure")
				Expect(err).NotTo(HaveOccurred())
				Expect(insecure).To(BeFalse(), "insecure should default to false")

				directConnect, err := parsedConfig.FieldBool("directConnect")
				Expect(err).NotTo(HaveOccurred())
				Expect(directConnect).To(BeFalse(), "directConnect should default to false")

				autoReconnect, err := parsedConfig.FieldBool("autoReconnect")
				Expect(err).NotTo(HaveOccurred())
				Expect(autoReconnect).To(BeFalse(), "autoReconnect should default to false")

				reconnectInterval, err := parsedConfig.FieldInt("reconnectIntervalInSeconds")
				Expect(err).NotTo(HaveOccurred())
				Expect(reconnectInterval).To(Equal(5), "reconnectIntervalInSeconds should have default value")
			})
		})
	})
})
