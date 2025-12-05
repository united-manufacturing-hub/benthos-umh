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

// Note: Individual field optionality tests were removed per Anti-Pattern 6
// (Excessive Test Granularity). The single "minimal config" test below
// proves all optional fields work correctly by successfully parsing without them.

var _ = Describe("OPC UA Connection Config", func() {
	Describe("ConfigSpec fields optionality", func() {
		It("should parse minimal config with only endpoint and apply defaults", func() {
			// This single test proves all 12+ fields are truly optional
			// If any field lacked proper .Optional() + .Default(), this test would fail
			env := service.NewEnvironment()
			minimalYAML := `
endpoint: "opc.tcp://localhost:4840"
`
			parsedConfig, err := OPCUAConnectionConfigSpec.ParseYAML(minimalYAML, env)

			Expect(err).NotTo(HaveOccurred(), "minimal config with only endpoint should parse successfully")
			Expect(parsedConfig).NotTo(BeNil(), "parsed config should not be nil")

			// Verify defaults are applied (proves optionality works)
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
