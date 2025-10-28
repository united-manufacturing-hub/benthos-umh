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
	"crypto/x509"
	"encoding/pem"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"
)

var _ = Describe("GenerateCertWithMode Certificate Generation", func() {

	Describe("Key Usage bits for OPC UA client certificates", func() {

		DescribeTable("should include all 4 required Key Usage bits regardless of security mode",
			func(securityMode string, securityPolicy string) {
				// Generate certificate
				certPEM, keyPEM, _, err := GenerateCertWithMode(
					365*24*time.Hour,
					securityMode,
					securityPolicy,
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(certPEM).ToNot(BeNil())
				Expect(keyPEM).ToNot(BeNil())

				// Parse certificate
				block, _ := pem.Decode(certPEM)
				Expect(block).ToNot(BeNil())
				cert, err := x509.ParseCertificate(block.Bytes)
				Expect(err).ToNot(HaveOccurred())

				// Verify OPC UA Part 6 required Key Usage bits
				requiredKeyUsage := x509.KeyUsageDigitalSignature |
					x509.KeyUsageContentCommitment |
					x509.KeyUsageKeyEncipherment |
					x509.KeyUsageDataEncipherment

				Expect(cert.KeyUsage & requiredKeyUsage).To(Equal(requiredKeyUsage),
					"Certificate should have all 4 required Key Usage bits set")
			},
			Entry("None security mode with Basic128Rsa15", "None", "Basic128Rsa15"),
			Entry("Sign security mode with Basic128Rsa15", "Sign", "Basic128Rsa15"),
			Entry("SignAndEncrypt with Basic128Rsa15", "SignAndEncrypt", "Basic128Rsa15"),
			Entry("None with Basic256", "None", "Basic256"),
			Entry("Sign with Basic256", "Sign", "Basic256"),
			Entry("SignAndEncrypt with Basic256", "SignAndEncrypt", "Basic256"),
			Entry("None with Basic256Sha256", "None", "Basic256Sha256"),
			Entry("Sign with Basic256Sha256", "Sign", "Basic256Sha256"),
			Entry("SignAndEncrypt with Basic256Sha256", "SignAndEncrypt", "Basic256Sha256"),
		)

		It("should NOT include CertSign bit for client certificates", func() {
			certPEM, _, _, err := GenerateCertWithMode(
				365*24*time.Hour,
				"SignAndEncrypt",
				"Basic256Sha256",
			)
			Expect(err).ToNot(HaveOccurred())

			block, _ := pem.Decode(certPEM)
			cert, err := x509.ParseCertificate(block.Bytes)
			Expect(err).ToNot(HaveOccurred())

			Expect(cert.KeyUsage & x509.KeyUsageCertSign).To(Equal(x509.KeyUsage(0)),
				"Client certificates should NOT have CertSign bit set")
		})
	})
})
