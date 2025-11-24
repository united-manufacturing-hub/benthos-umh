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
	"os"
	"crypto/x509"
	"encoding/pem"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"
)

var _ = Describe("GenerateCertWithMode Certificate Generation", func() {
	BeforeEach(func() {
		if os.Getenv("INTEGRATION_TESTS_ONLY") == "true" {
			Skip("Skipping unit tests in integration-only mode")
		}
	})


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

		It("should include KeyCertSign bit for Eclipse Milo compatibility (self-signed certificates)", func() {
			certPEM, _, _, err := GenerateCertWithMode(
				365*24*time.Hour,
				"SignAndEncrypt",
				"Basic256Sha256",
			)
			Expect(err).ToNot(HaveOccurred())

			block, _ := pem.Decode(certPEM)
			cert, err := x509.ParseCertificate(block.Bytes)
			Expect(err).ToNot(HaveOccurred())

			Expect(cert.KeyUsage & x509.KeyUsageCertSign).To(Equal(x509.KeyUsageCertSign),
				"Self-signed OPC UA client certificates MUST have KeyCertSign bit (bit 5) for Eclipse Milo compatibility. "+
				"While OPC UA Part 6 only requires bits 0-3 for end-entity certificates, Eclipse Milo enforces KeyCertSign "+
				"for self-signed certificates as they must be able to sign themselves. This is validated in "+
				"CertificateValidationUtil.java lines 554-557. Required for Ignition Gateway and other Milo-based servers.")
		})
	})

	Describe("Extended Key Usage for OPC UA client certificates", func() {

		It("should only include ClientAuth Extended Key Usage", func() {
			certPEM, _, _, err := GenerateCertWithMode(
				365*24*time.Hour,
				"SignAndEncrypt",
				"Basic256Sha256",
			)
			Expect(err).ToNot(HaveOccurred())

			block, _ := pem.Decode(certPEM)
			cert, err := x509.ParseCertificate(block.Bytes)
			Expect(err).ToNot(HaveOccurred())

			Expect(cert.ExtKeyUsage).To(HaveLen(1),
				"Certificate should have exactly 1 Extended Key Usage")
			Expect(cert.ExtKeyUsage).To(ContainElement(x509.ExtKeyUsageClientAuth),
				"Certificate should have ClientAuth Extended Key Usage")
		})

		It("should NOT include ServerAuth Extended Key Usage", func() {
			certPEM, _, _, err := GenerateCertWithMode(
				365*24*time.Hour,
				"SignAndEncrypt",
				"Basic256Sha256",
			)
			Expect(err).ToNot(HaveOccurred())

			block, _ := pem.Decode(certPEM)
			cert, err := x509.ParseCertificate(block.Bytes)
			Expect(err).ToNot(HaveOccurred())

			Expect(cert.ExtKeyUsage).ToNot(ContainElement(x509.ExtKeyUsageServerAuth),
				"Client certificates should NOT have ServerAuth Extended Key Usage")
		})
	})

	Describe("Subject Alternative Name (SAN) field validation", func() {

		Context("when certificate is generated with default hostname", func() {
			var cert *x509.Certificate

			BeforeEach(func() {
				// Generate certificate with default configuration
				certPEM, _, _, err := GenerateCertWithMode(
					365*24*time.Hour,
					"SignAndEncrypt",
					"Basic256Sha256",
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(certPEM).ToNot(BeNil())

				// Parse certificate for inspection
				block, _ := pem.Decode(certPEM)
				Expect(block).ToNot(BeNil())
				parsedCert, err := x509.ParseCertificate(block.Bytes)
				Expect(err).ToNot(HaveOccurred())
				cert = parsedCert
			})

			It("should place URNs ONLY in URIs field, NOT in DNSNames", func() {
				// Verify URIs contains URN entries
				Expect(cert.URIs).ToNot(BeEmpty(),
					"Certificate should have URIs field populated")

				hasURN := false
				for _, uri := range cert.URIs {
					if uri.Scheme == "urn" {
						hasURN = true
						break
					}
				}
				Expect(hasURN).To(BeTrue(),
					"Certificate URIs should contain at least one URN with 'urn' scheme")

				// Verify DNSNames does NOT contain any URN strings
				for _, dnsName := range cert.DNSNames {
					Expect(dnsName).ToNot(HavePrefix("urn:"),
						"DNSNames should NOT contain any entries starting with 'urn:' (found: %s)", dnsName)
				}
			})

			It("should NOT place hostname in URIs field if it's not a URI", func() {
				// Get the hostname (default: "benthos-umh")
				// The hostname should be in DNSNames OR the certificate subject
				hostname := "benthos-umh"

				// If DNSNames contains the hostname, verify it's NOT also in URIs
				foundInDNS := false
				for _, dnsName := range cert.DNSNames {
					if dnsName == hostname {
						foundInDNS = true
						break
					}
				}

				if foundInDNS {
					// Verify hostname is NOT in URIs
					for _, uri := range cert.URIs {
						Expect(uri.String()).ToNot(Equal(hostname),
							"Hostname '%s' should not appear in URIs when it's in DNSNames", hostname)
					}
				}
			})

			It("should place IP addresses ONLY in IPAddresses field", func() {
				// Note: Default configuration may not include IP addresses
				// This test validates that IF IPs are present, they're in the correct field

				// If IPAddresses is populated, verify no IPs in DNSNames or URIs
				for _, ip := range cert.IPAddresses {
					ipString := ip.String()

					// Verify IP is NOT in DNSNames
					Expect(cert.DNSNames).ToNot(ContainElement(ipString),
						"IP address %s should NOT be in DNSNames field", ipString)

					// Verify IP is NOT in URIs
					for _, uri := range cert.URIs {
						Expect(uri.String()).ToNot(Equal(ipString),
							"IP address %s should NOT be in URIs field", ipString)
						Expect(uri.Host).ToNot(Equal(ipString),
							"IP address %s should NOT be in URI Host field", ipString)
					}
				}
			})

			It("should separate DNS hostnames, URNs, and IPs into correct SAN fields", func() {
				// This is the comprehensive validation test

				// Rule 1: All URNs must be in URIs, never in DNSNames
				for _, uri := range cert.URIs {
					if uri.Scheme == "urn" {
						// This URN should NOT appear in DNSNames
						urnString := uri.String()
						Expect(cert.DNSNames).ToNot(ContainElement(urnString),
							"URN %s found in both URIs and DNSNames", urnString)
					}
				}

				// Rule 2: All DNSNames entries must be valid hostnames (not URNs, not IPs)
				for _, dnsName := range cert.DNSNames {
					Expect(dnsName).ToNot(HavePrefix("urn:"),
						"DNSNames contains URN: %s (should be in URIs only)", dnsName)

					// Check it's not an IP address string
					ip := net.ParseIP(dnsName)
					Expect(ip).To(BeNil(),
						"DNSNames contains IP address: %s (should be in IPAddresses only)", dnsName)
				}

				// Rule 3: All IPAddresses entries must NOT appear in other fields
				for _, ip := range cert.IPAddresses {
					ipString := ip.String()
					Expect(cert.DNSNames).ToNot(ContainElement(ipString),
						"IP %s found in both IPAddresses and DNSNames", ipString)

					for _, uri := range cert.URIs {
						Expect(uri.String()).ToNot(Equal(ipString),
							"IP %s found in both IPAddresses and URIs", ipString)
					}
				}
			})
		})

		Context("when validating SAN field types", func() {
			var cert *x509.Certificate

			BeforeEach(func() {
				certPEM, _, _, err := GenerateCertWithMode(
					365*24*time.Hour,
					"SignAndEncrypt",
					"Basic256Sha256",
				)
				Expect(err).ToNot(HaveOccurred())

				block, _ := pem.Decode(certPEM)
				cert, err = x509.ParseCertificate(block.Bytes)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have URIs field containing only valid URIs with schemes", func() {
				// Every entry in URIs must be a valid URI with a scheme
				for _, uri := range cert.URIs {
					Expect(uri.Scheme).ToNot(BeEmpty(),
						"URI %s is missing a scheme", uri.String())
					Expect([]string{"urn", "http", "https"}).To(ContainElement(uri.Scheme),
						"URI %s has unexpected scheme %s (expected urn, http, or https)", uri.String(), uri.Scheme)
				}
			})

			It("should have DNSNames field containing only valid hostnames", func() {
				// Every entry in DNSNames must be a valid hostname
				// (not a URN, not an IP, not a URI)
				for _, dnsName := range cert.DNSNames {
					// Must not be a URN
					Expect(dnsName).ToNot(HavePrefix("urn:"),
						"DNSNames contains URN: %s", dnsName)

					// Must not be an IP
					ip := net.ParseIP(dnsName)
					Expect(ip).To(BeNil(),
						"DNSNames contains IP: %s", dnsName)

					// Must not contain scheme separator (not a URI)
					Expect(dnsName).ToNot(ContainSubstring("://"),
						"DNSNames contains URI scheme separator: %s", dnsName)
				}
			})
		})
	})
})
