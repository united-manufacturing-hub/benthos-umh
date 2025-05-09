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

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Generate a self-signed X.509 certificate for a TLS server. Outputs to
// 'cert.pem' and 'key.pem' and will overwrite existing files.

// Based on src/crypto/tls/generate_cert.go from the Go SDK
// Modified by the Gopcua Authors for use in creating an OPC-UA compliant client certificate

package opcua_plugin

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

// GenerateCertWithMode generates a self-signed X.509 certificate for OPC UA
// usage, taking into account the security mode (Sign vs SignAndEncrypt) and
// the desired security policy (Basic128Rsa15, Basic256, Basic256Sha256, etc.).
//
//   - validFor:				Certificate validity duration
//   - securityMode:		The OPC UA message security mode (None, Sign, SignAndEncrypt)
//   - securityPolicy:  The OPC UA security policy (e.g., "Basic128Rsa15", "Basic256", "Basic256Sha256")
func GenerateCertWithMode(
	validFor time.Duration,
	securityMode string,
	securityPolicy string,
) (certPEM, keyPEM []byte, clientName string, err error) {
	var (
		rsaBits            int
		signatureAlgorithm x509.SignatureAlgorithm
	)

	clientUID := randomString(8)

	host := "urn:benthos-umh:client-predefined-" + clientUID

	switch securityPolicy {
	case "Basic256Rsa256":
		// typically a 2048-bit RSA key
		rsaBits = 2048
		signatureAlgorithm = x509.SHA256WithRSA
	case "Basic256":
		// typically a 2048-bit RSA key
		rsaBits = 2048
		signatureAlgorithm = x509.SHA1WithRSA
	case "Basic128Rsa15":
		// typically a 1024-bit RSA key, sometimes 2048-bit keys also work
		rsaBits = 1024
		signatureAlgorithm = x509.SHA1WithRSA
	default:
		// fallback, we could also err out here if we don't want to allow
		// something else
		rsaBits = 2048
		signatureAlgorithm = x509.SHA256WithRSA
	}

	priv, err := rsa.GenerateKey(rand.Reader, rsaBits)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to generate private key: %w", err)
	}

	// Set a fixed NotBefore so the certificate always starts at the beginning
	// of the year. Reasons for this could be incorrect setup of the plc's
	// cpu-clock, timezone, summertime...
	now := time.Now().UTC()
	notBefore := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	notAfter := notBefore.Add(validFor)

	// Use 127 bits instead of 128 to ensure the serial number is always positive.
	// In ASN.1 DER encoding (used by X.509), integers are signed. If the most significant bit (MSB)
	// is set (i.e., 1), the integer is interpreted as negative. By limiting the serial number
	// to 127 bits, we guarantee that the MSB is 0, ensuring the serial number remains positive
	// and complies with RFC 5280 requirements, thereby preventing parsing errors like
	// "x509: negative serial number".
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 127)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed togenerate serial number: %s", err)
	}

	// Prepare the certificate template
	// CommonName has to be different if you try to connect to one server with
	// different securityPolicies (edge-case)
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   "benthos-umh-predefined-" + clientUID,
			Organization: []string{"UMH"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		BasicConstraintsValid: true,
		SignatureAlgorithm:    signatureAlgorithm,
		IsCA:                  false,

		// ExtKeyUsage: Both server & client auth for OPC UA usage
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
	}

	clientName = template.Subject.CommonName

	// Fill in IPAddresses, DNSNames, URIs from the ApplicationURI string (comma-separated)
	hosts := strings.Split(host, ",")
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
		if uri, parseErr := url.Parse(h); parseErr == nil && (uri.Scheme == "urn" || uri.Scheme == "http" || uri.Scheme == "https") {
			template.URIs = append(template.URIs, uri)
		}
	}

	// Decide on the key usage bits based on security mode
	switch securityMode {
	case "Sign":
		// For Sign-only, we need DigitalSignature and ContentCommitment("NonRepudiation")
		// meaning the certificate can be used to sign data or communications that
		// the signer later cannot deny having signed it.
		template.KeyUsage = x509.KeyUsageDigitalSignature |
			x509.KeyUsageContentCommitment
	case "SignAndEncrypt":
		// For Sign and Encrypt, we need KeyEncipherment, DigitalSignature,
		// DataEncipherement, ContentCommitment and CertSign
		template.KeyUsage = x509.KeyUsageKeyEncipherment |
			x509.KeyUsageDigitalSignature |
			x509.KeyUsageDataEncipherment |
			x509.KeyUsageContentCommitment |
			x509.KeyUsageCertSign
	default:
		template.KeyUsage = x509.KeyUsageDigitalSignature
	}

	// Actually create the certificate
	derBytes, err := x509.CreateCertificate(
		rand.Reader,
		&template,
		&template, // self-signed
		publicKey(priv),
		priv,
	)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to create certificate: %w", err)
	}

	// PEM-encode the results
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM = pem.EncodeToMemory(pemBlockForKey(priv))

	return certPEM, keyPEM, clientName, nil
}

func publicKey(priv interface{}) interface{} {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &k.PublicKey
	case *ecdsa.PrivateKey:
		return &k.PublicKey
	default:
		return nil
	}
}

func pemBlockForKey(priv interface{}) *pem.Block {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}
	case *ecdsa.PrivateKey:
		b, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to marshal ECDSA private key: %v", err)
			os.Exit(2)
		}
		return &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}
	default:
		return nil
	}
}
