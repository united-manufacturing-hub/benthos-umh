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
	"hash/fnv"
	"io"
	"math/big"
	mrand "math/rand"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

type seededReader struct {
	src *mrand.Rand
}

func newSeededReader(seed int64) io.Reader {
	return &seededReader{src: mrand.New(mrand.NewSource(seed))}
}

func (r *seededReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r.src.Intn(256))
	}
	return len(p), nil
}

// GenerateCertWithMode generates a self-signed X.509 certificate for OPC UA
// usage, taking into account the security mode (Sign vs SignAndEncrypt) and
// the desired security policy (Basic128Rsa15, Basic256, Basic256Sha256, etc.).
//
//   - host:         CommonName & DNS/IP/URI entries to include
//   - validFor:     Certificate validity duration
//   - securityMode: The OPC UA message security mode (None, Sign, SignAndEncrypt)
//   - policy:       The OPC UA security policy (e.g., "Basic128Rsa15", "Basic256", "Basic256Sha256")
func GenerateCertWithMode(
	host string,
	validFor time.Duration,
	securityMode string,
	securityPolicy string,
	seedString string,
	clientNameUID string,
) (certPEM, keyPEM []byte, err error) {
	var rsaBits int

	if len(host) == 0 {
		return nil, nil, fmt.Errorf("missing required host parameter")
	}
	var signatureAlgorithm x509.SignatureAlgorithm

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

	h := fnv.New64a()
	h.Write([]byte(seedString))
	seed := int64(h.Sum64())

	// Create a custom io.Reader to ensure we don't use random Numbers to create
	// the private key, but instead use the 'certificateSeed'.
	seededReader := newSeededReader(seed)

	// Generate RSA private key
	priv, err := rsa.GenerateKey(seededReader, rsaBits)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(validFor)

	// Use 127 bits for the serial to ensure positivity in ASN.1
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 127)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate serial number: %w", err)
	}

	// Prepare the certificate template
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			// Modify as appropriate for your org
			CommonName:   "benthos-umh-predefined-" + clientNameUID,
			Organization: []string{"UMH"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		BasicConstraintsValid: true,
		SignatureAlgorithm:    signatureAlgorithm,

		// ExtKeyUsage: Both server & client auth for OPC UA usage
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
	}

	// Fill in IPAddresses, DNSNames, URIs from the host string (comma-separated)
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
		// For Sign-only, we need DigitalSignature
		template.KeyUsage = x509.KeyUsageDigitalSignature
	case "SignAndEncrypt":
		// For Sign and Encrypt, we need KeyEncipherment + DigitalSignature
		template.KeyUsage = x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageDataEncipherment
	default:
		// e.g. fallback for SecurityMode 'None'
		template.KeyUsage = x509.KeyUsageDigitalSignature
	}

	// Actually create the certificate
	derBytes, err := x509.CreateCertificate(
		//NOTE: We could also seed the certificate itself, but at least 1 component
		//			should stay random, either the certificate or the priv.key
		rand.Reader,
		&template,
		&template, // self-signed
		publicKey(priv),
		priv,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create certificate: %w", err)
	}

	// PEM-encode the results
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM = pem.EncodeToMemory(pemBlockForKey(priv))

	return certPEM, keyPEM, nil
}

func GenerateCert(host string, rsaBits int, validFor time.Duration) (certPEM, keyPEM []byte, err error) {
	if len(host) == 0 {
		return nil, nil, fmt.Errorf("missing required host parameter")
	}
	if rsaBits == 0 {
		rsaBits = 2048
	}

	priv, err := rsa.GenerateKey(rand.Reader, rsaBits)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate private key: %s", err)
	}

	notBefore := time.Now()
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
		return nil, nil, fmt.Errorf("failed to generate serial number: %s", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   "benthos-umh",
			Organization: []string{"UMH"},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              x509.KeyUsageContentCommitment | x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageDataEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	hosts := strings.Split(host, ",")
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
		if uri, err := url.Parse(h); err == nil {
			template.URIs = append(template.URIs, uri)
		}
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey(priv), priv)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create certificate: %s", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes}), pem.EncodeToMemory(pemBlockForKey(priv)), nil
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
