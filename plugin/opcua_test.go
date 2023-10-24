// Copyright 2023 UMH Systems GmbH
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

package plugin

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/stretchr/testify/assert"
)

func TestGetEndpointsLoggingOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var endpoints []*ua.EndpointDescription
	var err error

	input := &OPCUAInput{
		endpoint: "opc.tcp://localhost:46010",
		username: "",
		password: "",
		nodeIDs:  nil,
	}

	endpoints, err = opcua.GetEndpoints(ctx, input.endpoint)
	assert.NoError(t, err)

	for i, endpoint := range endpoints {
		t.Logf("Endpoint %d:", i+1)
		t.Logf("  EndpointURL: %s", endpoint.EndpointURL)
		t.Logf("  SecurityMode: %v", endpoint.SecurityMode)
		t.Logf("  SecurityPolicyURI: %s", endpoint.SecurityPolicyURI)
		t.Logf("  TransportProfileURI: %s", endpoint.TransportProfileURI)
		t.Logf("  SecurityLevel: %d", endpoint.SecurityLevel)

		// If Server is not nil, log its details
		if endpoint.Server != nil {
			t.Logf("  Server ApplicationURI: %s", endpoint.Server.ApplicationURI)
			t.Logf("  Server ProductURI: %s", endpoint.Server.ProductURI)
			t.Logf("  Server ApplicationName: %s", endpoint.Server.ApplicationName.Text)
			t.Logf("  Server ApplicationType: %v", endpoint.Server.ApplicationType)
			t.Logf("  Server GatewayServerURI: %s", endpoint.Server.GatewayServerURI)
			t.Logf("  Server DiscoveryProfileURI: %s", endpoint.Server.DiscoveryProfileURI)
			t.Logf("  Server DiscoveryURLs: %v", endpoint.Server.DiscoveryURLs)
		}

		// Output the certificate
		if len(endpoint.ServerCertificate) > 0 {
			// Convert to PEM format first, then log the certificate information
			pemCert := pem.EncodeToMemory(&pem.Block{
				Type:  "CERTIFICATE",
				Bytes: endpoint.ServerCertificate,
			})
			logCertificateInfo(t, pemCert)
		}

		// Loop through UserIdentityTokens
		for j, token := range endpoint.UserIdentityTokens {
			t.Logf("  UserIdentityToken %d:", j+1)
			t.Logf("    PolicyID: %s", token.PolicyID)
			t.Logf("    TokenType: %v", token.TokenType)
			t.Logf("    IssuedTokenType: %s", token.IssuedTokenType)
			t.Logf("    IssuerEndpointURL: %s", token.IssuerEndpointURL)
		}
	}
}

func TestOPCUAInput_ConnectAnonymous(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var err error

	input := &OPCUAInput{
		endpoint: "opc.tcp://localhost:46010",
		username: "",
		password: "",
		nodeIDs:  nil,
	}
	// Attempt to connect
	err = input.Connect(ctx)
	assert.NoError(t, err)

	// Close connection
	if input.client != nil {
		input.client.Close(ctx)
	}
}

func TestOPCUAInput_ConnectusernamePasswordFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var err error

	input := &OPCUAInput{
		endpoint: "opc.tcp://localhost:46010",
		username: "123", // bad user and password
		password: "123",
		nodeIDs:  nil,
	}
	// Attempt to connect
	err = input.Connect(ctx)
	assert.Error(t, err)

	// Close connection
	if input.client != nil {
		input.client.Close(ctx)
	}
}

func TestOPCUAInput_ConnectusernamePasswordSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var err error

	input := &OPCUAInput{
		endpoint: "opc.tcp://localhost:46010",
		username: "root",
		password: "secret",
		nodeIDs:  nil,
	}
	// Attempt to connect
	err = input.Connect(ctx)
	assert.NoError(t, err)

	// Close connection
	if input.client != nil {
		input.client.Close(ctx)
	}
}

func logCertificateInfo(t *testing.T, certBytes []byte) {
	t.Logf("  Server certificate:")

	// Decode the certificate from base64 to DER format
	block, _ := pem.Decode(certBytes)
	if block == nil {
		t.Log("Failed to decode certificate")
		return
	}

	// Parse the DER-format certificate
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Log("Failed to parse certificate:", err)
		return
	}

	// Log the details
	t.Log("    Not Before:", cert.NotBefore)
	t.Log("    Not After:", cert.NotAfter)
	t.Log("    DNS Names:", cert.DNSNames)
	t.Log("    IP Addresses:", cert.IPAddresses)
	t.Log("    URIs:", cert.URIs)
}
