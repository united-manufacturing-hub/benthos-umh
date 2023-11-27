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
	"encoding/json"
	"encoding/pem"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/stretchr/testify/assert"
)

func TestAgainstSimulator(t *testing.T) {

	t.Run("Logging Endpoints", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var endpoints []*ua.EndpointDescription
		var err error

		input := &OPCUAInput{
			endpoint: "opc.tcp://localhost:50000", // Important: ensure that the DNS name in the certificates of the server is also localhost (Hostname and DNS Name), as otherwise the server will refuse the connection
			username: "",
			password: "",
			nodeIDs:  nil,
			insecure: false,
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
		selectedEndpoint := input.getReasonableEndpoint(endpoints, ua.UserTokenTypeFromString("Anonymous"), input.insecure, "SignAndEncrypt", "Basic256Sha256")
		t.Logf("selected endpoint %v:", selectedEndpoint)
	})

	t.Run("ConnectAnonymousInsecure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: "opc.tcp://localhost:50000",
			username: "",
			password: "",
			nodeIDs:  nil,
			insecure: true, // It only works when not using encryption
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Username-Password fail Insecure", func(t *testing.T) {
		t.Skip() // Needs to be skipped, the current OPC-UA simulator does only logging in once, after that it fails
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: "opc.tcp://localhost:50000",
			username: "sysadmin_bad", // bad user and password
			password: "demo",
			insecure: true, // It only works when not using encryption
			nodeIDs:  nil,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.Error(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Anonymous Insecure", func(t *testing.T) {
		t.Skip() // Needs to be skipped, the current OPC-UA simulator does only logging in once, after that it fails
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: "opc.tcp://localhost:50000",
			username: "",
			password: "",
			insecure: true, // It only works when not using encryption
			nodeIDs:  nil,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.Error(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Username-Password success Insecure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: "opc.tcp://localhost:50000",
			username: "sysadmin",
			password: "demo",
			insecure: true, // It only works when not using encryption
			nodeIDs:  nil,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=3;s=Fast"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         "opc.tcp://localhost:50000",
			username:         "",
			password:         "",
			insecure:         true, // It only works when not using encryption
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: true,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 6, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe Boolean With Properties", func(t *testing.T) {
		// This test checks that properties are not browsed by default
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=6;s=DataAccess_AnalogType_Byte"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         "opc.tcp://localhost:50000",
			username:         "",
			password:         "",
			insecure:         true, // It only works when not using encryption
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: true,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 1, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "0"
			assert.Equal(t, exampleNumber, message) // it should be 0
			t.Log("Received message: ", message)
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe AnalogTypes (simple datatypes)", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=6;s=DataAccess_AnalogType_Byte", "ns=6;s=DataAccess_AnalogType_Double", "ns=6;s=DataAccess_AnalogType_Float", "ns=6;s=DataAccess_AnalogType_Int16", "ns=6;s=DataAccess_AnalogType_Int32", "ns=6;s=DataAccess_AnalogType_Int64", "ns=6;s=DataAccess_AnalogType_SByte", "ns=6;s=DataAccess_AnalogType_UInt16", "ns=6;s=DataAccess_AnalogType_UInt32", "ns=6;s=DataAccess_AnalogType_UInt64"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         "opc.tcp://localhost:50000",
			username:         "",
			password:         "",
			insecure:         true, // It only works when not using encryption
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: true,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 10, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe DataItem", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=6;s=DataAccess_DataItem"} // it will subscribe to all values with data type that is non-null.

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         "opc.tcp://localhost:50000",
			username:         "",
			password:         "",
			insecure:         true, // It only works when not using encryption
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: true,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 23, len(messageBatch))

		for _, message := range messageBatch {
			messageParsed, err := message.AsStructuredMut()
			if err != nil {
				t.Error(err)
			}
			opcuapath, found := message.MetaGet("opcua_path")
			if found != true {
				t.Fatal("Could not find opcua_path")
			}

			// Determine the data type from the OPC UA path
			dataType := strings.Split(opcuapath, "_")[5] // This will extract the data type part of the OPC UA path

			// Add checking based on the OPC UA path the resulting data type
			switch dataType {
			case "Boolean":
				var expectedType bool
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received Boolean message: ", messageParsed)

			case "Byte":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received Byte message: ", messageParsed)

			case "DateTime":
				// Assuming DateTime is parsed as time.Time in Go
				var expectedType time.Time
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received DateTime message: ", messageParsed)

			case "Double":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received Double message: ", messageParsed)

			case "Enumeration":
				// Enumeration type needs to be defined based on your application's specific enum types
				// var expectedType YourEnumType
				// assert.IsType(t, expectedType, messageParsed)
				// t.Log("Received Enumeration message: ", messageParsed)

			case "Float":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received Float message: ", messageParsed)

			case "Guid":
				// Assuming GUID is a string in your parsed message
				var expectedType string
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received GUID message: ", messageParsed)

			case "Int16":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received Int16 message: ", messageParsed)

			case "Int32":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received Int32 message: ", messageParsed)

			case "Int64":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received Int64 message: ", messageParsed)

			case "Integer":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received Integer message: ", messageParsed)

			case "LocalizedText":
				// LocalizedText usually is a string type
				var expectedType string
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received LocalizedText message: ", messageParsed)

			case "NodeId":
				// NodeId type needs to be defined based on your implementation
				// var expectedType NodeIdType
				// assert.IsType(t, expectedType, messageParsed)
				// t.Log("Received NodeId message: ", messageParsed)

			case "Number":
				// Number could be any numeric type; assuming float64 for general use
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received Number message: ", messageParsed)

			case "QualifiedName":
				// QualifiedName could be a struct or a string, depending on your implementation
				// var expectedType QualifiedNameType
				// assert.IsType(t, expectedType, messageParsed)
				// t.Log("Received QualifiedName message: ", messageParsed)

			case "SByte":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received SByte message: ", messageParsed)

			case "StatusCode":
				// StatusCode is likely an integer or specifically defined type
				// var expectedType StatusCodeType
				// assert.IsType(t, expectedType, messageParsed)
				// t.Log("Received StatusCode message: ", messageParsed)

			case "String":
				var expectedType string
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received String message: ", messageParsed)

			case "UInt16":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received UInt16 message: ", messageParsed)

			case "UInt32":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received UInt32 message: ", messageParsed)

			case "UInt64":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received UInt64 message: ", messageParsed)

			case "UInteger":
				var expectedType json.Number
				assert.IsType(t, expectedType, messageParsed)
				t.Log("Received UInteger message: ", messageParsed)

			default:
				t.Fatalf("Unsupported data type in OPC UA path: %s:%s", dataType, opcuapath)
			}
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

}

func TestAgainstRemoteInstance(t *testing.T) {

	// These information can be found in Bitwarden under WAGO PLC
	endpoint := os.Getenv("TEST_WAGO_ENDPOINT_URI")
	username := os.Getenv("TEST_WAGO_USERNAME")
	password := os.Getenv("TEST_WAGO_PASSWORD")

	// Check if environment variables are set
	if endpoint == "" || username == "" || password == "" {
		t.Skip("Skipping test: environment variables not set")
		return
	}

	t.Run("ConnectAnonymous", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: endpoint,
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
	})

	t.Run("ConnectAnonymousWithNoEncryption", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: endpoint,
			username: "",
			password: "",
			nodeIDs:  nil,
			insecure: true,
		}

		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Username-Password fail", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: endpoint,
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
	})

	t.Run("Connect Username-Password success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: endpoint,
			username: username,
			password: password,
			nodeIDs:  nil,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Parse nodes", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint: endpoint,
			username: username,
			password: password,
			nodeIDs:  parsedNodeIDs,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("ReadBatch", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint: endpoint,
			username: username,
			password: password,
			nodeIDs:  parsedNodeIDs,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 1, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Subscribe", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL", "ns=4;s=|vprop|WAGO 750-8101 PFC100 CS 2ETH.Application.RevisionCounter"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         endpoint,
			username:         username,
			password:         password,
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: true,
		}

		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		t.Log("Connected!")

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// expect 2 messages for both nodes
		assert.Equal(t, 2, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		messageBatch2, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// expect 1 message only as RevisionCounter will not change
		assert.Equal(t, 1, len(messageBatch2))

		for _, message := range messageBatch2 {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("ReadBatch_Insecure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint: endpoint,
			username: username,
			password: password,
			nodeIDs:  parsedNodeIDs,
			insecure: true,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 1, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("ReadBatch_SecurityMode_SecurityPolicy", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:       endpoint,
			username:       username,
			password:       password,
			nodeIDs:        parsedNodeIDs,
			insecure:       false,
			securityMode:   "SignAndEncrypt",
			securityPolicy: "Basic128Rsa15",
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 1, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

}

func MockGetEndpoints() []*ua.EndpointDescription {
	// Define the mock endpoints with the desired properties
	endpoint1 := &ua.EndpointDescription{
		EndpointURL: "opc.tcp://example.com:4840", // Replace with your actual server URL
		Server: &ua.ApplicationDescription{
			ApplicationURI:  "urn:example:server", // Replace with your server's URI
			ApplicationType: ua.ApplicationTypeServer,
		},
		ServerCertificate: []byte{},                                                    // Replace with your server certificate
		SecurityMode:      ua.MessageSecurityModeFromString("SignAndEncrypt"),          // Use appropriate security mode
		SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256", // Use appropriate security policy URI
		UserIdentityTokens: []*ua.UserTokenPolicy{
			{
				PolicyID:          "anonymous",
				TokenType:         ua.UserTokenTypeAnonymous,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#Anonymous",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256",
			},
			{
				PolicyID:          "username",
				TokenType:         ua.UserTokenTypeUserName,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#UserName",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256",
			},
		},
		TransportProfileURI: "http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary",
		SecurityLevel:       3, // Use an appropriate security level
	}

	endpoint2 := &ua.EndpointDescription{
		EndpointURL: "opc.tcp://example2.com:4840", // Replace with your actual server URL
		Server: &ua.ApplicationDescription{
			ApplicationURI:  "urn:example2:server", // Replace with your server's URI
			ApplicationType: ua.ApplicationTypeServer,
		},
		ServerCertificate: []byte("mock_certificate_2"),                      // Replace with your server certificate
		SecurityMode:      ua.MessageSecurityModeFromString("None"),          // Use appropriate security mode
		SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#None", // Use appropriate security policy URI
		UserIdentityTokens: []*ua.UserTokenPolicy{
			{
				PolicyID:          "anonymous",
				TokenType:         ua.UserTokenTypeAnonymous,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#Anonymous",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#None",
			},
			{
				PolicyID:          "username",
				TokenType:         ua.UserTokenTypeUserName,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#UserName",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#None",
			},
		},
		TransportProfileURI: "http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary",
		SecurityLevel:       0, // Use an appropriate security level
	}

	endpoint3 := &ua.EndpointDescription{
		EndpointURL: "opc.tcp://example3.com:4840", // Replace with your actual server URL
		Server: &ua.ApplicationDescription{
			ApplicationURI:  "urn:example3:server", // Replace with your server's URI
			ApplicationType: ua.ApplicationTypeServer,
		},
		ServerCertificate: []byte("mock_certificate_2"),                                    // Replace with your server certificate
		SecurityMode:      ua.MessageSecurityModeFromString("SignAndEncrypt"),              // Use appropriate security mode
		SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Aes256Sha256RsaPss", // Use appropriate security policy URI
		UserIdentityTokens: []*ua.UserTokenPolicy{
			{
				PolicyID:          "anonymous",
				TokenType:         ua.UserTokenTypeAnonymous,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#Anonymous",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Aes256Sha256RsaPss",
			},
			{
				PolicyID:          "username",
				TokenType:         ua.UserTokenTypeUserName,
				IssuedTokenType:   "http://opcfoundation.org/UA/UserTokenPolicy#UserName",
				SecurityPolicyURI: "http://opcfoundation.org/UA/SecurityPolicy#Aes256Sha256RsaPss",
			},
		},
		TransportProfileURI: "http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary",
		SecurityLevel:       0, // Use an appropriate security level
	}

	// Return the mock endpoints as a slice
	return []*ua.EndpointDescription{endpoint1, endpoint2, endpoint3}
}

func TestGetReasonableEndpoint_Insecure(t *testing.T) {
	input := &OPCUAInput{
		endpoint: "",
		username: "",
		password: "",
		nodeIDs:  nil,
		insecure: true,
	}

	endpoints := MockGetEndpoints()
	selectedEndpoint := input.getReasonableEndpoint(endpoints, ua.UserTokenTypeFromString("Anonymous"), input.insecure, "", "")

	if selectedEndpoint != nil {
		if selectedEndpoint.SecurityMode != ua.MessageSecurityModeFromString("None") {
			t.Errorf("Expected selected endpoint to have no encryption, but got %v", selectedEndpoint.SecurityMode)
		}
	} else {
		t.Error("Expected a reasonable endpoint, but got nil")
	}

	input2 := &OPCUAInput{
		endpoint: "",
		username: "",
		password: "",
		nodeIDs:  nil,
		insecure: false,
	}

	selectedEndpoint2 := input.getReasonableEndpoint(endpoints, ua.UserTokenTypeFromString("Anonymous"), input2.insecure, "", "")

	if selectedEndpoint2 != nil {
		if selectedEndpoint2.SecurityMode != ua.MessageSecurityModeFromString("SignAndEncrypt") {
			t.Errorf("Expected selected endpoint to have encryption, but got %v", selectedEndpoint.SecurityMode)
		}
	} else {
		t.Error("Expected a reasonable endpoint, but got nil")
	}
}

func TestGetReasonableEndpoint_SecurityModeAndPolicy(t *testing.T) {
	input := &OPCUAInput{
		endpoint:       "",
		username:       "123",
		password:       "213",
		nodeIDs:        nil,
		insecure:       false,
		securityMode:   "SignAndEncrypt",
		securityPolicy: "Aes256Sha256RsaPss",
	}

	endpoints := MockGetEndpoints()
	selectedEndpoint := input.getReasonableEndpoint(endpoints, ua.UserTokenTypeFromString("UserName"), input.insecure, input.securityMode, input.securityPolicy)

	if selectedEndpoint != nil {
		if selectedEndpoint.SecurityMode != ua.MessageSecurityModeFromString(input.securityMode) && selectedEndpoint.SecurityPolicyURI != "http://opcfoundation.org/UA/SecurityPolicy#"+input.securityPolicy {
			t.Errorf("Expected selected endpoint to have encryption with security mode %v and policy %v, but got %v and %v", input.securityMode, input.securityPolicy, selectedEndpoint.SecurityMode, selectedEndpoint.SecurityPolicyURI)
		}
	} else {
		t.Error("Expected a reasonable endpoint, but got nil")
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
