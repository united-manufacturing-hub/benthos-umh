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

package opcua_plugin

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
	endpoint := os.Getenv("TEST_WAGO_ENDPOINT_URI")
	username := os.Getenv("TEST_WAGO_USERNAME")
	password := os.Getenv("TEST_WAGO_PASSWORD")

	// Check if environment variables are set
	if endpoint != "" || username != "" || password != "" {
		t.Skip("Skipping test: environment variables are set")
		return
	}

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
		if input.client != nil {
			err = input.client.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
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
			err = input.client.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("ConnectAnonymousSecure", func(t *testing.T) {
		t.Skip("Secure is currently not working, as the OPC UA simulator server aborts the connection to to certificate problems")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		input := &OPCUAInput{
			endpoint: "opc.tcp://localhost:50000",
			username: "",
			password: "",
			nodeIDs:  nil,
			insecure: false,
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
			err = input.client.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
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
			err = input.client.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
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
			err = input.client.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("Connect Subscribe", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings = []string{"ns=3;s=Fast"}

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
			t.Logf("%+v", messageBatch)
			t.Fatal(err)
		}

		assert.GreaterOrEqual(t, len(messageBatch), 6)

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
			err = input.client.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("Connect Subscribe Boolean With Properties", func(t *testing.T) {
		// This test checks that properties are not browsed by default
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings = []string{"ns=6;s=DataAccess_AnalogType_Byte"}

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
			err = input.client.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("Connect Subscribe AnalogTypes (simple datatypes)", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings = []string{"ns=6;s=DataAccess_AnalogType_Byte", "ns=6;s=DataAccess_AnalogType_Double", "ns=6;s=DataAccess_AnalogType_Float", "ns=6;s=DataAccess_AnalogType_Int16", "ns=6;s=DataAccess_AnalogType_Int32", "ns=6;s=DataAccess_AnalogType_Int64", "ns=6;s=DataAccess_AnalogType_SByte", "ns=6;s=DataAccess_AnalogType_UInt16", "ns=6;s=DataAccess_AnalogType_UInt32", "ns=6;s=DataAccess_AnalogType_UInt64"}

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
			err = input.client.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("Connect Subscribe null", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=6;s=DataAccess_DataItem_Null"}

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

		assert.Equal(t, 0, len(messageBatch)) // should never subscribe to null datatype

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe AnalogTypeArray", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=6;s=DataAccess_AnalogType_Array"}

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

		assert.Equal(t, 26, len(messageBatch)) // Adjust the expected number of items as necessary

		for _, message := range messageBatch {
			messageParsed, err := message.AsStructuredMut()
			if err != nil {
				// This might happen if an empty string is returned from OPC-UA
				continue
			}

			opcuapath, found := message.MetaGet("opcua_path")
			if !found {
				t.Fatal("Could not find opcua_path")
			}

			// Determine the data type from the OPC UA path
			dataType := strings.Split(opcuapath, "_")[5] // This will extract the data type part of the OPC UA path
			t.Log(dataType)

			// Check if the data type is an array and handle accordingly
			if strings.HasSuffix(dataType, "Array") {
				dataTypeOfArray := strings.Split(opcuapath, "_")[6]
				t.Log(dataTypeOfArray)
				// Handle array data types
				switch dataTypeOfArray {
				case "Duration", "Guid", "LocaleId", "Boolean", "LocalizedText", "NodeId", "QualifiedName", "UtcTime", "DateTime", "Double", "Enumeration", "Float", "Int16", "Int32", "Int64", "Integer", "Number", "SByte", "StatusCode", "String", "UInt16", "UInt32", "UInt64", "UInteger", "Variant", "XmlElement", "ByteString":
					// Check if the messageParsed is of type slice (array)
					messageParsedArray, ok := messageParsed.([]interface{})
					if !ok {
						t.Errorf("Expected messageParsed to be an array, but got %T: %s : %s", messageParsed, opcuapath, messageParsed)
					} else {
						for _, item := range messageParsedArray {

							// Add checking based on the OPC UA path the resulting data type
							checkDatatypeOfOPCUATag(t, dataTypeOfArray, item, opcuapath)
						}
					}
				case "Byte":
					checkDatatypeOfOPCUATag(t, "ByteArray", messageParsed, opcuapath)
				default:
					t.Errorf("Unsupported array data type in OPC UA path: %s:%s", dataType, opcuapath)
				}
			} else {
				t.Fatalf("Received non-array: %s", opcuapath)
			}
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

		var nodeIDStrings = []string{"ns=6;s=DataAccess_DataItem"} // it will subscribe to all values with data type that is non-null.

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

		assert.Equal(t, 23, len(messageBatch)) // these are theoretically >30, but most of them are null, so the browse function ignores them

		for _, message := range messageBatch {
			messageParsed, err := message.AsStructuredMut()
			if err != nil {
				// This might happen if an empty string is returned from OPC-UA
				continue
			}

			opcuapath, found := message.MetaGet("opcua_path")
			if found != true {
				t.Fatal("Could not find opcua_path")
			}

			// Determine the data type from the OPC UA path
			dataType := strings.Split(opcuapath, "_")[5] // This will extract the data type part of the OPC UA path

			// Add checking based on the OPC UA path the resulting data type
			checkDatatypeOfOPCUATag(t, dataType, messageParsed, opcuapath)
		}

		// Close connection
		if input.client != nil {
			err = input.client.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("TestForFailedNodeCrash", func(t *testing.T) {
		// https://github.com/united-manufacturing-hub/MgmtIssues/issues/1088
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings = []string{
			"ns=3;s=Fast",
			"ns=3;s=Slow",
		}
		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)
		input := &OPCUAInput{
			endpoint:         "opc.tcp://localhost:50000", // Important: ensure that the DNS name in the certificates of the server is also localhost (Hostname and DNS Name), as otherwise the server will refuse the connection
			username:         "",
			password:         "",
			nodeIDs:          parsedNodeIDs,
			insecure:         true,
			subscribeEnabled: true,
		}

		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.NotEmpty(t, messageBatch)

		for _, message := range messageBatch {
			_, err := message.AsStructured()
			if err != nil {
				t.Fatal(err)
			}
		}

		// Close connection
		if input.client != nil {
			err = input.client.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("Connect Subscribe Scalar Arrays", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{
			"ns=6;s=Scalar_Static_Arrays_Boolean",
			"ns=6;s=Scalar_Static_Arrays_Byte",
			"ns=6;s=Scalar_Static_Arrays_ByteString",
			"ns=6;s=Scalar_Static_Arrays_DateTime",
			"ns=6;s=Scalar_Static_Arrays_Double",
			"ns=6;s=Scalar_Static_Arrays_Duration",
			"ns=6;s=Scalar_Static_Arrays_Float",
			"ns=6;s=Scalar_Static_Arrays_Guid",
			"ns=6;s=Scalar_Static_Arrays_Int16",
			"ns=6;s=Scalar_Static_Arrays_Int32",
			"ns=6;s=Scalar_Static_Arrays_Int64",
			"ns=6;s=Scalar_Static_Arrays_Integer",
			"ns=6;s=Scalar_Static_Arrays_LocaleId",
			"ns=6;s=Scalar_Static_Arrays_LocalizedText",
			"ns=6;s=Scalar_Static_Arrays_NodeId",
			"ns=6;s=Scalar_Static_Arrays_Number",
			"ns=6;s=Scalar_Static_Arrays_QualifiedName",
			"ns=6;s=Scalar_Static_Arrays_SByte",
			"ns=6;s=Scalar_Static_Arrays_String",
			"ns=6;s=Scalar_Static_Arrays_UInt16",
			"ns=6;s=Scalar_Static_Arrays_UInt32",
			"ns=6;s=Scalar_Static_Arrays_UInt64",
			"ns=6;s=Scalar_Static_Arrays_UInteger",
			"ns=6;s=Scalar_Static_Arrays_UtcTime",
			// "ns=6;s=Scalar_Static_Arrays_Variant", // This node causes a timeout
			"ns=6;s=Scalar_Static_Arrays_XmlElement",
		}

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

		assert.Equal(t, 25, len(messageBatch))

		for _, message := range messageBatch {
			messageParsed, err := message.AsStructuredMut()
			if err != nil {
				// This might happen if an empty string is returned from OPC-UA
				continue
			}

			opcuapath, found := message.MetaGet("opcua_path")
			if found != true {
				t.Fatal("Could not find opcua_path")
			}

			// Determine the data type from the OPC UA path
			dataType := strings.Split(opcuapath, "_")[5] // This will extract the data type part of the OPC UA path
			t.Log(dataType)

			// Check if the data type is an array and handle accordingly
			if strings.HasSuffix(dataType, "Arrays") {
				dataTypeOfArray := strings.Split(opcuapath, "_")[6]
				t.Log(dataTypeOfArray)
				// Handle array data types
				switch dataTypeOfArray {
				case "Duration", "Guid", "LocaleId", "Boolean", "LocalizedText", "NodeId", "QualifiedName", "UtcTime", "DateTime", "Double", "Enumeration", "Float", "Int16", "Int32", "Int64", "SByte", "StatusCode", "String", "UInt16", "UInt32", "UInt64", "XmlElement", "ByteString":
					// Check if the messageParsed is of type slice (array)
					messageParsedArray, ok := messageParsed.([]interface{})
					if !ok {
						t.Errorf("Expected messageParsed to be an array, but got %T: %s : %s", messageParsed, opcuapath, messageParsed)
					} else {
						for _, item := range messageParsedArray {

							// Add checking based on the OPC UA path the resulting data type
							checkDatatypeOfOPCUATag(t, dataTypeOfArray, item, opcuapath)
						}
					}
				case "Byte":
					checkDatatypeOfOPCUATag(t, "ByteArray", messageParsed, opcuapath)
				case "Integer", "Number", "Variant", "UInteger": // Variant Arrays are not supported by go upcua lib
					t.Logf("Unsupported array data type in OPC UA path: %s:%s", dataType, opcuapath)
				default:
					t.Errorf("Unsupported array data type in OPC UA path: %s:%s", dataType, opcuapath)
				}
			} else {
				t.Fatalf("Received non-array: %s", opcuapath)
			}
		}

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe does not fail when subscribing to entire simulator", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=3;s=OpcPlc"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         "opc.tcp://localhost:50000",
			username:         "",
			password:         "",
			insecure:         true, // It only works when not using encryption
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: false, // set to false because some values will change more often in a second resulting in too many messages
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.GreaterOrEqual(t, len(messageBatch), 125)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect does not fail when subscribing to everything", func(t *testing.T) {
		t.Skip("This might take too long...")
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"i=84"}

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

		assert.Equal(t, 25, len(messageBatch))

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe does subscribe to objects", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=3;s=SimulatorConfiguration"}

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

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe does not fail when subscribing to Anomaly", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=3;s=Anomaly"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         "opc.tcp://localhost:50000",
			username:         "",
			password:         "",
			insecure:         true, // It only works when not using encryption
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: false, // disabling subscribe because messages change moreo ften than once in a second reuslting in to many messages
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 4, len(messageBatch))

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe does not fail when subscribing to Basic", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=3;s=Basic"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         "opc.tcp://localhost:50000",
			username:         "",
			password:         "",
			insecure:         true, // It only works when not using encryption
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: false, // disabling subscribe because messages change moreo ften than once in a second reuslting in to many messages
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 4, len(messageBatch))

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe does not fail when subscribing to Deterministic GUID", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=3;s=Deterministic GUID"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         "opc.tcp://localhost:50000",
			username:         "",
			password:         "",
			insecure:         true, // It only works when not using encryption
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: false,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 5, len(messageBatch))

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe does not fail when subscribing to Fast", func(t *testing.T) {
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
			subscribeEnabled: false,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.GreaterOrEqual(t, len(messageBatch), 5)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe does not fail when subscribing to Slow", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=3;s=Slow"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         "opc.tcp://localhost:50000",
			username:         "",
			password:         "",
			insecure:         true, // It only works when not using encryption
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: false,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.GreaterOrEqual(t, len(messageBatch), 100)

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

	t.Run("Connect Subscribe does not fail when subscribing to Special", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings []string = []string{"ns=3;s=Special"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint:         "opc.tcp://localhost:50000",
			username:         "",
			password:         "",
			insecure:         true, // It only works when not using encryption
			nodeIDs:          parsedNodeIDs,
			subscribeEnabled: false,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 7, len(messageBatch))

		// Close connection
		if input.client != nil {
			input.client.Close(ctx)
		}
	})

}

func checkDatatypeOfOPCUATag(t *testing.T, dataType string, messageParsed any, opcuapath string) {
	t.Logf("%s, %+v, %s", dataType, messageParsed, opcuapath)
	switch dataType {
	case "Boolean":
		var expectedType bool
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received Boolean message: ", messageParsed)

	case "Byte":
		var expectedType json.Number
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received Byte message: ", messageParsed)

	case "DateTime": //warning: there is a bug when the date is a lot in the future year 30828
		var expectedType string
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received DateTime message: ", messageParsed)

	case "Double":
		var expectedType json.Number
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received Double message: ", messageParsed)

	case "Enumeration":
		var expectedType json.Number
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received Enumeration message: ", messageParsed)
	case "ExpandedNodeId":
		// Assert that messageParsed is of type map[string]interface{}
		parsedMap, ok := messageParsed.(map[string]interface{})
		if !ok {
			t.Errorf("Expected messageParsed to be of type map[string]interface{}, but got %T", messageParsed)
		} else {
			// Check if the keys inside the map are correct
			expectedKeys := []string{"NamespaceURI", "NodeID", "ServerIndex"}
			for _, key := range expectedKeys {
				if _, exists := parsedMap[key]; !exists {
					t.Errorf("Expected key %s missing in messageParsed", key)
				}
			}

			// Optionally, log the received message
			t.Log("Received ExpandedNodeId message: ", messageParsed)
		}
	case "Float":
		var expectedType json.Number
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received Float message: ", messageParsed)

	case "Guid":
		// Assert that messageParsed is of type map[string]interface{}
		parsedMap, ok := messageParsed.(map[string]interface{})
		if !ok {
			t.Errorf("Expected messageParsed to be of type map[string]interface{}, but got %T", messageParsed)
		} else {
			// Check if the keys inside the map are correct
			expectedKeys := []string{"Data1", "Data2", "Data3", "Data4"}
			for _, key := range expectedKeys {
				if _, exists := parsedMap[key]; !exists {
					t.Errorf("Expected key %s missing in messageParsed", key)
				}
			}

			// Optionally, log the received message
			t.Log("Received GUID message: ", messageParsed)
		}
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
		// Assert that messageParsed is of type map[string]interface{}
		parsedMap, ok := messageParsed.(map[string]interface{})
		if !ok {
			t.Errorf("Expected messageParsed to be of type map[string]interface{}, but got %T", messageParsed)
		} else {
			// Check if the keys inside the map are correct
			expectedKeys := []string{"EncodingMask", "Locale", "Text"}
			for _, key := range expectedKeys {
				if _, exists := parsedMap[key]; !exists {
					t.Errorf("Expected key %s missing in messageParsed", key)
				}
			}

			// Optionally, log the received message
			t.Log("Received LocalizedText message: ", messageParsed)
		}
	case "NodeId":
		var expectedType string
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received NodeId message: ", messageParsed)
	case "Number":
		var expectedType json.Number
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received Number message: ", messageParsed)
	case "QualifiedName":
		// Assert that messageParsed is of type map[string]interface{}
		parsedMap, ok := messageParsed.(map[string]interface{})
		if !ok {
			t.Errorf("Expected messageParsed to be of type map[string]interface{}, but got %T", messageParsed)
		} else {
			// Define the keys expected in a QualifiedName message
			expectedKeys := []string{"NamespaceIndex", "Name"}
			for _, key := range expectedKeys {
				if _, exists := parsedMap[key]; !exists {
					t.Errorf("Expected key %s missing in messageParsed", key)
				}
			}

			// Optionally, log the received message
			t.Log("Received QualifiedName message: ", messageParsed)
		}
	case "SByte":
		var expectedType json.Number
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received SByte message: ", messageParsed)
	case "StatusCode":
		var expectedType json.Number
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received StatusCode message: ", messageParsed)
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
	case "ByteArray": // case as an array of bytes is not a number, but a string
		var expectedType string
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received ByteArray message: ", messageParsed)
	case "ByteString":
		var expectedType string
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received ByteString message: ", messageParsed)
	case "Duration":
		var expectedType json.Number
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received Duration message: ", messageParsed)
	case "LocaleId":
		var expectedType string
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received LocaleId message: ", messageParsed)
	case "UtcTime":
		var expectedType string
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received UtcTime message: ", messageParsed)
	case "Variant":
		var expectedType map[string]interface{}
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received Variant message: ", messageParsed)
	case "XmlElement":
		var expectedType string
		assert.IsType(t, expectedType, messageParsed)
		t.Log("Received XmlElement message: ", messageParsed)
	default:
		t.Errorf("Unsupported data type in OPC UA path: %s:%s", dataType, opcuapath)
	}
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
		defer input.Close(ctx)
		assert.NoError(t, err)
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
		defer input.Close(ctx)
		assert.NoError(t, err)
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
		defer input.Close(ctx)
		assert.Error(t, err)
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
		defer input.Close(ctx)
		assert.NoError(t, err)
	})

	t.Run("Parse nodes", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint: endpoint,
			username: username,
			password: password,
			nodeIDs:  parsedNodeIDs,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		defer input.Close(ctx)
		assert.NoError(t, err)
	})

	t.Run("ReadBatch", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

		parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

		input := &OPCUAInput{
			endpoint: endpoint,
			username: username,
			password: password,
			nodeIDs:  parsedNodeIDs,
		}
		// Attempt to connect
		err = input.Connect(ctx)
		defer input.Close(ctx)
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
	})

	t.Run("Subscribe", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL", "ns=4;s=|vprop|WAGO 750-8101 PFC100 CS 2ETH.Application.RevisionCounter"}

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
		defer input.Close(ctx)
		assert.NoError(t, err)

		t.Log("Connected!")

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// expect 2 messages for both nodes
		assert.Equal(t, 2, len(messageBatch))

		for _, message := range messageBatch {
			messageContent, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, messageContent) // it should be a number
			t.Log("Received message: ", messageContent)
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
	})

	t.Run("ReadBatch_Insecure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

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
		defer input.Close(ctx)
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
	})

	t.Run("ReadBatch_SecurityMode_SecurityPolicy", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var nodeIDStrings = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

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
		defer input.Close(ctx)
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
		t.Fatalf("Expected a reasonable endpoint, but got nil") // This needs to be fatal, to prevent nil error in selectedEndpoint2 check
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
