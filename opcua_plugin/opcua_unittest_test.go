package opcua_plugin_test

import (
	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"
)

var _ = Describe("Unit Tests", func() {

	Describe("GetReasonableEndpoint Functionality", func() {
		var endpoints []*ua.EndpointDescription
		BeforeEach(func() {
			endpoints = MockGetEndpoints()

			endpoints = endpoints
			Skip("Implement this test")
		})
	})

	DescribeTable("should correctly update node paths",
		func(nodes []NodeDef, expected []NodeDef) {
			UpdateNodePaths(nodes)
			Expect(nodes).To(Equal(expected))
		},
		Entry("no duplicates", []NodeDef{
			{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
			{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
			{Path: "Folder.Tag3", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
		}, []NodeDef{
			{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
			{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
			{Path: "Folder.Tag3", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
		}),
		Entry("duplicates", []NodeDef{
			{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
			{Path: "Folder.Tag1", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
			{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
			{Path: "Folder.Tag3", NodeID: ua.MustParseNodeID("ns=1;s=node4")},
		}, []NodeDef{
			{Path: "Folder.ns_1_s_node1", NodeID: ua.MustParseNodeID("ns=1;s=node1")},
			{Path: "Folder.ns_1_s_node2", NodeID: ua.MustParseNodeID("ns=1;s=node2")},
			{Path: "Folder.Tag2", NodeID: ua.MustParseNodeID("ns=1;s=node3")},
			{Path: "Folder.Tag3", NodeID: ua.MustParseNodeID("ns=1;s=node4")},
		}),
	)
})

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
