package opcua_plugin_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/gopcua/opcua/id"
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

	var _ = Describe("Unit Tests for browse function", Label("browse_test"), func() {

		var (
			ctx                          context.Context
			nodeBrowser                  NodeBrowser
			path                         string
			level                        int
			logger                       Logger
			parentNodeId                 string
			nodeChan                     chan NodeDef
			errChan                      chan error
			pathIDMapChan                chan map[string]string
			wg                           *sync.WaitGroup
			browseHierarchicalReferences bool
		)
		BeforeEach(func() {
			ctx = context.Background()
			path = ""
			level = 0
			logger = &MockLogger{}
			parentNodeId = ""
			nodeChan = make(chan NodeDef, 100)
			errChan = make(chan error, 100)
			pathIDMapChan = make(chan map[string]string, 100)
			wg = &sync.WaitGroup{}
			browseHierarchicalReferences = false
		})

		Context("When browsing nodes with a node class value nil", func() {
			It("should return an error for nil node class in the error channel", func() {
				var attributes []*ua.DataValue
				attributes = append(attributes, getDataValueForNilNodeClass())
				rootNodeWithNilNodeClass := &MockOpcuaNodeWraper{
					id:             ua.NewNumericNodeID(0, 1234),
					attributes:     attributes,
					browseName:     &ua.QualifiedName{NamespaceIndex: 0, Name: "Test Node with nil node class"},
					referenceNodes: make(map[uint32][]NodeBrowser),
				}
				nodeBrowser = rootNodeWithNilNodeClass
				wg.Add(1)
				go func() {
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, pathIDMapChan, wg, browseHierarchicalReferences)
				}()
				wg.Wait()
				close(nodeChan)
				close(errChan)

				var errs []error
				for err := range errChan {
					errs = append(errs, err)
				}

				Expect(errs).Should(HaveLen(1))
				Expect(errs[0].Error()).To(Equal("opcua: node class is nil"))
			})

			It("should browse the root node with no children with right attributes", func() {

				rootNode := createMockVariableNode(1234, "TestNode")
				rootNode.attributes = append(rootNode.attributes, getDataValueForNodeClass(ua.NodeClassVariable))
				rootNode.attributes = append(rootNode.attributes, getDataValueForBrowseName("TestNode"))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDescription("Test Description"))
				rootNode.attributes = append(rootNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDataType(ua.TypeIDInt32))

				nodeBrowser = rootNode
				wg.Add(1)
				go func() {
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, pathIDMapChan, wg, browseHierarchicalReferences)
				}()
				wg.Wait()
				close(nodeChan)
				close(errChan)

				var nodes []NodeDef
				for nodeDef := range nodeChan {
					nodes = append(nodes, nodeDef)
				}

				var errs []error
				for err := range errChan {
					errs = append(errs, err)
				}

				Expect(errs).Should(BeEmpty())
				Expect(nodes).Should(HaveLen(1))
			})
			It("root node with id.HasComponent should return 1 child", func() {

				rootNode := createMockVariableNode(1234, "TestNode")
				rootNode.attributes = append(rootNode.attributes, getDataValueForNodeClass(ua.NodeClassObject))
				rootNode.attributes = append(rootNode.attributes, getDataValueForBrowseName("TestNode"))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDescription("Test Description"))
				rootNode.attributes = append(rootNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDataType(ua.TypeIDInt32))
				childNode := createMockVariableNode(1223, "TestChildNode")
				childNode.attributes = append(childNode.attributes, getDataValueForNodeClass(ua.NodeClassVariable))
				childNode.attributes = append(childNode.attributes, getDataValueForBrowseName("TestChildNode"))
				childNode.attributes = append(childNode.attributes, getDataValueForDescription("Test Child Description"))
				childNode.attributes = append(childNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				childNode.attributes = append(childNode.attributes, getDataValueForDataType(ua.TypeIDInt32))
				rootNode.AddReferenceNode(id.HasComponent, childNode)

				nodeBrowser = rootNode
				wg.Add(1)
				go func() {
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, pathIDMapChan, wg, browseHierarchicalReferences)
				}()
				wg.Wait()
				close(nodeChan)
				close(errChan)

				var nodes []NodeDef
				for nodeDef := range nodeChan {
					nodes = append(nodes, nodeDef)
				}

				var errs []error
				for err := range errChan {
					errs = append(errs, err)
				}

				Expect(errs).Should(BeEmpty())
				Expect(nodes).Should(HaveLen(1))
				Expect(nodes[0].NodeID.String()).To(Equal("i=1223"))
				Expect(nodes[0].BrowseName).To(Equal("TestChildNode"))
			})
			It("root node with id.HasChild should return 1 child", func() {

				rootNode := createMockVariableNode(1234, "TestNode")
				rootNode.attributes = append(rootNode.attributes, getDataValueForNodeClass(ua.NodeClassObject))
				rootNode.attributes = append(rootNode.attributes, getDataValueForBrowseName("TestNode"))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDescription("Test Description"))
				rootNode.attributes = append(rootNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDataType(ua.TypeIDInt32))
				childNode := createMockVariableNode(1223, "TestChildNode")
				childNode.attributes = append(childNode.attributes, getDataValueForNodeClass(ua.NodeClassVariable))
				childNode.attributes = append(childNode.attributes, getDataValueForBrowseName("TestChildNode"))
				childNode.attributes = append(childNode.attributes, getDataValueForDescription("Test Child Description"))
				childNode.attributes = append(childNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				childNode.attributes = append(childNode.attributes, getDataValueForDataType(ua.TypeIDInt32))
				rootNode.AddReferenceNode(id.HasChild, childNode)

				nodeBrowser = rootNode
				wg.Add(1)
				go func() {
					// set browseHierarchicalReferences to true for reference nodes like id.HasChild
					browseHierarchicalReferences := true
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, pathIDMapChan, wg, browseHierarchicalReferences)
				}()
				wg.Wait()
				close(nodeChan)
				close(errChan)

				var nodes []NodeDef
				for nodeDef := range nodeChan {
					nodes = append(nodes, nodeDef)
				}

				var errs []error
				for err := range errChan {
					errs = append(errs, err)
				}

				Expect(errs).Should(BeEmpty())
				Expect(nodes).Should(HaveLen(1))
				Expect(nodes[0].NodeID.String()).To(Equal("i=1223"))
				Expect(nodes[0].BrowseName).To(Equal("TestChildNode"))
			})
			It("root node with id.Organizes should return 1 child", func() {

				rootNode := createMockVariableNode(1234, "TestNode")
				rootNode.attributes = append(rootNode.attributes, getDataValueForNodeClass(ua.NodeClassObject))
				rootNode.attributes = append(rootNode.attributes, getDataValueForBrowseName("TestNode"))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDescription("Test Description"))
				rootNode.attributes = append(rootNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDataType(ua.TypeIDInt32))
				childNode := createMockVariableNode(1223, "TestChildNode")
				childNode.attributes = append(childNode.attributes, getDataValueForNodeClass(ua.NodeClassVariable))
				childNode.attributes = append(childNode.attributes, getDataValueForBrowseName("TestChildNode"))
				childNode.attributes = append(childNode.attributes, getDataValueForDescription("Test Child Description"))
				childNode.attributes = append(childNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				childNode.attributes = append(childNode.attributes, getDataValueForDataType(ua.TypeIDInt32))
				rootNode.AddReferenceNode(id.Organizes, childNode)

				nodeBrowser = rootNode
				wg.Add(1)
				go func() {
					// set browseHierarchicalReferences to true for reference nodes like id.Organizes
					browseHierarchicalReferences := true
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, pathIDMapChan, wg, browseHierarchicalReferences)
				}()
				wg.Wait()
				close(nodeChan)
				close(errChan)

				var nodes []NodeDef
				for nodeDef := range nodeChan {
					nodes = append(nodes, nodeDef)
				}

				var errs []error
				for err := range errChan {
					errs = append(errs, err)
				}

				Expect(errs).Should(BeEmpty())
				Expect(nodes).Should(HaveLen(1))
				Expect(nodes[0].NodeID.String()).To(Equal("i=1223"))
				Expect(nodes[0].BrowseName).To(Equal("TestChildNode"))
			})
		})
	})
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

type MockOpcuaNodeWraper struct {
	id             *ua.NodeID
	attributes     []*ua.DataValue
	browseName     *ua.QualifiedName
	referenceNodes map[uint32][]NodeBrowser
}

func getDataValueForNilNodeClass() *ua.DataValue {
	return &ua.DataValue{
		EncodingMask: ua.DataValueValue,
		Value:        nil,
		Status:       ua.StatusOK,
	}
}

func getDataValueForNodeClass(nodeClass ua.NodeClass) *ua.DataValue {
	return &ua.DataValue{
		EncodingMask: ua.DataValueValue,
		Value:        ua.MustVariant(int64(nodeClass)),
		Status:       ua.StatusOK,
	}
}

func getDataValueForBrowseName(name string) *ua.DataValue {
	return &ua.DataValue{
		EncodingMask: ua.DataValueValue,
		Value:        ua.MustVariant(&ua.QualifiedName{NamespaceIndex: 0, Name: name}),
		Status:       ua.StatusOK,
	}
}

func getDataValueForDescription(description string) *ua.DataValue {
	return &ua.DataValue{
		EncodingMask: ua.DataValueValue,
		Value:        ua.MustVariant(description),
		Status:       ua.StatusOK,
	}
}

func getDataValueForAccessLevel(accessLevel ua.AccessLevelType) *ua.DataValue {
	return &ua.DataValue{
		EncodingMask: ua.DataValueValue,
		Value:        ua.MustVariant(int64(accessLevel)),
		Status:       ua.StatusOK,
	}
}

func getDataValueForDataType(id ua.TypeID) *ua.DataValue {
	return &ua.DataValue{
		EncodingMask: ua.DataValueValue,
		Value:        ua.MustVariant(ua.NewNumericNodeID(0, uint32(id))),
		Status:       ua.StatusOK,
	}
}

func (m *MockOpcuaNodeWraper) Attributes(ctx context.Context, attrs ...ua.AttributeID) ([]*ua.DataValue, error) {
	return m.attributes, nil
}

// BrowseName implements NodeBrowser.
func (m *MockOpcuaNodeWraper) BrowseName(ctx context.Context) (*ua.QualifiedName, error) {
	return m.browseName, nil
}

// ID implements NodeBrowser.
func (m *MockOpcuaNodeWraper) ID() *ua.NodeID {
	return m.id
}

// Children returns all reference nodes of all refType. In real implementation, it will use id.HierarchicalReferences reference type
// If you want to browse children of a particular refType, use ReferencedNodes
func (m *MockOpcuaNodeWraper) Children(ctx context.Context, refType uint32, nodeClassMask ua.NodeClass) ([]NodeBrowser, error) {
	var result []NodeBrowser
	// return all reference nodes of all refType
	for _, nodes := range m.referenceNodes {
		result = append(result, nodes...)
	}
	return result, nil
}

// ReferencedNodes returns all reference nodes of a particular refType
func (m *MockOpcuaNodeWraper) ReferencedNodes(ctx context.Context, refType uint32, browseDir ua.BrowseDirection, nodeClassMask ua.NodeClass, includeSubtypes bool) ([]NodeBrowser, error) {
	if nodes, ok := m.referenceNodes[refType]; ok {
		return nodes, nil
	}
	return nil, nil
}

type MockLogger struct {
}

func (m *MockOpcuaNodeWraper) AddReferenceNode(refType uint32, node NodeBrowser) {
	m.referenceNodes[refType] = append(m.referenceNodes[refType], node)
}

func (m *MockLogger) Debugf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

// createMockVariableNode creates a mock OPC UA node with a given ID and name.
// id is the node id in namespace 0
// name is the browse name of the node
// both parameters can be overridden by using Attributes method
func createMockVariableNode(id uint32, name string) *MockOpcuaNodeWraper {
	return &MockOpcuaNodeWraper{
		id:             ua.NewNumericNodeID(0, id),
		browseName:     &ua.QualifiedName{NamespaceIndex: 0, Name: name},
		referenceNodes: make(map[uint32][]NodeBrowser),
	}
}

// Ensure that the MockOpcuaNodeWraper implements the NodeBrowser interface
var _ NodeBrowser = &MockOpcuaNodeWraper{}
