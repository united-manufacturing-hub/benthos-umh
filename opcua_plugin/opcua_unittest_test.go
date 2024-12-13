package opcua_plugin_test

import (
	"context"
	"errors"
	"fmt"
	"time"

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
			Expect(endpoints).NotTo(BeEmpty())
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
			cncl                         context.CancelFunc
			nodeBrowser                  NodeBrowser
			path                         string
			level                        int
			logger                       Logger
			parentNodeId                 string
			nodeChan                     chan NodeDef
			errChan                      chan error
			wg                           *TrackedWaitGroup
			browseHierarchicalReferences bool
			nodeIDChan                   chan []string
		)
		BeforeEach(func() {
			ctx, cncl = context.WithTimeout(context.Background(), 180*time.Second)
			path = ""
			level = 0
			logger = &MockLogger{}
			parentNodeId = ""
			nodeChan = make(chan NodeDef, 100)
			errChan = make(chan error, 100)
			wg = &TrackedWaitGroup{}
			browseHierarchicalReferences = false
			nodeIDChan = make(chan []string, 100)
		})
		AfterEach(func() {
			cncl()
		})

		Context("When browsing nodes with a node class value nil", func() {
			It("should return an error for nil node class in the error channel", func() {
				var attributes []*ua.DataValue
				attributes = append(attributes, getDataValueForNilNodeClass())
				attributes = append(attributes, getDataValueForBrowseName("TestNode"))
				attributes = append(attributes, getDataValueForDescription("Test Description", ua.StatusOK))
				attributes = append(attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				attributes = append(attributes, getDataValueForDataType(ua.TypeIDInt32, ua.StatusOK))

				rootNodeWithNilNodeClass := &MockOpcuaNodeWraper{
					id:             ua.NewNumericNodeID(0, 1234),
					attributes:     attributes,
					browseName:     &ua.QualifiedName{NamespaceIndex: 0, Name: "Test Node with nil node class"},
					referenceNodes: make(map[uint32][]NodeBrowser),
				}
				nodeBrowser = rootNodeWithNilNodeClass
				wg.Add(1)
				go func() {
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, wg, browseHierarchicalReferences, nodeIDChan)
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
				rootNode.attributes = append(rootNode.attributes, getDataValueForDescription("Test Description", ua.StatusOK))
				rootNode.attributes = append(rootNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDataType(ua.TypeIDInt32, ua.StatusOK))

				nodeBrowser = rootNode
				wg.Add(1)
				go func() {
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, wg, browseHierarchicalReferences, nodeIDChan)
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
				rootNode.attributes = append(rootNode.attributes, getDataValueForDescription("Test Description", ua.StatusOK))
				rootNode.attributes = append(rootNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDataType(ua.TypeIDInt32, ua.StatusOK))
				childNode := createMockVariableNode(1223, "TestChildNode")
				childNode.attributes = append(childNode.attributes, getDataValueForNodeClass(ua.NodeClassVariable))
				childNode.attributes = append(childNode.attributes, getDataValueForBrowseName("TestChildNode"))
				childNode.attributes = append(childNode.attributes, getDataValueForDescription("Test Child Description", ua.StatusOK))
				childNode.attributes = append(childNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				childNode.attributes = append(childNode.attributes, getDataValueForDataType(ua.TypeIDInt32, ua.StatusOK))
				rootNode.AddReferenceNode(id.HasComponent, childNode)

				nodeBrowser = rootNode
				wg.Add(1)
				go func() {
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, wg, browseHierarchicalReferences, nodeIDChan)
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
				rootNode.attributes = append(rootNode.attributes, getDataValueForDescription("Test Description", ua.StatusOK))
				rootNode.attributes = append(rootNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDataType(ua.TypeIDInt32, ua.StatusOK))
				childNode := createMockVariableNode(1223, "TestChildNode")
				childNode.attributes = append(childNode.attributes, getDataValueForNodeClass(ua.NodeClassVariable))
				childNode.attributes = append(childNode.attributes, getDataValueForBrowseName("TestChildNode"))
				childNode.attributes = append(childNode.attributes, getDataValueForDescription("Test Child Description", ua.StatusOK))
				childNode.attributes = append(childNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				childNode.attributes = append(childNode.attributes, getDataValueForDataType(ua.TypeIDInt32, ua.StatusOK))
				rootNode.AddReferenceNode(id.HasChild, childNode)

				nodeBrowser = rootNode
				wg.Add(1)
				go func() {
					// set browseHierarchicalReferences to true for reference nodes like id.HasChild
					browseHierarchicalReferences := true
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, wg, browseHierarchicalReferences, nodeIDChan)
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
				rootNode.attributes = append(rootNode.attributes, getDataValueForDescription("Test Description", ua.StatusOK))
				rootNode.attributes = append(rootNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDataType(ua.TypeIDInt32, ua.StatusOK))
				childNode := createMockVariableNode(1223, "TestChildNode")
				childNode.attributes = append(childNode.attributes, getDataValueForNodeClass(ua.NodeClassVariable))
				childNode.attributes = append(childNode.attributes, getDataValueForBrowseName("TestChildNode"))
				childNode.attributes = append(childNode.attributes, getDataValueForDescription("Test Child Description", ua.StatusOK))
				childNode.attributes = append(childNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				childNode.attributes = append(childNode.attributes, getDataValueForDataType(ua.TypeIDInt32, ua.StatusOK))
				rootNode.AddReferenceNode(id.Organizes, childNode)

				nodeBrowser = rootNode
				wg.Add(1)
				go func() {
					// set browseHierarchicalReferences to true for reference nodes like id.Organizes
					browseHierarchicalReferences := true
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, wg, browseHierarchicalReferences, nodeIDChan)
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

		Context("When browsing nodes where the folder has a PermissionDenied on its Value and ValueRank but one can still see the contents of its children, which is stupid but so is OPC UA. This edge case has been sponsored by Siemens S7-1500 and the community member Diederik", func() {
			It("root node with PermissionDenied should return 1 child ", func() {
				rootNode := createMockVariableNode(1234, "08DischargeCartGroup")
				rootNode.attributes = append(rootNode.attributes, getDataValueForNodeClass(ua.NodeClassVariable))
				rootNode.attributes = append(rootNode.attributes, getDataValueForBrowseName("3:08DischargeCartGroup"))
				rootNode.attributes = append(rootNode.attributes, getDataValueForDescription("", ua.StatusBadAttributeIDInvalid))
				rootNode.attributes = append(rootNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeNone)) // PermissionDenied and therefore this node should be ignored in the node-list, but still subscribed to
				rootNode.attributes = append(rootNode.attributes, getDataValueForDataType(0, ua.StatusBadNotReadable))

				childNode := createMockVariableNode(1223, "TestChildNode")
				childNode.attributes = append(childNode.attributes, getDataValueForNodeClass(ua.NodeClassVariable))
				childNode.attributes = append(childNode.attributes, getDataValueForBrowseName("TestChildNode"))
				childNode.attributes = append(childNode.attributes, getDataValueForDescription("Test Child Description", ua.StatusOK))
				childNode.attributes = append(childNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				childNode.attributes = append(childNode.attributes, getDataValueForDataType(ua.TypeIDInt32, ua.StatusOK))
				rootNode.AddReferenceNode(id.HasChild, childNode)

				nodeBrowser = rootNode
				wg.Add(1)
				go func() {
					browseHierarchicalReferences := true
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, wg, browseHierarchicalReferences, nodeIDChan)
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

		Context("When browsing a folder structure with HasTypeDefinition and HasChild references", func() {
			It("should browse through ABC folder and return the ProcessValue variable", func() {
				Skip("fake opc ua node browser cannot handle this, as children wiull always return all referencednodes independent of the nodeclass")
				// Create ABC folder node
				abcFolder := createMockVariableNode(1234, "ABC")
				abcFolder.attributes = append(abcFolder.attributes, getDataValueForNodeClass(ua.NodeClassObject))
				abcFolder.attributes = append(abcFolder.attributes, getDataValueForBrowseName("ABC"))
				abcFolder.attributes = append(abcFolder.attributes, getDataValueForDescription("ABC Folder", ua.StatusOK))
				abcFolder.attributes = append(abcFolder.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead))
				abcFolder.attributes = append(abcFolder.attributes, getDataValueForDataType(ua.TypeIDString, ua.StatusOK))

				// Create DEF folder node
				defFolder := createMockVariableNode(1235, "DEF")
				defFolder.attributes = append(defFolder.attributes, getDataValueForNodeClass(ua.NodeClassObject))
				defFolder.attributes = append(defFolder.attributes, getDataValueForBrowseName("DEF"))
				defFolder.attributes = append(defFolder.attributes, getDataValueForDescription("DEF Folder", ua.StatusOK))
				defFolder.attributes = append(defFolder.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead))
				defFolder.attributes = append(defFolder.attributes, getDataValueForDataType(ua.TypeIDString, ua.StatusOK))

				// Create the object node
				objectNode := createMockVariableNode(0, "0:6312FT000")
				objectNode.id = ua.MustParseNodeID("ns=3;s=0:6312FT000")
				objectNode.attributes = append(objectNode.attributes, getDataValueForNodeClass(ua.NodeClassObject))
				objectNode.attributes = append(objectNode.attributes, getDataValueForBrowseName("0:6312FT000"))
				objectNode.attributes = append(objectNode.attributes, getDataValueForDescription("Object Node", ua.StatusOK))
				objectNode.attributes = append(objectNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead))
				objectNode.attributes = append(objectNode.attributes, getDataValueForDataType(ua.TypeIDString, ua.StatusOK))

				// Create the ProcessValue variable node
				processValueNode := createMockVariableNode(0, "0:645645645.ProcessValue")
				processValueNode.id = ua.MustParseNodeID("ns=3;s=0:645645645.ProcessValue")
				processValueNode.attributes = append(processValueNode.attributes, getDataValueForNodeClass(ua.NodeClassVariable))
				processValueNode.attributes = append(processValueNode.attributes, getDataValueForBrowseName("ProcessValue"))
				processValueNode.attributes = append(processValueNode.attributes, getDataValueForDescription("Process Value", ua.StatusOK))
				processValueNode.attributes = append(processValueNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				processValueNode.attributes = append(processValueNode.attributes, getDataValueForDataType(ua.TypeIDString, ua.StatusOK))

				// Set up the references
				folderTypeNode := createMockVariableNode(61, "FolderType")
				folderTypeNode.attributes = append(folderTypeNode.attributes, getDataValueForNodeClass(ua.NodeClassVariableType))
				folderTypeNode.attributes = append(folderTypeNode.attributes, getDataValueForBrowseName("FolderType"))
				folderTypeNode.attributes = append(folderTypeNode.attributes, getDataValueForDescription("Folder Type", ua.StatusOK))
				folderTypeNode.attributes = append(folderTypeNode.attributes, getDataValueForAccessLevel(ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite))
				folderTypeNode.attributes = append(folderTypeNode.attributes, getDataValueForDataType(ua.TypeIDString, ua.StatusOK))

				abcFolder.AddReferenceNode(id.HasTypeDefinition, folderTypeNode)
				abcFolder.AddReferenceNode(id.HasChild, defFolder)
				defFolder.AddReferenceNode(id.HasChild, objectNode)
				objectNode.AddReferenceNode(id.HasChild, processValueNode)

				nodeBrowser = abcFolder
				wg.Add(1)
				go func() {
					browseHierarchicalReferences := true
					Browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, wg, browseHierarchicalReferences, nodeIDChan)
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
				Expect(nodes[0].NodeID.String()).To(Equal("ns=3;s=0:645645645.ProcessValue"))
				Expect(nodes[0].BrowseName).To(Equal("ProcessValue"))
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

func getDataValueForDescription(description string, statusCode ua.StatusCode) *ua.DataValue {
	if errors.Is(statusCode, ua.StatusOK) {
		return &ua.DataValue{
			EncodingMask: ua.DataValueValue,
			Value:        ua.MustVariant(description),
			Status:       ua.StatusOK,
		}
	} else {
		return &ua.DataValue{
			EncodingMask: ua.DataValueValue,
			Value:        nil,
			Status:       ua.StatusBadNotReadable,
		}
	}
}

func getDataValueForAccessLevel(accessLevel ua.AccessLevelType) *ua.DataValue {
	return &ua.DataValue{
		EncodingMask: ua.DataValueValue,
		Value:        ua.MustVariant(int64(accessLevel)),
		Status:       ua.StatusOK,
	}
}

func getDataValueForDataType(id ua.TypeID, statusCode ua.StatusCode) *ua.DataValue {
	if errors.Is(statusCode, ua.StatusOK) {
		return &ua.DataValue{
			EncodingMask: ua.DataValueValue,
			Value:        ua.MustVariant(ua.NewNumericNodeID(0, uint32(id))),
			Status:       ua.StatusOK,
		}
	} else {
		return &ua.DataValue{
			EncodingMask: ua.DataValueValue,
			Value:        nil,
			Status:       ua.StatusBadNotReadable,
		}
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

func (m *MockLogger) Warnf(format string, args ...interface{}) {
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
