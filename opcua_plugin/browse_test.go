package opcua_plugin

import (
	"context"
	"fmt"
	"sync"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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

// ReferencedNodes implements NodeBrowser.
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

var _ = Describe("Tests for browse function", Label("browse_test"), func() {

	var (
		ctx           context.Context
		nodeBrowser   NodeBrowser
		path          string
		level         int
		logger        Logger
		parentNodeId  string
		nodeChan      chan NodeDef
		errChan       chan error
		pathIDMapChan chan map[string]string
		wg            *sync.WaitGroup
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
	})

	Context("When browsing nodes with a node class value nil", func() {
		It("should return an error for nil node class in the error channel", func() {
			Skip("temp")
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
				browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, pathIDMapChan, wg)
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
				browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, pathIDMapChan, wg)
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
				browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, pathIDMapChan, wg)
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
				browse(ctx, nodeBrowser, path, level, logger, parentNodeId, nodeChan, errChan, pathIDMapChan, wg)
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
	})
})
