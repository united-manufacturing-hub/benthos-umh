package opcua_plugin

import (
	"context"
	"sync"

	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type MockOpcuaNodeWraper struct {
	id             *ua.NodeID
	attributes     map[ua.AttributeID]*ua.DataValue
	browseName     *ua.QualifiedName
	referenceNodes map[uint32][]NodeBrowser
}

// Attributes implements NodeBrowser.
func (m *MockOpcuaNodeWraper) Attributes(ctx context.Context, attrs ...ua.AttributeID) ([]*ua.DataValue, error) {
	panic("unimplemented")
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

// Ensure that the MockOpcuaNodeWraper implements the NodeBrowser interface
var _ NodeBrowser = &MockOpcuaNodeWraper{}

var _ = Describe("Tests for browse function", Label("browse_test"), func() {

	var (
		ctx           context.Context
		nodeBrowser   NodeBrowser
		path          string
		level         int
		logger        *service.Logger
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
		logger = &service.Logger{}
		parentNodeId = ""
		nodeChan = make(chan NodeDef, 100)
		errChan = make(chan error, 100)
		pathIDMapChan = make(chan map[string]string, 100)
		wg = &sync.WaitGroup{}
	})

	Context("When browsing nodes", func() {
		It("should process the nodes with right attributes", func() {
			mockNode := &MockOpcuaNodeWraper{
				id: ua.NewNumericNodeID(0, 1234),
				attributes: map[ua.AttributeID]*ua.DataValue{
					ua.AttributeIDNodeClass: {
						Value:  ua.MustVariant(uint32(ua.NodeClassVariable)),
						Status: ua.StatusOK,
					},
					ua.AttributeIDBrowseName: {
						Value:  ua.MustVariant(&ua.QualifiedName{NamespaceIndex: 0, Name: "TestNode"}),
						Status: ua.StatusOK,
					},
					ua.AttributeIDDescription: {
						Value:  ua.MustVariant("Test description"),
						Status: ua.StatusOK,
					},
					ua.AttributeIDAccessLevel: {
						Value:  ua.MustVariant(uint32(ua.AccessLevelTypeCurrentRead | ua.AccessLevelTypeCurrentWrite)),
						Status: ua.StatusOK,
					},
					ua.AttributeIDDataType: {
						Value:  ua.MustVariant(ua.NewNumericNodeID(0, uint32(ua.TypeIDInt32))),
						Status: ua.StatusOK,
					},
				},
				browseName:     &ua.QualifiedName{NamespaceIndex: 0, Name: "TestNode"},
				referenceNodes: make(map[uint32][]NodeBrowser),
			}

			nodeBrowser = mockNode
			wg.Add(1)
			go func() {
				defer wg.Done()
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

			Expect(errs).To(BeEmpty())
			Expect(nodes).To(HaveLen(1))
		})
	})
})
