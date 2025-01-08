package opcua_plugin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// OpcuaBrowserRecord is a struct that is used to pass information from the browse method to GetNodeTree method
// for every recursion call that happens inside the browse method in browse.go
type OpcuaBrowserRecord struct {
	Node       NodeDef
	BrowseName string
}

// GetNodeTree returns the tree structure of the OPC UA server nodes
// GetNodeTree is currently used by united-manufacturing-hub/ManagementConsole repo for the BrowseOPCUA tags functionality
func (g *OPCUAInput) GetNodeTree(ctx context.Context, msgChan chan<- string, rootNode *Node) (*Node, error) {
	if g.Client == nil {
		err := g.connect(ctx)
		if err != nil {
			g.Log.Infof("error setting up connection while getting the OPCUA nodes: %v", err)
			return nil, err
		}
	}
	defer func() {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			// Create a new context to close the connection if the existing context is canceled or timed out
			ctx = context.Background()
		}
		err := g.Client.Close(ctx)
		if err != nil {
			g.Log.Infof("error closing the connection while getting the OPCUA nodes: %v", err)
		}
	}()

	nodeChan := make(chan NodeDef, MaxTagsToBrowse)
	errChan := make(chan error, MaxTagsToBrowse)
	browserRecordChan := make(chan OpcuaBrowserRecord, MaxTagsToBrowse)

	nodeIDMap := make(map[string]string)
	nodes := make([]NodeDef, 0, MaxTagsToBrowse)

	var wg TrackedWaitGroup
	wg.Add(1)
	browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(rootNode.NodeId)), "", 0, g.Log, rootNode.NodeId.String(), nodeChan, errChan, &wg, g.BrowseHierarchicalReferences, browserRecordChan)
	go logBrowseStatus(ctx, nodeChan, msgChan, &wg)
	go logErrors(ctx, errChan, g.Log)
	go collectNodes(ctx, browserRecordChan, nodeIDMap, &nodes)

	wg.Wait()

	close(nodeChan)
	close(errChan)
	close(browserRecordChan)

	// TODO: Temporary workaround - Adding a timeout ensures all child nodes are properly
	// collected in the nodes[] array. Without this timeout, the last children nodes
	// may be missing from the results.
	time.Sleep(3 * time.Second)

	// By this time, nodeIDMap and nodes are populated with the nodes and nodeIDs
	for _, node := range nodes {
		constructNodeHierarchy(rootNode, node, nodeIDMap)
	}
	return rootNode, nil
}

func logBrowseStatus(ctx context.Context, nodeChan chan NodeDef, msgChan chan<- string, wg *TrackedWaitGroup) {
	for n := range nodeChan {
		select {
		case <-ctx.Done():
			return
		default:
			// Send a more detailed message about the browsing progress using WaitGroup count
			msgChan <- fmt.Sprintf("found node '%s' (%d active browse operations)",
				n.BrowseName, wg.Count())
		}
	}
}

func logErrors(ctx context.Context, errChan chan error, logger *service.Logger) {
	for err := range errChan {
		select {
		case <-ctx.Done():
			return
		default:
			logger.Errorf("error browsing children while constructing the node tree: %v", err)
		}
	}
}

func collectNodes(ctx context.Context, nodeIDChan chan OpcuaBrowserRecord, nodeIDMap map[string]string, nodes *[]NodeDef) {
	for i := range nodeIDChan {
		select {
		case <-ctx.Done():
			return
		default:
			nodeID := i.Node.NodeID.String()
			nodeIDMap[i.BrowseName] = nodeID
			*nodes = append(*nodes, i.Node)
		}
	}
}

func constructNodeHierarchy(rootNode *Node, node NodeDef, nodeIDMap map[string]string) {
	current := rootNode
	if current.ChildIDMap == nil {
		current.ChildIDMap = make(map[string]*Node)
	}
	if current.Children == nil {
		current.Children = make([]*Node, 0)
	}

	paths := strings.Split(node.Path, ".")
	for _, part := range paths {
		if _, exists := current.ChildIDMap[part]; !exists {
			current.ChildIDMap[part] = &Node{
				Name:       part,
				NodeId:     ua.NewStringNodeID(0, FindID(nodeIDMap, part)),
				ChildIDMap: make(map[string]*Node),
				Children:   make([]*Node, 0),
			}
			current.Children = append(current.Children, current.ChildIDMap[part])
		}
		current = current.ChildIDMap[part]
	}
}

// FindID finds the ID from the dictionary
func FindID(dictionary map[string]string, nodeName string) string {
	if id, ok := dictionary[nodeName]; ok {
		return id
	}
	return "unknown"
}
