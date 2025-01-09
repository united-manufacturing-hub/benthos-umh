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
	opcuaBrowserChan := make(chan NodeDef, MaxTagsToBrowse)

	nodeIDMap := make(map[string]*NodeDef)
	nodes := make([]NodeDef, 0, MaxTagsToBrowse)

	var wg TrackedWaitGroup
	wg.Add(1)
	browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(rootNode.NodeId)), "", 0, g.Log, rootNode.NodeId.String(), nodeChan, errChan, &wg, g.BrowseHierarchicalReferences, opcuaBrowserChan)
	go logBrowseStatus(ctx, nodeChan, msgChan, &wg)
	go logErrors(ctx, errChan, g.Log)
	go collectNodes(ctx, opcuaBrowserChan, nodeIDMap, &nodes)

	wg.Wait()

	close(nodeChan)
	close(errChan)
	close(opcuaBrowserChan)

	// TODO: Temporary workaround - Adding a timeout ensures all child nodes are properly
	// collected in the nodes[] array. Without this timeout, the last children nodes
	// may be missing from the results.
	time.Sleep(3 * time.Second)

	// By this time, nodeIDMap and nodes are populated with the nodes and nodeIDs
	for _, node := range nodes {
		constructNodeHierarchy(rootNode, node, nodeIDMap, g.Log)
	}
	return rootNode, nil
}

// logBrowseStatus logs the status of the browse operation. It sends a message to the channel
// every time a node is found, and the wait group counter status is sent to the channel
// to indicate that the browse operation is still active.
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

// logErrors logs errors from the error channel
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

// collectNodes collects the NodeDefs from the channel and adds them to the list of NodeDefs
func collectNodes(ctx context.Context, nodeBrowserChan chan NodeDef, nodeIDMap map[string]*NodeDef, nodes *[]NodeDef) {
	for node := range nodeBrowserChan {
		select {
		case <-ctx.Done():
			return
		default:
			nodeID := node.NodeID.String()
			nodeIDMap[nodeID] = &node
			*nodes = append(*nodes, node)
		}
	}
}

// constructNodeHierarchy constructs a tree structure from the list of NodeDefs
func constructNodeHierarchy(rootNode *Node, node NodeDef, nodeIDMap map[string]*NodeDef, logger *service.Logger) {
	current := rootNode
	if current.ChildIDMap == nil {
		current.ChildIDMap = make(map[string]*Node)
	}
	if current.Children == nil {
		current.Children = make([]*Node, 0)
	}

	paths := strings.Split(node.Path, ".")
	length := len(paths)
	for i, part := range paths {
		if _, exists := current.ChildIDMap[part]; !exists {
			parentNode := findNthParentNode(length-i-1, &node, nodeIDMap)
			id, err := ua.ParseNodeID(parentNode.NodeID.String())
			if err != nil {
				// This should never happen
				// All node ids should be valid
				logger.Errorf("error parsing node id: %v", err)
				return
			}

			current.ChildIDMap[part] = &Node{
				Name:       part,
				NodeId:     id,
				ChildIDMap: make(map[string]*Node),
				Children:   make([]*Node, 0),
			}
			current.Children = append(current.Children, current.ChildIDMap[part])
		}
		current = current.ChildIDMap[part]
	}
}

func findNthParentNode(n int, node *NodeDef, nodeIDMap map[string]*NodeDef) *NodeDef {
	if n == 0 {
		return node
	}

	for i := 0; i < n; i++ {
		parentNodeID := node.ParentNodeID
		parentNode := nodeIDMap[parentNodeID]

		if parentNode == nil {
			return node
		}
		node = parentNode
	}

	return node
}
