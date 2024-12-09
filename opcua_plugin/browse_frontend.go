package opcua_plugin

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	nodeIDChan := make(chan []string, MaxTagsToBrowse)

	nodeIDMap := make(map[string]string)
	nodes := make([]NodeDef, 0, MaxTagsToBrowse)

	go collectNodesFromChannel(ctx, nodeChan, nodes, msgChan)
	go logErrors(ctx, errChan, g.Log)
	go collectNodeIDFromChannel(ctx, nodeIDChan, nodeIDMap)

	var wg TrackedWaitGroup
	wg.Add(1)
	browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(rootNode.NodeId)), "", 0, g.Log, rootNode.NodeId.String(), nodeChan, errChan, &wg, g.BrowseHierarchicalReferences, nodeIDChan)
	wg.Wait()

	close(nodeChan)
	close(errChan)
	close(nodeIDChan)

	// By this time, nodeIDMap and nodes are populated with the nodes and nodeIDs
	for _, node := range nodes {
		InsertNode(rootNode, node, nodeIDMap)
	}
	return rootNode, nil
}

func collectNodesFromChannel(ctx context.Context, nodeChan chan NodeDef, nodes []NodeDef, msgChan chan<- string) {
	for n := range nodeChan {
		select {
		case <-ctx.Done():
			return
		default:
			nodes = append(nodes, n)
			msgChan <- fmt.Sprintf("Browsing node for %s", n.BrowseName)
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

func collectNodeIDFromChannel(ctx context.Context, nodeIDChan chan []string, nodeIDMap map[string]string) {
	for i := range nodeIDChan {
		select {
		case <-ctx.Done():
			return
		default:
			if len(i) == 2 {
				nodeIDMap[i[0]] = i[1]
			}
		}
	}
}

func InsertNode(rootNode *Node, node NodeDef, nodeIDMap map[string]string) {
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
		}
		current.Children = append(current.Children, current.ChildIDMap[part])
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
