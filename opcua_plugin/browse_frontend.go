package opcua_plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
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

	var wg sync.WaitGroup
	wg.Add(1)
	g.browseChildren(ctx, &wg, 0, rootNode, id.HierarchicalReferences, msgChan)
	wg.Wait()
	close(msgChan)
	return rootNode, nil
}

// browseChildren recursively browses the OPC UA server nodes and builds a tree structure
func (g *OPCUAInput) browseChildren(ctx context.Context, wg *sync.WaitGroup, level int, parent *Node, referenceType uint32, msgChan chan<- string) {
	defer wg.Done()
	// Recursion limit only goes up to 10 levels
	if level >= 10 {
		return
	}

	// Check if the context is cancelled
	select {
	case <-ctx.Done():
		return
	default:
		// continue to do other operation
	}

	go func() {
		msgChan <- fmt.Sprintf("Fetching result for node:  %s", parent.Name)
	}()

	node := g.Client.Node(parent.NodeId)
	nodeClass, err := node.NodeClass(ctx)
	if err != nil {
		g.Log.Warnf("error getting nodeClass for node ID %s: %v", parent.NodeId, err)
	}

	refs, err := node.ReferencedNodes(ctx, referenceType, ua.BrowseDirectionForward, ua.NodeClassAll, true)
	if err != nil {
		g.Log.Warnf("error browsing children: %v", err)
		return
	}

	for _, ref := range refs {
		newNode := g.Client.Node(ref.ID)
		newNodeName, err := newNode.BrowseName(ctx)
		if err != nil {
			g.Log.Warnf("error browsing children: %v", err)
			continue

		}
		child := &Node{
			NodeId:   ref.ID,
			Name:     newNodeName.Name,
			Children: make([]*Node, 0),
		}
		parent.Children = append(parent.Children, child)
		if nodeClass == ua.NodeClassVariable {
			wg.Add(1)
			go g.browseChildren(ctx, wg, level+1, child, id.HasComponent, msgChan)
		}

		if nodeClass == ua.NodeClassObject {
			wg.Add(4)
			go g.browseChildren(ctx, wg, level+1, child, id.HasComponent, msgChan)
			go g.browseChildren(ctx, wg, level+1, child, id.Organizes, msgChan)
			go g.browseChildren(ctx, wg, level+1, child, id.FolderType, msgChan)
			go g.browseChildren(ctx, wg, level+1, child, id.HasNotifier, msgChan)
		}
	}
}
