package opcua_plugin

import (
	"context"
	"errors"
	"sync"
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
	rootNodeWrapper := NewOpcuaNodeWrapper(g.Client.Node(rootNode.NodeId))
	browse(ctx, rootNodeWrapper, "", 0, g.Log, rootNode.NodeId.String(), nil, nil, &wg, g.BrowseHierarchicalReferences, msgChan)
	wg.Wait()
	close(msgChan)
	return rootNode, nil
}
