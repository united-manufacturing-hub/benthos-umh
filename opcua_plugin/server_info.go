package opcua_plugin

import (
	"context"
	"errors"

	"github.com/gopcua/opcua/ua"
)

// ServerInfo holds basic information about an OPC UA server, including the manufacturer name,
// product name, and software version.
type ServerInfo struct {
	ManufacturerName string
	ProductName      string
	SoftwareVersion  string
}

// GetOPCUAServerInformation retrieves essential information from the OPC UA server, such as
// the manufacturer name, product name, and software version. It queries specific nodes
// identified by their NodeIDs to gather this data and constructs a ServerInfo struct with the results.
func (g *OPCUAInput) GetOPCUAServerInformation(ctx context.Context) (ServerInfo, error) {

	if g.Client == nil {
		return ServerInfo{}, errors.New("client is nil")
	}
	// Fetch ManufacturerName node from i=2263
	manufacturerNameNodeID := ua.NewNumericNodeID(0, 2263)
	productNameNodeID := ua.NewNumericNodeID(0, 2261)
	softwareVersionNodeID := ua.NewNumericNodeID(0, 2264)

	nodeChan := make(chan NodeDef, 3)
	errChan := make(chan error, 3)
	// nodeIDChan is declared to satisfy the browse function signature.
	// The data inside nodeIDChan is not used for this function.
	// It is more useful for the GetNodeTree function.
	nodeIDChan := make(chan OpcuaBrowserRecord, 3)
	var wg TrackedWaitGroup

	wg.Add(3)
	go browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(manufacturerNameNodeID)), "", 0, g.Log, manufacturerNameNodeID.String(), nodeChan, errChan, &wg, g.BrowseHierarchicalReferences, nodeIDChan)
	go browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(productNameNodeID)), "", 0, g.Log, productNameNodeID.String(), nodeChan, errChan, &wg, g.BrowseHierarchicalReferences, nodeIDChan)
	go browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(softwareVersionNodeID)), "", 0, g.Log, softwareVersionNodeID.String(), nodeChan, errChan, &wg, g.BrowseHierarchicalReferences, nodeIDChan)
	wg.Wait()

	close(nodeChan)
	close(errChan)
	close(nodeIDChan)

	if len(errChan) > 0 {
		return ServerInfo{}, <-errChan
	}

	var nodeList []NodeDef
	for node := range nodeChan {
		nodeList = append(nodeList, node)
	}

	if len(nodeList) != 3 {
		g.Log.Warn("Could not find OPC UA Server Information")
		return ServerInfo{}, errors.New("could not find OPC UA Server Information")
	}

	var nodesToRead []*ua.ReadValueID
	for _, node := range nodeList {
		nodesToRead = append(nodesToRead, &ua.ReadValueID{
			NodeID: node.NodeID,
		})
	}

	req := &ua.ReadRequest{
		MaxAge:             2000,
		NodesToRead:        nodesToRead,
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	resp, err := g.Read(ctx, req)
	if err != nil {
		g.Log.Errorf("Read failed: %s", err)
		return ServerInfo{}, err
	}

	if len(resp.Results) != 3 {
		g.Log.Errorf("Expected 3 results, got %d", len(resp.Results))
		return ServerInfo{}, errors.New("expected 3 results")
	}

	serverInfo := ServerInfo{}

	for i, node := range nodeList {
		value := resp.Results[i]
		if value == nil || value.Value == nil {
			g.Log.Debugf("Received nil in item structure for OPC UA Server Information")
		}

		message := g.createMessageFromValue(value, node)
		if message != nil {
			messageBytes, err := message.AsBytes()
			if err != nil {
				return ServerInfo{}, err
			}

			if node.NodeID.IntID() == 2263 {
				serverInfo.ManufacturerName = string(messageBytes)
			} else if node.NodeID.IntID() == 2261 {
				serverInfo.ProductName = string(messageBytes)
			} else if node.NodeID.IntID() == 2264 {
				serverInfo.SoftwareVersion = string(messageBytes)
			}
		}
	}
	return serverInfo, nil
}
