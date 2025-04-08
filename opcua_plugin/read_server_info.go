// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opcua_plugin

import (
	"context"
	"errors"

	"github.com/gopcua/opcua/ua"
)

// TODO: this should be moved to core_server_info.go
// However, there is still some OPCUAInput specific code in this file
// which needs to be refactored.

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
	// opcuaBrowserChan is declared to satisfy the browse function signature.
	// The data inside opcuaBrowserChan is not used for this function.
	// It is more useful for the GetNodeTree function.
	opcuaBrowserChan := make(chan BrowseDetails, 3)
	var wg TrackedWaitGroup

	wg.Add(3)
	go Browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(manufacturerNameNodeID)), "", g.Log, manufacturerNameNodeID.String(), nodeChan, errChan, &wg, opcuaBrowserChan, &g.visited)
	go Browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(productNameNodeID)), "", g.Log, productNameNodeID.String(), nodeChan, errChan, &wg, opcuaBrowserChan, &g.visited)
	go Browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(softwareVersionNodeID)), "", g.Log, softwareVersionNodeID.String(), nodeChan, errChan, &wg, opcuaBrowserChan, &g.visited)
	wg.Wait()

	close(nodeChan)
	close(errChan)
	close(opcuaBrowserChan)

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
