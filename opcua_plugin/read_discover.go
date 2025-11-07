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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

// Then modify the discoverNodes function to use TrackedWaitGroup
func (g *OPCUAInput) discoverNodes(ctx context.Context) ([]NodeDef, map[string]string, error) {
	// This was previously 5 minutes, but we need to increase it to 1 hour to avoid context cancellation
	// when browsing a large number of nodes.
	timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Hour)
	defer cancel()

	nodeList := make([]NodeDef, 0)
	pathIDMap := make(map[string]string)
	nodeChan := make(chan NodeDef, MaxTagsToBrowse)
	errChan := make(chan error, MaxTagsToBrowse)
	// opcuaBrowserChan is created to just satisfy the browse function signature.
	// The data inside opcuaBrowserChan is not so useful for this function. It is more useful for the GetNodeTree function
	// Buffer size reduced to 1000 (from 100k) since consumer drains instantly - saves 33 MB memory
	opcuaBrowserChan := make(chan BrowseDetails, 1000)

	// Start concurrent consumer to drain opcuaBrowserChan as workers produce BrowseDetails
	// This prevents deadlock by ensuring channel never fills (discovered in ENG-3835 integration test)
	// Without this consumer, workers block after 100k nodes and hang until timeout
	opcuaBrowserConsumerDone := make(chan struct{})
	go func() {
		for range opcuaBrowserChan {
			// Discard - browse details not needed for subscription path (only for GetNodeTree)
		}
		close(opcuaBrowserConsumerDone)
	}()

	var wg TrackedWaitGroup
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				g.Log.Infof("Amount of found opcua tags currently in channel: %d, (%d active browse goroutines)",
					len(nodeChan), wg.Count())
			case <-done:
				return
			case <-timeoutCtx.Done():
				g.Log.Warn("browse function received timeout signal after 1 hour. Please select less nodes.")
				return
			}
		}
	}()

	for _, nodeID := range g.NodeIDs {
		if nodeID == nil {
			continue
		}

		g.Log.Debugf("Browsing nodeID: %s", nodeID.String())
		wg.Add(1)
		wrapperNodeID := NewOpcuaNodeWrapper(g.Client.Node(nodeID))
		go browse(timeoutCtx, wrapperNodeID, "", g.Log, nodeID.String(), nodeChan, errChan, &wg, opcuaBrowserChan, &g.visited, g.ServerProfile)
	}

	// Start concurrent consumer to drain nodeChan as workers produce nodes
	// This prevents deadlock when browse workers fill the 100k buffer
	consumerDone := make(chan struct{})
	go func() {
		for node := range nodeChan {
			nodeList = append(nodeList, node)
			if node.NodeID != nil {
				pathIDMap[node.Path] = node.NodeID.String()
			}
		}
		close(consumerDone)
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-timeoutCtx.Done():
		g.Log.Warn("browse function received timeout signal after 1 hour. Please select less nodes.")
		return nil, nil, timeoutCtx.Err()
	case <-done:
	}

	close(nodeChan)
	close(errChan)
	close(opcuaBrowserChan)

	// Wait for consumers to finish draining the channels
	<-consumerDone
	<-opcuaBrowserConsumerDone

	UpdateNodePaths(nodeList)

	if len(errChan) > 0 {
		var combinedErr strings.Builder
		for err := range errChan {
			combinedErr.WriteString(err.Error() + "; ")
		}
		return nil, nil, errors.New(combinedErr.String())
	}

	return nodeList, pathIDMap, nil
}

// BrowseAndSubscribeIfNeeded browses the specified OPC UA nodes, adds a heartbeat node if required,
// and sets up monitored requests for the nodes.
//
// The function performs the following steps:
// 1. **Browse Nodes:** Iterates through `NodeIDs` and concurrently browses each node to detect available nodes.
// 2. **Add Heartbeat Node:** If heartbeats are enabled, ensures the heartbeat node (`HeartbeatNodeId`) is included in the node list.
// 3. **Subscribe to Nodes:** If subscriptions are enabled, creates a subscription and sets up monitoring for the detected nodes.
func (g *OPCUAInput) BrowseAndSubscribeIfNeeded(ctx context.Context) (err error) {
	var nodeList []NodeDef

	// if all nodeIDs are fresh and fully discovered we can avoid spawning any
	// goroutines here
	if g.canSkipDiscovery() {
		g.Log.Infof("All requested nodes are fresh, skipping rebrowse. Using chaced nodelist")
		nodeList = g.buildNodeListFromCache()
	} else {
		nodeList, _, err = g.discoverNodes(ctx)
		if err != nil {
			g.Log.Infof("error while getting the node list: %v", err)
			return err
		}

	}

	// Now add i=2258 to the nodeList, which is the CurrentTime node, which is used for heartbeats
	// This is only added if the heartbeat is enabled
	// instead of i=2258 the g.HeartbeatNodeId is used, which can be different in tests
	if g.UseHeartbeat {

		// Check if the node is already in the list
		for _, node := range nodeList {
			if node.NodeID.Namespace() == g.HeartbeatNodeId.Namespace() && node.NodeID.IntID() == g.HeartbeatNodeId.IntID() {
				g.HeartbeatManualSubscribed = true
				break
			}
		}

		// If the node is not in the list, add it
		if !g.HeartbeatManualSubscribed {
			heartbeatNodeID := g.HeartbeatNodeId

			// Copied and pasted from above, just for one node
			nodeHeartbeatChan := make(chan NodeDef, 1)
			errChanHeartbeat := make(chan error, 1)
			opcuaBrowserChanHeartbeat := make(chan BrowseDetails, 1)
			var wgHeartbeat TrackedWaitGroup

			wgHeartbeat.Add(1)
			wrapperNodeID := NewOpcuaNodeWrapper(g.Client.Node(heartbeatNodeID))
			go browse(ctx, wrapperNodeID, "", g.Log, heartbeatNodeID.String(), nodeHeartbeatChan, errChanHeartbeat, &wgHeartbeat, opcuaBrowserChanHeartbeat, &g.visited, g.ServerProfile)

			wgHeartbeat.Wait()
			close(nodeHeartbeatChan)
			close(errChanHeartbeat)
			close(opcuaBrowserChanHeartbeat)

			for node := range nodeHeartbeatChan {
				nodeList = append(nodeList, node)
			}
			UpdateNodePaths(nodeList)
			if len(errChanHeartbeat) > 0 {
				return <-errChanHeartbeat
			}
		}
	}

	b, err := json.Marshal(nodeList)
	if err != nil {
		g.Log.Errorf("Unmarshalling failed: %s", err)
		_ = g.Close(ctx) // ensure that if something fails here, the connection is always safely closed
		return err
	}

	g.Log.Infof("Detected nodes: %s", b)

	g.NodeList = nodeList

	// If subscription is enabled, start subscribing to the nodes
	if g.SubscribeEnabled {
		g.Log.Infof("Subscription is enabled, therefore start subscribing to the selected notes...")

		g.Subscription, err = g.Client.Subscribe(ctx, &opcua.SubscriptionParameters{
			Interval: opcua.DefaultSubscriptionInterval,
		}, g.SubNotifyChan)
		if err != nil {
			g.Log.Errorf("Subscribing failed: %s", err)
			_ = g.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return err
		}

		monitoredNodes, err := g.MonitorBatched(ctx, nodeList)
		if err != nil {
			g.Log.Errorf("Monitoring failed: %s", err)
			_ = g.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return err
		}

		g.Log.Infof("Subscribed to %d nodes!", monitoredNodes)

	}

	return nil
}

// isNumericDataType checks if OPC UA type supports deadband filtering.
// Only numeric types (Int, UInt, Float, Double) can use DataChangeFilter.
// Returns true if deadband filter should be applied.
func isNumericDataType(typeID ua.TypeID) bool {
	numericTypes := map[ua.TypeID]bool{
		ua.TypeIDDouble: true,
		ua.TypeIDFloat:  true,
		ua.TypeIDInt16:  true,
		ua.TypeIDInt32:  true,
		ua.TypeIDInt64:  true,
		ua.TypeIDUint16: true,
		ua.TypeIDUint32: true,
		ua.TypeIDUint64: true,
		ua.TypeIDByte:   true,
		ua.TypeIDSByte:  true,
	}
	return numericTypes[typeID]
}

// MonitorBatched splits the nodes into manageable batches and starts monitoring them.
// This approach prevents the server from returning BadTcpMessageTooLarge by avoiding oversized monitoring requests.
//
// Batch Size Selection (ServerProfile.MaxBatchSize):
// Batch size controls nodes per CreateMonitoredItems call during Subscribe phase.
// This is different from workers (Browse phase) - batch size affects subscription setup, not browsing.
//
// Why batch size matters:
// - Too large = server rejects request (BadTcpMessageTooLarge, connection drop)
// - Too small = slow subscription setup (more API round-trips)
// - Sweet spot varies by server: S7-1200 = 100, Ignition/Kepware = 1000
//
// Performance trade-offs (validated in UMH-ENG-3852):
// - S7-1200 with batchSize=100: Fast subscription setup
// - S7-1200 with batchSize=200+: 50× slower (server throttles/times out)
// - Ignition/Kepware with batchSize=1000: 10× faster than batchSize=100
//
// Profile-based values come from ServerProfile.MaxBatchSize (see server_profiles.go).
// Each profile is tuned for specific server hardware and tested in production.
//
// Deadband Filtering:
// If deadbandType is set (absolute/percent) and deadbandValue > 0, the function applies
// server-side DataChangeFilter to reduce notification traffic by 50-70%. The filter
// suppresses notifications unless values change beyond the specified threshold.
// Note: Not all OPC UA servers support deadband filtering - unsupported servers will
// ignore the Filter field and send all data change notifications as usual.
//
// It returns the total number of nodes that were successfully monitored or an error if monitoring fails.
func (g *OPCUAInput) MonitorBatched(ctx context.Context, nodes []NodeDef) (int, error) {
	// Use profile-based batch size
	maxBatchSize := g.ServerProfile.MaxBatchSize
	if maxBatchSize == 0 {
		panic(fmt.Sprintf(
			"PROGRAMMING ERROR: ServerProfile.MaxBatchSize is 0. "+
				"This means ServerProfile was not initialized before MonitorBatched() was called. "+
				"Profile name: '%s'. This indicates a bug in the Connect() initialization flow.",
			g.ServerProfile.Name))
	}
	totalMonitored := 0
	totalNodes := len(nodes)

	if len(nodes) == 0 {
		g.Log.Errorf("Did not subscribe to any nodes. This can happen if the nodes that are selected are incompatible with this benthos version. Aborting...")
		return 0, fmt.Errorf("no valid nodes selected")
	}

	g.Log.With("batchSize", maxBatchSize).
		With("profile", g.ServerProfile.Name).
		Infof("Starting to monitor %d nodes in batches of %d", totalNodes, maxBatchSize)

	for startIdx := 0; startIdx < totalNodes; startIdx += maxBatchSize {
		endIdx := startIdx + maxBatchSize
		if endIdx > totalNodes {
			endIdx = totalNodes
		}

		batch := nodes[startIdx:endIdx]
		g.Log.Infof("Creating monitor for nodes %d to %d", startIdx, endIdx-1)

		monitoredRequests := make([]*ua.MonitoredItemCreateRequest, 0, len(batch))

		numFilteredNodes := 0
		for pos, nodeDef := range batch {
			var filter *ua.ExtensionObject

			// Only apply deadband filter to numeric node types
			if isNumericDataType(nodeDef.DataTypeID) && g.DeadbandType != "none" {
				filter = createDataChangeFilter(g.DeadbandType, g.DeadbandValue)
				numFilteredNodes++
			} else {
				// Non-numeric nodes: subscribe without filter
				filter = nil
				if g.DeadbandType != "none" {
					g.Log.Debugf("Skipping deadband for non-numeric node %s (type: %v)",
						nodeDef.NodeID, nodeDef.DataTypeID)
				}
			}

			request := &ua.MonitoredItemCreateRequest{
				ItemToMonitor: &ua.ReadValueID{
					NodeID:       nodeDef.NodeID,
					AttributeID:  ua.AttributeIDValue,
					DataEncoding: &ua.QualifiedName{},
				},
				MonitoringMode: ua.MonitoringModeReporting,
				RequestedParameters: &ua.MonitoringParameters{
					ClientHandle:     uint32(startIdx + pos),
					DiscardOldest:    true,
					Filter:           filter,
					QueueSize:        g.QueueSize,
					SamplingInterval: g.SamplingInterval,
				},
			}
			monitoredRequests = append(monitoredRequests, request)
		}

		response, err := g.Subscription.Monitor(ctx, ua.TimestampsToReturnBoth, monitoredRequests...)
		if err != nil {
			g.Log.Errorf("Failed to monitor batch %d-%d: %v", startIdx, endIdx-1, err)
			if closeErr := g.Close(ctx); closeErr != nil {
				g.Log.Errorf("Failed to close OPC UA connection: %v", closeErr)
			}
			return totalMonitored, fmt.Errorf("monitoring failed for batch %d-%d: %w", startIdx, endIdx-1, err)
		}

		if response == nil {
			g.Log.Error("Received nil response from Monitor call")
			if closeErr := g.Close(ctx); closeErr != nil {
				g.Log.Errorf("Failed to close OPC UA connection: %v", closeErr)
			}
			return totalMonitored, errors.New("received nil response from Monitor")
		}

		for i, result := range response.Results {
			if !errors.Is(result.StatusCode, ua.StatusOK) {
				failedNode := batch[i].NodeID.String()

				// Record metric for subscription failure
				RecordSubscriptionFailure(result.StatusCode, failedNode)

				g.Log.Errorf("Failed to monitor node %s: %v", failedNode, result.StatusCode)
				// Depending on requirements, you might choose to continue monitoring other nodes
				// instead of aborting. Here, we abort on the first failure.
				// g.Log.Debugf("MonitoredItem OK ns=%d;id=%s -> revisedSampling=%.0fms revisedQueue=%d itemID=%d",
				//	batch[i].NodeID.Namespace(), batch[i].NodeID.String(),
				//	result.RevisedSamplingInterval, result.RevisedQueueSize, result.MonitoredItemID)
				if closeErr := g.Close(ctx); closeErr != nil {
					g.Log.Errorf("Failed to close OPC UA connection: %v", closeErr)
				}
				return totalMonitored, fmt.Errorf("monitoring failed for node %s: %v", failedNode, result.StatusCode)
			}
		}

		monitoredNodes := len(response.Results)
		totalMonitored += monitoredNodes
		g.Log.Infof("Successfully monitored %d nodes in current batch", monitoredNodes)
		if g.DeadbandType != "none" {
			g.Log.Infof("Batch %d-%d: Applied %s deadband filter to %d numeric nodes (threshold: %.2f)",
				startIdx, endIdx-1, g.DeadbandType, numFilteredNodes, g.DeadbandValue)
		}
		time.Sleep(time.Second) // Sleep for some time to prevent overloading the server
	}

	g.Log.Infof("Monitoring completed. Total nodes monitored: %d/%d", totalMonitored, totalNodes)
	return totalMonitored, nil
}

// UpdateNodePaths updates the node paths to use the nodeID instead of the browseName
// if the browseName is not unique
func UpdateNodePaths(nodes []NodeDef) {
	// Track which paths have been seen and how many times
	pathCount := make(map[string][]int)

	// Count occurrences of each path and track indices
	for i, node := range nodes {
		pathCount[node.Path] = append(pathCount[node.Path], i)
	}

	// Update paths that have duplicates
	for path, indices := range pathCount {
		if len(indices) > 1 {
			// This path appears multiple times, update all of them
			for _, idx := range indices {
				nodePathSplit := strings.Split(path, ".")
				parentPath := ""
				if len(nodePathSplit) > 1 {
					parentPath = strings.Join(nodePathSplit[:len(nodePathSplit)-1], ".")
				}
				// Use join() to avoid leading dots for single-segment paths
				nodePath := join(parentPath, sanitize(nodes[idx].NodeID.String()))
				nodes[idx].Path = nodePath
			}
		}
	}
}

// buildNodeListFromCache enumerates the visited map
// and returns a slice of NodeDef from all cached nodes.
func (g *OPCUAInput) buildNodeListFromCache() []NodeDef {
	var nodeList []NodeDef
	g.visited.Range(func(k, v any) bool {
		vni, ok := v.(VisitedNodeInfo)
		if !ok {
			return true
		}
		// skip incomplete nodes, or include them anyway:
		// if !vni.FullyDiscovered { ... }
		nodeList = append(nodeList, vni.Def)
		return true
	})
	return nodeList
}

func (g *OPCUAInput) canSkipDiscovery() bool {
	for _, nodeID := range g.NodeIDs {
		val, found := g.visited.Load(nodeID)
		if !found {
			return false
		}
		vni, ok := val.(VisitedNodeInfo)
		if !ok {
			return false
		}
		if !vni.FullyDiscovered || time.Since(vni.LastSeen) > StaleTime {
			return false
		}
	}
	return true
}
