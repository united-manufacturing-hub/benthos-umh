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

	// Phase 2, Task 2.1: Create ONE GlobalWorkerPool for ALL browse operations
	// This replaces the per-browse worker pool pattern (each browse() call creating its own pool).
	// Example: Agristo has 300 NodeIDs × 5 workers = 1,500 concurrent (exceeds 64 server capacity).
	// With global pool: MaxWorkers=20 (from profile) caps concurrent operations safely.
	pool := NewGlobalWorkerPool(g.ServerProfile, g.Log)
	// Note: Shutdown moved to explicit call after wg.Wait() to prevent race condition (see line 150)

	// Spawn initial workers based on profile.MinWorkers
	// Profile determines optimal worker count (e.g., Ignition=5, Auto=1, S7-1200=2)
	workersSpawned := pool.SpawnWorkers(g.ServerProfile.MinWorkers)
	g.Log.Debugf("GlobalWorkerPool spawned %d initial workers for browse operations", workersSpawned)

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

	// Phase 2, Task 2.1: Submit tasks to GlobalWorkerPool
	// Note: Worker loop currently has stub implementation (Phase 1).
	// Task 2.2 will integrate actual browse() logic into workers.
	// For now, we keep old browse() spawning pattern to maintain functionality.
	g.Log.Debugf("Submitting %d NodeIDs as tasks to GlobalWorkerPool", len(g.NodeIDs))
	submittedCount := 0
	for _, nodeID := range g.NodeIDs {
		if nodeID == nil {
			continue
		}

		// Submit task to pool (stub for now - Task 2.2 will make this functional)
		// Note: ResultChan/ErrChan not set yet due to type mismatch (chan NodeDef vs chan<- any)
		// Task 2.2 will refactor GlobalPoolTask to handle proper types
		task := GlobalPoolTask{
			NodeID:     nodeID.String(),
			ResultChan: nil, // Keep nil (type mismatch - Task 2.2 will fix)
			ErrChan:    nil,
		}
		if err := pool.SubmitTask(task); err != nil {
			g.Log.Warnf("Failed to submit task for NodeID %s: %v", nodeID.String(), err)
		} else {
			submittedCount++
		}

		// TODO Phase 2, Task 2.3: Remove this old pattern once GlobalPoolTask calls browse()
		// For now, keep spawning browse() goroutines to maintain functionality
		g.Log.Debugf("Browsing nodeID: %s", nodeID.String())
		wg.Add(1)
		wrapperNodeID := NewOpcuaNodeWrapper(g.Client.Node(nodeID))
		go browse(timeoutCtx, wrapperNodeID, "", g.Log, nodeID.String(), nodeChan, errChan, &wg, opcuaBrowserChan, &g.visited, g.ServerProfile)
	}

	g.Log.Debugf("Successfully submitted %d/%d tasks to GlobalWorkerPool (buffer capacity: %d)",
		submittedCount, len(g.NodeIDs), cap(pool.taskChan))

	// TODO Phase 2, Task 2.2: Add pool.WaitForCompletion() to verify all tasks processed

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

	// Explicit shutdown after all browse() goroutines complete
	// This ensures wg.Wait() finishes before pool workers are terminated
	// Critical for Task 2.2 when browse() logic moves into pool workers
	if err := pool.Shutdown(30 * time.Second); err != nil {
		g.Log.Warnf("GlobalWorkerPool shutdown timeout: %v", err)
	}

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

// BatchRange represents a range of indices for batch processing
type BatchRange struct {
	Start int
	End   int
}

// CalculateBatches splits totalNodes into batches of maxBatchSize and returns the ranges
func CalculateBatches(totalNodes, maxBatchSize int) []BatchRange {
	var batches []BatchRange
	for startIdx := 0; startIdx < totalNodes; startIdx += maxBatchSize {
		endIdx := startIdx + maxBatchSize
		if endIdx > totalNodes {
			endIdx = totalNodes
		}
		batches = append(batches, BatchRange{Start: startIdx, End: endIdx})
	}
	return batches
}

// decideDataChangeFilterSupport determines whether to use DataChangeFilter based on profile capability.
// Returns (supportsFilter bool, shouldTrial bool).
//
// The function implements three-way decision logic using FilterCapability enum:
//   - FilterSupported: Use filter immediately without trial (Decision 1)
//   - FilterUnsupported: Never use filter, skip trial (Decision 2)
//   - FilterUnknown: Trial on first batch, cache result for subsequent batches (Decision 3)
//
// This replaces hardcoded vendor checks (e.g., if g.ServerProfile.Name == ProfileS71200)
// with declarative profile configuration, enabling zero-code support for new vendors.
func (g *OPCUAInput) decideDataChangeFilterSupport() (bool, bool) {
	switch g.ServerProfile.FilterCapability {
	case FilterSupported:
		// Decision 1: Profile declares support → trust immediately
		return true, false
	case FilterUnsupported:
		// Decision 2: Profile declares no support → never trial
		return false, false
	case FilterUnknown:
		// Decision 3: Unknown support → trial-based discovery
		// Early return for nil capabilities - use profile fallback
		if g.ServerCapabilities == nil {
			g.Log.Warnf("DataChangeFilter: ServerCapabilities not initialized, using profile default=%v", g.ServerProfile.SupportsDataChangeFilter)
			return g.ServerProfile.SupportsDataChangeFilter, false
		}

		// Trial if not yet attempted this connection
		if !g.ServerCapabilities.hasTrialedThisConnection {
			return true, true  // Trial attempt
		}

		// Use cached trial result
		return g.ServerCapabilities.SupportsDataChangeFilter, false
	default:
		// Invalid enum value - defensive fallback to trial mechanism
		g.Log.Warnf("Invalid FilterCapability value - defaulting to trial mechanism (filterCapability=%d)", g.ServerProfile.FilterCapability)
		return true, true
	}
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
// If deadbandType is set (absolute/percent), deadbandValue > 0, AND server supports
// DataChangeFilter (detected via profile defaults), the function
// applies server-side filtering to reduce notification traffic by 50-70%.
//
// Server capability detection uses profile-based defaults:
// 1. Profile-based defaults (ServerProfile.SupportsDataChangeFilter)
//
// If server doesn't support DataChangeFilter (e.g., S7-1200 with Micro Embedded Device
// profile), filters are omitted to prevent StatusBadFilterNotAllowed errors.
// All data change notifications will be sent without server-side filtering.
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

	// Three-way DataChangeFilter Decision Logic (Hardware-Validated on S7-1200 PLCs)
	//
	// This logic was validated through hardware tests on production S7-1200 PLCs (ENG-3880).
	// All three decision paths were tested on real PLCs at 10.13.37.180 and 10.13.37.183.
	//
	// **Decision 1**: Profile explicitly supports filter → Use immediately
	//   - Example: Kepware, S7-1500, Ignition, Prosys
	//   - No trial needed, filter always works
	//   - Hardware test: Validated via unit tests (no Kepware PLC available)
	//
	// **Decision 2**: S7-1200 profile → Never trial, skip filter
	//   - Critical: S7-1200 uses Micro Embedded Device Server profile (OPC UA Part 7, no DataChangeFilter)
	//   - Trial would always fail with StatusBadFilterNotAllowed (0x80450000)
	//   - This check prevents infinite retry loops on known-unsupported servers
	//   - Hardware test: PLC 180 at 19:25:46 - autodetected profile="siemens-s7-1200" from ProductURI
	//     → shouldTrial=false, supportsFilter=false → subscription succeeded
	//   - Hardware test: PLC 180 at 19:20:07 on master branch (hardcoded filter) → StatusBadFilterNotAllowed
	//
	// **Decision 3**: Unknown profile + not trialed → Trial on first batch
	//   - Attempt filter on first batch to discover runtime capability
	//   - If StatusBadFilterNotAllowed → cache failure, recursive retry without filter (see line 443)
	//   - If successful → cache success, use filter on subsequent batches
	//   - Hardware test: PLC 180 at 19:28:23 with profile="unknown" → trial succeeded
	//     → shouldTrial=true, supportsFilter=true → DataChangeFilter worked (PLC reconfigured between tests)
	//   - Cached result in hasTrialedThisConnection prevents repeated trials on same connection
	//
	// This prevents:
	// - Unnecessary trials on known-unsupported servers (Decision 2 - S7-1200 special case)
	// - Missing filter support on unknown servers (Decision 3 - trial-based discovery)
	// - Repeated trial failures (hasTrialedThisConnection cache prevents loops)
	// - Breaking existing working servers (Decision 1 - trusted profiles use filter immediately)
	//
	// See ENG-3880 Linear ticket for complete hardware test logs, screenshots, and validation evidence.

	// Use FilterCapability enum to decide filter support (replaces hardcoded vendor checks)
	supportsFilter, shouldTrial := g.decideDataChangeFilterSupport()

	// Log decision for debugging
	switch g.ServerProfile.FilterCapability {
	case FilterSupported:
		g.Log.Debugf("DataChangeFilter enabled by profile (profile=%s, FilterCapability=FilterSupported)", g.ServerProfile.Name)
	case FilterUnsupported:
		g.Log.Debugf("DataChangeFilter disabled by profile (profile=%s, FilterCapability=FilterUnsupported)", g.ServerProfile.Name)
	case FilterUnknown:
		if shouldTrial {
			g.Log.Infof("DataChangeFilter: Attempting trial for profile=%s (FilterCapability=FilterUnknown, first batch)", g.ServerProfile.Name)
		} else {
			g.Log.Debugf("DataChangeFilter: Using cached trial result=%v (profile=%s, FilterCapability=FilterUnknown)", supportsFilter, g.ServerProfile.Name)
		}
	}

	g.Log.With("batchSize", maxBatchSize).
		With("profile", g.ServerProfile.Name).
		With("supportsFilter", supportsFilter).
		With("shouldTrial", shouldTrial).
		Infof("Starting to monitor %d nodes in batches of %d", totalNodes, maxBatchSize)

	batches := CalculateBatches(totalNodes, maxBatchSize)
	for _, batchRange := range batches {
		batch := nodes[batchRange.Start:batchRange.End]
		g.Log.Infof("Creating monitor for nodes %d to %d", batchRange.Start, batchRange.End-1)

		monitoredRequests := make([]*ua.MonitoredItemCreateRequest, 0, len(batch))

		numFilteredNodes := 0
		for pos, nodeDef := range batch {
			var filter *ua.ExtensionObject

			// Only apply deadband filter if:
			// 1. Server supports DataChangeFilter (capability check)
			// 2. Node is numeric data type (required for deadband)
			if supportsFilter && isNumericDataType(nodeDef.DataTypeID) {
				filter = createDataChangeFilter(g.DeadbandType, g.DeadbandValue)
				numFilteredNodes++
			} else {
				filter = nil

				// Log why filter was skipped (expanded debug messaging)
				if !supportsFilter {
					g.Log.Debugf("Skipping deadband for node %s: server does not support DataChangeFilter (profile=%s, runtime=%v)",
						nodeDef.NodeID, g.ServerProfile.Name, g.ServerCapabilities != nil)
				} else if !isNumericDataType(nodeDef.DataTypeID) {
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
					ClientHandle:     uint32(batchRange.Start + pos),
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
			g.Log.Errorf("Failed to monitor batch %d-%d: %v", batchRange.Start, batchRange.End-1, err)
			if closeErr := g.Close(ctx); closeErr != nil {
				g.Log.Errorf("Failed to close OPC UA connection: %v", closeErr)
			}
			return totalMonitored, fmt.Errorf("monitoring failed for batch %d-%d: %w", batchRange.Start, batchRange.End-1, err)
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

				// Trial-and-retry logic (Hardware-Validated on S7-1200 PLCs)
				//
				// When shouldTrial=true and server rejects filter with StatusBadFilterNotAllowed:
				// 1. Update ServerCapabilities cache (hasTrialedThisConnection=true, SupportsDataChangeFilter=false)
				// 2. Recursive retry: MonitorBatched() called again with same nodes
				// 3. Second iteration hits Decision 3b (line 353-357, cached result) → omits filters
				// 4. Prevents infinite loops via hasTrialedThisConnection gate
				//
				// Hardware test validation (ENG-3880):
				// - PLC 180 (10.13.37.180:4840) initially rejected filter on master branch at 19:20:07
				// - After implementing this fix, PLC 180 with profile="unknown" automatically retried at 19:28:23
				//   (note: PLC was reconfigured between tests, so trial succeeded instead of triggering this path)
				// - This mechanism allows graceful degradation from deadband filtering to no filtering
				//   without requiring user intervention or config changes
				// - Second attempt uses cached false result, no repeated trials on subsequent subscriptions
				//
				// This mechanism is critical for S7-1200 PLCs and other servers that don't advertise
				// their DataChangeFilter limitations through ServerProfileArray (which most vendors don't populate).
				if shouldTrial && errors.Is(result.StatusCode, ua.StatusBadFilterNotAllowed) {
					// Trial failed - server doesn't support DataChangeFilter
					g.Log.Infof("DataChangeFilter trial failed for node %s: %v. This is expected for servers using Micro Embedded Device profile (e.g., S7-1200 PLCs). Updating capabilities cache and retrying without filter. Future subscriptions will skip filter automatically.",
						failedNode, result.StatusCode)

					// Update ServerCapabilities with learned result
					if g.ServerCapabilities != nil {
						g.ServerCapabilities.hasTrialedThisConnection = true
						g.ServerCapabilities.SupportsDataChangeFilter = false
					}

					// Retry from failed node onwards - the three-way logic will now hit Decision 3b (cached false)
					// This prevents infinite loops since hasTrialedThisConnection=true now
					// This prevents duplicate subscriptions for already-successful nodes
					g.Log.Debugf("Retrying from failed node onwards without DataChangeFilter (recursive call with cached result)...")

					// Calculate starting index: batchRange.Start (batch offset) + i (position within batch)
					failedNodeIndex := batchRange.Start + i

					// Recursive call: Only pass nodes from failed point onwards
					// This prevents re-monitoring nodes 0 to (failedNodeIndex-1) which already succeeded
					return g.MonitorBatched(ctx, nodes[failedNodeIndex:])
				}

				// Non-trial error or different error code - propagate normally
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

		// After successful batch processing, mark trial success if this was a trial
		if shouldTrial && g.ServerCapabilities != nil {
			g.ServerCapabilities.hasTrialedThisConnection = true
			g.ServerCapabilities.SupportsDataChangeFilter = true
			g.Log.Infof("DataChangeFilter trial succeeded. Server supports filter - capability confirmed and cached for this connection.")
		}

		monitoredNodes := len(response.Results)
		totalMonitored += monitoredNodes
		g.Log.Infof("Successfully monitored %d nodes in current batch", monitoredNodes)
		if g.DeadbandType != "none" {
			g.Log.Infof("Batch %d-%d: Applied %s deadband filter to %d numeric nodes (threshold: %.2f)",
				batchRange.Start, batchRange.End-1, g.DeadbandType, numFilteredNodes, g.DeadbandValue)
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
