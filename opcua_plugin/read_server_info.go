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
	"fmt"
	"time"

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

	// Setup channels for node discovery
	nodeChan := make(chan NodeDef, 3)
	errChan := make(chan error, 3)

	// Use profile or fallback to Auto if called before profile detection
	profile := g.ServerProfile
	if profile.Name == "" {
		profile = GetProfileByName(ProfileAuto)
	}

	// Create GlobalWorkerPool for browsing 3 server metadata nodes
	pool := NewGlobalWorkerPool(profile, g.Log)
	workersSpawned := pool.SpawnWorkers(profile.MinWorkers)
	g.Log.Debugf("Server info detection: spawned %d workers", workersSpawned)

	defer func() {
		if err := pool.Shutdown(DefaultPoolShutdownTimeout); err != nil {
			g.Log.Warnf("GlobalWorkerPool shutdown timeout: %v", err)
		}
	}()

	// Define 3 server info nodes to browse
	serverInfoNodes := []struct {
		nodeID *ua.NodeID
		name   string
	}{
		{ua.NewNumericNodeID(0, 2263), "ManufacturerName"},
		{ua.NewNumericNodeID(0, 2261), "ProductName"},
		{ua.NewNumericNodeID(0, 2264), "SoftwareVersion"},
	}

	// Submit browse tasks to GlobalWorkerPool
	for _, node := range serverInfoNodes {
		task := GlobalPoolTask{
			NodeID:       node.nodeID.String(),
			Ctx:          ctx,
			Node:         NewOpcuaNodeWrapper(g.Client.Node(node.nodeID)),
			Path:         "",
			Level:        0,
			ParentNodeID: node.nodeID.String(),
			Visited:      &g.visited,
			ResultChan:   nodeChan,
			ErrChan:      errChan,
			ProgressChan: nil, // No progress reporting for 3-node browse
		}

		if err := pool.SubmitTask(task); err != nil {
			return ServerInfo{}, fmt.Errorf("failed to submit %s task: %w", node.name, err)
		}
	}

	// Wait for all 3 tasks to complete
	if err := pool.WaitForCompletion(30 * time.Second); err != nil {
		return ServerInfo{}, fmt.Errorf("browse completion timeout: %w", err)
	}

	close(nodeChan)
	close(errChan)

	// Check for errors
	if len(errChan) > 0 {
		return ServerInfo{}, <-errChan
	}

	// Collect discovered nodes
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
			continue
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

// OPC UA ServerCapabilities NodeIDs (Part 5 - Information Model)
// Verified against OPC Foundation NodeIds.csv v1.04
var (
	ServerCapabilitiesNodeID          = ua.NewNumericNodeID(0, 2268)  // ServerCapabilities
	OperationLimitsNodeID             = ua.NewNumericNodeID(0, 11704) // OperationLimits
	MaxNodesPerBrowseNodeID           = ua.NewNumericNodeID(0, 11710) // Fixed: was 11712
	MaxMonitoredItemsPerCallNodeID    = ua.NewNumericNodeID(0, 11714) // Correct (CodeRabbit was wrong)
	MaxNodesPerReadNodeID             = ua.NewNumericNodeID(0, 11705) // Correct
	MaxNodesPerWriteNodeID            = ua.NewNumericNodeID(0, 11707) // Fixed: was 11708
	MaxBrowseContinuationPointsNodeID = ua.NewNumericNodeID(0, 3089)  // Fixed: was 12165
)

// IMPORTANT: Why We Don't Use ServerProfileArray for DataChangeFilter Detection
//
// The OPC UA specification (Part 7, Section 6.4.3) defines ServerProfileArray (NodeID 2269)
// as the standard mechanism for profile-based capability detection. In theory, servers
// should declare conformance to specification facets via this array.
//
// In practice, major OPC UA servers do NOT reliably populate ServerProfileArray:
//
// 1. Kepware KEPServerEX: ServerProfileArray is empty or not exposed
// 2. Ignition Gateway (Eclipse Milo): ServerProfileArray is not populated
// 3. Siemens S7 PLCs: ServerProfileArray exists but is inconsistently populated
//    - S7-1200: May not declare Micro Embedded Device profile
//    - S7-1500: May not declare Standard facet despite supporting filters
//
// This widespread non-compliance makes ServerProfileArray unusable for reliable detection.
// Instead, we implement a hybrid approach:
// 1. Profile defaults (production-validated, prevent wasted attempts for S7-1200)
// 2. Trial-based learning (try with filter, catch StatusBadFilterNotAllowed, retry without)
//
// Note: Trial-based learning will be implemented in the MonitorBatched function (read_discover.go).
// The current implementation uses preventative checking based on profile defaults.
//
// This approach is self-correcting and handles firmware upgrades that add filter support.

// queryOperationLimits queries the OPC UA server for its operation limits from OperationLimits node.
//
// Returns ServerCapabilities with operation limits populated from OperationLimits node:
// - MaxNodesPerBrowse, MaxMonitoredItemsPerCall, MaxNodesPerRead, MaxNodesPerWrite, MaxBrowseContinuationPoints
//
// Note: Does NOT query ServerProfileArray - major vendors (Kepware, Ignition, Siemens) don't reliably populate it.
// DataChangeFilter support comes from profile-based defaults in ServerProfile struct instead.
//
// Returns error if the server doesn't support OperationLimits (common for PLCs like S7-1200/1500).
// Read-only logging for diagnostics.
func (g *OPCUAInput) queryOperationLimits(ctx context.Context) (*ServerCapabilities, error) {
	if g == nil || g.OPCUAConnection == nil || g.Client == nil {
		return nil, errors.New("client is nil")
	}

	caps := &ServerCapabilities{}

	// Query OperationLimits nodes
	nodeIDs := []*ua.NodeID{
		MaxNodesPerBrowseNodeID,
		MaxMonitoredItemsPerCallNodeID,
		MaxNodesPerReadNodeID,
		MaxNodesPerWriteNodeID,
		MaxBrowseContinuationPointsNodeID,
	}

	var nodesToRead []*ua.ReadValueID
	for _, nodeID := range nodeIDs {
		nodesToRead = append(nodesToRead, &ua.ReadValueID{
			NodeID: nodeID,
		})
	}

	req := &ua.ReadRequest{
		MaxAge:             2000,
		NodesToRead:        nodesToRead,
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	resp, err := g.Read(ctx, req)
	if err != nil {
		return nil, err
	}

	if len(resp.Results) != len(nodeIDs) {
		return nil, errors.New("unexpected number of results")
	}

	// Parse results - many servers return BadNodeIdUnknown if they don't support this
	// Map index to capability field for cleaner assignment
	setCapability := []func(uint32){
		func(v uint32) { caps.MaxNodesPerBrowse = v },
		func(v uint32) { caps.MaxMonitoredItemsPerCall = v },
		func(v uint32) { caps.MaxNodesPerRead = v },
		func(v uint32) { caps.MaxNodesPerWrite = v },
		func(v uint32) { caps.MaxBrowseContinuationPoints = v },
	}

	for i, result := range resp.Results {
		if result.Status != ua.StatusOK || result.Value == nil {
			// Skip unsupported capabilities (server may not expose all limits)
			continue
		}

		// All OperationLimits values are UInt32
		if value, ok := result.Value.Value().(uint32); ok {
			setCapability[i](value)
		}
	}

	return caps, nil
}

// logServerCapabilities logs the discovered server operation limits and compares them
// with the current profile settings. Warns if the profile exceeds server limits.
func (g *OPCUAInput) logServerCapabilities(caps *ServerCapabilities) {
	if caps == nil || g.Log == nil {
		return
	}

	// Only log if at least one operation limit is available
	hasLimits := caps.MaxMonitoredItemsPerCall > 0 || caps.MaxNodesPerBrowse > 0 ||
		caps.MaxNodesPerRead > 0 || caps.MaxNodesPerWrite > 0 ||
		caps.MaxBrowseContinuationPoints > 0

	if !hasLimits {
		g.Log.Info("OperationLimits not available (normal for many PLCs) - using profile defaults")
		return
	}

	g.Log.Info("Server OperationLimits discovered:")

	// Log MaxMonitoredItemsPerCall with profile comparison
	if caps.MaxMonitoredItemsPerCall > 0 {
		g.Log.Infof("  MaxMonitoredItemsPerCall: %d (profile MaxBatchSize: %d)",
			caps.MaxMonitoredItemsPerCall, g.ServerProfile.MaxBatchSize)

		if uint32(g.ServerProfile.MaxBatchSize) > caps.MaxMonitoredItemsPerCall {
			g.Log.Warnf("Profile MaxBatchSize (%d) exceeds server limit MaxMonitoredItemsPerCall (%d) - subscriptions may fail",
				g.ServerProfile.MaxBatchSize, caps.MaxMonitoredItemsPerCall)
		}
	}

	// Log other limits without profile comparison (profile doesn't have these fields yet)
	if caps.MaxNodesPerBrowse > 0 {
		g.Log.Infof("  MaxNodesPerBrowse: %d", caps.MaxNodesPerBrowse)
	}

	if caps.MaxNodesPerRead > 0 {
		g.Log.Infof("  MaxNodesPerRead: %d", caps.MaxNodesPerRead)
	}

	if caps.MaxNodesPerWrite > 0 {
		g.Log.Infof("  MaxNodesPerWrite: %d", caps.MaxNodesPerWrite)
	}

	if caps.MaxBrowseContinuationPoints > 0 {
		g.Log.Infof("  MaxBrowseContinuationPoints: %d", caps.MaxBrowseContinuationPoints)
	}

	g.Log.Info("Using server-reported limits for subscriptions")
}
