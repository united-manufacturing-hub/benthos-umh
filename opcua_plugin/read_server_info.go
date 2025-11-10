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

	// Use profile or fallback to Auto if called before profile detection
	profile := g.ServerProfile
	if profile.Name == "" {
		profile = GetProfileByName(ProfileAuto)
	}

	wg.Add(3)
	go Browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(manufacturerNameNodeID)), "", g.Log, manufacturerNameNodeID.String(), nodeChan, errChan, &wg, opcuaBrowserChan, &g.visited, profile)
	go Browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(productNameNodeID)), "", g.Log, productNameNodeID.String(), nodeChan, errChan, &wg, opcuaBrowserChan, &g.visited, profile)
	go Browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(softwareVersionNodeID)), "", g.Log, softwareVersionNodeID.String(), nodeChan, errChan, &wg, opcuaBrowserChan, &g.visited, profile)
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

// OPC UA ServerCapabilities NodeIDs (Part 5 - Information Model)
// Verified against OPC Foundation NodeIds.csv v1.04
var (
	ServerCapabilitiesNodeID          = ua.NewNumericNodeID(0, 2268)  // ServerCapabilities
	ServerProfileArrayNodeID          = ua.NewNumericNodeID(0, 2269)  // ServerProfileArray
	OperationLimitsNodeID             = ua.NewNumericNodeID(0, 11704) // OperationLimits
	MaxNodesPerBrowseNodeID           = ua.NewNumericNodeID(0, 11710) // Fixed: was 11712
	MaxMonitoredItemsPerCallNodeID    = ua.NewNumericNodeID(0, 11714) // Correct (CodeRabbit was wrong)
	MaxNodesPerReadNodeID             = ua.NewNumericNodeID(0, 11705) // Correct
	MaxNodesPerWriteNodeID            = ua.NewNumericNodeID(0, 11707) // Fixed: was 11708
	MaxBrowseContinuationPointsNodeID = ua.NewNumericNodeID(0, 3089)  // Fixed: was 12165
)

// Standard DataChange Subscription Server Facet URI (OPC UA Part 7, Section 6.4.3)
// Presence of this profile indicates the server supports DataChangeFilter in MonitoredItem creation.
const StandardDataChangeSubscriptionFacetURI = "http://opcfoundation.org/UA-Profile/Server/StandardDataChangeSubscription"

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
// Instead, we will implement a hybrid approach:
// 1. Profile defaults (production-validated, prevent wasted attempts for S7-1200)
// 2. Trial-based learning (try with filter, catch StatusBadFilterNotAllowed, retry without)
//
// Note: Trial-based learning will be implemented in the MonitorBatched function (read_discover.go).
// The current implementation uses preventative checking based on profile defaults.
//
// This approach is self-correcting and handles firmware upgrades that add filter support.

// detectDataChangeFilterSupport checks if the server profile array contains the Standard DataChange Subscription facet.
//
// OPC UA Part 7, Section 6.4.3 defines profile-based capability detection:
// - Servers declare conformance to specification facets via ServerProfileArray (NodeID 2269)
// - Standard DataChange Subscription Server Facet indicates full MonitoredItem filter support
// - Embedded DataChange Subscription Server Facet MAY omit filter support (resource-constrained devices)
//
// Why this matters:
// - S7-1200 implements "Micro Embedded Device Server Profile" which uses Embedded facet WITHOUT filters
// - Attempting to use DataChangeFilter on S7-1200 returns StatusBadFilterNotAllowed
// - This detection allows dynamic behavior based on actual server capabilities
//
// Returns true if the Standard DataChange Subscription Server Facet is present in the profile array,
// indicating the server supports DataChangeFilter (OPC UA Part 4, Section 7.17).
//
// Returns false if:
// - Profile array is nil (server doesn't expose ServerProfileArray)
// - Profile array is empty (no conformance profiles declared)
// - Standard facet URI is not present (e.g., Micro Embedded Device profile)
func detectDataChangeFilterSupport(profileArray []string) bool {
	// Safe default for nil or empty array
	if profileArray == nil || len(profileArray) == 0 {
		return false
	}

	// Check for exact URI match (case-sensitive per OPC UA spec)
	for _, profile := range profileArray {
		if profile == StandardDataChangeSubscriptionFacetURI {
			return true
		}
	}

	return false
}

// queryOperationLimits queries the OPC UA server for its operation limits from OperationLimits node
// and detects DataChangeFilter support via ServerProfileArray.
//
// Returns ServerCapabilities with:
// - SupportsDataChangeFilter: Populated from ServerProfileArray (OPC UA Part 7, Section 6.4.3)
// - Operation limits: Populated from OperationLimits node (MaxNodesPerBrowse, etc.)
//
// Returns error if the server doesn't support OperationLimits (common for PLCs like S7-1200/1500).
func (g *OPCUAInput) queryOperationLimits(ctx context.Context) (*ServerCapabilities, error) {
	if g == nil || g.OPCUAConnection == nil || g.Client == nil {
		return nil, errors.New("client is nil")
	}

	caps := &ServerCapabilities{}

	// Step 1: Query ServerProfileArray for profile-based capability detection
	// OPC UA Part 7, Section 6.4.3: Standard DataChange Subscription facet indicates filter support
	profileReq := &ua.ReadRequest{
		MaxAge: 2000,
		NodesToRead: []*ua.ReadValueID{
			{NodeID: ServerProfileArrayNodeID},
		},
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	// Mark as capability probe - server may not expose profile array
	probeCtx := WithCapabilityProbe(ctx)
	profileResp, err := g.Read(probeCtx, profileReq)
	if err == nil && len(profileResp.Results) > 0 {
		result := profileResp.Results[0]
		if result.Status == ua.StatusOK && result.Value != nil {
			// ServerProfileArray is Array of String
			if profileArray, ok := result.Value.Value().([]string); ok {
				caps.SupportsDataChangeFilter = detectDataChangeFilterSupport(profileArray)
			}
		}
	}
	// If profile query fails or returns non-OK status, SupportsDataChangeFilter remains false (safe default)

	// Step 2: Query OperationLimits nodes (existing behavior)
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

	// Mark this as a capability probe - failures are expected for servers without OperationLimits
	resp, err := g.Read(probeCtx, req)
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
