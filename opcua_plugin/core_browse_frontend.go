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

//
// ╔══════════════════════════════════════════════════════════════════════════╗
// ║                    LEGACY UI CODE PATH - DEPRECATED                       ║
// ╠══════════════════════════════════════════════════════════════════════════╣
// ║  This file implements the LEGACY browse workflow for ManagementConsole v1║
// ║  Used by: united-manufacturing-hub/ManagementConsole (React UI only)     ║
// ║  Entry point: GetNodeTree() → Browse() wrapper → browse()               ║
// ║                                                                          ║
// ║  ⚠️  DEPRECATION WARNING:                                                ║
// ║  • ONLY used by ManagementConsole v1 BrowseOPCUA UI (legacy React app)  ║
// ║  • Do NOT use in new code - use read_discover.go::discoverNodes()       ║
// ║  • Maintained for backwards compatibility with existing deployments     ║
// ║  • Will be removed when ManagementConsole v2 fully deployed             ║
// ║                                                                          ║
// ║  Key Differences from Production:                                        ║
// ║  • Builds hierarchical tree structure (parent-child relationships)      ║
// ║  • Uses defensive Auto profile (5 workers max, safe defaults)           ║
// ║  • One-time browse operation (no subscription to nodes)                 ║
// ║  • Tree construction overhead not suitable for production streaming     ║
// ║                                                                          ║
// ║  For production use: See read_discover.go (PRODUCTION CODE PATH)        ║
// ║  See: ARCHITECTURE.md for detailed comparison of code paths             ║
// ╚══════════════════════════════════════════════════════════════════════════╝

package opcua_plugin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/time/rate"
)

var logLimiter = rate.NewLimiter(rate.Every(1*time.Second), 1) // Allows 1 message per second

// BrowseDetails represents the details of a browse operation.
type BrowseDetails struct {
	NodeDef               NodeDef
	TaskCount             int64
	WorkerCount           int64
	AvgServerResponseTime time.Duration
}

// GetNodeTree returns the tree structure of the OPC UA server nodes for UI display.
//
// ⚠️ LEGACY CODE PATH - ManagementConsole v1 Only
//
// DEPRECATION STATUS:
// - This function is ONLY used by ManagementConsole v1 BrowseOPCUA UI (legacy React app)
// - Do NOT use in new code - production code uses read_discover.go::discoverNodes() instead
// - Maintained for backwards compatibility with existing UI installations
// - Will be removed when ManagementConsole v2 is fully deployed
//
// Why separate from production:
// - UI needs hierarchical tree structure (parent-child relationships)
// - Production needs flat node list for subscription (no tree building overhead)
// - UI uses defensive Auto profile (5 workers max)
// - Production uses auto-detected/tuned profiles (10-60 workers)
// - UI doesn't subscribe to nodes (one-time browse operation)
// - Production subscribes for continuous data streaming
//
// LEGACY UI CODE PATH:
// This function is ONLY used by united-manufacturing-hub/ManagementConsole v1 BrowseOPCUA UI.
// Production code uses read_discover.go::discoverNodes() instead.
//
// Architecture:
// - Uses same browse() implementation as production (core_browse.go)
// - Always uses Auto profile (defensive defaults: 5 workers max)
// - Builds hierarchical tree structure (parent-child relationships)
// - No subscription (one-time browse operation)
//
// Why separate from production:
// - UI needs tree structure, production needs flat node list for subscription
// - UI uses defensive profile, production uses auto-detected/tuned profile
// - UI doesn't subscribe to nodes, production subscribes for streaming
//
// See ARCHITECTURE.md for detailed comparison of production vs. legacy UI paths.
func (g *OPCUAConnection) GetNodeTree(ctx context.Context, msgChan chan<- string, rootNode *Node) (*Node, error) {
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
	opcuaBrowserChan := make(chan BrowseDetails, MaxTagsToBrowse)

	nodeIDMap := make(map[string]*NodeDef)
	nodes := make([]NodeDef, 0, MaxTagsToBrowse)

	var wg TrackedWaitGroup
	var consumerWg sync.WaitGroup
	wg.Add(1)
	// OPCUAConnection doesn't have ServerProfile - use Auto profile
	profile := GetProfileByName(ProfileAuto)
	pool := NewGlobalWorkerPool(profile, g.Log)
	defer func() {
		if err := pool.Shutdown(30 * time.Second); err != nil {
			g.Log.Warnf("GlobalWorkerPool shutdown timeout: %v", err)
		}
	}()
	Browse(ctx, NewOpcuaNodeWrapper(g.Client.Node(rootNode.NodeId)), "", pool, rootNode.NodeId.String(), nodeChan, errChan, &wg, opcuaBrowserChan, &g.visited)

	// Track consumer goroutines to ensure they finish processing before hierarchy construction
	consumerWg.Add(2)
	go func() {
		defer consumerWg.Done()
		logErrors(ctx, errChan, g.Log)
	}()
	go func() {
		defer consumerWg.Done()
		collectNodes(ctx, opcuaBrowserChan, nodeIDMap, &nodes, msgChan)
	}()

	wg.Wait()

	close(nodeChan)
	close(errChan)
	close(opcuaBrowserChan)

	// Wait for consumer goroutines to finish processing all channel data.
	// This ensures nodeIDMap and nodes[] are fully populated before hierarchy construction.
	// Synchronization guarantees:
	// 1. wg.Wait() ensures all Browse() operations completed and channels closed
	// 2. consumerWg.Wait() ensures logErrors() and collectNodes() drained all channels
	// 3. Safe to read nodes[] and nodeIDMap after this point
	consumerWg.Wait()

	// By this time, nodeIDMap and nodes are populated with the nodes and nodeIDs
	for _, node := range nodes {
		constructNodeHierarchy(rootNode, node, nodeIDMap, g.Log)
	}
	return rootNode, nil
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
func collectNodes(ctx context.Context, nodeBrowserChan chan BrowseDetails, nodeIDMap map[string]*NodeDef, nodes *[]NodeDef, msgChan chan<- string) {
	for browseRecord := range nodeBrowserChan {
		select {
		case <-ctx.Done():
			return
		default:
			if logLimiter.Allow() {
				msgChan <- fmt.Sprintf("found node '%s' (%d pending tasks, %d active browse operations, average server response time: %v ms)",
					browseRecord.NodeDef.BrowseName,
					browseRecord.TaskCount,
					browseRecord.WorkerCount,
					browseRecord.AvgServerResponseTime)
			}

			nodeID := normalizeNodeID(browseRecord.NodeDef.NodeID)
			nodeIDMap[nodeID] = &browseRecord.NodeDef
			*nodes = append(*nodes, browseRecord.NodeDef)
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
			normalizedParentID := normalizeNodeID(parentNode.NodeID)
			id, err := ua.ParseNodeID(normalizedParentID)
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

// normalizeNodeID normalizes a node id string representation by removing unwanted 's=' prefixes
// that can occur when the nodeID type is wrongly interpreted as a string type instead of a numeric type.
// The prefix 's=' happens rarely but this function acts as a defensive mechanism to handle such scenarios
func normalizeNodeID(nodeID *ua.NodeID) string {
	id := nodeID.String()
	if strings.HasPrefix(id, "s=i=") {
		return strings.TrimPrefix(id, "s=")
	}
	if strings.HasPrefix(id, "s=ns=") {
		return strings.TrimPrefix(id, "s=")
	}
	return id
}
