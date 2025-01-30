package opcua_plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/errors"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

const (
	MaxTagsToBrowse = 100_000
)

type NodeDef struct {
	NodeID       *ua.NodeID
	NodeClass    ua.NodeClass
	BrowseName   string
	Description  string
	AccessLevel  ua.AccessLevelType
	DataType     string
	ParentNodeID string // custom, not an official opcua attribute
	Path         string // custom, not an official opcua attribute
}

// join concatenates two strings with a dot separator.
//
// This function is used to construct hierarchical paths by joining parent and child
// node names. If the parent string (`a`) is empty, it returns the child string (`b`)
// without adding a dot, ensuring that paths do not start with an unnecessary separator.
func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}

// sanitize cleans a string by replacing invalid characters with underscores.
//
// OPC UA node names may contain characters that are not suitable for certain contexts
// (e.g., identifiers in code, file systems). This function ensures that the string
// only contains alphanumeric characters, hyphens, or underscores by replacing any
// invalid character with an underscore. This sanitization helps prevent errors and
// ensures consistency when using node names in various parts of the application.
func sanitize(s string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	return re.ReplaceAllString(s, "_")
}

type Logger interface {
	Debugf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
}

// Browse is a public wrapper function for the browse function
// Avoid using this function directly, use it only for testing
func Browse(ctx context.Context, n NodeBrowser, path string, level int, logger Logger, parentNodeId string, nodeChan chan NodeDef, errChan chan error, wg *TrackedWaitGroup, opcuaBrowserChan chan NodeDef, visited *sync.Map) {
	browse(ctx, n, path, level, logger, parentNodeId, nodeChan, errChan, wg, opcuaBrowserChan, visited)
}

// AttributeHandler defines how to handle different attribute statuses and values
type AttributeHandler struct {
	onOK             func(value *ua.Variant) error
	onNotReadable    func()
	onInvalidAttr    bool // whether to ignore invalid attribute errors
	requiresValue    bool // whether a nil value is acceptable
	affectsNodeClass bool // whether errors should mark node as Object
}

// handleAttributeStatus processes an attribute's status and value according to defined handlers
func handleAttributeStatus(
	attr *ua.DataValue,
	def *NodeDef,
	path string,
	logger Logger,
	handler AttributeHandler,
) error {
	switch err := attr.Status; {
	case errors.Is(err, ua.StatusOK):
		if attr.Value == nil && handler.requiresValue {
			return fmt.Errorf("attribute value is nil")
		}
		if handler.onOK != nil && attr.Value != nil {
			if err := handler.onOK(attr.Value); err != nil {
				return err
			}
		}
	case errors.Is(err, ua.StatusBadSecurityModeInsufficient):
		return errors.New("insufficient security mode")
	case errors.Is(err, ua.StatusBadAttributeIDInvalid):
		if !handler.onInvalidAttr {
			return fmt.Errorf("invalid attribute ID")
		}
		// ignore if handler allows invalid attributes
	case errors.Is(err, ua.StatusBadNotReadable):
		if handler.affectsNodeClass {
			def.NodeClass = ua.NodeClassObject
		}
		if handler.onNotReadable != nil {
			handler.onNotReadable()
		}
		logger.Warnf("Access denied for node: %s, continuing...\n", path)
	default:
		return err
	}
	return nil
}

// processNodeAttributes processes all attributes for a node
// This function is used to process the attributes of a node and set the NodeDef struct
func processNodeAttributes(attrs []*ua.DataValue, def *NodeDef, path string, logger Logger) error {
	// NodeClass (attrs[0])
	nodeClassHandler := AttributeHandler{
		onOK: func(value *ua.Variant) error {
			def.NodeClass = ua.NodeClass(value.Int())
			return nil
		},
		requiresValue:    true,
		affectsNodeClass: true,
	}
	if err := handleAttributeStatus(attrs[0], def, path, logger, nodeClassHandler); err != nil {
		return err
	}

	// BrowseName (attrs[1])
	browseNameHandler := AttributeHandler{
		onOK: func(value *ua.Variant) error {
			def.BrowseName = value.String()
			return nil
		},
		requiresValue: true,
	}
	if err := handleAttributeStatus(attrs[1], def, path, logger, browseNameHandler); err != nil {
		return err
	}

	// Description (attrs[2])
	descriptionHandler := AttributeHandler{
		onOK: func(value *ua.Variant) error {
			if value != nil {
				def.Description = value.String()
			} else {
				def.Description = ""
			}
			return nil
		},
		onInvalidAttr:    true,
		affectsNodeClass: true,
	}
	if err := handleAttributeStatus(attrs[2], def, path, logger, descriptionHandler); err != nil {
		return err
	}

	// AccessLevel (attrs[3])
	accessLevelHandler := AttributeHandler{
		onOK: func(value *ua.Variant) error {
			def.AccessLevel = ua.AccessLevelType(value.Int())
			return nil
		},
		onInvalidAttr: true,
	}
	if err := handleAttributeStatus(attrs[3], def, path, logger, accessLevelHandler); err != nil {
		return err
	}

	// Check AccessLevel None
	if def.AccessLevel == ua.AccessLevelTypeNone {
		logger.Warnf("Node %s has AccessLevel None, marking as Object\n", path)
		def.NodeClass = ua.NodeClassObject
	}

	// DataType (attrs[4])
	dataTypeHandler := AttributeHandler{
		onOK: func(value *ua.Variant) error {
			if value == nil {
				logger.Debugf("ignoring node: %s as its datatype is nil...\n", path)
				return fmt.Errorf("datatype is nil")
			}
			def.DataType = getDataTypeString(value.NodeID().IntID())
			return nil
		},
		onInvalidAttr:    true,
		affectsNodeClass: true,
	}
	if err := handleAttributeStatus(attrs[4], def, path, logger, dataTypeHandler); err != nil {
		return err
	}

	return nil
}

// getDataTypeString maps OPC UA data type IDs to Go type strings
func getDataTypeString(typeID uint32) string {
	dataTypeMap := map[uint32]string{
		id.DateTime: "time.Time",
		id.Boolean:  "bool",
		id.SByte:    "int8",
		id.Int16:    "int16",
		id.Int32:    "int32",
		id.Byte:     "byte",
		id.UInt16:   "uint16",
		id.UInt32:   "uint32",
		id.UtcTime:  "time.Time",
		id.String:   "string",
		id.Float:    "float32",
		id.Double:   "float64",
	}

	if dtype, ok := dataTypeMap[typeID]; ok {
		return dtype
	}
	return fmt.Sprintf("ns=%d;i=%d", 0, typeID)
}

// browse recursively explores OPC UA nodes to build a comprehensive list of NodeDefs.
//
// The `browse` function is essential for discovering the structure and details of OPC UA nodes.
// It performs the following operations:
//   - Fetches essential attributes of a node, such as NodeClass, BrowseName, Description,
//     AccessLevel, and DataType.
//   - Determines if a node has child components (e.g., variables within an object) and recursively
//     browses them if necessary, ensuring a complete traversal of the node hierarchy.
//
// **Why This Function is Needed:** s
//   - To build a structured representation (`nodeList`) of all relevant nodes for further processing
//     such as subscribing to data changes.
//
// **Parameters:**
// - `ctx` (`context.Context`): Manages cancellation and timeouts for the browsing operations.
// - `n` (`*opcua.Node`): The current OPC UA node to browse.
// - `path` (`string`): The hierarchical path accumulated so far.
// - `level` (`int`): The current depth level in the node hierarchy, used to prevent excessive recursion.
// - `logger` (`*service.Logger`): Logs debug and error messages for monitoring and troubleshooting.
// - `parentNodeId` (`string`): The NodeID of the parent node, used for reference tracking.
// - `nodeChan` (`chan NodeDef`): Channel to send discovered NodeDefs for collection.
// - `errChan` (`chan error`): Channel to send encountered errors for centralized handling.
// - `pathIDMapChan` (`chan map[string]string`): Channel to send the mapping of node names to NodeIDs.
// - `wg` (`*sync.WaitGroup`): WaitGroup to synchronize the completion of goroutines.
// **Returns:**
// - `void`: Errors are sent through `errChan`, and discovered nodes are sent through `nodeChan`.
func browse(ctx context.Context, n NodeBrowser, path string, level int, logger Logger, parentNodeId string, nodeChan chan NodeDef, errChan chan error, wg *TrackedWaitGroup, opcuaBrowserChan chan NodeDef, visited *sync.Map) {
	defer wg.Done()

	// Check if the current node is already visited else proceed
	if _, exists := visited.LoadOrStore(n.ID(), struct{}{}); exists {
		logger.Debugf("node %s is visited already, hence doing an early exit from the browse routine", n.ID().String())
		return
	}

	// Limits browsing depth to a maximum of 25 levels in the node hierarchy.
	// Performance impact is minimized since most browse operations terminate earlier
	// due to other exit conditions before reaching this maximum depth.
	if level > 25 {
		return
	}

	select {
	case <-ctx.Done():
		logger.Warnf("browse function received cancellation signal")
		return
	default:
		// Continue processing
	}

	attrs, err := n.Attributes(ctx, ua.AttributeIDNodeClass, ua.AttributeIDBrowseName, ua.AttributeIDDescription, ua.AttributeIDAccessLevel, ua.AttributeIDDataType)
	if err != nil {
		sendError(ctx, err, errChan, logger)
		return
	}

	if len(attrs) != 5 {
		sendError(ctx, errors.Errorf("only got %d attr, needed 5", len(attrs)), errChan, logger)
		return
	}

	browseName, err := n.BrowseName(ctx)
	if err != nil {
		sendError(ctx, err, errChan, logger)
		return
	}

	newPath := sanitize(browseName.Name)
	if path != "" {
		newPath = path + "." + newPath
	}

	var def = NodeDef{
		NodeID:       n.ID(),
		Path:         newPath,
		ParentNodeID: parentNodeId,
	}

	// def.NodeClass, def.BrowseName, def.Description, def.AccessLevel, def.DataType are set in the processNodeAttributes function
	if err := processNodeAttributes(attrs, &def, newPath, logger); err != nil {
		sendError(ctx, err, errChan, logger)
		return
	}

	logger.Debugf("%d: def.Path:%s def.NodeClass:%s\n", level, def.Path, def.NodeClass)

	browseChildrenV2 := func(refType uint32) error {
		children, err := n.Children(ctx, refType, ua.NodeClassVariable|ua.NodeClassObject)
		if err != nil {
			sendError(ctx, errors.Errorf("Children: %d: %s", refType, err), errChan, logger)
			return err
		}
		for _, child := range children {
			wg.Add(1)
			go browse(ctx, child, def.Path, level+1, logger, def.NodeID.String(), nodeChan, errChan, wg, opcuaBrowserChan, visited)
		}
		return nil
	}

	// Create a copy of the def(current Node). Do no modify the original nodeDef. This nodeDefForOPCUABrowser is used only for OPCUA browser operation
	nodeDefForOPCUABrowser := def
	nodeDefForOPCUABrowser.Path = join(path, nodeDefForOPCUABrowser.BrowseName)
	select {
	case opcuaBrowserChan <- nodeDefForOPCUABrowser:
	// do nothing if message publish to the channel is successful
	default:
		logger.Debugf("opcuaBrowserChan is blocked, skipping nodeDefForOPCUABrowser send")
	}
	// In OPC Unified Architecture (UA), hierarchical references are a type of reference that establishes a containment relationship between nodes in the address space. They are used to model a hierarchical structure, such as a system composed of components, where each component may contain sub-components.
	// Refer link: https://qiyuqi.gitbooks.io/opc-ua/content/Part3/Chapter7.html
	// HasEventSource, HasChild, HasComponent, Organizes, FolderType, HasNotifier and all their sub-hierarchical references

	switch def.NodeClass {

	// If its a variable, we add it to the node list and browse all its children
	case ua.NodeClassVariable:
		if err := browseChildrenV2(id.HierarchicalReferences); err != nil {
			sendError(ctx, err, errChan, logger)
			return
		}

		// adding it to the node list
		def.Path = join(path, def.BrowseName)
		select {
		case nodeChan <- def:
		case <-ctx.Done():
			logger.Warnf("Failed to send node due to context cancellation")
			return
		}
		return

		// If its an object, we browse all its children but DO NOT add it to the node list and therefore not subscribe
	case ua.NodeClassObject:
		if err := browseChildrenV2(id.HierarchicalReferences); err != nil {
			sendError(ctx, err, errChan, logger)
			return
		}
		return
	}
}

// Node represents a node in the tree structure
type Node struct {
	NodeId   *ua.NodeID `json:"nodeId"`
	Name     string     `json:"name"`
	Children []*Node    `json:"children,omitempty"`
	// ChildIDMap is a map of children nodes, key is the browseName
	ChildIDMap map[string]*Node `json:"-"`
}

// Add this type at the top of the file with other type definitions
type TrackedWaitGroup struct {
	sync.WaitGroup
	count int64
}

func (twg *TrackedWaitGroup) Add(delta int) {
	atomic.AddInt64(&twg.count, int64(delta))
	twg.WaitGroup.Add(delta)
}

func (twg *TrackedWaitGroup) Done() {
	atomic.AddInt64(&twg.count, -1)
	twg.WaitGroup.Done()
}

func (twg *TrackedWaitGroup) Count() int64 {
	return atomic.LoadInt64(&twg.count)
}

// Then modify the discoverNodes function to use TrackedWaitGroup
func (g *OPCUAInput) discoverNodes(ctx context.Context) ([]NodeDef, map[string]string, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	nodeList := make([]NodeDef, 0)
	pathIDMap := make(map[string]string)
	nodeChan := make(chan NodeDef, MaxTagsToBrowse)
	errChan := make(chan error, MaxTagsToBrowse)
	// opcuaBrowserChan is created to just satisfy the browse function signature.
	// The data inside opcuaBrowserChan is not so useful for this function. It is more useful for the GetNodeTree function
	opcuaBrowserChan := make(chan NodeDef, MaxTagsToBrowse)
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
				g.Log.Warn("browse function received timeout signal after 5 minutes. Please select less nodes.")
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
		go browse(timeoutCtx, wrapperNodeID, "", 0, g.Log, nodeID.String(), nodeChan, errChan, &wg, opcuaBrowserChan, &g.visited)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-timeoutCtx.Done():
		g.Log.Warn("browse function received timeout signal after 5 minutes. Please select less nodes.")
		return nil, nil, timeoutCtx.Err()
	case <-done:
	}

	close(nodeChan)
	close(errChan)
	close(opcuaBrowserChan)

	for node := range nodeChan {
		nodeList = append(nodeList, node)
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
func (g *OPCUAInput) BrowseAndSubscribeIfNeeded(ctx context.Context) error {

	nodeList, _, err := g.discoverNodes(ctx)
	if err != nil {
		g.Log.Infof("error while getting the node list: %v", err)
		return err
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
			opcuaBrowserChanHeartbeat := make(chan NodeDef, 1)
			var wgHeartbeat TrackedWaitGroup

			wgHeartbeat.Add(1)
			wrapperNodeID := NewOpcuaNodeWrapper(g.Client.Node(heartbeatNodeID))
			go browse(ctx, wrapperNodeID, "", 1, g.Log, heartbeatNodeID.String(), nodeHeartbeatChan, errChanHeartbeat, &wgHeartbeat, opcuaBrowserChanHeartbeat, &g.visited)

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

// MonitorBatched splits the nodes into manageable batches and starts monitoring them.
// This approach prevents the server from returning BadTcpMessageTooLarge by avoiding oversized monitoring requests.
// It returns the total number of nodes that were successfully monitored or an error if monitoring fails.
func (g *OPCUAInput) MonitorBatched(ctx context.Context, nodes []NodeDef) (int, error) {
	const maxBatchSize = 100
	totalMonitored := 0
	totalNodes := len(nodes)

	if len(nodes) == 0 {
		g.Log.Errorf("Did not subscribe to any nodes. This can happen if the nodes that are selected are incompatible with this benthos version. Aborting...")
		return 0, fmt.Errorf("no valid nodes selected")
	}

	g.Log.Infof("Starting to monitor %d nodes in batches of %d", totalNodes, maxBatchSize)

	for startIdx := 0; startIdx < totalNodes; startIdx += maxBatchSize {
		endIdx := startIdx + maxBatchSize
		if endIdx > totalNodes {
			endIdx = totalNodes
		}

		batch := nodes[startIdx:endIdx]
		g.Log.Infof("Creating monitor for nodes %d to %d", startIdx, endIdx-1)

		monitoredRequests := make([]*ua.MonitoredItemCreateRequest, 0, len(batch))

		for pos, nodeDef := range batch {
			request := opcua.NewMonitoredItemCreateRequestWithDefaults(
				nodeDef.NodeID,
				ua.AttributeIDValue,
				uint32(startIdx+pos),
			)
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
				g.Log.Errorf("Failed to monitor node %s: %v", failedNode, result.StatusCode)
				// Depending on requirements, you might choose to continue monitoring other nodes
				// instead of aborting. Here, we abort on the first failure.
				if closeErr := g.Close(ctx); closeErr != nil {
					g.Log.Errorf("Failed to close OPC UA connection: %v", closeErr)
				}
				return totalMonitored, fmt.Errorf("monitoring failed for node %s: %v", failedNode, result.StatusCode)
			}
		}

		monitoredNodes := len(response.Results)
		totalMonitored += monitoredNodes
		g.Log.Infof("Successfully monitored %d nodes in current batch", monitoredNodes)
		time.Sleep(time.Second) // Sleep for some time to prevent overloading the server
	}

	g.Log.Infof("Monitoring completed. Total nodes monitored: %d/%d", totalMonitored, totalNodes)
	return totalMonitored, nil
}

// UpdateNodePaths updates the node paths to use the nodeID instead of the browseName
// if the browseName is not unique
func UpdateNodePaths(nodes []NodeDef) {
	for i, node := range nodes {
		for j, otherNode := range nodes {
			if i == j {
				continue
			}
			if node.Path == otherNode.Path {
				// update only the last element of the path, after the last dot
				nodePathSplit := strings.Split(node.Path, ".")
				nodePath := strings.Join(nodePathSplit[:len(nodePathSplit)-1], ".")
				nodePath = nodePath + "." + sanitize(node.NodeID.String())
				nodes[i].Path = nodePath

				otherNodePathSplit := strings.Split(otherNode.Path, ".")
				otherNodePath := strings.Join(otherNodePathSplit[:len(otherNodePathSplit)-1], ".")
				otherNodePath = otherNodePath + "." + sanitize(otherNode.NodeID.String())
				nodes[j].Path = otherNodePath
			}
		}
	}
}

// helper function to send an error to a channel, with logging if the channel is blocked or the context is done
func sendError(ctx context.Context, err error, errChan chan<- error, logger Logger) {
	select {
	case errChan <- err:
	case <-ctx.Done():
		logger.Warnf("Failed to send error due to context cancellation: %v", err)
	default:
		logger.Warnf("Channel is blocked, skipping error send: %v", err)
	}
}
