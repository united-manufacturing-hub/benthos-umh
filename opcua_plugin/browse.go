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
func Browse(ctx context.Context, n NodeBrowser, path string, level int, logger Logger, parentNodeId string, nodeChan chan NodeDef, errChan chan error, wg *TrackedWaitGroup, browseHierarchicalReferences bool) {
	browse(ctx, n, path, level, logger, parentNodeId, nodeChan, errChan, wg, browseHierarchicalReferences)
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
// - `browseHierarchicalReferences` (`bool`): Indicates whether to browse hierarchical references.
// **Returns:**
// - `void`: Errors are sent through `errChan`, and discovered nodes are sent through `nodeChan`.
func browse(ctx context.Context, n NodeBrowser, path string, level int, logger Logger, parentNodeId string, nodeChan chan NodeDef, errChan chan error, wg *TrackedWaitGroup, browseHierarchicalReferences bool) {
	defer wg.Done()

	if level > 10 {
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
		select {
		case errChan <- err:
		case <-ctx.Done():
			logger.Warnf("Failed to send error due to context cancellation: %v", err)
		default:
			logger.Warnf("Channel is blocked, skipping error send: %v", err)
		}
		return
	}

	browseName, err := n.BrowseName(ctx)
	if err != nil {
		select {
		case errChan <- err:
		case <-ctx.Done():
			logger.Warnf("Failed to send error due to context cancellation: %v", err)
		default:
			logger.Warnf("Channel is blocked, skipping error send: %v", err)
		}
		return
	}

	var newPath string
	if path == "" {
		newPath = sanitize(browseName.Name)
	} else {
		newPath = path + "." + sanitize(browseName.Name)
	}

	var def = NodeDef{
		NodeID: n.ID(),
		Path:   newPath,
	}

	switch err := attrs[0].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[0].Value == nil {
			select {
			case errChan <- errors.New("node class is nil"):
			case <-ctx.Done():
				logger.Warnf("Failed to send error due to context cancellation: node class is nil")
			default:
				logger.Warnf("Channel is blocked, skipping error send: node class is nil")
			}
			return
		} else {
			def.NodeClass = ua.NodeClass(attrs[0].Value.Int())
		}
	case errors.Is(err, ua.StatusBadSecurityModeInsufficient):
		return
	case errors.Is(err, ua.StatusBadNotReadable): // fallback option to not throw an error (this is "normal" for some servers)
		logger.Warnf("Tried to browse node: %s but got access denied on getting the NodeClass, do not subscribe to it, continuing browsing its children...\n", path)
		def.NodeClass = ua.NodeClassObject // by setting it as an object, we will not subscribe to it
		// no need to return here, as we can continue without the NodeClass for browsing
	default:
		select {
		case errChan <- err:
		case <-ctx.Done():
			logger.Warnf("Failed to send error due to context cancellation: %v", err)
		default:
			logger.Warnf("Channel is blocked, skipping error send: %v", err)
		}
		return
	}

	switch err := attrs[1].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[1].Value == nil {
			select {
			case errChan <- errors.New("browse name is nil"):
			case <-ctx.Done():
				logger.Warnf("Failed to send error due to context cancellation: browse name is nil")
			default:
				logger.Warnf("Channel is blocked, skipping error send: browse name is nil")
			}
			return
		} else {
			def.BrowseName = attrs[1].Value.String()
		}
	case errors.Is(err, ua.StatusBadSecurityModeInsufficient):
		return
	case errors.Is(err, ua.StatusBadNotReadable): // fallback option to not throw an error (this is "normal" for some servers)
		logger.Warnf("Tried to browse node: %s but got access denied on getting the BrowseName, skipping...\n", path)
		return // We need to return here, as we can't continue without the BrowseName (we need it at least for the path when browsing the children)
	default:
		select {
		case errChan <- err:
		case <-ctx.Done():
			logger.Warnf("Failed to send error due to context cancellation: %v", err)
		default:
			logger.Warnf("Channel is blocked, skipping error send: %v", err)
		}
		return
	}

	switch err := attrs[2].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[2].Value == nil {
			def.Description = "" // this can happen for example in Kepware v6, where the description is OPCUAType_Null
		} else {
			def.Description = attrs[2].Value.String()
		}
	case errors.Is(err, ua.StatusBadAttributeIDInvalid):
		// ignore
	case errors.Is(err, ua.StatusBadSecurityModeInsufficient):
		return
	case errors.Is(err, ua.StatusBadNotReadable): // fallback option to not throw an error (this is "normal" for some servers)
		logger.Warnf("Tried to browse node: %s but got access denied on getting the Description, do not subscribe to it, continuing browsing its children...\n", path)
		def.NodeClass = ua.NodeClassObject // by setting it as an object, we will not subscribe to it
		// no need to return here, as we can continue without the Description
	default:
		select {
		case errChan <- err:
		case <-ctx.Done():
			logger.Warnf("Failed to send error due to context cancellation: %v", err)
		default:
			logger.Warnf("Channel is blocked, skipping error send: %v", err)
		}
		return
	}

	switch err := attrs[3].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[3].Value == nil {
			select {
			case errChan <- errors.New("access level is nil"):
			case <-ctx.Done():
				logger.Warnf("Failed to send error due to context cancellation: access level is nil")
			default:
				logger.Warnf("Channel is blocked, skipping error send: access level is nil")
			}
			return
		} else {
			def.AccessLevel = ua.AccessLevelType(attrs[3].Value.Int())
		}
	case errors.Is(err, ua.StatusBadAttributeIDInvalid):
		// ignore
	case errors.Is(err, ua.StatusBadSecurityModeInsufficient):
		return
	case errors.Is(err, ua.StatusBadNotReadable): // fallback option to not throw an error (this is "normal" for some servers)
		logger.Warnf("Tried to browse node: %s but got access denied on getting the AccessLevel, continuing...\n", path)
		// no need to return here, as we can continue without the AccessLevel for browsing
	default:
		select {
		case errChan <- err:
		case <-ctx.Done():
			logger.Warnf("Failed to send error due to context cancellation: %v", err)
		default:
			logger.Warnf("Channel is blocked, skipping error send: %v", err)
		}
		return
	}

	// if AccessLevel exists and it is set to None
	if def.AccessLevel == ua.AccessLevelTypeNone && errors.Is(err, ua.StatusOK) {
		logger.Warnf("Tried to browse node: %s but access level is None ('access denied'). Do not subscribe to it, continuing browsing its children...\n", path)
		def.NodeClass = ua.NodeClassObject // by setting it as an object, we will not subscribe to it
		// we need to continue here, as we still want to browse the children of this node
	}

	switch err := attrs[4].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[4].Value == nil {
			// This is not an error, it can happen for some OPC UA servers...
			// in oru case it is the amine amaach opcua simulator
			// if the data type is nil, we simpy ignore it
			// errChan <- errors.New("data type is nil")
			logger.Debugf("ignoring node: %s as its datatype is nil...\n", path)
			return
		} else {
			switch v := attrs[4].Value.NodeID().IntID(); v {
			case id.DateTime:
				def.DataType = "time.Time"
			case id.Boolean:
				def.DataType = "bool"
			case id.SByte:
				def.DataType = "int8"
			case id.Int16:
				def.DataType = "int16"
			case id.Int32:
				def.DataType = "int32"
			case id.Byte:
				def.DataType = "byte"
			case id.UInt16:
				def.DataType = "uint16"
			case id.UInt32:
				def.DataType = "uint32"
			case id.UtcTime:
				def.DataType = "time.Time"
			case id.String:
				def.DataType = "string"
			case id.Float:
				def.DataType = "float32"
			case id.Double:
				def.DataType = "float64"
			default:
				def.DataType = attrs[4].Value.NodeID().String()
			}
		}

	case errors.Is(err, ua.StatusBadAttributeIDInvalid):
		// ignore
	case errors.Is(err, ua.StatusBadSecurityModeInsufficient):
		return
	case errors.Is(err, ua.StatusBadNotReadable): // fallback option to not throw an error (this is "normal" for some servers)
		logger.Warnf("Tried to browse node: %s but got access denied on getting the DataType, do not subscribe to it, continuing browsing its children...\n", path)
		def.NodeClass = ua.NodeClassObject // by setting it as an object, we will not subscribe to it
		// no need to return here, as we can continue without the DataType
	default:
		select {
		case errChan <- err:
		case <-ctx.Done():
			logger.Warnf("Failed to send error due to context cancellation: %v", err)
		default:
			logger.Warnf("Channel is blocked, skipping error send: %v", err)
		}
		return
	}

	logger.Debugf("%d: def.Path:%s def.NodeClass:%s\n", level, def.Path, def.NodeClass)
	def.ParentNodeID = parentNodeId

	hasNodeReferencedComponents := func() bool {
		refs, err := n.ReferencedNodes(ctx, id.HasComponent, ua.BrowseDirectionForward, ua.NodeClassAll, true)
		if err != nil || len(refs) == 0 {
			return false
		}
		return true
	}

	browseChildrenV2 := func(refType uint32) error {
		children, err := n.Children(ctx, refType, ua.NodeClassAll)
		if err != nil {
			return errors.Errorf("Children: %d: %s", refType, err)
		}
		for _, child := range children {
			wg.Add(1)
			go browse(ctx, child, def.Path, level+1, logger, def.NodeID.String(), nodeChan, errChan, wg, browseHierarchicalReferences)
		}
		return nil
	}

	browseChildren := func(refType uint32) error {
		refs, err := n.ReferencedNodes(ctx, refType, ua.BrowseDirectionForward, ua.NodeClassAll, true)
		if err != nil {
			return errors.Errorf("References: %d: %s", refType, err)
		}
		logger.Debugf("found %d child refs\n", len(refs))
		for _, rn := range refs {
			wg.Add(1)
			go browse(ctx, rn, def.Path, level+1, logger, def.NodeID.String(), nodeChan, errChan, wg, browseHierarchicalReferences)
		}
		return nil
	}

	// In OPC Unified Architecture (UA), hierarchical references are a type of reference that establishes a containment relationship between nodes in the address space. They are used to model a hierarchical structure, such as a system composed of components, where each component may contain sub-components.
	// Refer link: https://qiyuqi.gitbooks.io/opc-ua/content/Part3/Chapter7.html
	// With browseHierarchicalReferences set to true, we can browse the hierarchical references of a node which will check for references of type
	// HasEventSource, HasChild, HasComponent, Organizes, FolderType, HasNotifier and all their sub-hierarchical references
	// Setting browseHierarchicalReferences to true will be the new way to browse for tags and folders properly without any duplicate browsing
	if browseHierarchicalReferences {
		switch def.NodeClass {

		// If its a variable, we add it to the node list and browse all its children
		case ua.NodeClassVariable:
			if hasNodeReferencedComponents() {
				if err := browseChildrenV2(id.HierarchicalReferences); err != nil {
					select {
					case errChan <- err:
					case <-ctx.Done():
						logger.Warnf("Failed to send error due to context cancellation: %v", err)
					default:
						logger.Warnf("Channel is blocked, skipping error send: %v", err)
					}
					return
				}
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
				select {
				case errChan <- err:
				case <-ctx.Done():
					logger.Warnf("Failed to send error due to context cancellation: %v", err)
				default:
					logger.Warnf("Channel is blocked, skipping error send: %v", err)
				}
				return
			}
			return
		}
	}

	// This old way of browsing is deprecated and will be removed in the future
	// This method is called only when browseHierarchicalReferences is false
	browseReferencesDeprecated(ctx, def, nodeChan, errChan, path, logger, hasNodeReferencedComponents, browseChildren)
}

// browseReferencesDeprecated is the old way to browse for tags and folders without any duplicate browsing
// It browses the following references: HasComponent, HasProperty, HasEventSource, HasOrder, HasNotifier, Organizes, FolderType
func browseReferencesDeprecated(ctx context.Context, def NodeDef, nodeChan chan<- NodeDef, errChan chan<- error, path string, logger Logger, hasNodeReferencedComponents func() bool, browseChildren func(refType uint32) error) {

	// If a node has a Variable class, it probably means that it is a tag
	// Normally, there is no need to browse further. However, structs will be a variable on the top level,
	// but it then will have HasComponent references to its children
	if def.NodeClass == ua.NodeClassVariable {

		if hasNodeReferencedComponents() {
			if err := browseChildren(id.HasComponent); err != nil {
				select {
				case errChan <- err:
				case <-ctx.Done():
					logger.Warnf("Failed to send error due to context cancellation: %v", err)
				default:
					logger.Warnf("Channel is blocked, skipping error send: %v", err)
				}
				return
			}
			return
		}

		def.Path = join(path, def.BrowseName)
		select {
		case nodeChan <- def:
		case <-ctx.Done():
			logger.Warnf("Failed to send node due to context cancellation")
		default:
			logger.Warnf("Channel is blocked, skipping node send")
		}
		return
	}

	// If a node has an Object class, it probably means that it is a folder
	// Therefore, browse its children
	if def.NodeClass == ua.NodeClassObject {
		// To determine if an Object is a folder, we need to check different references
		// Add here all references that should be checked

		if err := browseChildren(id.HasComponent); err != nil {
			select {
			case errChan <- err:
			case <-ctx.Done():
				logger.Warnf("Failed to send error due to context cancellation: %v", err)
			default:
				logger.Warnf("Channel is blocked, skipping error send: %v", err)
			}
			return
		}
		if err := browseChildren(id.Organizes); err != nil {
			select {
			case errChan <- err:
			case <-ctx.Done():
				logger.Warnf("Failed to send error due to context cancellation: %v", err)
			default:
				logger.Warnf("Channel is blocked, skipping error send: %v", err)
			}
			return
		}
		if err := browseChildren(id.FolderType); err != nil {
			select {
			case errChan <- err:
			case <-ctx.Done():
				logger.Warnf("Failed to send error due to context cancellation: %v", err)
			default:
				logger.Warnf("Channel is blocked, skipping error send: %v", err)
			}
			return
		}
		if err := browseChildren(id.HasNotifier); err != nil {
			select {
			case errChan <- err:
			case <-ctx.Done():
				logger.Warnf("Failed to send error due to context cancellation: %v", err)
			default:
				logger.Warnf("Channel is blocked, skipping error send: %v", err)
			}
			return
		}
		// For hasProperty it makes sense to show it very close to the tag itself, e.g., use the tagName as tagGroup and then the properties as subparts of it
		/*
			if err := browseChildren(id.HasProperty); err != nil {
				return nil, err
			}
		*/
	}
}

// Node represents a node in the tree structure
type Node struct {
	NodeId   *ua.NodeID `json:"nodeId"`
	Name     string     `json:"name"`
	Children []*Node    `json:"children,omitempty"`
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
		go browse(timeoutCtx, wrapperNodeID, "", 0, g.Log, nodeID.String(), nodeChan, errChan, &wg, g.BrowseHierarchicalReferences)
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

	for node := range nodeChan {
		nodeList = append(nodeList, node)
	}

	UpdateNodePaths(nodeList)

	if len(errChan) > 0 {
		var combinedErr strings.Builder
		for err := range errChan {
			combinedErr.WriteString(err.Error() + "; ")
		}
		return nil, nil, fmt.Errorf(combinedErr.String())
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
			pathIDMapChan := make(chan map[string]string, 1)
			var wgHeartbeat TrackedWaitGroup

			wgHeartbeat.Add(1)
			wrapperNodeID := NewOpcuaNodeWrapper(g.Client.Node(heartbeatNodeID))
			go browse(ctx, wrapperNodeID, "", 1, g.Log, heartbeatNodeID.String(), nodeHeartbeatChan, errChanHeartbeat, &wgHeartbeat, g.BrowseHierarchicalReferences)

			wgHeartbeat.Wait()
			close(nodeHeartbeatChan)
			close(errChanHeartbeat)
			close(pathIDMapChan)

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
