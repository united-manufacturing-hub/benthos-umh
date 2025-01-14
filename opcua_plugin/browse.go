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
	"github.com/redpanda-data/benthos/v4/public/service"
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
func Browse(ctx context.Context, n NodeBrowser, path string, level int, logger Logger, parentNodeId string, nodeChan chan NodeDef, errChan chan error, wg *TrackedWaitGroup, browseHierarchicalReferences bool, opcuaBrowserChan chan NodeDef) {
	browse(ctx, n, path, level, logger, parentNodeId, nodeChan, errChan, wg, browseHierarchicalReferences, opcuaBrowserChan)
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
func browse(ctx context.Context, n NodeBrowser, path string, level int, logger Logger, parentNodeId string, nodeChan chan NodeDef, errChan chan error, wg *TrackedWaitGroup, browseHierarchicalReferences bool, opcuaBrowserChan chan NodeDef) {
	defer wg.Done()

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
			sendError(ctx, errors.New("node class is nil"), errChan, logger)
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
		sendError(ctx, err, errChan, logger)
		return
	}

	switch err := attrs[1].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[1].Value == nil {
			sendError(ctx, errors.New("browse name is nil"), errChan, logger)
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
		sendError(ctx, err, errChan, logger)
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
		sendError(ctx, err, errChan, logger)
		return
	}

	switch err := attrs[3].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[3].Value == nil {
			sendError(ctx, errors.New("access level is nil"), errChan, logger)
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
		sendError(ctx, err, errChan, logger)
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
		sendError(ctx, err, errChan, logger)
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
		children, err := n.Children(ctx, refType, ua.NodeClassVariable|ua.NodeClassObject)
		if err != nil {
			sendError(ctx, errors.Errorf("Children: %d: %s", refType, err), errChan, logger)
			return err
		}
		for _, child := range children {
			wg.Add(1)
			go browse(ctx, child, def.Path, level+1, logger, def.NodeID.String(), nodeChan, errChan, wg, browseHierarchicalReferences, opcuaBrowserChan)
		}
		return nil
	}

	browseChildren := func(refType uint32) error {
		refs, err := n.ReferencedNodes(ctx, refType, ua.BrowseDirectionForward, ua.NodeClassAll, true)
		if err != nil {
			sendError(ctx, errors.Errorf("References: %d: %s", refType, err), errChan, logger)
			return err
		}
		logger.Debugf("found %d child refs\n", len(refs))
		for _, rn := range refs {
			wg.Add(1)
			go browse(ctx, rn, def.Path, level+1, logger, def.NodeID.String(), nodeChan, errChan, wg, browseHierarchicalReferences, opcuaBrowserChan)
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
	// With browseHierarchicalReferences set to true, we can browse the hierarchical references of a node which will check for references of type
	// HasEventSource, HasChild, HasComponent, Organizes, FolderType, HasNotifier and all their sub-hierarchical references
	// Setting browseHierarchicalReferences to true will be the new way to browse for tags and folders properly without any duplicate browsing
	if browseHierarchicalReferences {
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
				sendError(ctx, err, errChan, logger)
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
			sendError(ctx, err, errChan, logger)
			return
		}
		if err := browseChildren(id.Organizes); err != nil {
			sendError(ctx, err, errChan, logger)
			return
		}
		if err := browseChildren(id.FolderType); err != nil {
			sendError(ctx, err, errChan, logger)
			return
		}
		if err := browseChildren(id.HasNotifier); err != nil {
			sendError(ctx, err, errChan, logger)
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

// nodeClass returns the node class of the node
func nodeClass(ctx context.Context, node *opcua.Node) (ua.NodeClass, error) {
	return node.NodeClass(ctx)
}

// browseName returns the browse name of the node. The returned value is sanitized by the sanitize function
func sanitizedBrowseName(ctx context.Context, node *opcua.Node) (string, error) {
	browseName, err := node.BrowseName(ctx)
	if err != nil {
		return "", err
	}
	return sanitize(browseName.Name), nil
}

// description returns the description of the node
func description(ctx context.Context, node *opcua.Node) (string, error) {
	description, err := node.Description(ctx)
	if err != nil {
		return "", err
	}
	return description.Text, nil
}

func accessLevel(ctx context.Context, node *opcua.Node) (ua.AccessLevelType, error) {
	accessLevel, err := node.AccessLevel(ctx)
	if err != nil {
		return 0, err
	}
	return accessLevel, nil
}

func dataType(ctx context.Context, node *opcua.Node) (string, error) {

	dataTypeAttribute, err := node.Attribute(ctx, ua.AttributeIDDataType)
	if err != nil {
		return "", nil
	}
	dataType := dataTypeAttribute.NodeID().String()
	switch dataTypeAttribute.NodeID().IntID() {
	case id.DateTime:
		dataType = "time.Time"
	case id.Boolean:
		dataType = "bool"
	case id.SByte:
		dataType = "int8"
	case id.Int16:
		dataType = "int16"
	case id.Int32:
		dataType = "int32"
	case id.Byte:
		dataType = "byte"
	case id.UInt16:
		dataType = "uint16"
	case id.UInt32:
		dataType = "uint32"
	case id.UtcTime:
		dataType = "time.Time"
	case id.String:
		dataType = "string"
	case id.Float:
		dataType = "float32"
	case id.Double:
		dataType = "float64"
	}
	return dataType, nil
}

func getNodeDefForVariableNode(ctx context.Context, nodeId *ua.NodeID, node *opcua.Node) (*NodeDef, error) {
	var isVariableNode bool

	nodeClass, err := nodeClass(ctx, node)
	if err != nil {
		return nil, err
	}

	isVariableNode = nodeClass == ua.NodeClassVariable
	if !isVariableNode {
		return nil, errors.New("node is not a variable node")
	}

	browseName, err := sanitizedBrowseName(ctx, node)
	if err != nil {
		return nil, err
	}

	description, err := description(ctx, node)
	if err != nil {
		return nil, err
	}

	accessLevel, err := accessLevel(ctx, node)
	if err != nil {
		return nil, err
	}

	dataType, err := dataType(ctx, node)
	if err != nil {
		return nil, err
	}

	nodeDef := &NodeDef{
		NodeID:       nodeId,
		NodeClass:    nodeClass,
		BrowseName:   browseName,
		Description:  description,
		AccessLevel:  accessLevel,
		DataType:     dataType,
		ParentNodeID: nodeId.String(), // ParentNodeID is not significant for variable nodes. Hence set the same nodeID as ParentNodeID
		Path:         browseName,      // Path is not significant for variable nodes. Hence set the browseName as Path
	}

	return nodeDef, err
}

func logWaitGroupInfo(ctx context.Context, logger *service.Logger, ticker *time.Ticker, done <-chan struct{}, nodeChan chan NodeDef, wg *TrackedWaitGroup) {
	for {
		select {
		case <-ctx.Done():
			logger.Warn("browse function received timeout signal after 5 minutes. Please select less nodes.")
			return
		case <-done:
			return
		case <-ticker.C:
			logger.Infof("Amount of found opcua tags currently in channel: %d, (%d active browse goroutines)", len(nodeChan), wg.Count())
		}
	}
}

// addBrowsedNodesToNodeList is a go routine that listens to the nodeChan and adds the nodes to the nodeList
// This routine ensures that the nodes are added to the nodeList in a thread safe manner and nodeChan buffer is not exhausted
func addBrowsedNodesToNodeList(ctx context.Context, nodeChan chan NodeDef, nodeList *[]NodeDef) {
	for {
		select {
		case node := <-nodeChan:
			*nodeList = append(*nodeList, node)
		case <-ctx.Done():
			return
		}
	}

}

// discoverNodes gets a list of NodeIds from OPCUAInput and tries to check
// if the nodeID is whether a variable class or not. If it is a variable class, then it directly adds it to the output nodeList.
// If the nodeID is not a variable class, then it browses the children nodes and adds them to the nodeList
func (g *OPCUAInput) discoverNodes(ctx context.Context) ([]NodeDef, error) {

	childCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	nodeList := make([]NodeDef, 0)
	nodeChan := make(chan NodeDef, MaxTagsToBrowse)
	errChan := make(chan error, MaxTagsToBrowse)
	done := make(chan struct{})

	// opcuaBrowserChan is created to just satisfy the browse function signature.
	// The data inside opcuaBrowserChan is not so useful for this function. It is more useful for the GetNodeTree function
	opcuaBrowserChan := make(chan NodeDef, MaxTagsToBrowse)

	var wg TrackedWaitGroup
	wgInfoTicker := time.NewTicker(10 * time.Second)
	defer wgInfoTicker.Stop()
	go logWaitGroupInfo(childCtx, g.Log, wgInfoTicker, done, nodeChan, &wg)

	inputNodeIDs := g.NodeIDs
	if len(inputNodeIDs) == 0 {
		return nil, fmt.Errorf("no node ids provided to browse")
	}

	for _, nodeId := range inputNodeIDs {
		g.Log.Debugf("Browsing Nodes for the nodeID: %v", nodeId.String())

		node := g.Client.Node(nodeId)

		// Check if the current Node id is of variable class type.
		// If it is a variable node, then we can avoid further browsing and directly add it to the nodeList
		// Note: Variable class types don't have children nodes
		// This also helps in avoiding the unnecessary browsing of the node that don't have children nodes
		nodeDef, err := getNodeDefForVariableNode(childCtx, nodeId, node)
		if err != nil {
			g.Log.Debugf("error while trying to discover the variable nodeID: %v, err: %v", nodeId.String(), err)
		}

		// For a nodeDef that is not nil, we can confirm that this is a variable node and we can avoid further browsing down the line
		if nodeDef != nil {
			nodeList = append(nodeList, *nodeDef)
			continue
		}

		// Any code below this line needs browsing as it is not a varible node and we need to get the children nodes
		wg.Add(1)
		wrapperNodeID := NewOpcuaNodeWrapper(node)
		go addBrowsedNodesToNodeList(childCtx, nodeChan, &nodeList)
		go browse(childCtx, wrapperNodeID, "", 0, g.Log, nodeId.String(), nodeChan, errChan, &wg, g.BrowseHierarchicalReferences, opcuaBrowserChan)
	}

	wg.Wait()
	close(done)

	close(nodeChan)
	close(errChan)
	close(opcuaBrowserChan)

	UpdateNodePaths(nodeList)

	if len(errChan) > 0 {
		var combinedErr strings.Builder
		for err := range errChan {
			combinedErr.WriteString(err.Error() + "; ")
		}
		return nil, errors.New(combinedErr.String())
	}

	return nodeList, nil
}

// BrowseAndSubscribeIfNeeded browses the specified OPC UA nodes, adds a heartbeat node if required,
// and sets up monitored requests for the nodes.
//
// The function performs the following steps:
// 1. **Browse Nodes:** Iterates through `NodeIDs` and concurrently browses each node to detect available nodes.
// 2. **Add Heartbeat Node:** If heartbeats are enabled, ensures the heartbeat node (`HeartbeatNodeId`) is included in the node list.
// 3. **Subscribe to Nodes:** If subscriptions are enabled, creates a subscription and sets up monitoring for the detected nodes.
func (g *OPCUAInput) BrowseAndSubscribeIfNeeded(ctx context.Context) error {

	nodeList, err := g.discoverNodes(ctx)
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
			go browse(ctx, wrapperNodeID, "", 1, g.Log, heartbeatNodeID.String(), nodeHeartbeatChan, errChanHeartbeat, &wgHeartbeat, g.BrowseHierarchicalReferences, opcuaBrowserChanHeartbeat)

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
