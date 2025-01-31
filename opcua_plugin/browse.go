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
func Browse(ctx context.Context, n NodeBrowser, path string, logger Logger, parentNodeId string, nodeChan chan NodeDef, errChan chan error, wg *TrackedWaitGroup, opcuaBrowserChan chan NodeDef, visited *sync.Map) {
	browse(ctx, n, path, logger, parentNodeId, nodeChan, errChan, wg, opcuaBrowserChan, visited)
}

// NodeTask represents a task for workers to process
type NodeTask struct {
	node         NodeBrowser
	path         string
	level        int
	parentNodeId string
}

// browse uses a worker pool pattern to process nodes
func browse(ctx context.Context, startNode NodeBrowser, startPath string, logger Logger, parentNodeId string,
	nodeChan chan NodeDef, errChan chan error, wg *TrackedWaitGroup, opcuaBrowserChan chan NodeDef, visited *sync.Map) {

	var taskWg TrackedWaitGroup
	var workerWg TrackedWaitGroup
	const numWorkers = 10 // Adjust based on your needs
	taskChan := make(chan NodeTask, numWorkers*10)

	// Start worker pool
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go worker(ctx, i, taskChan, nodeChan, errChan, opcuaBrowserChan, visited, logger, &taskWg, &workerWg)
	}

	// Send initial task
	taskWg.Add(1)
	taskChan <- NodeTask{
		node:         startNode,
		path:         startPath,
		level:        0,
		parentNodeId: parentNodeId,
	}

	// Close task channel when all tasks are processed
	go func() {
		taskWg.Wait()
		close(taskChan)
		workerWg.Wait()
		wg.Done()
	}()
}

func worker(
	ctx context.Context,
	id int,
	taskChan chan NodeTask,
	nodeChan chan NodeDef,
	errChan chan error,
	opcuaBrowserChan chan NodeDef,
	visited *sync.Map,
	logger Logger,
	taskWg *TrackedWaitGroup,
	workerWg *TrackedWaitGroup,
) {
	defer workerWg.Done()
	for {
		select {
		case task, ok := <-taskChan:
			if !ok {
				// Channel is closed
				return
			}

			// Skip if already visited or too deep
			if _, exists := visited.LoadOrStore(task.node.ID(), struct{}{}); exists {
				logger.Debugf("Worker %d: node %s already visited", id, task.node.ID().String())
				taskWg.Done()
				continue
			}

			if task.level > 25 {
				taskWg.Done()
				continue
			}

			// Get node attributes
			attrs, err := task.node.Attributes(ctx, ua.AttributeIDNodeClass, ua.AttributeIDBrowseName,
				ua.AttributeIDDescription, ua.AttributeIDAccessLevel, ua.AttributeIDDataType)
			if err != nil {
				sendError(ctx, err, errChan, logger)
				taskWg.Done()
				continue
			}

			if len(attrs) != 5 {
				sendError(ctx, errors.Errorf("only got %d attr, needed 5", len(attrs)), errChan, logger)
				taskWg.Done()
				continue
			}

			browseName, err := task.node.BrowseName(ctx)
			if err != nil {
				sendError(ctx, err, errChan, logger)
				taskWg.Done()
				continue
			}

			newPath := sanitize(browseName.Name)
			if task.path != "" {
				newPath = task.path + "." + newPath
			}

			def := NodeDef{
				NodeID:       task.node.ID(),
				Path:         newPath,
				ParentNodeID: task.parentNodeId,
			}

			if err := processNodeAttributes(attrs, &def, newPath, logger); err != nil {
				sendError(ctx, err, errChan, logger)
				taskWg.Done()
				continue
			}

			logger.Debugf("\nWorker %d: level %d: def.Path:%s def.NodeClass:%s\n",
				id, task.level, def.Path, def.NodeClass)
			logger.Debugf("TrackedWaitGroup count: %d\n", taskWg.Count())
			logger.Debugf("WorkerWg count: %d\n", workerWg.Count())

			// Handle browser channel
			browserDef := def
			browserDef.Path = join(task.path, browserDef.BrowseName)
			select {
			case opcuaBrowserChan <- browserDef:
			default:
				logger.Debugf("Worker %d: opcuaBrowserChan blocked, skipping", id)
			}

			// Process based on node class
			switch def.NodeClass {
			case ua.NodeClassVariable:
				select {
				case nodeChan <- def:
				case <-ctx.Done():
					logger.Warnf("Worker %d: Failed to send node due to cancellation", id)
					taskWg.Done()
					return
				}
				if err := browseChildren(ctx, task, def, taskChan, taskWg); err != nil {
					sendError(ctx, err, errChan, logger)
				}

			case ua.NodeClassObject:
				if err := browseChildren(ctx, task, def, taskChan, taskWg); err != nil {
					sendError(ctx, err, errChan, logger)
				}
			}
			taskWg.Done()

		case <-ctx.Done():
			logger.Warnf("Worker %d: received cancellation signal", id)
			return
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

// Add this helper function after the worker function
func browseChildren(ctx context.Context, task NodeTask, def NodeDef, taskChan chan NodeTask, wg *TrackedWaitGroup) error {
	children, err := task.node.Children(ctx, id.HierarchicalReferences,
		ua.NodeClassVariable|ua.NodeClassObject)
	if err != nil {
		return errors.Errorf("Children: %s", err)
	}

	// Queue child tasks
	for _, child := range children {
		wg.Add(1)
		fmt.Printf("Adding child task to channel: %s", child.ID().String())
		taskChan <- NodeTask{
			node:         child,
			path:         def.Path,
			level:        task.level + 1,
			parentNodeId: def.NodeID.String(),
		}
	}
	return nil
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
		go browse(timeoutCtx, wrapperNodeID, "", g.Log, nodeID.String(), nodeChan, errChan, &wg, opcuaBrowserChan, &g.visited)
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
			go browse(ctx, wrapperNodeID, "", g.Log, heartbeatNodeID.String(), nodeHeartbeatChan, errChanHeartbeat, &wgHeartbeat, opcuaBrowserChanHeartbeat, &g.visited)

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
