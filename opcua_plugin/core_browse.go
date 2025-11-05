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
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gopcua/opcua/errors"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

const (
	MaxTagsToBrowse = 100_000
	// StaleTime is used to define whether a node that was discovered is marked
	// as stale to rediscover -> if maybe an attribute changed from the previous run
	StaleTime = 15 * time.Minute
)

var sanitizeRegex = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

type NodeDef struct {
	NodeID       *ua.NodeID
	NodeClass    ua.NodeClass
	BrowseName   string
	Description  string
	AccessLevel  ua.AccessLevelType
	DataType     string      // String representation for metadata
	DataTypeID   ua.TypeID   // TypeID for filter compatibility checking
	ParentNodeID string      // custom, not an official opcua attribute
	Path         string      // custom, not an official opcua attribute
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
	return sanitizeRegex.ReplaceAllString(s, "_")
}

type Logger interface {
	Debugf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
}

// Browse is a public wrapper function for the browse function
// Avoid using this function directly, use it only for testing
func Browse(ctx context.Context, n NodeBrowser, path string, logger Logger, parentNodeId string, nodeChan chan NodeDef, errChan chan error, wg *TrackedWaitGroup, opcuaBrowserChan chan BrowseDetails, visited *sync.Map) {
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
func browse(
	ctx context.Context,
	startNode NodeBrowser,
	startPath string,
	logger Logger,
	parentNodeId string,
	nodeChan chan NodeDef,
	errChan chan error,
	wg *TrackedWaitGroup,
	opcuaBrowserChan chan BrowseDetails,
	visited *sync.Map,
) {
	metrics := NewServerMetrics()
	var taskWg TrackedWaitGroup
	var workerWg TrackedWaitGroup
	// Buffer = 2× MaxTagsToBrowse to handle branching factor safely.
	// This prevents deadlock when workers queue children to taskChan.
	// Backpressure is applied via context cancellation in browseChildren.
	taskChan := make(chan NodeTask, MaxTagsToBrowse*2)

	workerID := make(map[uuid.UUID]struct{})
	// Start worker pool manager
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				toAdd, toRemove := metrics.adjustWorkers(logger)
				for i := 0; i < toAdd; i++ {
					id := uuid.New()
					workerWg.Add(1)
					stopChan := metrics.addWorker(id)
					workerID[id] = struct{}{}
					go worker(ctx, id, taskChan, nodeChan, errChan, opcuaBrowserChan, visited, logger, &taskWg, &workerWg, metrics, stopChan)
				}

				for i := 0; i < toRemove; i++ {
					for id := range workerID {
						metrics.removeWorker(id)
						delete(workerID, id)
						// This break make sure that only one worker is removed on each iteration
						break
					}
				}
			}
		}

	}()

	// Start worker pool
	for i := 0; i < metrics.currentWorkers; i++ {
		id := uuid.New()
		workerWg.Add(1)
		stopChan := metrics.addWorker(id)
		workerID[id] = struct{}{}
		go worker(ctx, id, taskChan, nodeChan, errChan, opcuaBrowserChan, visited, logger, &taskWg, &workerWg, metrics, stopChan)
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

type VisitedNodeInfo struct {
	Def             NodeDef
	LastSeen        time.Time
	FullyDiscovered bool
}

func worker(
	ctx context.Context,
	id uuid.UUID,
	taskChan chan NodeTask,
	nodeChan chan NodeDef,
	errChan chan error,
	opcuaBrowserChan chan BrowseDetails,
	visited *sync.Map,
	logger Logger,
	taskWg *TrackedWaitGroup,
	workerWg *TrackedWaitGroup,
	metrics *ServerMetrics,
	stopChan chan struct{},
) {

	defer workerWg.Done()
	for {
		select {
		case <-stopChan:
			logger.Debugf("Worker %s removed stop signal to reduce load on the opcua server", id)
			return

		case <-ctx.Done():
			logger.Warnf("Worker %s: received cancellation signal", id)
			return

		case task, ok := <-taskChan:
			if !ok {
				// Channel is closed
				return
			}

			if task.node == nil {
				continue
			}

			startTime := time.Now()
			// Skip if already visited or too deep
			val, found := visited.Load(task.node.ID())
			if found {
				vni, ok := val.(VisitedNodeInfo)
				if !ok {
					vni = VisitedNodeInfo{}
				}
				// to skip nodes that are already FullyDiscovered and fresh
				if vni.FullyDiscovered && time.Since(vni.LastSeen) < StaleTime {
					logger.Debugf("Worker %s: node %s is fully discovered and fresh, skipping..", id, task.node.ID().String())
					taskWg.Done()
					continue
				}

				// set the node.ID() but not yet fully discovered if sth breaks, we can
				// start over from here
				visited.Store(task.node.ID(), VisitedNodeInfo{
					Def:             vni.Def,
					LastSeen:        time.Now(),
					FullyDiscovered: false,
				})
			} else {
				visited.Store(task.node.ID(), VisitedNodeInfo{
					Def:             NodeDef{},
					LastSeen:        time.Now(),
					FullyDiscovered: false,
				})
			}

			if task.level > 25 {
				logger.Debugf("Skipping node %s at browse level %d", task.node.ID().String(), task.level)
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

			logger.Debugf("\nWorker %s: level %d: def.Path:%s def.NodeClass:%s TaskWaitGroup count: %d WorkerWaitGroup count: %d\n",
				id, task.level, def.Path, def.NodeClass, taskWg.Count(), workerWg.Count())

			visited.Store(task.node.ID(), VisitedNodeInfo{
				Def:             def,
				LastSeen:        time.Now(),
				FullyDiscovered: false,
			})

			// Handle browser channel
			browserDetails := BrowseDetails{
				NodeDef:               def,
				TaskCount:             taskWg.Count(),
				WorkerCount:           workerWg.Count(),
				AvgServerResponseTime: metrics.AverageResponseTime(),
			}
			browserDetails.NodeDef.Path = join(task.path, def.BrowseName)
			select {
			case opcuaBrowserChan <- browserDetails:
				// Successfully sent node details for topic browser
			case <-ctx.Done():
				logger.Warnf("Worker %s: Context canceled while sending to opcuaBrowserChan", id)
				taskWg.Done()
				return
			}

			// Process based on node class
			switch def.NodeClass {
			case ua.NodeClassVariable:
				select {
				case nodeChan <- def:
				case <-ctx.Done():
					logger.Warnf("Worker %s: Failed to send node due to cancellation", id)
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

			// now set the node.ID() to fully discovered
			visited.Store(task.node.ID(), VisitedNodeInfo{
				Def:             def,
				LastSeen:        time.Now(),
				FullyDiscovered: true,
			})
			metrics.recordResponseTime(time.Since(startTime))
			taskWg.Done()

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

// browseChildren browses the child nodes of the given node and queues them as tasks.
// It only browses for variables and objects through hierarchical references.
// The child tasks are queued with an incremented level and the current node's path and ID as parent.
// Parameters:
//   - ctx: Context for cancellation
//   - task: The current node task being processed
//   - def: Node definition containing path and ID information
//   - taskChan: Channel to queue child tasks
//   - taskWg: WaitGroup to track task completion
//
// Returns an error if browsing children fails
func browseChildren(ctx context.Context, task NodeTask, def NodeDef, taskChan chan NodeTask, taskWg *TrackedWaitGroup) error {
	children, err := task.node.Children(ctx, id.HierarchicalReferences,
		ua.NodeClassVariable|ua.NodeClassObject)
	if err != nil {
		return errors.Errorf("Children: %s", err)
	}

	// Queue child tasks with context cancellation support.
	// Use select to prevent deadlock: if taskChan is full and context is cancelled,
	// we must exit immediately rather than block forever. The ctx.Done() case provides
	// an escape path that pure blocking (taskChan <- task) lacks.
	//
	// Important: taskWg.Add(1) is called BEFORE sending to channel to prevent race condition.
	// If Add() is called after send, the worker may call Done() before Add() executes,
	// causing "sync: negative WaitGroup counter" panic. The ctx.Done() case must call
	// Done() to roll back the increment if context is cancelled before sending.
	for _, child := range children {
		taskWg.Add(1) // Increment BEFORE sending to prevent race
		select {
		case taskChan <- NodeTask{
			node:         child,
			path:         def.Path,
			level:        task.level + 1,
			parentNodeId: def.NodeID.String(),
		}:
			// Successfully queued
		case <-ctx.Done():
			taskWg.Done() // Roll back the Add() since task wasn't queued
			return ctx.Err() // Respect cancellation
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
