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

// Core OPC UA Browse Implementation (Internal Worker Pool)
//
// This file implements the browse() function, which recursively discovers OPC UA nodes
// using an internal worker pool. This is the ONLY worker system used in production.
//
// Architecture:
// - browse() creates isolated worker pool per invocation (not shared between calls)
// - Workers pull NodeTasks from taskChan and queue child tasks
// - ServerMetrics (core_browse_workers.go) dynamically adjusts worker count
// - ServerProfile.MaxWorkers/MinWorkers control pool size bounds
//
// Used by:
// - Production: read_discover.go::discoverNodes() → browse() (with ServerProfile tuning)
// - Legacy UI: core_browse_frontend.go::GetNodeTree() → browse() (with Auto profile)
//
// NOT used by:
// - GlobalWorkerPool (legacy unused pattern - DO NOT conflate with this system)
//
// See ARCHITECTURE.md for detailed explanation of two code paths (production vs. legacy UI).

//
// ╔══════════════════════════════════════════════════════════════════════════╗
// ║                 INTERNAL WORKER POOL IMPLEMENTATION                       ║
// ╠══════════════════════════════════════════════════════════════════════════╣
// ║  This file implements the browse() internal function with worker pool.   ║
// ║  Used by: Both production (read_discover.go) and legacy UI paths        ║
// ║  Entry points:                                                           ║
// ║  • PRODUCTION: discoverNodes() → browse() [auto-tuned ServerProfile]    ║
// ║  • LEGACY UI: GetNodeTree() → Browse() wrapper → browse() [Auto profile]║
// ║  • TESTING: Browse() wrapper → browse() [test-injected config]          ║
// ║                                                                          ║
// ║  Architecture:                                                           ║
// ║  • Creates isolated worker pool per browse() invocation (NOT shared)    ║
// ║  • Workers pull NodeTasks from taskChan and queue children              ║
// ║  • ServerMetrics dynamically adjusts worker count (5-60 workers)        ║
// ║  • ServerProfile.MaxWorkers/MinWorkers control pool size bounds         ║
// ║                                                                          ║
// ║  ⚠️  Do NOT call browse() directly - use entry points above!            ║
// ║  See: Decision flowchart below for complete usage guidance              ║
// ╚══════════════════════════════════════════════════════════════════════════╝

package opcua_plugin

import (
	"context"
	"fmt"
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

// DECISION FLOWCHART: Which Browse Function Should I Use?
//
// ┌─────────────────────────────────────────────────────────────────────┐
// │ QUESTION: What am I trying to do?                                   │
// └─────────────────────────────────────────────────────────────────────┘
//                                  │
//         ┌────────────────────────┼────────────────────────┐
//         │                        │                        │
//         ▼                        ▼                        ▼
// ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
// │ Production:  │        │ Legacy UI:   │        │ Testing:     │
// │ Subscribe to │        │ Build tree   │        │ Unit tests   │
// │ OPC UA nodes │        │ for UI       │        │ for browse() │
// └──────────────┘        └──────────────┘        └──────────────┘
//         │                        │                        │
//         ▼                        ▼                        ▼
// ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
// │ USE:         │        │ USE:         │        │ USE:         │
// │ read_discov- │        │ core_browse_ │        │ Browse()     │
// │ er.go        │        │ frontend.go  │        │ wrapper      │
// │              │        │              │        │              │
// │ discoverNo-  │        │ GetNodeTree()│        │ (this func)  │
// │ des()        │        │              │        │              │
// └──────────────┘        └──────────────┘        └──────────────┘
//         │                        │                        │
//         ▼                        ▼                        ▼
// ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
// │ Calls:       │        │ Calls:       │        │ Calls:       │
// │ browse()     │        │ Browse()     │        │ browse()     │
// │ (internal)   │        │ wrapper      │        │ (internal)   │
// └──────────────┘        └──────────────┘        └──────────────┘
//                                  │
//                                  ▼
//                         ┌──────────────┐
//                         │ Which calls: │
//                         │ browse()     │
//                         │ (internal)   │
//                         └──────────────┘
//
// KEY INSIGHT:
// - Production code NEVER calls Browse() or GetNodeTree() directly
// - Only GetNodeTree() (legacy UI) and tests call the Browse() wrapper
// - All paths eventually use browse() internal implementation
//
// See ARCHITECTURE.md for detailed explanation of two code paths.
//
// Browse is a public wrapper function for the browse() internal implementation.
//
// ⚠️ DEPRECATION WARNING: Do NOT use this function directly in production code!
//
// ONLY use Browse() for:
// - Unit tests that need to test browse() behavior in isolation
// - Test fixtures that mock OPC UA browsing operations
//
// For production use cases:
// - Subscribe to OPC UA nodes → Use read_discover.go::discoverNodes()
// - Build UI node tree → Use core_browse_frontend.go::GetNodeTree()
//
// Why this exists:
// - Exposes browse() internal function for testing purposes
// - Allows test cases to inject mock NodeBrowser implementations
// - Not intended for production usage (lacks proper error handling/logging setup)
//
// See decision flowchart above for complete usage guidance.
func Browse(ctx context.Context, n NodeBrowser, path string, pool *GlobalWorkerPool, parentNodeId string, nodeChan chan NodeDef, errChan chan error, wg *TrackedWaitGroup, opcuaBrowserChan chan BrowseDetails, visited *sync.Map) {
	browse(ctx, n, path, pool, parentNodeId, nodeChan, errChan, wg, opcuaBrowserChan, visited)
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
	pool *GlobalWorkerPool,
	parentNodeId string,
	nodeChan chan NodeDef,
	errChan chan error,
	wg *TrackedWaitGroup,
	opcuaBrowserChan chan BrowseDetails,
	visited *sync.Map,
) {
	// Access logger and profile from pool
	logger := pool.logger
	profile := pool.Profile()

	metrics := NewServerMetrics(profile)
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
				sendError(ctx, fmt.Errorf("node %s: %w", task.node.ID().String(), err), errChan, logger)
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
				continue
			}

			// Process based on node class
			switch def.NodeClass {
			case ua.NodeClassVariable:
				select {
				case nodeChan <- def:
				case <-ctx.Done():
					logger.Warnf("Worker %s: Failed to send node due to cancellation", id)
					taskWg.Done()
					continue
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
			// Context cancellation is graceful shutdown, not an error
			return nil
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
