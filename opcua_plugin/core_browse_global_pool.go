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
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gopcua/opcua/ua"
)

// GlobalPoolTask represents a browse operation to execute in the global worker pool.
// This is a simplified task structure focused on queueing and result delivery.
// It differs from NodeTask in core_browse.go which contains browse execution state.
//
// Task 2.2: ResultChan now accepts any channel type via interface{}.
// Workers use type assertion to send results to the correct typed channel.
// This enables browse() to provide chan NodeDef while maintaining flexibility.
//
// Task 3.2: ProgressChan added for optional BrowseDetails progress reporting.
// When nil, no progress updates are sent. When set, workers send non-blocking
// progress updates during task processing.
type GlobalPoolTask struct {
	NodeID       string             // Node identifier to browse
	ResultChan   interface{}        // Accepts any channel type (e.g., chan NodeDef, chan<- NodeDef)
	ErrChan      chan<- error       // Where to send errors
	ProgressChan chan<- BrowseDetails // Optional progress updates (nil = no reporting)
}

// GlobalWorkerPool manages a shared pool of workers for OPC UA browse operations.
//
// Architecture Note: This is a NEW global worker pool implementation that will
// replace the per-browse worker pool pattern in Phase 2 (Integration).
// Unlike the old pattern where each browse() call spawned its own pool,
// this design uses a single shared pool for all concurrent browse operations.
//
// Key differences from old pattern:
// - Explicit 0-initialization (caller must spawn workers)
// - Profile limits apply GLOBALLY (not per-browse)
// - Task queueing via shared channel (not per-browse WaitGroup)
//
// For example, a production server has 64 concurrent operation capacity. With per-nodeid pools,
// we get 1,500 concurrent workers (23× overload) causing EOF errors. Global pool caps at
// profile.MaxWorkers (e.g., 20 for Ignition, 5 for Auto) preventing overload.
type GlobalWorkerPool struct {
	profile        ServerProfile // Which profile this pool was created with
	maxWorkers     int
	minWorkers     int
	currentWorkers int
	taskChan       chan GlobalPoolTask
	workerControls map[uuid.UUID]chan struct{}
	shutdown       bool // Indicates if pool is shutting down
	workerWg       sync.WaitGroup
	mu             sync.Mutex
	logger         Logger // For debug logging in sendTaskResult

	// Task 3.1: Metrics tracking (using atomic operations for lock-free access)
	metricsTasksSubmitted uint64 // Total tasks submitted (atomic)
	metricsTasksCompleted uint64 // Successfully completed tasks (atomic)
	metricsTasksFailed    uint64 // Failed tasks (errors) (atomic)
}

// PoolMetrics contains current pool metrics snapshot.
// Task 3.1: Metrics aggregation for visibility into pool operations.
type PoolMetrics struct {
	ActiveWorkers  int    // Current worker count
	TasksSubmitted uint64 // Total submitted
	TasksCompleted uint64 // Successfully completed
	TasksFailed    uint64 // Failed with errors
	QueueDepth     int    // Pending tasks in queue
}

// NewGlobalWorkerPool creates a new global worker pool initialized with profile constraints.
//
// The pool starts with 0 workers. Use SpawnWorkers() to add workers up to profile limits:
//   - MaxWorkers: Hard upper limit (hardware capacity)
//   - MinWorkers: Soft lower limit (for scaling policies, not enforced at creation)
//   - If MaxWorkers = 0, no upper limit (unlimited mode)
//
// Example usage:
//   pool := NewGlobalWorkerPool(profileAuto, logger)  // MaxWorkers=5
//   pool.SpawnWorkers(3)                              // Start with 3 workers
//
// The taskChan buffer is sized at 2× MaxTagsToBrowse (200k) to handle browse
// operation branching factor safely without blocking.
func NewGlobalWorkerPool(profile ServerProfile, logger Logger) *GlobalWorkerPool {
	return &GlobalWorkerPool{
		profile:        profile, // Store which profile this pool uses
		maxWorkers:     profile.MaxWorkers,
		minWorkers:     profile.MinWorkers,
		currentWorkers: 0, // Start with no workers - caller must spawn them
		taskChan:       make(chan GlobalPoolTask, MaxTagsToBrowse*2), // 200k buffer
		workerControls: make(map[uuid.UUID]chan struct{}),
		logger:         logger, // For debug logging in sendTaskResult
	}
}

// SpawnWorkers starts n new workers if under MaxWorkers limit.
// Returns actual number of workers spawned (may be less than n if hitting limit).
//
// Behavior:
//   - If MaxWorkers=0 (unlimited), spawns all n workers
//   - If MaxWorkers>0, spawns only up to limit (currentWorkers + spawned <= MaxWorkers)
//   - Thread-safe (uses mutex)
func (gwp *GlobalWorkerPool) SpawnWorkers(n int) int {
	gwp.mu.Lock()
	defer gwp.mu.Unlock()

	// Prevent spawning after shutdown
	if gwp.shutdown {
		return 0
	}

	// Calculate how many workers we can actually spawn
	canSpawn := n
	if gwp.maxWorkers > 0 {
		available := gwp.maxWorkers - gwp.currentWorkers
		if available <= 0 {
			return 0
		}
		if canSpawn > available {
			canSpawn = available
		}
	}

	// Spawn workers
	for i := 0; i < canSpawn; i++ {
		workerID := uuid.New()
		controlChan := make(chan struct{})
		gwp.workerControls[workerID] = controlChan
		gwp.currentWorkers++
		gwp.workerWg.Add(1)

		// Task 3.3: Debug log worker spawn with total count
		if gwp.logger != nil {
			gwp.logger.Debugf("Worker spawned: workerID=%s totalWorkers=%d", workerID, gwp.currentWorkers)
		}

		// Launch worker goroutine
		go gwp.workerLoop(workerID, controlChan)
	}

	return canSpawn
}

// SubmitTask queues a GlobalPoolTask for processing by available workers.
// Blocks if taskChan buffer is full (200k capacity should prevent this in practice).
// Returns error if pool is shutdown (channel closed) or if ResultChan is not a channel type.
//
// The method is thread-safe (channel operations are atomic) and non-blocking
// in practice due to large buffer size (200k = 2× MaxTagsToBrowse).
func (gwp *GlobalWorkerPool) SubmitTask(task GlobalPoolTask) (err error) {
	// Task 2.3: Runtime validation - verify ResultChan is actually a channel type
	if task.ResultChan != nil {
		rv := reflect.ValueOf(task.ResultChan)
		if rv.Kind() != reflect.Chan {
			return fmt.Errorf("invalid ResultChan type: expected channel, got %T", task.ResultChan)
		}
	}

	// Check shutdown flag first (before attempting send)
	gwp.mu.Lock()
	if gwp.shutdown {
		gwp.mu.Unlock()
		return errors.New("pool is shutdown")
	}
	gwp.mu.Unlock()

	defer func() {
		// Recover from panic if channel is closed (send on closed channel panics)
		if r := recover(); r != nil {
			err = errors.New("pool is shutdown")
		}
	}()

	// Task 3.1: Increment tasksSubmitted counter (atomic)
	atomic.AddUint64(&gwp.metricsTasksSubmitted, 1)

	// Task 3.3: Debug log task submission with metrics
	if gwp.logger != nil {
		metrics := gwp.GetMetrics()
		gwp.logger.Debugf("Task submitted: nodeID=%s queueDepth=%d workers=%d", task.NodeID, metrics.QueueDepth, metrics.ActiveWorkers)
	}

	// Send task to channel (will panic if closed, recovered by defer)
	gwp.taskChan <- task
	return nil
}

// Shutdown gracefully stops all workers and closes the task channel.
// Waits up to timeout for workers to finish current tasks.
// Returns error if timeout exceeded before all workers exited.
//
// This method is idempotent - calling multiple times is safe.
// After shutdown, SubmitTask rejects new tasks and SpawnWorkers blocks.
func (gwp *GlobalWorkerPool) Shutdown(timeout time.Duration) error {
	gwp.mu.Lock()

	// Idempotency: Check if already shutdown
	if gwp.shutdown {
		gwp.mu.Unlock()
		return nil
	}

	// Task 3.3: Debug log shutdown initiation
	workerCount := gwp.currentWorkers
	if gwp.logger != nil {
		gwp.logger.Debugf("Shutdown initiated: workers=%d timeout=%v", workerCount, timeout)
	}

	// Mark as shutdown (prevents new tasks/workers)
	gwp.shutdown = true

	// Close taskChan to reject new tasks
	close(gwp.taskChan)

	// Signal all workers to exit
	for _, controlChan := range gwp.workerControls {
		close(controlChan)
	}

	gwp.mu.Unlock()

	// Wait for workers to exit (with timeout)
	done := make(chan struct{})
	go func() {
		gwp.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Task 3.3: Debug log shutdown completion
		if gwp.logger != nil {
			gwp.logger.Debugf("Shutdown complete: all workers exited")
		}
		return nil
	case <-time.After(timeout):
		return errors.New("shutdown timeout: workers still running")
	}
}

// Profile returns the ServerProfile this pool was created with.
// Useful for Phase 2 integration where browse() selects pool based on server profile.
func (gwp *GlobalWorkerPool) Profile() ServerProfile {
	return gwp.profile
}

// GetMetrics returns current pool metrics snapshot.
// Task 3.1: Thread-safe metrics access for visibility into pool operations.
// Uses atomic loads for lock-free metric reads with single mutex for pool state.
func (gwp *GlobalWorkerPool) GetMetrics() PoolMetrics {
	// Atomic loads for metrics (lock-free)
	tasksSubmitted := atomic.LoadUint64(&gwp.metricsTasksSubmitted)
	tasksCompleted := atomic.LoadUint64(&gwp.metricsTasksCompleted)
	tasksFailed := atomic.LoadUint64(&gwp.metricsTasksFailed)

	// Single mutex for pool state snapshot
	gwp.mu.Lock()
	activeWorkers := gwp.currentWorkers
	queueDepth := len(gwp.taskChan)
	gwp.mu.Unlock()

	return PoolMetrics{
		ActiveWorkers:  activeWorkers,
		TasksSubmitted: tasksSubmitted,
		TasksCompleted: tasksCompleted,
		TasksFailed:    tasksFailed,
		QueueDepth:     queueDepth,
	}
}

// sendTaskResult sends task result to ResultChan using type assertion.
// Returns true if result was sent, false if channel type unsupported or nil.
//
// Task 2.3: Extracted helper to eliminate 24 lines of duplicated type assertion code
// in workerLoop (main loop + drain loop). Supports:
//   - chan NodeDef (new browse() integration)
//   - chan<- NodeDef (send-only variant)
//   - chan any (backward compatibility)
//   - chan<- any (send-only backward compat)
//
// Logs debug message when channel type is unsupported (helps diagnose integration issues).
func (gwp *GlobalWorkerPool) sendTaskResult(task GlobalPoolTask, stubNode NodeDef, logger Logger) bool {
	if task.ResultChan == nil {
		return false
	}

	// Type assert to correct channel variant
	switch ch := task.ResultChan.(type) {
	case chan NodeDef:
		ch <- stubNode
		return true
	case chan<- NodeDef:
		ch <- stubNode
		return true
	case chan any:
		ch <- struct{}{} // Backward compat
		return true
	case chan<- any:
		ch <- struct{}{} // Backward compat
		return true
	default:
		// Log unsupported channel type for debugging
		if logger != nil {
			logger.Debugf("GlobalWorkerPool: unsupported ResultChan type: %T (expected chan NodeDef or chan any)", task.ResultChan)
		}
		return false
	}
}

// sendTaskProgress sends progress update to ProgressChan if set (non-blocking).
// Task 3.2: Extracted helper to eliminate code duplication in workerLoop.
// Uses select with default to avoid blocking on full/closed channels.
func (gwp *GlobalWorkerPool) sendTaskProgress(task GlobalPoolTask, stubNode NodeDef) {
	if task.ProgressChan == nil {
		return
	}

	gwp.mu.Lock()
	workerCount := gwp.currentWorkers
	gwp.mu.Unlock()

	progress := BrowseDetails{
		NodeDef:     stubNode,
		TaskCount:   1, // Stub value
		WorkerCount: int64(workerCount),
	}

	select {
	case task.ProgressChan <- progress:
		// Progress sent successfully
	default:
		// Channel full or closed, don't block task processing
	}
}

// workerLoop runs in a goroutine and processes tasks from taskChan until shutdown signal.
func (gwp *GlobalWorkerPool) workerLoop(workerID uuid.UUID, controlChan chan struct{}) {
	defer func() {
		// Clean up worker registration BEFORE panic recovery
		gwp.mu.Lock()
		delete(gwp.workerControls, workerID)
		gwp.currentWorkers--
		gwp.mu.Unlock()

		// Signal WaitGroup that this worker is done
		gwp.workerWg.Done()

		// THEN handle panic recovery
		if r := recover(); r != nil {
			// Log and exit gracefully on panic (stub for now)
			// In production, would use proper logger here
		}
	}()

	// Priority: Process tasks before checking shutdown signal
	for {
		select {
		case task, ok := <-gwp.taskChan:
			if !ok {
				// Channel closed, exit
				return
			}

			// Process task
		// Browse operations are executed outside pool workers via browse() goroutines
		// Pool provides worker coordination and metrics tracking
			stubNode := NodeDef{NodeID: &ua.NodeID{}} // Empty NodeID placeholder
			sent := gwp.sendTaskResult(task, stubNode, gwp.logger)
			if !sent && task.ResultChan != nil {
				// Debug log if send failed (unsupported channel type)
				if gwp.logger != nil {
					gwp.logger.Debugf("Worker %s: failed to send result for NodeID %s (unsupported channel type)", workerID, task.NodeID)
				}
			}

			// Task 3.2: Send progress update (non-blocking)
			gwp.sendTaskProgress(task, stubNode)

			// Task 3.1: Increment tasksCompleted counter on success (atomic)
			atomic.AddUint64(&gwp.metricsTasksCompleted, 1)

			// Task 3.3: Debug log task completion
			if gwp.logger != nil {
				gwp.logger.Debugf("Task completed: nodeID=%s", task.NodeID)
			}
		case <-controlChan:
			// Shutdown signal received - drain remaining buffered tasks before exit
			// Use non-blocking drain to avoid hanging if taskChan is still open
		drainLoop:
			for {
				select {
				case task, ok := <-gwp.taskChan:
					if !ok {
						// Channel closed, finished draining
						return
					}
					// Process remaining task (stub implementation)
					stubNode := NodeDef{NodeID: &ua.NodeID{}}
					gwp.sendTaskResult(task, stubNode, gwp.logger)

					// Task 3.2: Send progress update (non-blocking)
					gwp.sendTaskProgress(task, stubNode)

					// Task 3.1: Increment tasksCompleted counter (atomic)
					atomic.AddUint64(&gwp.metricsTasksCompleted, 1)
				default:
					// No more tasks in buffer, exit
					break drainLoop
				}
			}
			return
		}
	}
}
