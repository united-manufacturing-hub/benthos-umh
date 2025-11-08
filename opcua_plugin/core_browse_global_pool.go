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
	"sync"
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
type GlobalPoolTask struct {
	NodeID     string      // Node identifier to browse
	ResultChan interface{} // Accepts any channel type (e.g., chan NodeDef, chan<- NodeDef)
	ErrChan    chan<- error // Where to send errors
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
// For example, Agristo server has 64 concurrent operation capacity. With per-nodeid pools,
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
}

// NewGlobalWorkerPool creates a new global worker pool initialized with profile constraints.
//
// The pool starts with 0 workers. Use SpawnWorkers() to add workers up to profile limits:
//   - MaxWorkers: Hard upper limit (hardware capacity)
//   - MinWorkers: Soft lower limit (for scaling policies, not enforced at creation)
//   - If MaxWorkers = 0, no upper limit (unlimited mode)
//
// Example usage:
//   pool := NewGlobalWorkerPool(profileAuto)  // MaxWorkers=5
//   pool.SpawnWorkers(3)                      // Start with 3 workers
//
// The taskChan buffer is sized at 2× MaxTagsToBrowse (200k) to handle browse
// operation branching factor safely without blocking.
func NewGlobalWorkerPool(profile ServerProfile) *GlobalWorkerPool {
	return &GlobalWorkerPool{
		profile:        profile, // Store which profile this pool uses
		maxWorkers:     profile.MaxWorkers,
		minWorkers:     profile.MinWorkers,
		currentWorkers: 0, // Start with no workers - caller must spawn them
		taskChan:       make(chan GlobalPoolTask, MaxTagsToBrowse*2), // 200k buffer
		workerControls: make(map[uuid.UUID]chan struct{}),
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

		// Launch worker goroutine
		go gwp.workerLoop(workerID, controlChan)
	}

	return canSpawn
}

// SubmitTask queues a GlobalPoolTask for processing by available workers.
// Blocks if taskChan buffer is full (200k capacity should prevent this in practice).
// Returns error if pool is shutdown (channel closed).
//
// The method is thread-safe (channel operations are atomic) and non-blocking
// in practice due to large buffer size (200k = 2× MaxTagsToBrowse).
func (gwp *GlobalWorkerPool) SubmitTask(task GlobalPoolTask) (err error) {
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

			// Process task (stub implementation)
			// TODO Phase 2, Task 2.3: Replace with actual Browse RPC call
			// - On success: send NodeDef result to task.ResultChan
			// - On error: send error to task.ErrChan
			// For now, stub sends a NodeDef to typed channel
			if task.ResultChan != nil {
				// Task 2.2: Type assert to send result to correct channel type
				// Support both NodeDef channels (new) and any channels (backward compat)
				if nodeDefChan, ok := task.ResultChan.(chan NodeDef); ok {
					// Create stub NodeDef for testing
					// Task 2.3 will replace this with actual browse() results
					stubNode := NodeDef{
						NodeID: &ua.NodeID{}, // Empty NodeID for stub
					}
					nodeDefChan <- stubNode
				} else if anyNodeDefChan, ok := task.ResultChan.(chan<- NodeDef); ok {
					// Handle send-only NodeDef channel variant
					stubNode := NodeDef{
						NodeID: &ua.NodeID{}, // Empty NodeID for stub
					}
					anyNodeDefChan <- stubNode
				} else if anyChan, ok := task.ResultChan.(chan any); ok {
					// Backward compatibility for existing tests using chan any
					anyChan <- struct{}{}
				} else if anySendChan, ok := task.ResultChan.(chan<- any); ok {
					// Backward compatibility for send-only chan<- any
					anySendChan <- struct{}{}
				}
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
					if task.ResultChan != nil {
						// Task 2.2: Type assert to send result to correct channel type
						if nodeDefChan, ok := task.ResultChan.(chan NodeDef); ok {
							stubNode := NodeDef{NodeID: &ua.NodeID{}}
							nodeDefChan <- stubNode
						} else if anyNodeDefChan, ok := task.ResultChan.(chan<- NodeDef); ok {
							stubNode := NodeDef{NodeID: &ua.NodeID{}}
							anyNodeDefChan <- stubNode
						} else if anyChan, ok := task.ResultChan.(chan any); ok {
							anyChan <- struct{}{}
						} else if anySendChan, ok := task.ResultChan.(chan<- any); ok {
							anySendChan <- struct{}{}
						}
					}
				default:
					// No more tasks in buffer, exit
					break drainLoop
				}
			}
			return
		}
	}
}
