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
	"github.com/google/uuid"
	"sync"
)

// GlobalPoolTask represents a browse operation to execute in the global worker pool.
// This is a simplified task structure focused on queueing and result delivery.
// It differs from NodeTask in core_browse.go which contains browse execution state.
type GlobalPoolTask struct {
	NodeID     string       // Node identifier to browse
	ResultChan chan<- any   // Where to send result (for now, accepts any type for stub)
	ErrChan    chan<- error // Where to send errors
}

// GlobalWorkerPool manages a shared pool of workers for OPC UA browse operations.
// Instead of each nodeid spawning its own worker pool (300 nodeids × 5 workers = 1,500),
// this provides a single global pool that respects server capacity limits.
//
// For example, Agristo server has 64 concurrent operation capacity. With per-nodeid pools,
// we get 1,500 concurrent workers (23× overload) causing EOF errors. Global pool caps at
// profile.MaxWorkers (e.g., 20 for Ignition, 5 for Auto) preventing overload.
type GlobalWorkerPool struct {
	maxWorkers     int
	minWorkers     int
	currentWorkers int
	taskChan       chan GlobalPoolTask
	workerControls map[uuid.UUID]chan struct{}
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

// workerLoop runs in a goroutine and processes tasks from taskChan until shutdown signal.
func (gwp *GlobalWorkerPool) workerLoop(workerID uuid.UUID, controlChan chan struct{}) {
	defer func() {
		// Clean up worker registration BEFORE panic recovery
		gwp.mu.Lock()
		delete(gwp.workerControls, workerID)
		gwp.currentWorkers--
		gwp.mu.Unlock()

		// THEN handle panic recovery
		if r := recover(); r != nil {
			// Log and exit gracefully on panic (stub for now)
			// In production, would use proper logger here
		}
	}()

	for {
		select {
		case <-controlChan:
			// Shutdown signal received
			return
		case task, ok := <-gwp.taskChan:
			if !ok {
				// Channel closed, exit
				return
			}

			// Process task (stub implementation)
			// TODO: Replace with actual Browse RPC in Phase 2
			// For now, just signal completion with empty result
			if task.ResultChan != nil {
				// Simulate successful browse (empty result)
				// Using empty struct since we're using any type for stub
				task.ResultChan <- struct{}{}
			}
		}
	}
}
