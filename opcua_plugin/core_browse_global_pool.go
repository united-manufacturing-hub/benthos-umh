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
	"github.com/google/uuid"
	"sync"
)

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
	taskChan       chan NodeTask
	workerControls map[uuid.UUID]chan struct{}
	mu             sync.Mutex
}

// NewGlobalWorkerPool creates a new global worker pool initialized with profile constraints.
//
// The pool starts with InitialWorkers (10) but clamps to profile bounds:
//   - If MinWorkers > InitialWorkers, uses MinWorkers
//   - If MaxWorkers < InitialWorkers, uses MaxWorkers (hardware limit takes priority)
//   - If MaxWorkers = 0, no upper limit (unlimited mode)
//
// Example profile behaviors:
//   - profileAuto (MaxWorkers=5): Clamps to 5 workers
//   - profileIgnition (MaxWorkers=20): Starts with 10 workers
//   - profileS71200 (MaxWorkers=10, MinWorkers=3): Starts with 10 workers
//
// The taskChan buffer is sized at 2× MaxTagsToBrowse (200k) to handle browse
// operation branching factor safely without blocking.
func NewGlobalWorkerPool(profile ServerProfile) *GlobalWorkerPool {
	initial := InitialWorkers

	// Clamp to MinWorkers if profile specifies a minimum
	if profile.MinWorkers > 0 && initial < profile.MinWorkers {
		initial = profile.MinWorkers
	}

	// Clamp to MaxWorkers if profile specifies a maximum (takes priority over MinWorkers)
	if profile.MaxWorkers > 0 && initial > profile.MaxWorkers {
		initial = profile.MaxWorkers
	}

	return &GlobalWorkerPool{
		maxWorkers:     profile.MaxWorkers,
		minWorkers:     profile.MinWorkers,
		currentWorkers: initial,
		taskChan:       make(chan NodeTask, MaxTagsToBrowse*2), // 200k buffer
		workerControls: make(map[uuid.UUID]chan struct{}),
	}
}
