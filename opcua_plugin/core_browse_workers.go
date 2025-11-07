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
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// Default worker pool settings (overridden by ServerProfile)
	DefaultMaxWorkers  = 200
	DefaultMinWorkers  = 5
	InitialWorkers     = 10
	SampleSize         = 5 // Number of requests to measure response time
)

// ServerMetrics tracks worker pool performance during Browse phase.
// Workers are concurrent goroutines that traverse the OPC UA node tree in parallel.
// Each worker processes NodeTasks from a shared queue (see core_browse.go browse() function).
//
// Why worker pool matters:
// - Sequential browsing = slow (one Browse call at a time)
// - Parallel browsing = fast (MaxWorkers Browse calls simultaneously)
// - Example: 10,000 nodes with 5 workers ≈ 2000 Browse calls per worker vs 10,000 sequential
//
// ServerProfile.MaxWorkers/MinWorkers control pool size bounds (see server_profiles.go).
type ServerMetrics struct {
	mu             sync.Mutex
	responseTimes  []time.Duration
	currentWorkers int
	minWorkers     int
	maxWorkers     int
	workerControls map[uuid.UUID]chan struct{} // Channel to signal workers to stop
}

func NewServerMetrics(profile ServerProfile) *ServerMetrics {
	// Clamp initial workers to profile bounds to prevent violations
	// E.g., Auto profile (MaxWorkers=5) should start with 5, not 10
	// Priority order: MinWorkers first, then MaxWorkers (hardware limit always wins)
	initial := InitialWorkers

	// Clamp to MinWorkers if profile specifies a minimum
	if profile.MinWorkers > 0 && initial < profile.MinWorkers {
		initial = profile.MinWorkers
	}

	// Clamp to MaxWorkers if profile specifies a maximum (takes priority over MinWorkers)
	if profile.MaxWorkers > 0 && initial > profile.MaxWorkers {
		initial = profile.MaxWorkers
	}

	return &ServerMetrics{
		responseTimes:  make([]time.Duration, 0),
		workerControls: make(map[uuid.UUID]chan struct{}),
		currentWorkers: initial,
		minWorkers:     profile.MinWorkers,
		maxWorkers:     profile.MaxWorkers,
	}
}

// addWorker adds new worker browser and registers the id to the workerControls map
func (sm *ServerMetrics) addWorker(id uuid.UUID) chan struct{} {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	stopChan := make(chan struct{})
	sm.workerControls[id] = stopChan
	return stopChan
}

// removeWorker removes a worker from the workerControls map and
// stops a worker by closing the channel for the worker
func (sm *ServerMetrics) removeWorker(id uuid.UUID) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if stopChan, exists := sm.workerControls[id]; exists {
		close(stopChan)
		delete(sm.workerControls, id)
	}
}

func (sm *ServerMetrics) AverageResponseTime() time.Duration {
	if len(sm.responseTimes) == 0 {
		return 0
	}

	var totalTime time.Duration
	for _, t := range sm.responseTimes {
		totalTime += t
	}
	return totalTime / time.Duration(len(sm.responseTimes))
}

// adjustWorkers calculates the number of workers to adjust based on the response time of the last SampleSize requests
func (sm *ServerMetrics) adjustWorkers(logger Logger) (toAdd, toRemove int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(sm.responseTimes) < SampleSize {
		return 0, 0
	}

	// Latency-based scaling removed (TargetLatency constant removed per Part 5.3)
	// Static worker pool maintained until queue-based scaling implemented

	sm.responseTimes = sm.responseTimes[:0]

	// Worker scaling logic removed - will be replaced with queue-based scaling in future task
	// For now, maintain static worker pool
	// TODO: Implement queue-depth based scaling (see IMPLEMENTATION_PLAN_REVISED.md Part 5.3)
	return 0, 0
}

// recordResponseTime records the response time of a request
func (sm *ServerMetrics) recordResponseTime(duration time.Duration) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	// Discard the oldest response time once the sample size is reached
	if len(sm.responseTimes) >= SampleSize {
		sm.responseTimes = sm.responseTimes[1:]
	}
	sm.responseTimes = append(sm.responseTimes, duration)
}
