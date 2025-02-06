package opcua_plugin

import (
	"sync"
	"time"
)

const (
	MaxWorkers     = 200
	MinWorkers     = 5
	InitialWorkers = 10
	SampleSize     = 5 // Number of requsts to measure response time
	TargetLatency  = 250 * time.Millisecond
)

// ServerMetrics is a struct that holds the metrics for the OPCUA server requests
type ServerMetrics struct {
	mu             sync.Mutex
	responseTimes  []time.Duration
	currentWorkers int
	targetLatency  time.Duration
	workerControls map[int]chan struct{} // Channel to signal workers to stop
}

func NewServerMetrics() *ServerMetrics {
	return &ServerMetrics{
		responseTimes:  make([]time.Duration, 0),
		workerControls: make(map[int]chan struct{}),
		targetLatency:  TargetLatency,
		currentWorkers: InitialWorkers,
	}
}

// addWorker adds new worker browser and registers the id to the workerControls map
func (sm *ServerMetrics) addWorker(id int) chan struct{} {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	stopChan := make(chan struct{})
	sm.workerControls[id] = stopChan
	return stopChan
}

// removeWorker removes a worker from the workerControls map and
// stops a worker by closing the channel for the worker
func (sm *ServerMetrics) removeWorker(id int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if stopChan, exists := sm.workerControls[id]; exists {
		close(stopChan)
		delete(sm.workerControls, id)
	}
}

// adjustWorkers calculates the number of workers to adjust based on the response time of the last SampleSize requests
func (sm *ServerMetrics) adjustWorkers(logger Logger) (toAdd, toRemove int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(sm.responseTimes) < SampleSize {
		return 0, 0
	}

	var totalTime time.Duration
	for _, t := range sm.responseTimes {
		totalTime += t
	}
	avgResponse := totalTime / time.Duration(len(sm.responseTimes))
	oldWorkerCount := sm.currentWorkers

	if avgResponse > sm.targetLatency {
		// Response time is too high. Reduce workers
		sm.currentWorkers = max(MinWorkers, sm.currentWorkers-10)
		logger.Debugf("Response time is high (%v > %v target Latency), reducing workers from %d to %d", avgResponse, sm.targetLatency, oldWorkerCount, sm.currentWorkers)
	}

	if avgResponse < sm.targetLatency {
		// Response time is too low. Increase workers
		sm.currentWorkers = min(MaxWorkers, sm.currentWorkers+10)
		logger.Debugf("Response time is low (%v < %v target Latency), increasing workers from %d to %d", avgResponse, sm.targetLatency, oldWorkerCount, sm.currentWorkers)
	}

	sm.responseTimes = sm.responseTimes[:0]
	if sm.currentWorkers > oldWorkerCount {
		return sm.currentWorkers - oldWorkerCount, 0
	}

	return 0, oldWorkerCount - sm.currentWorkers
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
