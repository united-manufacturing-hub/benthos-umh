package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	"go.uber.org/zap"
)

// StarvationChecker is a manager that checks if the control loop has been starved
type StarvationChecker struct {
	starvationThreshold time.Duration
	lastReconcileTime   time.Time
	mutex               sync.RWMutex
	logger              *zap.SugaredLogger
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
}

// NewStarvationChecker creates a new starvation checker
func NewStarvationChecker(threshold time.Duration) *StarvationChecker {
	ctx, cancel := context.WithCancel(context.Background())
	sc := &StarvationChecker{
		starvationThreshold: threshold,
		lastReconcileTime:   time.Now(),
		logger:              logger.For(logger.ComponentStarvationChecker),
		ctx:                 ctx,
		cancel:              cancel,
	}

	sc.wg.Add(1)
	go sc.checkStarvationLoop()

	sc.logger.Infof("Starvation checker created with threshold %s", threshold)

	return sc
}

// checkStarvationLoop continuously checks for starvation every second
func (s *StarvationChecker) checkStarvationLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mutex.RLock()
			timeSinceLastReconcile := time.Since(s.lastReconcileTime)
			s.mutex.RUnlock()

			if timeSinceLastReconcile > s.starvationThreshold {
				starvationTime := timeSinceLastReconcile.Seconds()
				AddStarvationTime(starvationTime)
				s.logger.Warnf("Control loop starvation detected: %.2f seconds since last reconcile", starvationTime)
			}
		}
	}
}

// Stop stops the starvation checker
func (s *StarvationChecker) Stop() {
	s.logger.Info("Stopping starvation checker")
	s.cancel()
	s.wg.Wait()
	s.logger.Info("Starvation checker stopped")
}

// UpdateLastReconcileTime updates the timestamp of the last reconciliation
func (s *StarvationChecker) UpdateLastReconcileTime() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.lastReconcileTime = time.Now()
}

// GetLastReconcileTime returns the timestamp of the last reconciliation
func (s *StarvationChecker) GetLastReconcileTime() time.Time {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.lastReconcileTime
}

// GetManagerName returns the name of the manager
func (s *StarvationChecker) GetManagerName() string {
	return logger.ComponentStarvationChecker
}

// Reconcile checks if the control loop has been starved
// Returns error (always nil in this implementation), and whether a starvation was detected
func (s *StarvationChecker) Reconcile(ctx context.Context, config config.FullConfig) (error, bool) {
	s.UpdateLastReconcileTime()

	// No starvation detected
	return nil, false
}
