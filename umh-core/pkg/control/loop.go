package control

// This package contains the control loop for the UMH Lite.
// It is responsible for creating the managers, starting and executing the single-threaded control loop, and then calling
// the managers reconcile functions

import (
	"context"
	"errors"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	"go.uber.org/zap"
)

const (
	defaultTickerTime = 30 * time.Millisecond
)

type ControlLoop struct {
	tickerTime    time.Duration
	managers      []fsm.FSMManager
	configManager config.ConfigManager
	logger        *zap.SugaredLogger
}

func NewControlLoop() *ControlLoop {
	// Get a component-specific logger
	log := logger.For(logger.ComponentControlLoop)

	// Create the managers
	managers := []fsm.FSMManager{
		s6.NewS6Manager(),
	}

	// Create the config manager
	configManager := config.NewFileConfigManager()

	return &ControlLoop{
		managers:      managers,
		tickerTime:    defaultTickerTime,
		configManager: configManager,
		logger:        log,
	}
}

// Execute the control loop
// This function will block until the context is cancelled
// it uses a ticker to reconcile the managers at a regular interval
func (c *ControlLoop) Execute(ctx context.Context) error {
	ticker := time.NewTicker(c.tickerTime)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Create a timeout context for the reconcile
			timeoutCtx, cancel := context.WithTimeout(ctx, c.tickerTime)
			defer cancel()

			// Measure reconcile time
			start := time.Now()
			// Reconcile the managers
			err := c.Reconcile(timeoutCtx)
			cycleTime := time.Since(start)
			c.logger.Debugf("Reconcile cycle took %v", cycleTime)

			// Handle errors differently based on type
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					// For timeouts, log warning but continue
					c.logger.Warnf("Control loop reconcile timed out: %v", err)
				} else if errors.Is(err, context.Canceled) {
					// For cancellation, exit the loop
					return nil
				} else {
					// Any other unhandled error will result in the control loop stopping
					return err
				}
			}
		}
	}
}

// Reconcile first fetches the config from the config manager
// then reconciles each manager
func (c *ControlLoop) Reconcile(ctx context.Context) error {

	// Measure config fetch time
	start := time.Now()

	// Get the config
	config, err := c.configManager.GetConfig(ctx)

	// Log the config fetch time
	fetchTime := time.Since(start)
	c.logger.Debugf("Config fetch took %v", fetchTime)
	if err != nil {
		return err
	}

	// Reconcile each manager
	for _, manager := range c.managers {
		err, reconciled := manager.Reconcile(ctx, config)
		if err != nil {
			return err
		}

		// If the manager was reconciled, skip the reconcilation of the next managers
		if reconciled {
			return nil
		}
	}

	// Return nil if no errors occurred
	return nil
}

func (c *ControlLoop) Stop(ctx context.Context) error {
	ctx.Done()
	return nil
}
