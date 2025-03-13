package control

// This package contains the control loop for the UMH Lite.
// It is responsible for creating the managers, starting and executing the single-threaded control loop, and then calling
// the managers reconcile functions

import (
	"context"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/fsm"
)

const (
	defaultTickerTime = 1 * time.Second
)

type ControlLoop struct {
	tickerTime    time.Duration
	managers      []fsm.FSMManager
	configManager config.ConfigManager
}

func NewControlLoop(managers []fsm.FSMManager, configManager config.ConfigManager) *ControlLoop {
	return &ControlLoop{
		managers:      managers,
		tickerTime:    defaultTickerTime,
		configManager: configManager,
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
			timeoutCtx, cancel := context.WithTimeout(ctx, c.tickerTime)
			defer cancel()

			err := c.Reconcile(timeoutCtx)

			// Any unhandled error will result in the control loop stopping
			if err != nil {
				return err
			}
		}
	}
}

// Reconcile first fetches the config from the config manager
// then reconciles each manager
func (c *ControlLoop) Reconcile(ctx context.Context) error {
	config, err := c.configManager.GetConfig(ctx)
	if err != nil {
		return err
	}

	for _, manager := range c.managers {
		err := manager.Reconcile(ctx, config)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ControlLoop) Stop(ctx context.Context) error {
	ctx.Done()
	return nil
}
