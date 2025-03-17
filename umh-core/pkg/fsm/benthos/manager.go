package benthos

import (
	"fmt"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	public_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/metrics"
)

const (
	baseBenthosDir = "/run/service" // Same as s6 for now
)

// BenthosManager implements FSM management for Benthos services.
type BenthosManager struct {
	*public_fsm.BaseFSMManager[config.BenthosConfig]

	// TODO: Add a port manager to allocate unique ports for Benthos metrics endpoints
	// portManager PortManager
}

func NewBenthosManager(name string) *BenthosManager {
	managerName := fmt.Sprintf("%s%s", logger.ComponentBenthosManager, name)

	baseManager := public_fsm.NewBaseFSMManager[config.BenthosConfig](
		managerName,
		baseBenthosDir,
		// Extract Benthos configs from full config
		func(fullConfig config.FullConfig) ([]config.BenthosConfig, error) {
			return fullConfig.Benthos, nil
		},
		// Get name from Benthos config
		func(cfg config.BenthosConfig) (string, error) {
			return cfg.Name, nil
		},
		// Get desired state from Benthos config
		func(cfg config.BenthosConfig) (string, error) {
			return cfg.DesiredFSMState, nil
		},
		// Create Benthos instance from config
		func(cfg config.BenthosConfig) (public_fsm.FSMInstance, error) {
			return NewBenthosInstance(baseBenthosDir, cfg), nil
		},
		// Compare Benthos configs
		func(instance public_fsm.FSMInstance, cfg config.BenthosConfig) (bool, error) {
			benthosInstance, ok := instance.(*BenthosInstance)
			if !ok {
				return false, fmt.Errorf("instance is not a BenthosInstance")
			}
			return benthosInstance.config.Equal(cfg.BenthosServiceConfig), nil
		},
		// Set Benthos config
		func(instance public_fsm.FSMInstance, cfg config.BenthosConfig) error {
			benthosInstance, ok := instance.(*BenthosInstance)
			if !ok {
				return fmt.Errorf("instance is not a BenthosInstance")
			}
			benthosInstance.config = cfg.BenthosServiceConfig
			return nil
		},
	)

	metrics.InitErrorCounter(metrics.ComponentBenthosManager, name)

	return &BenthosManager{
		BaseFSMManager: baseManager,
	}
}

// TODO: Add a PortManager for allocating unique ports for Benthos metrics endpoints
// type PortManager interface {
//     AllocatePort(instanceName string) (int, error)
//     ReleasePort(instanceName string) error
//     GetPort(instanceName string) (int, bool)
// }
