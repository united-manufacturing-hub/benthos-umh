package benthos

import (
	"context"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	s6_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
)

// MockBenthosService is a mock implementation of the IBenthosService interface for testing
type MockBenthosService struct {
	// Tracks calls to methods
	GenerateS6ConfigForBenthosCalled bool
	GetConfigCalled                  bool
	StatusCalled                     bool
	AddBenthosToS6ManagerCalled      bool
	RemoveBenthosFromS6ManagerCalled bool
	StartBenthosCalled               bool
	StopBenthosCalled                bool
	ReconcileManagerCalled           bool

	// Return values for each method
	GenerateS6ConfigForBenthosResult config.S6ServiceConfig
	GenerateS6ConfigForBenthosError  error
	GetConfigResult                  config.BenthosServiceConfig
	GetConfigError                   error
	StatusResult                     ServiceInfo
	StatusError                      error
	AddBenthosToS6ManagerError       error
	RemoveBenthosFromS6ManagerError  error
	StartBenthosError                error
	StopBenthosError                 error
	ReconcileManagerError            error
	ReconcileManagerReconciled       bool

	// For more complex testing scenarios
	ServiceStates    map[string]ServiceInfo
	ExistingServices map[string]bool
	S6ServiceConfigs []config.S6FSMConfig

	// State control for FSM testing
	stateFlags map[string]*ServiceStateFlags
}

// Ensure MockBenthosService implements IBenthosService
var _ IBenthosService = (*MockBenthosService)(nil)

// ServiceStateFlags contains all the state flags needed for FSM testing
type ServiceStateFlags struct {
	IsS6Running            bool
	IsConfigLoaded         bool
	IsHealthchecksPassed   bool
	IsRunningWithoutErrors bool
	HasProcessingActivity  bool
	IsDegraded             bool
	IsS6Stopped            bool
}

// NewMockBenthosService creates a new mock Benthos service
func NewMockBenthosService() *MockBenthosService {
	return &MockBenthosService{
		ServiceStates:    make(map[string]ServiceInfo),
		ExistingServices: make(map[string]bool),
		S6ServiceConfigs: make([]config.S6FSMConfig, 0),
		stateFlags:       make(map[string]*ServiceStateFlags),
	}
}

// SetServiceState sets all state flags for a service at once
func (m *MockBenthosService) SetServiceState(serviceName string, flags ServiceStateFlags) {
	m.stateFlags[serviceName] = &flags
}

// GetServiceState gets the state flags for a service
func (m *MockBenthosService) GetServiceState(serviceName string) *ServiceStateFlags {
	if flags, exists := m.stateFlags[serviceName]; exists {
		return flags
	}
	// Initialize with default flags if not exists
	flags := &ServiceStateFlags{}
	m.stateFlags[serviceName] = flags
	return flags
}

// GenerateS6ConfigForBenthos mocks generating S6 config for Benthos
func (m *MockBenthosService) GenerateS6ConfigForBenthos(benthosConfig *config.BenthosServiceConfig, name string) (config.S6ServiceConfig, error) {
	m.GenerateS6ConfigForBenthosCalled = true
	return m.GenerateS6ConfigForBenthosResult, m.GenerateS6ConfigForBenthosError
}

// GetConfig mocks getting the Benthos configuration
func (m *MockBenthosService) GetConfig(ctx context.Context, path string) (config.BenthosServiceConfig, error) {
	m.GetConfigCalled = true
	return m.GetConfigResult, m.GetConfigError
}

// Status mocks getting the status of a Benthos service
func (m *MockBenthosService) Status(ctx context.Context, serviceName string, metricsPort int, tick uint64) (ServiceInfo, error) {
	m.StatusCalled = true

	if state, exists := m.ServiceStates[serviceName]; exists {
		// Update state based on flags
		if flags := m.GetServiceState(serviceName); flags != nil {
			state.BenthosStatus.HealthCheck.IsLive = flags.IsS6Running
			state.BenthosStatus.HealthCheck.IsReady = flags.IsConfigLoaded && flags.IsHealthchecksPassed
			if flags.IsDegraded {
				state.BenthosStatus.HealthCheck.ReadyError = "Service is degraded"
			}
			if state.BenthosStatus.MetricsState == nil {
				state.BenthosStatus.MetricsState = NewBenthosMetricsState()
			}
			state.BenthosStatus.MetricsState.IsActive = flags.HasProcessingActivity
		}
		return state, m.StatusError
	}

	return m.StatusResult, m.StatusError
}

// Helper methods for state checks
func (m *MockBenthosService) IsBenthosS6Running(serviceName string) bool {
	if flags := m.GetServiceState(serviceName); flags != nil {
		return flags.IsS6Running
	}
	return false
}

func (m *MockBenthosService) IsBenthosConfigLoaded(serviceName string) bool {
	if flags := m.GetServiceState(serviceName); flags != nil {
		return flags.IsConfigLoaded
	}
	return false
}

func (m *MockBenthosService) IsBenthosHealthchecksPassed(serviceName string) bool {
	if flags := m.GetServiceState(serviceName); flags != nil {
		return flags.IsHealthchecksPassed
	}
	return false
}

func (m *MockBenthosService) IsBenthosRunningForSomeTimeWithoutErrors(serviceName string) bool {
	if flags := m.GetServiceState(serviceName); flags != nil {
		return flags.IsRunningWithoutErrors
	}
	return false
}

func (m *MockBenthosService) IsBenthosDegraded(serviceName string) bool {
	if flags := m.GetServiceState(serviceName); flags != nil {
		return flags.IsDegraded
	}
	return false
}

func (m *MockBenthosService) IsBenthosS6Stopped(serviceName string) bool {
	if flags := m.GetServiceState(serviceName); flags != nil {
		return flags.IsS6Stopped
	}
	return false
}

func (m *MockBenthosService) HasProcessingActivity(status BenthosStatus) bool {
	return status.MetricsState != nil && status.MetricsState.IsActive
}

// AddBenthosToS6Manager mocks adding a Benthos instance to the S6 manager
func (m *MockBenthosService) AddBenthosToS6Manager(ctx context.Context, cfg *config.BenthosServiceConfig, serviceName string) error {
	m.AddBenthosToS6ManagerCalled = true

	// Check whether the service already exists
	for _, s6Config := range m.S6ServiceConfigs {
		if s6Config.Name == serviceName {
			return ErrServiceAlreadyExists
		}
	}

	// Add the service to the list of existing services
	m.ExistingServices[serviceName] = true

	// Create an S6FSMConfig for this service
	s6FSMConfig := config.S6FSMConfig{
		FSMInstanceConfig: config.FSMInstanceConfig{
			Name:            serviceName,
			DesiredFSMState: s6_fsm.OperationalStateRunning,
		},
		S6ServiceConfig: m.GenerateS6ConfigForBenthosResult,
	}

	// Add the S6FSMConfig to the list of S6FSMConfigs
	m.S6ServiceConfigs = append(m.S6ServiceConfigs, s6FSMConfig)

	return m.AddBenthosToS6ManagerError
}

// RemoveBenthosFromS6Manager mocks removing a Benthos instance from the S6 manager
func (m *MockBenthosService) RemoveBenthosFromS6Manager(ctx context.Context, serviceName string) error {
	m.RemoveBenthosFromS6ManagerCalled = true

	found := false

	// Remove the service from the list of S6FSMConfigs
	for i, s6Config := range m.S6ServiceConfigs {
		if s6Config.Name == serviceName {
			m.S6ServiceConfigs = append(m.S6ServiceConfigs[:i], m.S6ServiceConfigs[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return ErrServiceNotExist
	}

	// Remove the service from the list of existing services
	delete(m.ExistingServices, serviceName)
	delete(m.ServiceStates, serviceName)

	return m.RemoveBenthosFromS6ManagerError
}

// StartBenthos mocks starting a Benthos instance
func (m *MockBenthosService) StartBenthos(ctx context.Context, serviceName string) error {
	m.StartBenthosCalled = true

	found := false

	// Set the desired state to running for the given instance
	for i, s6Config := range m.S6ServiceConfigs {
		if s6Config.Name == serviceName {
			m.S6ServiceConfigs[i].DesiredFSMState = s6_fsm.OperationalStateRunning
			found = true
			break
		}
	}

	if !found {
		return ErrServiceNotExist
	}

	return m.StartBenthosError
}

// StopBenthos mocks stopping a Benthos instance
func (m *MockBenthosService) StopBenthos(ctx context.Context, serviceName string) error {
	m.StopBenthosCalled = true

	found := false

	// Set the desired state to stopped for the given instance
	for i, s6Config := range m.S6ServiceConfigs {
		if s6Config.Name == serviceName {
			m.S6ServiceConfigs[i].DesiredFSMState = s6_fsm.OperationalStateStopped
			found = true
			break
		}
	}

	if !found {
		return ErrServiceNotExist
	}

	return m.StopBenthosError
}

// ReconcileManager mocks reconciling the Benthos manager
func (m *MockBenthosService) ReconcileManager(ctx context.Context, tick uint64) (error, bool) {
	m.ReconcileManagerCalled = true
	return m.ReconcileManagerError, m.ReconcileManagerReconciled
}

// IsLogsFine mocks checking if the logs are fine
func (m *MockBenthosService) IsLogsFine(logs []string) bool {
	// For testing purposes, we'll consider logs fine if they're empty or nil
	// This can be enhanced based on testing needs
	return len(logs) == 0
}

// IsMetricsErrorFree mocks checking if metrics are error-free
func (m *MockBenthosService) IsMetricsErrorFree(metrics Metrics) bool {
	// For testing purposes, we'll consider metrics error-free
	// This can be enhanced based on testing needs
	return true
}
