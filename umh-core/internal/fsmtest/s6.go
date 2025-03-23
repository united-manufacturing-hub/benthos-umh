package fsmtest

import (
	"fmt"
	"path/filepath"
	"time"

	internal_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/internal/fsm"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	s6fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

// Common test states
const (
	s6RunningState  = s6fsm.OperationalStateRunning
	s6StoppedState  = s6fsm.OperationalStateStopped
	s6StartingState = s6fsm.OperationalStateStarting
	s6StoppingState = s6fsm.OperationalStateStopping
)

// CreateS6TestConfig creates a minimal S6 service config for testing
func CreateS6TestConfig(name string, desiredState string) config.S6FSMConfig {
	return config.S6FSMConfig{
		FSMInstanceConfig: config.FSMInstanceConfig{
			Name:            name,
			DesiredFSMState: desiredState,
		},
		S6ServiceConfig: config.S6ServiceConfig{
			Command:     []string{"/bin/sh", "-c", "echo test"},
			Env:         map[string]string{},
			ConfigFiles: map[string]string{},
		},
	}
}

// ConfigureServiceForState sets up a mock S6 service to return the given state
func ConfigureServiceForState(mockService *s6service.MockService, servicePath string, state string) {
	mockService.ExistingServices = make(map[string]bool)
	if mockService.ServiceStates == nil {
		mockService.ServiceStates = make(map[string]s6service.ServiceInfo)
	}

	mockService.ExistingServices[servicePath] = true

	// Configure service state based on FSM state
	switch state {
	case s6RunningState:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 10,
			Pid:    12345,
		}
	case s6StoppedState:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}
	case s6StartingState:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceRestarting, // Use as proxy for "starting"
		}
	case s6StoppingState:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown, // Use as proxy for "stopping"
		}
	default:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUnknown,
		}
	}
}

// ConfigureS6ServiceConfig configures the mock service with default config
func ConfigureS6ServiceConfig(mockService *s6service.MockService) {
	mockService.GetConfigResult = config.S6ServiceConfig{
		Command:     []string{"/bin/sh", "-c", "echo test"},
		Env:         map[string]string{},
		ConfigFiles: map[string]string{},
	}
}

// TransitionToS6State simulates a transition to a new state
func TransitionToS6State(mockService *s6service.MockService, servicePath string, fromState, toState string) {
	// First configure for the current state
	ConfigureServiceForState(mockService, servicePath, fromState)

	// Then after a delay, transition to the new state
	go func() {
		time.Sleep(500 * time.Millisecond)
		ConfigureServiceForState(mockService, servicePath, toState)
	}()
}

// SetS6InstanceMockService sets the mock service on an S6Instance
// This is a testing-only utility to replace the real service with a mock
func SetS6InstanceMockService(instance interface{}, mockService *s6service.MockService, servicePath string) error {
	// We need to use type assertion to access private fields
	s6Instance, ok := instance.(*s6fsm.S6Instance)
	if !ok {
		return fmt.Errorf("instance is not an *s6fsm.S6Instance")
	}

	// Since we can't use reflection easily here, we'll use a pattern similar to createMockS6Instance in the tests
	s6Instance.SetService(mockService)
	s6Instance.SetServicePath(filepath.Join(servicePath))

	return nil
}

// SetS6InstanceState sets the state of an S6Instance and configures the mock service to match
func SetS6InstanceState(instance interface{}, state string) error {
	// Type assertion to get the S6Instance
	s6Instance, ok := instance.(*s6fsm.S6Instance)
	if !ok {
		return fmt.Errorf("instance is not an *s6fsm.S6Instance")
	}

	// Cannot set directly since fsm instance is not exported
	// Instead, we'll need to manipulate the state indirectly through the service
	mockService, ok := s6Instance.GetService().(*s6service.MockService)
	if !ok {
		return fmt.Errorf("service is not a *s6service.MockService")
	}

	// Set up the service path
	servicePath := s6Instance.GetServicePath()

	// Update the mock service state based on the desired FSM state
	switch state {
	case s6fsm.OperationalStateRunning:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUp,
			Uptime: 10,
			Pid:    12345,
		}
		mockService.ExistingServices[servicePath] = true
	case s6fsm.OperationalStateStopped:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}
		mockService.ExistingServices[servicePath] = true
	case s6fsm.OperationalStateStarting:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceRestarting, // Use as proxy for "starting"
		}
		mockService.ExistingServices[servicePath] = true
	case s6fsm.OperationalStateStopping:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown, // Use as proxy for "stopping"
		}
		mockService.ExistingServices[servicePath] = true
	case internal_fsm.LifecycleStateToBeCreated:
		mockService.ExistingServices[servicePath] = false
		delete(mockService.ServiceStates, servicePath)
	case internal_fsm.LifecycleStateCreating:
		mockService.ExistingServices[servicePath] = true
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceDown,
		}
	default:
		mockService.ServiceStates[servicePath] = s6service.ServiceInfo{
			Status: s6service.ServiceUnknown,
		}
	}

	return nil
}
