package portmanager

import (
	"fmt"
	"sync"
)

// ErrPortInUse is returned when a port is already in use by another service
var ErrPortInUse = fmt.Errorf("port is already in use by another service")

// MockPortManager is a mock implementation of PortManager for testing
type MockPortManager struct {
	sync.Mutex
	Ports              map[string]int
	AllocatedPorts     map[int]string
	ReservedPorts      map[int]bool
	AllocatePortCalled bool
	ReleasePortCalled  bool
	GetPortCalled      bool
	ReservePortCalled  bool
	AllocatePortResult int
	AllocatePortError  error
	ReleasePortError   error
	ReservePortError   error
}

// Ensure MockPortManager implements PortManager
var _ PortManager = (*MockPortManager)(nil)

// NewMockPortManager creates a new MockPortManager
func NewMockPortManager() *MockPortManager {
	return &MockPortManager{
		Ports:          make(map[string]int),
		AllocatedPorts: make(map[int]string),
		ReservedPorts:  make(map[int]bool),
	}
}

// AllocatePort allocates a port for the given service
func (m *MockPortManager) AllocatePort(serviceName string) (int, error) {
	m.Lock()
	defer m.Unlock()

	m.AllocatePortCalled = true

	if m.AllocatePortError != nil {
		return 0, m.AllocatePortError
	}

	// If result is preset, return it
	if m.AllocatePortResult != 0 {
		m.Ports[serviceName] = m.AllocatePortResult
		m.AllocatedPorts[m.AllocatePortResult] = serviceName
		return m.AllocatePortResult, nil
	}

	// If already allocated, return existing port
	if port, ok := m.Ports[serviceName]; ok {
		return port, nil
	}

	// Otherwise allocate a new port (simple implementation for testing)
	port := 9000 + len(m.Ports)
	m.Ports[serviceName] = port
	m.AllocatedPorts[port] = serviceName
	return port, nil
}

// ReleasePort releases a port for the given service
func (m *MockPortManager) ReleasePort(serviceName string) error {
	m.Lock()
	defer m.Unlock()

	m.ReleasePortCalled = true

	if m.ReleasePortError != nil {
		return m.ReleasePortError
	}

	if port, ok := m.Ports[serviceName]; ok {
		delete(m.Ports, serviceName)
		delete(m.AllocatedPorts, port)
	}

	return nil
}

// GetPort returns the port for the given service
func (m *MockPortManager) GetPort(serviceName string) (int, bool) {
	m.Lock()
	defer m.Unlock()

	m.GetPortCalled = true

	port, ok := m.Ports[serviceName]
	return port, ok
}

// ReservePort reserves a specific port for the given service
func (m *MockPortManager) ReservePort(serviceName string, port int) error {
	m.Lock()
	defer m.Unlock()

	m.ReservePortCalled = true

	if m.ReservePortError != nil {
		return m.ReservePortError
	}

	// Check if port is already reserved by another service
	if existingService, ok := m.AllocatedPorts[port]; ok && existingService != serviceName {
		return ErrPortInUse
	}

	// Reserve the port
	m.ReservedPorts[port] = true
	m.Ports[serviceName] = port
	m.AllocatedPorts[port] = serviceName

	return nil
}
