package config

import (
	benthos_service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/benthos"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

type FullConfig struct {
	Agent    AgentConfig     `yaml:"agent"`    // Agent config, requires restart to take effect
	Services []S6FSMConfig   `yaml:"services"` // Services to manage, can be updated while running
	Benthos  []BenthosConfig `yaml:"benthos"`  // Benthos services to manage, can be updated while running
}

type AgentConfig struct {
	MetricsPort int `yaml:"metricsPort"` // Port to expose metrics on
}

// S6FSMConfig contains configuration for creating a service
type S6FSMConfig struct {
	// For the FSM
	FSMInstanceConfig `yaml:",inline"`

	// For the S6 service
	S6ServiceConfig s6service.S6ServiceConfig `yaml:"s6ServiceConfig"`
}

// BenthosConfig contains configuration for creating a Benthos service
type BenthosConfig struct {
	// For the FSM
	FSMInstanceConfig `yaml:",inline"`

	// For the Benthos service
	BenthosServiceConfig benthos_service.BenthosServiceConfig `yaml:"benthosServiceConfig"`
}

// FSMInstanceConfig is the config for a FSM instance
type FSMInstanceConfig struct {
	Name            string `yaml:"name"`
	DesiredFSMState string `yaml:"desiredState"`
}
