package config

import (
	benthos_service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/benthos"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

type FullConfig struct {
	Services []S6FSMConfig   `yaml:"services"`
	Benthos  []BenthosConfig `yaml:"benthos"`
}

// S6FSMConfig contains configuration for creating a service
type S6FSMConfig struct {
	// For the S6 FSM
	Name         string `yaml:"name"`
	DesiredState string `yaml:"desiredState"`

	// For the S6 service
	S6ServiceConfig s6service.S6ServiceConfig `yaml:"s6ServiceConfig"`
}

// BenthosConfig contains configuration for creating a Benthos service
type BenthosConfig struct {
	// For the Benthos FSM
	Name         string `yaml:"name"`
	DesiredState string `yaml:"desiredState"`

	// For the Benthos service
	BenthosServiceConfig benthos_service.BenthosServiceConfig `yaml:"benthosServiceConfig"`
}
