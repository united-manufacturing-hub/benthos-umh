package config

import (
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/service/s6"
)

type FullConfig struct {
	Services []S6FSMConfig `yaml:"services"`
}

// S6FSMConfig contains configuration for creating a service
type S6FSMConfig struct {
	// For the S6 FSM
	Name         string `yaml:"name"`
	DesiredState string `yaml:"desiredState"`

	// For the S6 service
	S6ServiceConfig s6service.S6ServiceConfig `yaml:"s6ServiceConfig"`
}
