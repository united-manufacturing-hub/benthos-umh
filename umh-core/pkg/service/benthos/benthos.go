package benthos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"text/template"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	"go.uber.org/zap"

	s6_fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
)

// ServiceInfo contains information about a Benthos service
type ServiceInfo struct {
}

// Service is the default implementation of the S6 Service interface
type BenthosService struct {
	logger           *zap.SugaredLogger
	template         *template.Template
	s6Manager        *s6_fsm.S6Manager
	s6ServiceConfigs []config.S6FSMConfig
}

// NewDefaultBenthosService creates a new default Benthos service
func NewDefaultBenthosService(name string) *BenthosService {
	managerName := fmt.Sprintf("%s%s", logger.ComponentBenthosService, name)
	return &BenthosService{
		logger:    logger.For(managerName),
		template:  benthosYamlTemplate,
		s6Manager: s6_fsm.NewS6Manager(managerName),
	}
}

// generateBenthosYaml generates a Benthos YAML configuration from a BenthosServiceConfig
func (s *BenthosService) generateBenthosYaml(config *config.BenthosServiceConfig) (string, error) {
	var b bytes.Buffer
	err := s.template.Execute(&b, config)
	return b.String(), err
}

// generateS6ConfigForBenthos creates a S6 config for a given benthos instance
func (s *BenthosService) GenerateS6ConfigForBenthos(benthosConfig *config.BenthosServiceConfig, name string) (s6Config config.S6ServiceConfig, err error) {
	benthosConfigFileName := "benthos.yaml"

	S6ServiceName := fmt.Sprintf("benthos-%s", name)
	configPath := fmt.Sprintf("/run/service/%s/%s", S6ServiceName, benthosConfigFileName)

	yamlConfig, err := s.generateBenthosYaml(benthosConfig)
	if err != nil {
		return config.S6ServiceConfig{}, err
	}

	// TODO: Only add the service config if it doesn't already exist
	s6Config = config.S6ServiceConfig{
		Command: []string{
			"/usr/local/bin/benthos",
			"-c",
			configPath,
		},
		Env: map[string]string{},
		ConfigFiles: map[string]string{
			benthosConfigFileName: yamlConfig,
		},
	}

	return s6Config, nil
}

// GetConfig returns the actual Benthos config from the S6 service
func (s *BenthosService) GetConfig(ctx context.Context, path string) (config.BenthosServiceConfig, error) {
	return config.BenthosServiceConfig{}, nil
}

// Status checks the status of a Benthos service and returns ServiceInfo
func (s *BenthosService) Status(ctx context.Context, serviceName string) (ServiceInfo, error) {
	// For now, get the status of the underlying s6 service
	// TODO: do this through the s6 manager
	return ServiceInfo{}, nil
}

// AddBenthosToS6Manager adds a Benthos instance to the S6 manager
func (s *BenthosService) AddBenthosToS6Manager(ctx context.Context, cfg *config.BenthosServiceConfig, serviceName string) error {
	if s.s6Manager == nil {
		return errors.New("s6 manager not initialized")
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Check whether s6ServiceConfigs already contains an entry for this instance
	for _, s6Config := range s.s6ServiceConfigs {
		if s6Config.Name == serviceName {
			return ErrServiceAlreadyExists
		}
	}

	// Generate the S6 config for this instance
	s6Config, err := s.GenerateS6ConfigForBenthos(cfg, serviceName)
	if err != nil {
		return fmt.Errorf("failed to generate S6 config for Benthos service %s: %w", serviceName, err)
	}

	// Create the S6 FSM config for this instance
	s6FSMConfig := config.S6FSMConfig{
		FSMInstanceConfig: config.FSMInstanceConfig{
			Name:            serviceName,
			DesiredFSMState: s6_fsm.OperationalStateRunning,
		},
		S6ServiceConfig: s6Config,
	}

	// Add the S6 FSM config to the list of S6 FSM configs
	// so that the S6 manager will start the service
	s.s6ServiceConfigs = append(s.s6ServiceConfigs, s6FSMConfig)

	return nil
}

// RemoveBenthosFromS6Manager removes a Benthos instance from the S6 manager
func (s *BenthosService) RemoveBenthosFromS6Manager(ctx context.Context, serviceName string) error {
	if s.s6Manager == nil {
		return errors.New("s6 manager not initialized")
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	found := false

	// Remove the S6 FSM config from the list of S6 FSM configs
	// so that the S6 manager will stop the service
	// The S6 manager itself will handle a graceful shutdown of the udnerlying S6 service
	for i, s6Config := range s.s6ServiceConfigs {
		if s6Config.Name == serviceName {
			s.s6ServiceConfigs = append(s.s6ServiceConfigs[:i], s.s6ServiceConfigs[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return ErrServiceNotExist
	}

	return nil
}

// StartBenthos starts a Benthos instance
func (s *BenthosService) StartBenthos(ctx context.Context, serviceName string) error {
	if s.s6Manager == nil {
		return errors.New("s6 manager not initialized")
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	found := false

	// Set the desired state to running for the given instance
	for i, s6Config := range s.s6ServiceConfigs {
		if s6Config.Name == serviceName {
			s.s6ServiceConfigs[i].DesiredFSMState = s6_fsm.OperationalStateRunning
			found = true
			break
		}
	}

	if !found {
		return ErrServiceNotExist
	}

	return nil
}

// StopBenthos stops a Benthos instance
func (s *BenthosService) StopBenthos(ctx context.Context, serviceName string) error {
	if s.s6Manager == nil {
		return errors.New("s6 manager not initialized")
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	found := false

	// Set the desired state to stopped for the given instance
	for i, s6Config := range s.s6ServiceConfigs {
		if s6Config.Name == serviceName {
			s.s6ServiceConfigs[i].DesiredFSMState = s6_fsm.OperationalStateStopped
			found = true
			break
		}
	}

	if !found {
		return ErrServiceNotExist
	}

	return nil
}

// ReconcileManager reconciles the Benthos manager
func (s *BenthosService) ReconcileManager(ctx context.Context, tick uint64) (err error, reconciled bool) {
	if s.s6Manager == nil {
		return errors.New("s6 manager not initialized"), false
	}

	if ctx.Err() != nil {
		return ctx.Err(), false
	}

	return s.s6Manager.Reconcile(ctx, config.FullConfig{Services: s.s6ServiceConfigs}, tick)
}
