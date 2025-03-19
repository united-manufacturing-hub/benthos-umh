package benthos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"text/template"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	"go.uber.org/zap"

	s6fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
)

// HTTPClient interface for making HTTP requests
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// defaultHTTPClient is the default implementation of HTTPClient
type defaultHTTPClient struct {
	client *http.Client
}

func newDefaultHTTPClient() *defaultHTTPClient {
	transport := &http.Transport{
		MaxIdleConns:      10,
		IdleConnTimeout:   30 * time.Second,
		DisableKeepAlives: false,
	}

	return &defaultHTTPClient{
		client: &http.Client{
			Transport: transport,
		},
	}
}

func (c *defaultHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return c.client.Do(req)
}

// ServiceInfo contains information about a Benthos service
type ServiceInfo struct {
	// S6ObservedState contains information about the S6 service
	S6ObservedState s6fsm.S6ObservedState
	// S6FSMState contains the current state of the S6 FSM
	S6FSMState string
	// HealthCheck contains information about the health of the Benthos service
	HealthCheck HealthCheck
}

// HealthCheck contains information about the health of the Benthos service
// https://docs.redpanda.com/redpanda-connect/guides/monitoring/
type HealthCheck struct {
	// IsLive is true if the Benthos service is live
	IsLive bool
	// IsReady is true if the Benthos service is ready to process data
	IsReady bool
	// Version contains the version of the Benthos service
	Version string
	// ReadyError contains any error message from the ready check
	ReadyError string `json:"ready_error,omitempty"`
	// ConnectionStatuses contains the detailed connection status of inputs and outputs
	ConnectionStatuses []connStatus `json:"connection_statuses,omitempty"`
}

// versionResponse represents the JSON structure returned by the /version endpoint
type versionResponse struct {
	Version string `json:"version"`
	Built   string `json:"built"`
}

// readyResponse represents the JSON structure returned by the /ready endpoint
type readyResponse struct {
	Error    string       `json:"error,omitempty"`
	Statuses []connStatus `json:"statuses"`
}

type connStatus struct {
	Label     string `json:"label"`
	Path      string `json:"path"`
	Connected bool   `json:"connected"`
	Error     string `json:"error,omitempty"`
}

// Service is the default implementation of the S6 Service interface
type BenthosService struct {
	logger           *zap.SugaredLogger
	template         *template.Template
	s6Manager        *s6fsm.S6Manager
	s6ServiceConfigs []config.S6FSMConfig
	httpClient       HTTPClient
}

// BenthosServiceOption is a function that modifies a BenthosService
type BenthosServiceOption func(*BenthosService)

// WithHTTPClient sets a custom HTTP client for the BenthosService
func WithHTTPClient(client HTTPClient) BenthosServiceOption {
	return func(s *BenthosService) {
		s.httpClient = client
	}
}

// NewDefaultBenthosService creates a new default Benthos service
func NewDefaultBenthosService(name string, opts ...BenthosServiceOption) *BenthosService {
	managerName := fmt.Sprintf("%s%s", logger.ComponentBenthosService, name)
	service := &BenthosService{
		logger:     logger.For(managerName),
		template:   benthosYamlTemplate,
		s6Manager:  s6fsm.NewS6Manager(managerName),
		httpClient: newDefaultHTTPClient(),
	}

	// Apply options
	for _, opt := range opts {
		opt(service)
	}

	return service
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
func (s *BenthosService) Status(ctx context.Context, serviceName string, metricsPort int) (ServiceInfo, error) {
	if ctx.Err() != nil {
		return ServiceInfo{}, ctx.Err()
	}

	// Let's get the status of the underlying s6 service
	s6ServiceObservedStateRaw, err := s.s6Manager.GetLastObservedState(serviceName)
	if err != nil {
		return ServiceInfo{}, fmt.Errorf("failed to get last observed state: %w", err)
	}

	s6ServiceObservedState, ok := s6ServiceObservedStateRaw.(s6fsm.S6ObservedState)
	if !ok {
		return ServiceInfo{}, fmt.Errorf("observed state is not a S6ObservedState: %v", s6ServiceObservedStateRaw)
	}

	// Let's get the current FSM state of the underlying s6 FSM
	s6FSMState, err := s.s6Manager.GetCurrentFSMState(serviceName)
	if err != nil {
		return ServiceInfo{}, fmt.Errorf("failed to get current FSM state: %w", err)
	}

	// Let's get the health check of the Benthos service
	healthCheck, err := s.GetHealthCheck(ctx, serviceName, metricsPort)
	if err != nil {
		return ServiceInfo{}, fmt.Errorf("failed to get health check: %w", err)
	}

	serviceInfo := ServiceInfo{
		S6ObservedState: s6ServiceObservedState,
		S6FSMState:      s6FSMState,
		HealthCheck:     healthCheck,
	}

	// TODO: Fetch Benthos-specific metrics and health data

	// TODO: Fetch Benthos-specific metrics and health data
	// - Collect metrics from Benthos HTTP endpoint
	// - Check logs for warnings/errors
	// - Update processing state based on throughput data
	return serviceInfo, nil
}

// GetHealthCheck returns the health check of a Benthos service
// by fetching the health check endpoint of the Benthos service
// https://docs.redpanda.com/redpanda-connect/guides/monitoring/
// https://docs.redpanda.com/redpanda-connect/components/http/about/
func (s *BenthosService) GetHealthCheck(ctx context.Context, serviceName string, metricsPort int) (HealthCheck, error) {
	if ctx.Err() != nil {
		return HealthCheck{}, ctx.Err()
	}

	if metricsPort == 0 {
		return HealthCheck{}, fmt.Errorf("could not find metrics port for service %s", serviceName)
	}

	baseURL := fmt.Sprintf("http://localhost:%d", metricsPort)

	// Helper function to make HTTP requests with context
	doRequest := func(endpoint string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request for %s: %w", endpoint, err)
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to execute request for %s: %w", endpoint, err)
		}
		return resp, nil
	}

	var healthCheck HealthCheck

	// Check liveness
	if resp, err := doRequest("/ping"); err == nil {
		healthCheck.IsLive = resp.StatusCode == http.StatusOK
		resp.Body.Close()
	} else {
		return HealthCheck{}, fmt.Errorf("failed to check liveness: %w", err)
	}

	// Check readiness
	if resp, err := doRequest("/ready"); err == nil {
		defer resp.Body.Close()

		// Even if status is 503, we still want to read the body to get the detailed status
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return HealthCheck{}, fmt.Errorf("failed to read ready response body: %w", err)
		}

		var readyResp readyResponse
		if err := json.Unmarshal(body, &readyResp); err != nil {
			return HealthCheck{}, fmt.Errorf("failed to unmarshal ready response: %w", err)
		}

		// Service is ready if status is 200 and there's no error
		healthCheck.IsReady = resp.StatusCode == http.StatusOK && readyResp.Error == ""
		healthCheck.ReadyError = readyResp.Error
		healthCheck.ConnectionStatuses = readyResp.Statuses

		// Log detailed status if not ready
		if !healthCheck.IsReady {
			s.logger.Debugw("Service not ready",
				"service", serviceName,
				"error", readyResp.Error,
				"statuses", readyResp.Statuses)
		}
	} else {
		return HealthCheck{}, fmt.Errorf("failed to check readiness: %w", err)
	}

	// Get version
	if resp, err := doRequest("/version"); err == nil {
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return HealthCheck{}, fmt.Errorf("failed to read version response body: %w", err)
		}

		var vData versionResponse
		if err := json.Unmarshal(body, &vData); err != nil {
			return HealthCheck{}, fmt.Errorf("failed to unmarshal version response: %w", err)
		}
		healthCheck.Version = vData.Version
	} else {
		return HealthCheck{}, fmt.Errorf("failed to get version: %w", err)
	}

	return healthCheck, nil
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
			DesiredFSMState: s6fsm.OperationalStateRunning,
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
			s.s6ServiceConfigs[i].DesiredFSMState = s6fsm.OperationalStateRunning
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
			s.s6ServiceConfigs[i].DesiredFSMState = s6fsm.OperationalStateStopped
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
