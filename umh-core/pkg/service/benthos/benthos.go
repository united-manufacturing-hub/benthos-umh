package benthos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"text/template"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	"go.uber.org/zap"

	s6fsm "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/fsm/s6"
)

// IBenthosService is the interface for managing Benthos services
type IBenthosService interface {
	GenerateS6ConfigForBenthos(benthosConfig *config.BenthosServiceConfig, name string) (config.S6ServiceConfig, error)
	GetConfig(ctx context.Context, path string) (config.BenthosServiceConfig, error)
	Status(ctx context.Context, serviceName string, metricsPort int, tick uint64) (ServiceInfo, error)
	AddBenthosToS6Manager(ctx context.Context, cfg *config.BenthosServiceConfig, serviceName string) error
	RemoveBenthosFromS6Manager(ctx context.Context, serviceName string) error
	StartBenthos(ctx context.Context, serviceName string) error
	StopBenthos(ctx context.Context, serviceName string) error
	ReconcileManager(ctx context.Context, tick uint64) (error, bool)
	IsLogsFine(logs []string) bool
	IsMetricsErrorFree(metrics Metrics) bool
	HasProcessingActivity(status BenthosStatus) bool
}

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
	// BenthosStatus contains information about the status of the Benthos service
	BenthosStatus BenthosStatus
}

// BenthosStatus contains information about the status of the Benthos service
type BenthosStatus struct {
	// HealthCheck contains information about the health of the Benthos service
	HealthCheck HealthCheck
	// Metrics contains information about the metrics of the Benthos service
	Metrics Metrics
	// MetricsState contains information about the metrics of the Benthos service
	MetricsState *BenthosMetricsState
	// Logs contains the logs of the Benthos service
	Logs []string
}

// Metrics contains information about the metrics of the Benthos service
type Metrics struct {
	Input   InputMetrics   `json:"input,omitempty"`
	Output  OutputMetrics  `json:"output,omitempty"`
	Process ProcessMetrics `json:"process,omitempty"`
}

// InputMetrics contains input-specific metrics
type InputMetrics struct {
	ConnectionFailed int64   `json:"connection_failed"`
	ConnectionLost   int64   `json:"connection_lost"`
	ConnectionUp     int64   `json:"connection_up"`
	LatencyNS        Latency `json:"latency_ns"`
	Received         int64   `json:"received"`
}

// OutputMetrics contains output-specific metrics
type OutputMetrics struct {
	BatchSent        int64   `json:"batch_sent"`
	ConnectionFailed int64   `json:"connection_failed"`
	ConnectionLost   int64   `json:"connection_lost"`
	ConnectionUp     int64   `json:"connection_up"`
	Error            int64   `json:"error"`
	LatencyNS        Latency `json:"latency_ns"`
	Sent             int64   `json:"sent"`
}

// ProcessMetrics contains processor-specific metrics
type ProcessMetrics struct {
	Processors map[string]ProcessorMetrics `json:"processors"` // key is the processor path (e.g. "root.pipeline.processors.0")
}

// ProcessorMetrics contains metrics for a single processor
type ProcessorMetrics struct {
	Label         string  `json:"label"`
	Received      int64   `json:"received"`
	BatchReceived int64   `json:"batch_received"`
	Sent          int64   `json:"sent"`
	BatchSent     int64   `json:"batch_sent"`
	Error         int64   `json:"error"`
	LatencyNS     Latency `json:"latency_ns"`
}

// Latency contains latency metrics
type Latency struct {
	P50   float64 `json:"p50"`   // 50th percentile
	P90   float64 `json:"p90"`   // 90th percentile
	P99   float64 `json:"p99"`   // 99th percentile
	Sum   float64 `json:"sum"`   // Total sum
	Count int64   `json:"count"` // Number of samples
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

// BenthosService is the default implementation of the IBenthosService interface
type BenthosService struct {
	logger           *zap.SugaredLogger
	template         *template.Template
	s6Manager        *s6fsm.S6Manager
	s6ServiceConfigs []config.S6FSMConfig
	httpClient       HTTPClient
	metricsState     *BenthosMetricsState
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
		logger:       logger.For(managerName),
		template:     benthosYamlTemplate,
		s6Manager:    s6fsm.NewS6Manager(managerName),
		httpClient:   newDefaultHTTPClient(),
		metricsState: NewBenthosMetricsState(),
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
func (s *BenthosService) Status(ctx context.Context, serviceName string, metricsPort int, tick uint64) (ServiceInfo, error) {
	if ctx.Err() != nil {
		return ServiceInfo{}, ctx.Err()
	}

	// First, check if the service exists in the S6 manager
	// This is a crucial check that prevents "instance not found" errors
	// during reconciliation when a service is being created or removed
	if _, exists := s.s6Manager.GetInstance(serviceName); !exists {
		s.logger.Debugf("Service %s not found in S6 manager", serviceName)
		return ServiceInfo{}, ErrServiceNotExist
	}

	// Let's get the status of the underlying s6 service
	s6ServiceObservedStateRaw, err := s.s6Manager.GetLastObservedState(serviceName)
	if err != nil {
		// If we still get an "instance not found" error despite our earlier check,
		// it's likely that the service was removed between our check and this call
		if strings.Contains(err.Error(), "instance "+serviceName+" not found") ||
			strings.Contains(err.Error(), "not found") {
			s.logger.Debugf("Service %s was removed during status check", serviceName)
			return ServiceInfo{}, ErrServiceNotExist
		}
		return ServiceInfo{}, fmt.Errorf("failed to get last observed state: %w", err)
	}

	s6ServiceObservedState, ok := s6ServiceObservedStateRaw.(s6fsm.S6ObservedState)
	if !ok {
		return ServiceInfo{}, fmt.Errorf("observed state is not a S6ObservedState: %v", s6ServiceObservedStateRaw)
	}

	// Let's get the current FSM state of the underlying s6 FSM
	s6FSMState, err := s.s6Manager.GetCurrentFSMState(serviceName)
	if err != nil {
		// Similar to above, if the service was removed during our check
		if strings.Contains(err.Error(), "instance "+serviceName+" not found") ||
			strings.Contains(err.Error(), "not found") {
			s.logger.Debugf("Service %s was removed during status check", serviceName)
			return ServiceInfo{}, ErrServiceNotExist
		}
		return ServiceInfo{}, fmt.Errorf("failed to get current FSM state: %w", err)
	}

	// Let's get the health check of the Benthos service
	benthosStatus, err := s.GetHealthCheckAndMetrics(ctx, serviceName, metricsPort, tick)
	if err != nil {
		return ServiceInfo{}, fmt.Errorf("failed to get health check: %w", err)
	}

	serviceInfo := ServiceInfo{
		S6ObservedState: s6ServiceObservedState,
		S6FSMState:      s6FSMState,
		BenthosStatus:   benthosStatus,
	}

	return serviceInfo, nil
}

// parseMetrics parses prometheus metrics into structured format
func parseMetrics(data []byte) (Metrics, error) {
	var parser expfmt.TextParser
	metrics := Metrics{
		Process: ProcessMetrics{
			Processors: make(map[string]ProcessorMetrics),
		},
	}

	// Parse the metrics text into prometheus format
	mf, err := parser.TextToMetricFamilies(bytes.NewReader(data))
	if err != nil {
		return metrics, fmt.Errorf("failed to parse metrics: %w", err)
	}

	// Helper function to get metric value
	getValue := func(m *dto.Metric) float64 {
		if m.Counter != nil {
			return m.Counter.GetValue()
		}
		if m.Gauge != nil {
			return m.Gauge.GetValue()
		}
		if m.Untyped != nil {
			return m.Untyped.GetValue()
		}
		return 0
	}

	// Helper function to get label value
	getLabel := func(m *dto.Metric, name string) string {
		for _, label := range m.Label {
			if label.GetName() == name {
				return label.GetValue()
			}
		}
		return ""
	}

	// Process each metric family
	for name, family := range mf {
		switch {
		// Input metrics
		case name == "input_connection_failed":
			if len(family.Metric) > 0 {
				metrics.Input.ConnectionFailed = int64(getValue(family.Metric[0]))
			}
		case name == "input_connection_lost":
			if len(family.Metric) > 0 {
				metrics.Input.ConnectionLost = int64(getValue(family.Metric[0]))
			}
		case name == "input_connection_up":
			if len(family.Metric) > 0 {
				metrics.Input.ConnectionUp = int64(getValue(family.Metric[0]))
			}
		case name == "input_received":
			if len(family.Metric) > 0 {
				metrics.Input.Received = int64(getValue(family.Metric[0]))
			}
		case name == "input_latency_ns":
			updateLatencyFromFamily(&metrics.Input.LatencyNS, family)

		// Output metrics
		case name == "output_batch_sent":
			if len(family.Metric) > 0 {
				metrics.Output.BatchSent = int64(getValue(family.Metric[0]))
			}
		case name == "output_connection_failed":
			if len(family.Metric) > 0 {
				metrics.Output.ConnectionFailed = int64(getValue(family.Metric[0]))
			}
		case name == "output_connection_lost":
			if len(family.Metric) > 0 {
				metrics.Output.ConnectionLost = int64(getValue(family.Metric[0]))
			}
		case name == "output_connection_up":
			if len(family.Metric) > 0 {
				metrics.Output.ConnectionUp = int64(getValue(family.Metric[0]))
			}
		case name == "output_error":
			if len(family.Metric) > 0 {
				metrics.Output.Error = int64(getValue(family.Metric[0]))
			}
		case name == "output_sent":
			if len(family.Metric) > 0 {
				metrics.Output.Sent = int64(getValue(family.Metric[0]))
			}
		case name == "output_latency_ns":
			updateLatencyFromFamily(&metrics.Output.LatencyNS, family)

		// Process metrics
		case name == "processor_received", name == "processor_batch_received",
			name == "processor_sent", name == "processor_batch_sent",
			name == "processor_error", name == "processor_latency_ns":
			for _, metric := range family.Metric {
				path := getLabel(metric, "path")
				if path == "" {
					continue
				}

				// Initialize processor metrics if not exists
				if _, exists := metrics.Process.Processors[path]; !exists {
					metrics.Process.Processors[path] = ProcessorMetrics{
						Label: getLabel(metric, "label"),
					}
				}

				proc := metrics.Process.Processors[path]
				switch name {
				case "processor_received":
					proc.Received = int64(getValue(metric))
				case "processor_batch_received":
					proc.BatchReceived = int64(getValue(metric))
				case "processor_sent":
					proc.Sent = int64(getValue(metric))
				case "processor_batch_sent":
					proc.BatchSent = int64(getValue(metric))
				case "processor_error":
					proc.Error = int64(getValue(metric))
				case "processor_latency_ns":
					updateLatencyFromMetric(&proc.LatencyNS, metric)
				}
				metrics.Process.Processors[path] = proc
			}
		}
	}

	return metrics, nil
}

func updateLatencyFromFamily(latency *Latency, family *dto.MetricFamily) {
	for _, metric := range family.Metric {
		if metric.Summary == nil {
			continue
		}

		latency.Sum = metric.Summary.GetSampleSum()
		latency.Count = int64(metric.Summary.GetSampleCount())

		for _, quantile := range metric.Summary.Quantile {
			switch quantile.GetQuantile() {
			case 0.5:
				latency.P50 = quantile.GetValue()
			case 0.9:
				latency.P90 = quantile.GetValue()
			case 0.99:
				latency.P99 = quantile.GetValue()
			}
		}
	}
}

func updateLatencyFromMetric(latency *Latency, metric *dto.Metric) {
	if metric.Summary == nil {
		return
	}

	latency.Sum = metric.Summary.GetSampleSum()
	latency.Count = int64(metric.Summary.GetSampleCount())

	for _, quantile := range metric.Summary.Quantile {
		switch quantile.GetQuantile() {
		case 0.5:
			latency.P50 = quantile.GetValue()
		case 0.9:
			latency.P90 = quantile.GetValue()
		case 0.99:
			latency.P99 = quantile.GetValue()
		}
	}
}

// GetHealthCheckAndMetrics returns the health check of a Benthos service as well as the metrics
// by fetching the health check endpoint of the Benthos service
// https://docs.redpanda.com/redpanda-connect/guides/monitoring/
// https://docs.redpanda.com/redpanda-connect/components/http/about/
func (s *BenthosService) GetHealthCheckAndMetrics(ctx context.Context, serviceName string, metricsPort int, tick uint64) (BenthosStatus, error) {
	if ctx.Err() != nil {
		return BenthosStatus{}, ctx.Err()
	}

	// Skip health checks and metrics if the service doesn't exist yet
	// This avoids unnecessary errors in Status() when the service is still being created
	if _, exists := s.s6Manager.GetInstance(serviceName); !exists {
		return BenthosStatus{
			HealthCheck: HealthCheck{
				IsLive:  false,
				IsReady: false,
			},
			Metrics: Metrics{},
			Logs:    []string{},
		}, nil
	}

	if metricsPort == 0 {
		return BenthosStatus{}, fmt.Errorf("could not find metrics port for service %s", serviceName)
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
		return BenthosStatus{}, fmt.Errorf("failed to check liveness: %w", err)
	}

	// Check readiness
	if resp, err := doRequest("/ready"); err == nil {
		defer resp.Body.Close()

		// Even if status is 503, we still want to read the body to get the detailed status
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return BenthosStatus{}, fmt.Errorf("failed to read ready response body: %w", err)
		}

		var readyResp readyResponse
		if err := json.Unmarshal(body, &readyResp); err != nil {
			return BenthosStatus{}, fmt.Errorf("failed to unmarshal ready response: %w", err)
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
		return BenthosStatus{}, fmt.Errorf("failed to check readiness: %w", err)
	}

	// Get version
	if resp, err := doRequest("/version"); err == nil {
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return BenthosStatus{}, fmt.Errorf("failed to read version response body: %w", err)
		}

		var vData versionResponse
		if err := json.Unmarshal(body, &vData); err != nil {
			return BenthosStatus{}, fmt.Errorf("failed to unmarshal version response: %w", err)
		}
		healthCheck.Version = vData.Version
	} else {
		return BenthosStatus{}, fmt.Errorf("failed to get version: %w", err)
	}

	var metrics Metrics

	// Get metrics
	if resp, err := doRequest("/metrics"); err == nil {
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return BenthosStatus{}, fmt.Errorf("failed to read metrics response body: %w", err)
		}

		metrics, err = parseMetrics(body)
		if err != nil {
			return BenthosStatus{}, fmt.Errorf("failed to parse metrics: %w", err)
		}
	}

	// Update the metrics state
	if s.metricsState == nil {
		return BenthosStatus{}, fmt.Errorf("metrics state not initialized")
	}

	s.metricsState.UpdateFromMetrics(metrics, tick)

	return BenthosStatus{
		HealthCheck:  healthCheck,
		Metrics:      metrics,
		MetricsState: s.metricsState,
	}, nil
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

// IsLogsFine analyzes Benthos logs to determine if there are any critical issues
func (s *BenthosService) IsLogsFine(logs []string) bool {
	if len(logs) == 0 {
		return true
	}

	// Compile regex patterns for different types of logs
	benthosLogRegex := regexp.MustCompile(`^level=(error|warn)\s+msg="(.+)"`)
	configErrorRegex := regexp.MustCompile(`^configuration file read error:`)
	loggerErrorRegex := regexp.MustCompile(`^failed to create logger:`)
	linterErrorRegex := regexp.MustCompile(`^Config lint error:`)

	for _, log := range logs {
		// Check for critical system errors first
		if configErrorRegex.MatchString(log) ||
			loggerErrorRegex.MatchString(log) ||
			linterErrorRegex.MatchString(log) {
			return false
		}

		// Parse structured Benthos logs
		if matches := benthosLogRegex.FindStringSubmatch(log); matches != nil {
			level := matches[1]
			message := matches[2]

			// Error logs are always critical
			if level == "error" {
				return false
			}

			// For warnings, only consider them critical if they indicate serious issues
			if level == "warn" {
				criticalWarnings := []string{
					"failed to",
					"connection lost",
					"unable to",
				}

				for _, criticalPattern := range criticalWarnings {
					if strings.Contains(message, criticalPattern) {
						return false
					}
				}
			}
		}
	}

	return true
}

// IsMetricsErrorFree checks if there are any errors in the Benthos metrics
func (s *BenthosService) IsMetricsErrorFree(metrics Metrics) bool {
	// Check output errors
	if metrics.Output.Error > 0 {
		return false
	}

	// Check processor errors
	for _, proc := range metrics.Process.Processors {
		if proc.Error > 0 {
			return false
		}
	}

	return true
}

// HasProcessingActivity checks if the Benthos instance has active data processing based on metrics state
func (s *BenthosService) HasProcessingActivity(status BenthosStatus) bool {
	return status.MetricsState.IsActive
}
