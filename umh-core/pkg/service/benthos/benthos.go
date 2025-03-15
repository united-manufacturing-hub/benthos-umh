package benthos

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	s6service "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
	"go.uber.org/zap"
)

// BenthosServiceConfig represents the configuration for a Benthos service
type BenthosServiceConfig struct {
	// Benthos-specific configuration
	Input              []map[string]interface{} `yaml:"input"`
	Pipeline           []map[string]interface{} `yaml:"pipeline"`
	Output             []map[string]interface{} `yaml:"output"`
	CacheResources     []map[string]interface{} `yaml:"cache_resources"`
	RateLimitResources []map[string]interface{} `yaml:"rate_limit_resources"`
	Buffer             []map[string]interface{} `yaml:"buffer"`

	// Advanced configuration
	MetricsPort int    `yaml:"metrics_port"`
	LogLevel    string `yaml:"log_level"`
}

// Service is the default implementation of the S6 Service interface
type BenthosService struct {
	logger    *zap.SugaredLogger
	template  *template.Template
	s6Service s6service.Service
}

// NewDefaultBenthosService creates a new default Benthos service
func NewDefaultBenthosService() *BenthosService {
	return &BenthosService{
		logger:    logger.For(logger.ComponentBenthosService),
		template:  benthosYamlTemplate,
		s6Service: s6service.NewDefaultService(),
	}
}

// generateBenthosYaml generates a Benthos YAML configuration from a BenthosServiceConfig
func (s *BenthosService) generateBenthosYaml(config *BenthosServiceConfig) (string, error) {
	var b bytes.Buffer
	err := s.template.Execute(&b, config)
	return b.String(), err
}

// generateS6ConfigForBenthos creates a S6 config for a given benthos instance
func (s *BenthosService) GenerateS6ConfigForBenthos(benthosConfig *BenthosServiceConfig, name string) (s6Config s6service.S6ServiceConfig, err error) {
	benthosConfigFileName := "benthos.yaml"

	S6ServiceName := fmt.Sprintf("benthos-%s", name)
	configPath := fmt.Sprintf("/run/service/%s/%s", S6ServiceName, benthosConfigFileName)

	yamlConfig, err := s.generateBenthosYaml(benthosConfig)
	if err != nil {
		return s6service.S6ServiceConfig{}, err
	}

	// TODO: Only add the service config if it doesn't already exist
	s6Config = s6service.S6ServiceConfig{
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
