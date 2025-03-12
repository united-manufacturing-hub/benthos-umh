package config

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// UMHLiteConfig represents the structure of the umh-lite bootstrap YAML
type UMHLiteConfig struct {
	APIKey       string             `yaml:"apiKey"`
	Metadata     MetadataConfig     `yaml:"metadata"`
	ExternalMQTT ExternalMQTTConfig `yaml:"externalMQTT"`
	LogLevel     string             `yaml:"logLevel"`
	Metrics      MetricsConfig      `yaml:"metrics"`
	Benthos      []BenthosConfig    `yaml:"benthos"`
}

// MetadataConfig contains information to identify the edge device
type MetadataConfig struct {
	DeviceID string `yaml:"deviceId"`
	Location string `yaml:"location"`
}

// ExternalMQTTConfig contains settings for the external MQTT broker
type ExternalMQTTConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Broker   string `yaml:"broker"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// MetricsConfig contains settings for Prometheus metrics
type MetricsConfig struct {
	Port int `yaml:"port"`
}

// BenthosConfig represents a Benthos pipeline configuration
type BenthosConfig struct {
	ID      string `yaml:"id"`
	Type    string `yaml:"type"`
	Config  string `yaml:"config,omitempty"`
	Enabled bool   `yaml:"enabled"`
}

// ConfigWatcher watches for changes to a configuration file
type ConfigWatcher struct {
	configPath       string
	lastModifiedTime time.Time
	lastHash         string
}

// NewConfigWatcher creates a new instance to watch config file changes
func NewConfigWatcher(configPath string) (*ConfigWatcher, error) {
	info, err := os.Stat(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat config file: %w", err)
	}

	content, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	hash := fmt.Sprintf("%x", sha256.Sum256(content))

	return &ConfigWatcher{
		configPath:       configPath,
		lastModifiedTime: info.ModTime(),
		lastHash:         hash,
	}, nil
}

// CheckForChanges checks if the configuration file has changed
func (cw *ConfigWatcher) CheckForChanges() (bool, error) {
	info, err := os.Stat(cw.configPath)
	if err != nil {
		return false, fmt.Errorf("failed to stat config file: %w", err)
	}

	if !info.ModTime().After(cw.lastModifiedTime) {
		return false, nil
	}

	// File was modified, check if content actually changed
	content, err := ioutil.ReadFile(cw.configPath)
	if err != nil {
		return false, fmt.Errorf("failed to read config file: %w", err)
	}

	hash := fmt.Sprintf("%x", sha256.Sum256(content))
	if hash == cw.lastHash {
		// File was modified but content didn't change
		cw.lastModifiedTime = info.ModTime()
		return false, nil
	}

	// Content changed
	cw.lastModifiedTime = info.ModTime()
	cw.lastHash = hash
	return true, nil
}

// LoadConfig loads the configuration from the given file path
func LoadConfig(path string) (*UMHLiteConfig, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config UMHLiteConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// SaveConfig saves the configuration to the specified file path
func SaveConfig(config *UMHLiteConfig, path string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Make sure the directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	if err := ioutil.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}
