package config

import (
	"context"
	"fmt"
	"path/filepath"

	"gopkg.in/yaml.v3"

	filesystem "github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/service/filesystem"
)

const (
	// DefaultConfigPath is the default path to the config file
	DefaultConfigPath = "/data/config.yaml"
)

// ConfigManager is the interface for config management
type ConfigManager interface {
	// GetConfig returns the current config
	GetConfig(ctx context.Context) (FullConfig, error)
}

// FileConfigManager implements the ConfigManager interface by reading from a file
type FileConfigManager struct {
	// configPath is the path to the config file
	configPath string

	// fsService handles filesystem operations
	fsService filesystem.Service
}

// NewFileConfigManager creates a new FileConfigManager
func NewFileConfigManager(configPath string) *FileConfigManager {
	if configPath == "" {
		configPath = DefaultConfigPath
	}

	return &FileConfigManager{
		configPath: configPath,
		fsService:  filesystem.NewDefaultService(),
	}
}

// WithFileSystemService allows setting a custom filesystem service
// useful for testing or advanced use cases
func (m *FileConfigManager) WithFileSystemService(fsService filesystem.Service) *FileConfigManager {
	m.fsService = fsService
	return m
}

// GetConfig returns the current config, always reading fresh from disk
func (m *FileConfigManager) GetConfig(ctx context.Context) (FullConfig, error) {
	// Create the directory if it doesn't exist
	dir := filepath.Dir(m.configPath)
	if err := m.fsService.EnsureDirectory(ctx, dir); err != nil {
		return FullConfig{}, fmt.Errorf("failed to create config directory: %w", err)
	}

	// Check if the file exists
	exists, err := m.fsService.FileExists(ctx, m.configPath)
	if err != nil {
		return FullConfig{}, err
	}

	// Return empty config if the file doesn't exist
	if !exists {
		return FullConfig{}, nil
	}

	// Read the file
	data, err := m.fsService.ReadFile(ctx, m.configPath)
	if err != nil {
		return FullConfig{}, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse the YAML
	var config FullConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return FullConfig{}, fmt.Errorf("failed to parse config file: %w", err)
	}

	return config, nil
}
