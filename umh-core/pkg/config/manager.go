package config

import (
	"context"
	"fmt"
	"path/filepath"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	filesystem "github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/filesystem"
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

	// logger is the logger for the config manager
	logger *zap.SugaredLogger
}

// NewFileConfigManager creates a new FileConfigManager
func NewFileConfigManager() *FileConfigManager {

	configPath := DefaultConfigPath
	logger := logger.For(logger.ComponentConfigManager)

	return &FileConfigManager{
		configPath: configPath,
		fsService:  filesystem.NewDefaultService(),
		logger:     logger,
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
		return FullConfig{}, fmt.Errorf("config file does not exist: %s", m.configPath)
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
