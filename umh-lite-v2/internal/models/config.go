package models

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// BenthosConfig represents a configuration for a Benthos instance
type BenthosConfig struct {
	// ConfigPath is the path to the configuration file
	ConfigPath string
	// Content is the actual configuration content
	Content string
	// Version is a version identifier for the configuration (hash or timestamp)
	Version string
	// CreateTime is when the configuration was created
	CreateTime time.Time
	// UpdateTime is when the configuration was last updated
	UpdateTime time.Time
}

// NewBenthosConfig creates a new configuration with version based on content hash
func NewBenthosConfig(content string) *BenthosConfig {
	now := time.Now()
	hash := sha256.Sum256([]byte(content))

	return &BenthosConfig{
		Content:    content,
		Version:    hex.EncodeToString(hash[:]),
		CreateTime: now,
		UpdateTime: now,
	}
}

// SetConfigPath updates the configuration path
func (c *BenthosConfig) SetConfigPath(path string) {
	c.ConfigPath = path
	c.UpdateTime = time.Now()
}

// Equal checks if two BenthosConfigs are functionally equivalent
func (c *BenthosConfig) Equal(other *BenthosConfig) bool {
	if c == nil || other == nil {
		return c == other
	}

	// Compare content - this is the most important part
	return c.Content == other.Content
}
