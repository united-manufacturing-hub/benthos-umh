// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stream_processor_plugin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// StreamProcessor is the main processor implementation
type StreamProcessor struct {
	config StreamProcessorConfig
	logger *service.Logger

	// State management
	stateManager *StateManager

	// Metrics
	messagesProcessed *service.MetricCounter
	messagesErrored   *service.MetricCounter
	messagesDropped   *service.MetricCounter
}

// ProcessorState holds the variable state for the processor.
//
// It provides thread-safe storage for variables using a RWMutex to allow
// concurrent reads while ensuring write safety. The Variables map stores
// variable names to their current values with metadata.
//
// This struct handles the low-level storage mechanics, while StateManager
// provides the higher-level coordination and business logic.
type ProcessorState struct {
	Variables map[string]*VariableValue // Thread-safe variable storage
	mutex     sync.RWMutex              // Protects concurrent access to Variables
}

// VariableValue represents a stored variable value with metadata.
//
// Each field serves a specific purpose in the dependency-based evaluation system:
//
// Value: The actual data value received from the source topic (e.g., 1.23, "active", true).
// This is what gets used in JavaScript expressions and calculations.
//
// Timestamp: When this variable was last updated. Critical for timeseries data to:
// - Ensure proper ordering of events
// - Detect stale data that might need refreshing
// - Enable time-based calculations and filtering
// - Support debugging by showing when variables were last seen
//
// Source: The source identifier (e.g., "press", "tF", "r") that provided this value.
// Essential for:
// - DEBUGGING: Trace which input caused a calculation failure
// - VALIDATION: Verify variables come from expected sources
// - MONITORING: Track which sources are active/inactive
// - AUDITING: Maintain clear data lineage through the system
//
// Example: VariableValue{Value: 1.23, Source: "press", Timestamp: 2024-01-15T10:30:00Z}
// means the "press" variable was set to 1.23 at the given timestamp from topic
// "umh.v1.corpA.plant-A.aawd._raw.press".
type VariableValue struct {
	Value     interface{} // The actual data value from the source
	Timestamp time.Time   // When this variable was last updated
	Source    string      // Which source provided this value (for traceability)
}

// newStreamProcessor creates a new stream processor instance
func newStreamProcessor(config StreamProcessorConfig, logger *service.Logger, metrics *service.Metrics) (*StreamProcessor, error) {
	// Validate configuration
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Analyze mappings to identify static vs dynamic
	if err := analyzeMappingsWithDetection(&config); err != nil {
		return nil, fmt.Errorf("failed to analyze mappings: %w", err)
	}

	processor := &StreamProcessor{
		config:            config,
		logger:            logger,
		stateManager:      NewStateManager(&config),
		messagesProcessed: metrics.NewCounter("messages_processed"),
		messagesErrored:   metrics.NewCounter("messages_errored"),
		messagesDropped:   metrics.NewCounter("messages_dropped"),
	}

	// Log configuration analysis results
	logger.Infof("Stream processor initialized with %d static mappings and %d dynamic mappings",
		len(config.StaticMappings), len(config.DynamicMappings))

	return processor, nil
}

// ProcessBatch processes a batch of messages
func (p *StreamProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	p.logger.Debugf("Processing batch of %d messages", len(batch))

	var resultBatch service.MessageBatch

	for _, msg := range batch {
		// For now, just pass through the message
		// TODO: Implement actual stream processing logic
		resultBatch = append(resultBatch, msg)
		p.messagesProcessed.Incr(1)
	}

	if len(resultBatch) == 0 {
		return nil, nil
	}

	return []service.MessageBatch{resultBatch}, nil
}

// Close closes the processor
func (p *StreamProcessor) Close(ctx context.Context) error {
	p.logger.Debug("Closing stream processor")
	return nil
}

// validateConfig validates the stream processor configuration
func validateConfig(config StreamProcessorConfig) error {
	if config.Mode == "" {
		return fmt.Errorf("mode is required")
	}

	if config.Mode != "timeseries" {
		return fmt.Errorf("unsupported mode: %s (only 'timeseries' is supported)", config.Mode)
	}

	if config.Model.Name == "" {
		return fmt.Errorf("model name is required")
	}

	if config.Model.Version == "" {
		return fmt.Errorf("model version is required")
	}

	if config.OutputTopic == "" {
		return fmt.Errorf("output_topic is required")
	}

	if len(config.Sources) == 0 {
		return fmt.Errorf("at least one source mapping is required")
	}

	return nil
}
