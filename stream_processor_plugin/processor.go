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
	"encoding/json"
	"fmt"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// StreamProcessor is the main processor implementation
type StreamProcessor struct {
	config StreamProcessorConfig
	logger *service.Logger

	// State management
	stateManager *StateManager

	// JavaScript engine for expression evaluation
	jsEngine *JSEngine

	// Metrics
	metrics *ProcessorMetrics

	// Object pools for memory optimization
	pools *ObjectPools
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

	// Extract source variable names for JavaScript engine
	sourceNames := make([]string, 0, len(config.Sources))
	for sourceName := range config.Sources {
		sourceNames = append(sourceNames, sourceName)
	}

	processor := &StreamProcessor{
		config:       config,
		logger:       logger,
		stateManager: NewStateManager(&config),
		jsEngine:     NewJSEngine(logger, sourceNames),
		metrics:      NewProcessorMetrics(metrics),
		pools:        NewObjectPools(),
	}

	// Pre-compile all JavaScript expressions for performance
	if err := processor.precompileExpressions(); err != nil {
		return nil, fmt.Errorf("failed to pre-compile expressions: %w", err)
	}

	// Log configuration analysis results
	logger.Infof("Stream processor initialized with %d static mappings and %d dynamic mappings",
		len(config.StaticMappings), len(config.DynamicMappings))

	// Initialize gauge metrics
	processor.updateGaugeMetrics()

	return processor, nil
}

// ProcessBatch processes a batch of messages according to the stream processing workflow
func (p *StreamProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	batchStart := time.Now()
	p.logger.Debugf("Processing batch of %d messages", len(batch))

	var resultBatch service.MessageBatch

	for _, msg := range batch {
		messageStart := time.Now()
		outputMessages, err := p.processMessage(msg)
		processingTime := time.Since(messageStart)

		if err != nil {
			p.logger.Warnf("Failed to process message: %v", err)
			p.metrics.LogMessageErrored()
			continue
		}

		if len(outputMessages) == 0 {
			p.metrics.LogMessageDropped()
			continue
		}

		resultBatch = append(resultBatch, outputMessages...)
		p.metrics.LogMessageProcessed(processingTime)
		p.metrics.LogOutputGeneration(len(outputMessages))
	}

	// Log batch processing completion
	p.metrics.LogBatchProcessing(time.Since(batchStart))

	// Update gauge metrics periodically (at end of each batch)
	p.updateGaugeMetrics()

	if len(resultBatch) == 0 {
		return nil, nil
	}

	return []service.MessageBatch{resultBatch}, nil
}

// processMessage processes a single message following the stream processing workflow
func (p *StreamProcessor) processMessage(msg *service.Message) ([]*service.Message, error) {
	// Step 1: Check if message has umh_topic metadata
	umhTopic, exists := msg.MetaGet("umh_topic")
	if !exists {
		p.logger.Debugf("Message missing umh_topic metadata, skipping")
		return nil, nil
	}

	// Step 2: Validate timeseries format
	messageData, err := p.parseTimeseriesMessage(msg)
	if err != nil {
		p.logger.Debugf("Invalid timeseries format: %v", err)
		return nil, nil
	}

	// Step 3: Check if topic matches configured sources
	variableName, found := p.stateManager.ResolveVariableFromTopic(umhTopic)
	if !found {
		p.logger.Debugf("Topic '%s' does not match configured sources, skipping", umhTopic)
		return nil, nil
	}

	// Step 4: Extract metadata once for efficiency using pooled map
	metadata := p.pools.GetMetadataMap()
	defer p.pools.PutMetadataMap(metadata)

	_ = msg.MetaWalk(func(key, value string) error {
		if key != "umh_topic" { // Skip umh_topic as it will be overridden
			metadata[key] = value
		}
		return nil
	})

	// Step 5: Store variable value in processor state
	p.stateManager.GetState().SetVariable(variableName, messageData.Value, variableName)
	p.logger.Debugf("Updated variable '%s' with value %v", variableName, messageData.Value)

	// Step 6: Find and process mappings
	var outputMessages []*service.Message

	// Always process static mappings (they don't depend on variables)
	staticOutputs, err := p.processStaticMappings(metadata, messageData.TimestampMs)
	if err != nil {
		p.logger.Warnf("Error processing static mappings: %v", err)
		p.metrics.LogMessageErrored()
	} else {
		outputMessages = append(outputMessages, staticOutputs...)
	}

	// Process dynamic mappings that depend on the updated variable
	dynamicOutputs, err := p.processDynamicMappings(metadata, variableName, messageData.TimestampMs)
	if err != nil {
		p.logger.Warnf("Error processing dynamic mappings: %v", err)
		p.metrics.LogMessageErrored()
	} else {
		outputMessages = append(outputMessages, dynamicOutputs...)
	}

	return outputMessages, nil
}

// TimeseriesMessage represents a UMH timeseries message format
type TimeseriesMessage struct {
	Value       interface{} `json:"value"`
	TimestampMs int64       `json:"timestamp_ms"`
}

// parseTimeseriesMessage validates and parses a UMH timeseries message
func (p *StreamProcessor) parseTimeseriesMessage(msg *service.Message) (*TimeseriesMessage, error) {
	// Get message payload
	payloadBytes, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get message payload: %w", err)
	}

	// Parse JSON payload
	var tsMsg TimeseriesMessage
	if err := json.Unmarshal(payloadBytes, &tsMsg); err != nil {
		return nil, fmt.Errorf("invalid JSON payload: %w", err)
	}

	// Validate required fields
	if tsMsg.TimestampMs == 0 {
		return nil, fmt.Errorf("missing or invalid timestamp_ms")
	}

	// Value can be any type (number, string, boolean, etc.)
	if tsMsg.Value == nil {
		return nil, fmt.Errorf("missing value field")
	}

	return &tsMsg, nil
}

// processStaticMappings processes all static mappings (expressions with no variable dependencies)
func (p *StreamProcessor) processStaticMappings(metadata map[string]string, timestampMs int64) ([]*service.Message, error) {
	var outputMessages []*service.Message

	// Get all static mappings from state manager
	staticMappings := p.stateManager.GetStaticMappings()

	for _, mapping := range staticMappings {
		// Execute static JavaScript expression with timing
		jsStart := time.Now()
		result := p.jsEngine.EvaluateStatic(mapping.Expression)
		jsTime := time.Since(jsStart)

		p.metrics.LogJavaScriptExecution(jsTime, result.Success)

		if !result.Success {
			p.logger.Warnf("Static mapping '%s' failed: %s", mapping.VirtualPath, result.Error)
			continue
		}

		// Create output message
		outputMsg, err := p.createOutputMessage(metadata, mapping.VirtualPath, result.Value, timestampMs)
		if err != nil {
			p.logger.Warnf("Failed to create output message for static mapping '%s': %v", mapping.VirtualPath, err)
			p.metrics.LogMessageErrored()
			continue
		}

		outputMessages = append(outputMessages, outputMsg)
		p.logger.Debugf("Generated static mapping output: %s = %v", mapping.VirtualPath, result.Value)
	}

	return outputMessages, nil
}

// processDynamicMappings processes dynamic mappings that depend on the updated variable
func (p *StreamProcessor) processDynamicMappings(metadata map[string]string, updatedVariable string, timestampMs int64) ([]*service.Message, error) {
	var outputMessages []*service.Message

	// Get mappings that depend on the updated variable
	dependentMappings := p.stateManager.GetDependentMappings(updatedVariable)

	for _, mapping := range dependentMappings {
		// Check if all required variables are available
		allDeps, _ := p.checkAllDependencies(mapping.Dependencies)

		if !allDeps {
			p.logger.Debugf("Skipping mapping '%s' - missing dependencies", mapping.VirtualPath)
			continue
		}

		// Get variable context for JavaScript execution using pooled context
		variableContext := p.pools.GetVariableContext()
		p.stateManager.GetState().FillVariableContext(variableContext)

		// Execute dynamic JavaScript expression with timing
		jsStart := time.Now()
		result := p.jsEngine.EvaluateDynamic(mapping.Expression, variableContext)
		jsTime := time.Since(jsStart)

		// Return context to pool
		p.pools.PutVariableContext(variableContext)

		p.metrics.LogJavaScriptExecution(jsTime, result.Success)

		if !result.Success {
			p.logger.Warnf("Dynamic mapping '%s' failed: %s", mapping.VirtualPath, result.Error)
			continue
		}

		// Create output message
		outputMsg, err := p.createOutputMessage(metadata, mapping.VirtualPath, result.Value, timestampMs)
		if err != nil {
			p.logger.Warnf("Failed to create output message for dynamic mapping '%s': %v", mapping.VirtualPath, err)
			p.metrics.LogMessageErrored()
			continue
		}

		outputMessages = append(outputMessages, outputMsg)
		p.logger.Debugf("Generated dynamic mapping output: %s = %v", mapping.VirtualPath, result.Value)
	}

	return outputMessages, nil
}

// checkAllDependencies checks if all required variables are available in the state
// Returns (allSatisfied, partialSatisfied) where:
// - allSatisfied: true if all dependencies are available
// - partialSatisfied: true if some (but not all) dependencies are available
func (p *StreamProcessor) checkAllDependencies(dependencies []string) (bool, bool) {
	state := p.stateManager.GetState()
	satisfiedCount := 0

	for _, dep := range dependencies {
		if state.HasVariable(dep) {
			satisfiedCount++
		}
	}

	allSatisfied := satisfiedCount == len(dependencies)
	partialSatisfied := satisfiedCount > 0 && satisfiedCount < len(dependencies)

	return allSatisfied, partialSatisfied
}

// updateGaugeMetrics updates gauge metrics for active resources
func (p *StreamProcessor) updateGaugeMetrics() {
	totalMappings := int64(len(p.config.StaticMappings) + len(p.config.DynamicMappings))
	totalVariables := int64(p.stateManager.GetState().GetVariableCount())

	p.metrics.UpdateActiveMetrics(totalMappings, totalVariables)
}

// createOutputMessage creates a new UMH output message with proper topic construction
func (p *StreamProcessor) createOutputMessage(metadata map[string]string, virtualPath string, value interface{}, timestampMs int64) (*service.Message, error) {
	// Construct data contract using pooled string builder
	sb := p.pools.GetStringBuilder()
	sb.WriteByte('_')
	sb.WriteString(p.config.Model.Name)
	sb.WriteByte('_')
	sb.WriteString(p.config.Model.Version)
	dataContract := sb.String()
	p.pools.PutStringBuilder(sb)

	// Get cached or construct output topic efficiently
	outputTopic := p.pools.GetTopic(p.config.OutputTopic, dataContract, virtualPath)

	// Create UMH-core timeseries message payload
	payload := TimeseriesMessage{
		Value:       value,
		TimestampMs: timestampMs,
	}

	// Serialize payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Create new message
	outputMsg := service.NewMessage(payloadBytes)

	// Set umh_topic metadata
	outputMsg.MetaSet("umh_topic", outputTopic)

	// Preserve metadata from original message (already extracted, excluding umh_topic)
	for key, value := range metadata {
		outputMsg.MetaSet(key, value)
	}

	return outputMsg, nil
}

// Close closes the processor
func (p *StreamProcessor) Close(ctx context.Context) error {
	p.logger.Debug("Closing stream processor")

	// Clean up JavaScript engine resources
	if p.jsEngine != nil {
		if err := p.jsEngine.Close(); err != nil {
			p.logger.Warnf("Error closing JavaScript engine: %v", err)
		}
	}

	return nil
}

// precompileExpressions pre-compiles all JavaScript expressions for performance
func (p *StreamProcessor) precompileExpressions() error {
	p.logger.Debug("Pre-compiling JavaScript expressions")

	// Compile static mappings
	for virtualPath, mapping := range p.config.StaticMappings {
		if err := p.jsEngine.CompileExpression(mapping.Expression); err != nil {
			return fmt.Errorf("failed to compile static mapping '%s': %w", virtualPath, err)
		}
	}

	// Compile dynamic mappings
	for virtualPath, mapping := range p.config.DynamicMappings {
		if err := p.jsEngine.CompileExpression(mapping.Expression); err != nil {
			return fmt.Errorf("failed to compile dynamic mapping '%s': %w", virtualPath, err)
		}
	}

	p.logger.Infof("Successfully pre-compiled %d JavaScript expressions",
		len(p.config.StaticMappings)+len(p.config.DynamicMappings))

	return nil
}
