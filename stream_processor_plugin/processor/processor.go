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

package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/js_engine"
	metrics2 "github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/metrics"
	pools2 "github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/pools"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/state"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// StreamProcessor is the main processor implementation
type StreamProcessor struct {
	config config.StreamProcessorConfig
	logger *service.Logger

	// State management
	stateManager *state.StateManager

	// JavaScript engine for expression evaluation
	jsEngine *js_engine.JSEngine

	// Metrics
	metrics *metrics2.ProcessorMetrics

	// Object pools for memory optimization
	pools *pools2.ObjectPools

	// Note: Removed worker pool - sequential processing is simpler and more reliable
}

// NewStreamProcessor creates a new StreamProcessor instance
func NewStreamProcessor(cfg config.StreamProcessorConfig, logger *service.Logger, metrics *service.Metrics) (*StreamProcessor, error) {
	// Validate configuration
	if err := config.ValidateConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Analyze mappings to identify static vs dynamic
	if err := js_engine.AnalyzeMappingsWithDetection(&cfg); err != nil {
		return nil, fmt.Errorf("failed to analyze mappings: %w", err)
	}

	// Extract source names from configuration
	sourceNames := make([]string, 0, len(cfg.Sources))
	for sourceName := range cfg.Sources {
		sourceNames = append(sourceNames, sourceName)
	}

	// Create pools first as they're needed by JSEngine
	pools := pools2.NewObjectPools(sourceNames, logger)

	processor := &StreamProcessor{
		config:       cfg,
		logger:       logger,
		stateManager: state.NewStateManager(&cfg),
		jsEngine:     js_engine.NewJSEngine(logger, sourceNames, pools),
		metrics:      metrics2.NewProcessorMetrics(metrics),
		pools:        pools,
	}

	// Pre-compile all JavaScript expressions for performance
	if err := processor.precompileExpressions(); err != nil {
		return nil, fmt.Errorf("failed to pre-compile expressions: %w", err)
	}

	// Log configuration analysis results
	logger.Infof("Stream processor initialized with %d static mappings and %d dynamic mappings",
		len(cfg.StaticMappings), len(cfg.DynamicMappings))

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

	// Parse JSON payload using pooled operations
	var tsMsg TimeseriesMessage
	if err := p.pools.UnmarshalFromJSON(payloadBytes, &tsMsg); err != nil {
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
	// Get all static mappings from state manager
	staticMappings := p.stateManager.GetStaticMappings()

	if len(staticMappings) == 0 {
		return nil, nil
	}

	// Get variable context for JavaScript execution using pooled context (reuse across iterations)
	variableContext := p.pools.GetVariableContext()
	defer p.pools.PutVariableContext(variableContext)

	// Process mappings sequentially with pre-allocated slice
	outputMessages := make([]*service.Message, 0, len(staticMappings))
	for _, mapping := range staticMappings {
		p.stateManager.GetState().FillVariableContext(variableContext)

		// Execute static JavaScript expression with timing using pre-compiled program
		jsStart := time.Now()
		result := p.jsEngine.EvaluateStaticPrecompiled(mapping.Expression)
		jsTime := time.Since(jsStart)

		p.metrics.LogJavaScriptExecution(jsTime, result.Success)

		if !result.Success {
			p.logger.Warnf("Static mapping '%s' failed: %s", mapping.VirtualPath, result.Error)
			p.metrics.LogMessageErrored()
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
	// Get mappings that depend on the updated variable
	dependentMappings := p.stateManager.GetDependentMappings(updatedVariable)

	if len(dependentMappings) == 0 {
		return nil, nil
	}

	// Filter mappings that have all dependencies satisfied
	executableMappings := make([]config.MappingInfo, 0, len(dependentMappings))
	for _, mapping := range dependentMappings {
		if allDeps, _ := p.checkAllDependencies(mapping.Dependencies); allDeps {
			executableMappings = append(executableMappings, mapping)
		} else {
			p.logger.Debugf("Skipping mapping '%s' - missing dependencies", mapping.VirtualPath)
		}
	}

	if len(executableMappings) == 0 {
		return nil, nil
	}

	// Get variable context for JavaScript execution using pooled context (reuse across iterations)
	variableContext := p.pools.GetVariableContext()
	defer p.pools.PutVariableContext(variableContext)

	// Process mappings sequentially with pre-allocated slice
	outputMessages := make([]*service.Message, 0, len(executableMappings))
	for _, mapping := range executableMappings {
		p.stateManager.GetState().FillVariableContext(variableContext)

		// Execute dynamic JavaScript expression with timing using pre-compiled program
		jsStart := time.Now()
		result := p.jsEngine.EvaluateDynamicPrecompiled(mapping.Expression, variableContext)
		jsTime := time.Since(jsStart)

		p.metrics.LogJavaScriptExecution(jsTime, result.Success)

		if !result.Success {
			p.logger.Warnf("Dynamic mapping '%s' failed: %s", mapping.VirtualPath, result.Error)
			p.metrics.LogMessageErrored()
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
	// Use the new high-performance JavaScript engine pre-compilation
	// This is a major optimization that eliminates parsing overhead during execution
	return p.jsEngine.PrecompileExpressions(p.config.StaticMappings, p.config.DynamicMappings)
}
