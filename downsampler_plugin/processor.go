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

package downsampler_plugin

import (
	"errors"
	"fmt"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

// MessageProcessor handles the processing of individual messages
type MessageProcessor struct {
	processor *DownsamplerProcessor
}

// NewMessageProcessor creates a new message processor
func NewMessageProcessor(processor *DownsamplerProcessor) *MessageProcessor {
	return &MessageProcessor{
		processor: processor,
	}
}

// ProcessMessage processes a single message and returns the result
func (mp *MessageProcessor) ProcessMessage(msg *service.Message, index int) MessageProcessingResult {
	result := MessageProcessingResult{
		OriginalMessage: msg,
	}

	// Process UMH-core time-series messages
	if !mp.isTimeSeriesMessage(msg) {
		result.ProcessedMessages = []*service.Message{msg}
		mp.processor.metrics.IncrementPassed()
		return result
	}

	// Parse structured payload
	data, err := msg.AsStructured()
	if err != nil {
		result.Error = fmt.Errorf("failed to parse structured data: %w", err)
		return result
	}

	dataMap, ok := data.(map[string]interface{})
	if !ok {
		result.Error = fmt.Errorf("payload is not a JSON object, got %T", data)
		return result
	}

	// Extract timestamp
	timestamp, err := mp.extractTimestamp(dataMap)
	if err != nil {
		result.Error = fmt.Errorf("failed to extract timestamp: %w", err)
		return result
	}

	// Process as UMH-core format (single "value" field)
	processedMsg, err := mp.processUMHCoreMessage(msg, dataMap, timestamp)
	if err != nil {
		result.Error = fmt.Errorf("failed to process UMH-core message: %w", err)
		return result
	}

	if processedMsg != nil {
		result.ProcessedMessages = []*service.Message{processedMsg}
	} else {
		result.WasFiltered = true
		// Empty ProcessedMessages indicates the message was filtered (not an error)
	}

	return result
}

// isTimeSeriesMessage determines if a message should be processed for downsampling
func (mp *MessageProcessor) isTimeSeriesMessage(msg *service.Message) bool {
	// Check for umh_topic metadata (required for UMH-core)
	_, hasUmhTopic := msg.MetaGet("umh_topic")
	if !hasUmhTopic {
		return false
	}

	// UMH-core format: structured payload with timestamp_ms and value fields
	dataMap, err := msg.AsStructured()
	if err != nil {
		return false
	}

	// Convert to map[string]interface{} for field access
	data, ok := dataMap.(map[string]interface{})
	if !ok {
		return false
	}

	// Check for required UMH-core fields
	_, hasTimestamp := data["timestamp_ms"]
	_, hasValue := data["value"]

	// Require both timestamp_ms and value fields for UMH-core format
	return hasTimestamp && hasValue
}

// extractTimestamp extracts and converts timestamp from message data
func (mp *MessageProcessor) extractTimestamp(dataMap map[string]interface{}) (time.Time, error) {
	timestampMs, ok := dataMap["timestamp_ms"]
	if !ok {
		return time.Time{}, errors.New("missing timestamp_ms field")
	}

	var ts int64
	switch v := timestampMs.(type) {
	case float64:
		ts = int64(v)
	case int:
		ts = int64(v)
	case int64:
		ts = v
	default:
		return time.Time{}, fmt.Errorf("invalid timestamp_ms type: %T", timestampMs)
	}

	return time.Unix(0, ts*int64(time.Millisecond)), nil
}

// processUMHCoreMessage processes a single UMH-core format message
func (mp *MessageProcessor) processUMHCoreMessage(msg *service.Message, dataMap map[string]interface{}, timestamp time.Time) (*service.Message, error) {
	// Extract series ID
	seriesID, err := mp.extractSeriesID(dataMap)
	if err != nil {
		return nil, err
	}

	// Extract value
	value, err := mp.extractValue(dataMap)
	if err != nil {
		return nil, err
	}

	// Get or create series state
	state, err := mp.processor.getOrCreateSeriesState(seriesID)
	if err != nil {
		return nil, fmt.Errorf("failed to get series state: %w", err)
	}

	// Process the message
	return mp.processWithState(msg, dataMap, value, timestamp, seriesID, state)
}

// extractSeriesID extracts the series ID from the message data
func (mp *MessageProcessor) extractSeriesID(dataMap map[string]interface{}) (string, error) {
	topicInterface, exists := dataMap["umh_topic"]
	if !exists {
		return "", fmt.Errorf("missing required field: umh_topic")
	}

	seriesID, ok := topicInterface.(string)
	if !ok {
		return "", fmt.Errorf("umh_topic must be a string, got %T", topicInterface)
	}

	return seriesID, nil
}

// extractValue extracts the value from the message data
func (mp *MessageProcessor) extractValue(dataMap map[string]interface{}) (interface{}, error) {
	value, exists := dataMap["value"]
	if !exists {
		return nil, fmt.Errorf("missing required field: value")
	}
	return value, nil
}

// processWithState processes the message using the series state
func (mp *MessageProcessor) processWithState(msg *service.Message, dataMap map[string]interface{}, value interface{}, timestamp time.Time, seriesID string, state *SeriesState) (*service.Message, error) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	// Use ProcessorWrapper to process the value and get emitted points
	emittedPoints, err := state.processor.Ingest(value, timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to process message: %w", err)
	}

	// If no points were emitted, the message was filtered out
	if len(emittedPoints) == 0 {
		mp.processor.metrics.IncrementFiltered()
		mp.processor.logger.Debug(fmt.Sprintf("Message filtered out for series %s: value=%v", seriesID, value))
		return nil, nil // Return nil to indicate message should be dropped
	}

	// Handle emitted points
	return mp.createOutputMessage(msg, dataMap, emittedPoints, state)
}

// createOutputMessage creates the output message from emitted points
func (mp *MessageProcessor) createOutputMessage(msg *service.Message, dataMap map[string]interface{}, emittedPoints []algorithms.Point, state *SeriesState) (*service.Message, error) {
	// Points were emitted - we need to handle them
	// For now, we'll emit the first point (most common case)
	if len(emittedPoints) > 1 {
		mp.processor.logger.Warnf("Algorithm emitted %d points, only using first one. This may indicate SDT final emission.", len(emittedPoints))
	}

	emittedPoint := emittedPoints[0]
	mp.processor.metrics.IncrementProcessed()

	// Update internal tracking state
	mp.processor.updateProcessedTime(state, emittedPoint.Timestamp)

	// Create output message with emitted point data
	outputData := make(map[string]interface{})
	for key, val := range dataMap {
		outputData[key] = val
	}

	// Update the value and timestamp with the emitted point
	outputData["value"] = emittedPoint.Value
	outputData["timestamp"] = emittedPoint.Timestamp.UnixNano()

	// Add algorithm metadata
	mp.addAlgorithmMetadata(outputData, state)

	// Create new message with processed data
	outputMsg := msg.Copy()
	outputMsg.SetStructured(outputData)

	return outputMsg, nil
}

// addAlgorithmMetadata adds downsampling algorithm metadata to the output data
func (mp *MessageProcessor) addAlgorithmMetadata(outputData map[string]interface{}, state *SeriesState) {
	if outputData["metadata"] == nil {
		outputData["metadata"] = make(map[string]interface{})
	}

	metadata, ok := outputData["metadata"].(map[string]interface{})
	if !ok {
		metadata = make(map[string]interface{})
		outputData["metadata"] = metadata
	}

	metadata["downsampling_algorithm"] = state.processor.Config()
}
