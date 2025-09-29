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

// MessageProcessor handles the processing of individual messages within a batch.
// It provides the core message processing logic that integrates with Benthos
// message handling patterns while maintaining series state and ACK buffering.
type MessageProcessor struct {
	processor *DownsamplerProcessor
}

// NewMessageProcessor creates a new message processor that wraps the given
// DownsamplerProcessor to handle individual message processing operations.
func NewMessageProcessor(processor *DownsamplerProcessor) *MessageProcessor {
	return &MessageProcessor{
		processor: processor,
	}
}

// ProcessMessage processes a single message from a batch and returns the processing result.
// This is the main entry point for individual message processing, handling:
//   - UMH-core time-series message detection and filtering
//   - Structured payload parsing and validation
//   - Timestamp extraction and series identification
//   - Integration with downsampling algorithms via ACK buffering
//
// Returns MessageProcessingResult containing either processed messages, error, or filter status.
func (mp *MessageProcessor) ProcessMessage(msg *service.Message, index int) MessageProcessingResult {
	result := MessageProcessingResult{
		OriginalMessage: msg,
	}

	// Check for ds_ignore metadata first (highest precedence)
	if ignoreValue, hasIgnore := msg.MetaGet("ds_ignore"); hasIgnore && ignoreValue != "" {
		// Create copy of message with ignore metadata
		ignoredMsg := msg.Copy()
		ignoredMsg.MetaDelete("ds_ignore") // keep internal flags internal
		ignoredMsg.MetaSet("downsampled_by", "ignored")

		// Extract topic for logging if available
		topicName := "unknown"
		if topic, hasTopic := msg.MetaGet("umh_topic"); hasTopic {
			topicName = topic
		}

		mp.processor.logger.Debugf("Message ignored due to ds_ignore metadata: topic=%s, ignore_value=%s", topicName, ignoreValue)
		mp.processor.metrics.IncrementIgnored()
		mp.processor.metrics.IncrementPassed() // maintain metric consistency

		result.ProcessedMessages = []*service.Message{ignoredMsg}
		return result
	}

	// Process UMH-core time-series messages
	if !mp.isTimeSeriesMessage(msg) {
		result.ProcessedMessages = []*service.Message{msg}
		mp.processor.metrics.IncrementPassed()

		// Debug log why message was passed through
		if umhTopic, hasUmhTopic := msg.MetaGet("umh_topic"); !hasUmhTopic {
			mp.processor.logger.Debugf("Message passed through: missing umh_topic metadata")
		} else if umhTopic == "" {
			mp.processor.logger.Debugf("Message passed through: empty umh_topic metadata")
		} else {
			// Has umh_topic but missing required fields
			data, err := msg.AsStructured()
			if err != nil {
				mp.processor.logger.Debugf("Message passed through: failed to parse structured data: %v", err)
			} else if dataMap, ok := data.(map[string]interface{}); !ok {
				mp.processor.logger.Debugf("Message passed through: payload is not a JSON object")
			} else {
				missingFields := []string{}
				if _, hasTimestamp := dataMap["timestamp_ms"]; !hasTimestamp {
					missingFields = append(missingFields, "timestamp_ms")
				}
				if _, hasValue := dataMap["value"]; !hasValue {
					missingFields = append(missingFields, "value")
				}
				if len(missingFields) > 0 {
					mp.processor.logger.Debugf("Message passed through for topic '%s': missing required fields: %v", umhTopic, missingFields)
				} else {
					mp.processor.logger.Debugf("Message passed through for topic '%s': has all fields but failed isTimeSeriesMessage check", umhTopic)
				}
			}
		}

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

// isTimeSeriesMessage determines if a message should be processed for downsampling.
// UMH-core time-series messages must have:
//   - umh_topic metadata field (used as series identifier)
//   - Structured JSON payload with timestamp_ms and value fields
//
// Non-time-series messages are passed through unchanged to preserve data flow.
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

	// Validate UMH-core strict format: only timestamp_ms and value fields allowed
	if len(data) != 2 {
		return false // UMH-core format must have exactly 2 fields
	}

	// Require both timestamp_ms and value fields for UMH-core format
	return hasTimestamp && hasValue
}

// extractTimestamp extracts and validates the timestamp_ms field from message data.
// Supports multiple numeric types (float64, int, int64) and converts to time.Time
// for consistent internal processing across different data sources.
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

// processUMHCoreMessage processes a validated UMH-core format message through
// the downsampling pipeline. Handles series identification, value extraction,
// and state management integration.
func (mp *MessageProcessor) processUMHCoreMessage(msg *service.Message, dataMap map[string]interface{}, timestamp time.Time) (*service.Message, error) {
	// Extract series ID
	seriesID, err := mp.extractSeriesID(msg)
	if err != nil {
		return nil, err
	}

	// Extract value
	value, err := mp.extractValue(dataMap)
	if err != nil {
		return nil, err
	}

	// Get or create series state
	state, err := mp.processor.getOrCreateSeriesState(seriesID, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to get series state: %w", err)
	}

	// Process the message
	return mp.processWithState(msg, dataMap, value, timestamp, seriesID, state)
}

// extractSeriesID extracts the series identifier from umh_topic metadata.
// The umh_topic serves as the unique series identifier for time-series processing
// and configuration matching (pattern overrides, algorithm selection).
func (mp *MessageProcessor) extractSeriesID(msg *service.Message) (string, error) {
	seriesID, exists := msg.MetaGet("umh_topic")
	if !exists {
		return "", fmt.Errorf("missing required metadata field: umh_topic")
	}

	if seriesID == "" {
		return "", fmt.Errorf("umh_topic metadata cannot be empty")
	}

	return seriesID, nil
}

// extractValue extracts the value field from the message payload.
// Supports any JSON-serializable value type (numeric, string, boolean)
// as required by the UMH-core data contract.
func (mp *MessageProcessor) extractValue(dataMap map[string]interface{}) (interface{}, error) {
	value, exists := dataMap["value"]
	if !exists {
		return nil, fmt.Errorf("missing required field: value")
	}
	return value, nil
}

// processWithState processes the message using series-specific state and algorithm.
// Implements the core ACK buffering strategy for emit-previous algorithms:
//   - Handles duplicate timestamp detection and filtering
//   - Manages message stashing/releasing for SDT and similar algorithms
//   - Provides immediate ACK for deadband algorithms that don't need buffering
//   - Coordinates between algorithm output and Benthos message lifecycle
//
// This function is the heart of the downsampling processor's ACK buffering architecture.
func (mp *MessageProcessor) processWithState(msg *service.Message, dataMap map[string]interface{}, value interface{}, timestamp time.Time, seriesID string, state *SeriesState) (*service.Message, error) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	// Check for same timestamp as last processed message
	if !state.lastProcessedTime.IsZero() && timestamp.Equal(state.lastProcessedTime) {
		mp.processor.logger.Warnf("Dropping message with duplicate timestamp %v for series '%s' - subsequent messages with identical timestamps are not allowed. To prevent this issue for batched messages you can set the metafield 'timestamp_ms'.",
			timestamp, seriesID)
		mp.processor.metrics.IncrementFiltered()
		return nil, nil // Drop the message
	}

	// Update lastProcessedTime for all messages that reach this point (for late arrival detection)
	mp.processor.updateProcessedTime(state, timestamp)

	// Use ProcessorWrapper to process the value and get emitted points
	emittedPoints, err := state.processor.Ingest(value, timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to process message: %w", err)
	}

	// Convert timestamp to Unix milliseconds for comparison
	timestampMs := timestamp.UnixNano() / int64(time.Millisecond)

	// Handle different emission scenarios
	switch len(emittedPoints) {
	case 0:
		// Algorithm filtered - handle based on whether algorithm needs emit-previous buffering
		if !state.holdsPrev {
			// Deadband and other algorithms that never emit previous points:
			// ACK the message immediately since it will never be needed again
			mp.processor.metrics.IncrementFiltered()
			mp.processor.logger.Debugf("Message filtered and ACKed immediately for series '%s' (algorithm doesn't need buffering): value=%v, timestamp=%v", seriesID, value, timestamp)
			return nil, nil // Return nil to let Benthos ACK the message automatically
		} else {
			// SDT and other emit-previous algorithms: stash message as candidate
			state.stash(msg)
			mp.processor.metrics.IncrementFiltered()
			mp.processor.logger.Debugf("Message filtered and stashed for series '%s' (algorithm needs buffering): value=%v, timestamp=%v", seriesID, value, timestamp)
			return nil, nil // No immediate output
		}

	case 1:
		// Single point emitted - this could be current or previous point
		emittedPoint := emittedPoints[0]
		emittedPointMs := emittedPoint.Timestamp.UnixNano() / int64(time.Millisecond)

		if emittedPointMs == timestampMs {
			// Current point emitted - release any buffered candidate and emit current
			state.releaseCandidate() // ACK the buffered message if any
			mp.processor.metrics.IncrementProcessed()
			mp.processor.updateProcessedTime(state, emittedPoint.Timestamp)
			mp.processor.logger.Debugf("Current point emitted for series '%s': value=%v, timestamp=%v", seriesID, emittedPoint.Value, emittedPoint.Timestamp)
			return mp.createOutputMessage(msg, dataMap, emittedPoints, state)
		} else {
			// Previous point emitted - emit buffered candidate, then stash current
			bufferedMsg := state.releaseCandidate()
			if bufferedMsg != nil {
				// Clone template from buffered message and emit it with algorithm point
				outputMsg, err := mp.cloneTemplate(bufferedMsg, emittedPoint, state)
				if err != nil {
					return nil, fmt.Errorf("failed to create output from buffered message: %w", err)
				}
				// Stash current message as new candidate
				state.stash(msg)
				mp.processor.metrics.IncrementProcessed()
				mp.processor.updateProcessedTime(state, emittedPoint.Timestamp)
				mp.processor.logger.Debugf("Previous point emitted for series '%s': value=%v, timestamp=%v (current stashed: %v)", seriesID, emittedPoint.Value, emittedPoint.Timestamp, value)
				return outputMsg, nil
			} else {
				// No buffered message - this shouldn't happen in normal operation
				mp.processor.logger.Warnf("Algorithm emitted previous point but no candidate was buffered for series '%s' - this indicates a potential algorithm issue", seriesID)
				state.stash(msg)
				return nil, nil
			}
		}

	case 2:
		// Two points emitted (emit-previous scenario) - emit first (previous), stash current
		previousPoint := emittedPoints[0]
		currentPoint := emittedPoints[1]

		// Release buffered candidate and emit it with previous point
		bufferedMsg := state.releaseCandidate()
		if bufferedMsg != nil {
			outputMsg, err := mp.cloneTemplate(bufferedMsg, previousPoint, state)
			if err != nil {
				return nil, fmt.Errorf("failed to create output from buffered message: %w", err)
			}
			// Stash current message with current point timestamp
			state.stash(msg)
			mp.processor.metrics.IncrementProcessed()
			mp.processor.updateProcessedTime(state, previousPoint.Timestamp)
			mp.processor.logger.Debugf("Two points emitted for series '%s': emitting previous point value=%v, timestamp=%v (current stashed: %v)", seriesID, previousPoint.Value, previousPoint.Timestamp, currentPoint.Value)
			return outputMsg, nil
		} else {
			// No buffered message - emit current point directly
			mp.processor.logger.Warnf("Algorithm emitted two points but no candidate was buffered for series '%s' - emitting current point only", seriesID)
			mp.processor.metrics.IncrementProcessed()
			mp.processor.updateProcessedTime(state, currentPoint.Timestamp)
			return mp.createOutputMessage(msg, dataMap, []algorithms.GenericPoint{currentPoint}, state)
		}

	default:
		// Multiple points emitted (unusual) - emit all points to avoid data loss
		mp.processor.logger.Warnf("Algorithm emitted %d points for series '%s' - this is unusual, emitting first point only", len(emittedPoints), seriesID)
		mp.processor.metrics.IncrementProcessed()
		// Update processed time with the latest timestamp
		latestTime := emittedPoints[0].Timestamp
		for _, pt := range emittedPoints {
			if pt.Timestamp.After(latestTime) {
				latestTime = pt.Timestamp
			}
		}
		mp.processor.updateProcessedTime(state, latestTime)
		// Log all points for debugging
		for i, pt := range emittedPoints {
			mp.processor.logger.Debugf("Multiple emission point %d for series '%s': value=%v, timestamp=%v", i, seriesID, pt.Value, pt.Timestamp)
		}
		return mp.createOutputMessage(msg, dataMap, emittedPoints, state)
	}
}

// createOutputMessage creates the final output message from algorithm-emitted points.
// Preserves original message structure and metadata while updating value and timestamp
// with the downsampled data. Adds algorithm metadata for traceability.
func (mp *MessageProcessor) createOutputMessage(msg *service.Message, dataMap map[string]interface{}, emittedPoints []algorithms.GenericPoint, state *SeriesState) (*service.Message, error) {
	if len(emittedPoints) == 0 {
		return nil, fmt.Errorf("no points to emit")
	}

	// Use the first point for the output message
	emittedPoint := emittedPoints[0]

	// Create output message with emitted point data
	outputData := make(map[string]interface{})
	for key, val := range dataMap {
		outputData[key] = val
	}

	// Update the value and timestamp with the emitted point
	// Preserve the original value type (string, bool, or numeric)
	outputData["value"] = emittedPoint.Value
	outputData["timestamp_ms"] = emittedPoint.Timestamp.UnixNano() / int64(time.Millisecond)

	// Create new message with processed data
	outputMsg := msg.Copy()
	outputMsg.SetStructured(outputData)

	// Add algorithm metadata to message metadata
	mp.addAlgorithmMetadata(outputMsg, state)

	return outputMsg, nil
}

// cloneTemplate creates a new output message from a buffered message template.
// Used when emit-previous algorithms release buffered messages with algorithm point data.
// This enables proper message metadata preservation while updating the payload.
func (mp *MessageProcessor) cloneTemplate(templateMsg *service.Message, point algorithms.GenericPoint, state *SeriesState) (*service.Message, error) {
	// Get the structured data from the template message
	templateData, err := templateMsg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("failed to get structured data from template: %w", err)
	}

	templateMap, ok := templateData.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("template data is not a map, got %T", templateData)
	}

	// Create output data by copying template
	outputData := make(map[string]interface{})
	for key, val := range templateMap {
		outputData[key] = val
	}

	// Update with algorithm point data
	// Preserve the original value type (string, bool, or numeric)
	outputData["value"] = point.Value
	outputData["timestamp_ms"] = point.Timestamp.UnixNano() / int64(time.Millisecond)

	// Create new message with processed data
	outputMsg := templateMsg.Copy()
	outputMsg.SetStructured(outputData)

	// Add algorithm metadata to message metadata
	mp.addAlgorithmMetadata(outputMsg, state)

	return outputMsg, nil
}

// addAlgorithmMetadata adds downsampling metadata to output messages.
// Provides traceability by recording which algorithm processed the message
// and its configuration for debugging and monitoring purposes.
func (mp *MessageProcessor) addAlgorithmMetadata(outputMsg *service.Message, state *SeriesState) {
	// Get algorithm name for metadata
	algorithmName := state.processor.Name()

	// Add downsampled_by metadata that tests expect
	outputMsg.MetaSet("downsampled_by", algorithmName)

	// Add full algorithm configuration as JSON string for debugging
	// Note: Config() currently returns a string, but we ensure type safety here
	algorithmConfig := state.processor.Config()

	// Ensure we always set a string value for MetaSet (defensive programming)
	// This provides future-proofing if Config() ever returns structured data
	if algorithmConfig != "" {
		outputMsg.MetaSet("downsampling_config", algorithmConfig)
	} else {
		// Fallback to algorithm name if config is empty
		outputMsg.MetaSet("downsampling_config", algorithmName)
	}
}
