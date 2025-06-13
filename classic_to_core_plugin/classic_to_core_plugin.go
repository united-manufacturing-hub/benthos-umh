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

package classic_to_core_plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// ClassicToCoreConfig holds the configuration for the Classic to Core processor.
// It defines how messages should be converted from UMH Historian Data Contract
// format to Core format, including field handling, limits, and metadata preservation.
type ClassicToCoreConfig struct {
	TargetDataContract string `json:"target_data_contract" yaml:"target_data_contract"`
}

// Constants for hardcoded configuration values
const (
	maxRecursionDepth = 10
	maxTagsPerMessage = 1000
	preserveMeta      = true
	timestampField    = "timestamp_ms"
)

func init() {
	spec := service.NewConfigSpec().
		Version("1.0.0").
		Summary("Convert UMH Historian Data Contract format to Core format").
		Description(`The classic_to_core processor converts Historian Data Contract messages containing multiple values 
and tag groups into individual Core format messages, following the "one tag, one message, one topic" principle.

Input format (Historian Data Contract):
- Single message with timestamp_ms and multiple data fields or tag groups
- Topic: umh.v1.<location>._historian.<context>
- Supports flat tags: {"timestamp_ms": 123, "temperature": 23.4}
- Supports tag groups: {"timestamp_ms": 123, "axis": {"x": 1.0, "y": 2.0}}

Output format (Core):
- Multiple messages, one per tag (including flattened tag groups)
- Each with {"value": <field_value>, "timestamp_ms": <timestamp>}
- Topics: umh.v1.<location>.<target_data_contract>.<context>.<tag_name>
- Tag groups flattened with dot separators: "axis.x", "axis.y"

The processor will:
1. Extract the timestamp field from the payload
2. Flatten any nested tag groups using dot separator for intuitive paths
3. Create one output message per tag
4. Construct new topics by appending tag names
5. Preserve original metadata while updating topic-related fields`).
		Field(service.NewStringField("target_data_contract").
			Description("Target data contract for output topics. If empty, uses the input's data contract (e.g., _historian)").
			Default("").
			Optional())

	err := service.RegisterBatchProcessor(
		"classic_to_core",
		spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			targetDataContract, err := conf.FieldString("target_data_contract")
			if err != nil {
				return nil, err
			}

			config := ClassicToCoreConfig{
				TargetDataContract: targetDataContract,
			}

			return newClassicToCoreProcessor(config, mgr.Logger(), mgr.Metrics())
		})
	if err != nil {
		panic(err)
	}
}

// ClassicToCoreProcessor processes UMH Historian Data Contract messages and converts
// them to Core format. It maintains configuration, metrics, and efficient field lookups
// to handle the transformation of single multi-value messages into multiple single-value messages.
type ClassicToCoreProcessor struct {
	config            ClassicToCoreConfig
	logger            *service.Logger
	messagesProcessed *service.MetricCounter
	messagesErrored   *service.MetricCounter
	messagesExpanded  *service.MetricCounter
	messagesDropped   *service.MetricCounter
	recursionLimitHit *service.MetricCounter
	tagLimitExceeded  *service.MetricCounter
	excludeFieldsMap  map[string]bool
}

// newClassicToCoreProcessor creates a new instance of the Classic to Core processor.
// It initializes the processor with the provided configuration, sets up metrics,
// and creates an optimized field exclusion map for fast lookup during processing.
// The timestamp field is automatically added to the exclusion map since it's handled separately.
func newClassicToCoreProcessor(config ClassicToCoreConfig, logger *service.Logger, metrics *service.Metrics) (*ClassicToCoreProcessor, error) {
	// Create exclude fields map for fast lookup
	excludeFieldsMap := make(map[string]bool)
	excludeFieldsMap["timestamp_ms"] = true // Always exclude timestamp field

	return &ClassicToCoreProcessor{
		config:            config,
		logger:            logger,
		messagesProcessed: metrics.NewCounter("messages_processed"),
		messagesErrored:   metrics.NewCounter("messages_errored"),
		messagesExpanded:  metrics.NewCounter("messages_expanded"),
		messagesDropped:   metrics.NewCounter("messages_dropped"),
		recursionLimitHit: metrics.NewCounter("recursion_limit_hit"),
		tagLimitExceeded:  metrics.NewCounter("tag_limit_exceeded"),
		excludeFieldsMap:  excludeFieldsMap,
	}, nil
}

// ProcessBatch processes a batch of messages, converting each UMH Historian Data Contract
// message into multiple Core format messages. It applies tag limits, handles errors gracefully,
// and tracks comprehensive metrics. Each input message can produce multiple output messages
// (one per data field), following the "one tag, one message, one topic" principle.
func (p *ClassicToCoreProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var outputBatch service.MessageBatch

	for _, msg := range batch {
		p.messagesProcessed.Incr(1)

		// Process single message and add results to output batch
		expandedMessages, err := p.processMessage(msg)
		if err != nil {
			p.logger.Errorf("Failed to process message: %v", err)
			p.messagesErrored.Incr(1)
			continue
		}

		// Check tag limit per message
		if len(expandedMessages) > maxTagsPerMessage {
			p.logger.Errorf("Message produced %d tags, exceeding limit of %d", len(expandedMessages), maxTagsPerMessage)
			p.tagLimitExceeded.Incr(1)
			p.messagesDropped.Incr(1)
			continue
		}

		outputBatch = append(outputBatch, expandedMessages...)
		p.messagesExpanded.Incr(int64(len(expandedMessages)))
	}

	if len(outputBatch) == 0 {
		return nil, nil
	}

	return []service.MessageBatch{outputBatch}, nil
}

// processMessage handles the conversion of a single input message to multiple Core format messages.
// It orchestrates the entire conversion process: parsing payload, extracting timestamp, validating topic,
// flattening nested tag groups, and creating individual Core messages for each data field.
// This is the core transformation logic that converts "one message, many values" to "many messages, one value each".
func (p *ClassicToCoreProcessor) processMessage(msg *service.Message) ([]*service.Message, error) {
	// Parse and validate payload
	payload, err := p.parsePayload(msg)
	if err != nil {
		return nil, fmt.Errorf("payload parsing failed: %w", err)
	}

	// Extract and validate timestamp
	timestamp, err := p.validateAndExtractTimestamp(payload)
	if err != nil {
		return nil, fmt.Errorf("timestamp validation failed: %w", err)
	}

	// Parse and validate topic
	topicComponents, err := p.validateAndParseTopic(msg)
	if err != nil {
		return nil, fmt.Errorf("topic parsing failed: %w", err)
	}

	// Flatten payload with recursion limit
	flattenedTags, err := p.flattenPayload(payload, "", 0)
	if err != nil {
		return nil, fmt.Errorf("payload flattening failed: %w", err)
	}

	// Check tag limit
	if len(flattenedTags) > maxTagsPerMessage {
		p.logger.Warnf("Message exceeds maximum tag limit of %d, truncating", maxTagsPerMessage)
		p.tagLimitExceeded.Incr(1)
		// Truncate the payload to the limit
		truncatedTags := make(map[string]interface{})
		count := 0
		for k, v := range flattenedTags {
			if count >= maxTagsPerMessage {
				break
			}
			truncatedTags[k] = v
			count++
		}
		flattenedTags = truncatedTags
	}

	// Convert to Core messages
	var expandedMessages []*service.Message
	for tagName, tagValue := range flattenedTags {
		if p.excludeFieldsMap[tagName] {
			continue
		}

		newMsg, err := p.createCoreMessage(msg, tagName, tagValue, timestamp, topicComponents)
		if err != nil {
			return nil, fmt.Errorf("failed to create Core message for tag '%s': %w", tagName, err)
		}

		expandedMessages = append(expandedMessages, newMsg)
	}

	return expandedMessages, nil
}

// parsePayload extracts and validates the JSON payload from the message.
// It ensures the message contains valid JSON structured data that can be processed as a map.
// This is the first validation step that determines if a message can be converted to Core format.
func (p *ClassicToCoreProcessor) parsePayload(msg *service.Message) (map[string]interface{}, error) {
	structured, err := msg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("failed to parse as structured data: %w", err)
	}

	payload, ok := structured.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("payload is not a JSON object")
	}

	return payload, nil
}

// validateAndExtractTimestamp validates the timestamp field exists and extracts its value.
// It ensures the configured timestamp field is present in the payload and converts it to a numeric timestamp.
// The timestamp is critical for Core format messages as it provides the temporal context for each data point.
func (p *ClassicToCoreProcessor) validateAndExtractTimestamp(payload map[string]interface{}) (int64, error) {
	timestampValue, exists := payload[timestampField]
	if !exists {
		return 0, fmt.Errorf("timestamp field '%s' not found in payload", timestampField)
	}

	timestamp, err := p.extractTimestamp(timestampValue)
	if err != nil {
		return 0, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	return timestamp, nil
}

// validateAndParseTopic extracts and validates the topic from message metadata.
// It retrieves the topic from metadata (trying "topic" first, then "umh_topic" as fallback)
// and parses it into components needed for Core topic reconstruction. This ensures the input
// follows the expected UMH topic structure before proceeding with conversion.
func (p *ClassicToCoreProcessor) validateAndParseTopic(msg *service.Message) (*TopicComponents, error) {
	originalTopic, exists := msg.MetaGet("topic")
	if !exists {
		// Try to get it from umh_topic as fallback
		if umhTopic, exists := msg.MetaGet("umh_topic"); exists {
			originalTopic = umhTopic
		} else {
			return nil, fmt.Errorf("no topic found in message metadata")
		}
	}

	topicComponents, err := p.parseClassicTopic(originalTopic)
	if err != nil {
		return nil, fmt.Errorf("failed to parse topic '%s': %w", originalTopic, err)
	}

	return topicComponents, nil
}

// TopicComponents represents the parsed parts of a UMH topic.
// This structure breaks down the hierarchical UMH topic format into its constituent parts,
// enabling reconstruction of Core format topics by appending field names to the appropriate components.
// The parsing follows UMH topic conventions: umh.v1.<location>.<schema>.<tag_group>
type TopicComponents struct {
	Prefix   string // "umh.v1"
	Location string // "enterprise.site.area"
	Schema   string // "_historian"
	TagGroup string // Additional context after schema
}

// parseClassicTopic parses a UMH Historian Data Contract topic into its components.
// It validates the topic structure, ensures it follows UMH v1 conventions, and extracts
// the location path, schema, and context. This parsing is essential for reconstructing
// proper Core format topics that maintain the original hierarchical structure while adding field names.
func (p *ClassicToCoreProcessor) parseClassicTopic(topic string) (*TopicComponents, error) {
	parts := strings.Split(topic, ".")
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid topic structure, expected at least 4 parts: %s", topic)
	}

	// Validate UMH topic prefix
	if len(parts) < 2 || parts[0] != "umh" || parts[1] != "v1" {
		return nil, fmt.Errorf("invalid UMH topic prefix, expected 'umh.v1': %s", topic)
	}

	// Find the schema (starts with underscore)
	var schemaIndex = -1
	for i := 2; i < len(parts); i++ { // Start from index 2 (after umh.v1)
		if strings.HasPrefix(parts[i], "_") {
			schemaIndex = i
			break
		}
	}

	if schemaIndex == -1 {
		return nil, fmt.Errorf("no schema found in topic: %s", topic)
	}

	if schemaIndex == 2 {
		return nil, fmt.Errorf("missing location path in topic: %s", topic)
	}

	// Reconstruct components
	prefix := strings.Join(parts[:2], ".") // umh.v1
	locationPath := strings.Join(parts[2:schemaIndex], ".")
	schema := parts[schemaIndex]

	var context string
	if len(parts) > schemaIndex+1 {
		context = strings.Join(parts[schemaIndex+1:], ".")
	}

	return &TopicComponents{
		Prefix:   prefix,
		Location: locationPath,
		Schema:   schema,
		TagGroup: context,
	}, nil
}

// createCoreMessage creates a single Core format message from a field extracted from the original payload.
// It constructs the Core payload format ({"value": <field_value>, "timestamp_ms": <timestamp>}),
// generates the appropriate Core topic, and sets up all required metadata fields.
// This function embodies the "one tag, one message" principle of Core format.
func (p *ClassicToCoreProcessor) createCoreMessage(originalMsg *service.Message, fieldName string, fieldValue interface{}, timestamp int64, topicComponents *TopicComponents) (*service.Message, error) {
	// Create Core format payload
	corePayload := map[string]interface{}{
		"value":        fieldValue,
		"timestamp_ms": timestamp,
	}

	// Marshal payload
	payloadBytes, err := json.Marshal(corePayload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Core payload: %v", err)
	}

	// Create new message
	newMsg := service.NewMessage(payloadBytes)

	// Always preserve metadata
	if preserveMeta {
		// Copy all metadata from original message
		originalMsg.MetaWalk(func(k, v string) error {
			newMsg.MetaSet(k, v)
			return nil
		})
	}

	// Determine target data contract
	targetSchema := p.config.TargetDataContract
	if targetSchema == "" {
		// Use the original data contract if not specified
		targetSchema = topicComponents.Schema
	}

	// Construct new topic
	newTopic := p.constructCoreTopic(topicComponents, fieldName, targetSchema)
	newMsg.MetaSet("topic", newTopic)
	newMsg.MetaSet("umh_topic", newTopic)

	// Set Core-specific metadata
	newMsg.MetaSet("location_path", topicComponents.Location)
	newMsg.MetaSet("schema", targetSchema)
	newMsg.MetaSet("tag_name", fieldName)

	// Set virtual path if there was context in the original topic
	if topicComponents.TagGroup != "" {
		newMsg.MetaSet("virtual_path", topicComponents.TagGroup)
	}

	return newMsg, nil
}

// constructCoreTopic builds a Core format topic from the parsed topic components and field name.
// It maintains the UMH hierarchical structure while adapting it for Core format by using the
// target data contract and appending the field name. The resulting topic follows the pattern:
// umh.v1.<location>.<target_contract>.<context>.<field_name>
func (p *ClassicToCoreProcessor) constructCoreTopic(components *TopicComponents, fieldName string, targetSchema string) string {
	parts := []string{
		components.Prefix,
		components.Location,
		targetSchema,
	}

	if components.TagGroup != "" {
		parts = append(parts, components.TagGroup)
	}

	parts = append(parts, fieldName)

	return strings.Join(parts, ".")
}

// flattenPayload recursively flattens nested objects using dot separator to create intuitive paths.
// It converts complex tag group structures into flat key-value pairs with dot-notation names.
// For example: {"axis": {"x": 1.0, "y": 2.0}} becomes {"axis.x": 1.0, "axis.y": 2.0}.
// The function respects recursion depth limits to prevent stack overflow and infinite loops.
func (p *ClassicToCoreProcessor) flattenPayload(payload map[string]interface{}, prefix string, depth int) (map[string]interface{}, error) {
	// Check recursion depth limit
	if depth > maxRecursionDepth {
		p.recursionLimitHit.Incr(1)
		return nil, fmt.Errorf("maximum recursion depth of %d reached, stopping flattening", maxRecursionDepth)
	}

	result := make(map[string]interface{})

	for key, value := range payload {
		// Skip the timestamp field as it's handled separately
		if key == timestampField {
			continue
		}

		var fullKey string
		if prefix == "" {
			fullKey = key
		} else {
			fullKey = prefix + "." + key
		}

		// Check if value is a nested object (tag group)
		if nestedMap, ok := value.(map[string]interface{}); ok {
			// Recursively flatten nested objects with incremented depth
			nestedResults, err := p.flattenPayload(nestedMap, fullKey, depth+1)
			if err != nil {
				return nil, err
			}
			for nestedKey, nestedValue := range nestedResults {
				result[nestedKey] = nestedValue
			}
		} else {
			// Regular tag - add to result
			result[fullKey] = value
		}
	}

	return result, nil
}

// extractTimestamp converts various timestamp formats to a standardized int64 value.
// It handles multiple input types commonly found in JSON: float64, int64, int, json.Number, and string.
// This flexibility allows the processor to work with timestamps from different sources and formats
// while ensuring all Core messages have consistent timestamp representation.
func (p *ClassicToCoreProcessor) extractTimestamp(value interface{}) (int64, error) {
	switch v := value.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return int64(f), nil
		}
		return 0, fmt.Errorf("invalid timestamp number format")
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, nil
		}
		return 0, fmt.Errorf("invalid timestamp string format")
	default:
		return 0, fmt.Errorf("unsupported timestamp type: %T", v)
	}
}

// Close gracefully shuts down the processor and releases any resources.
// Currently, the processor doesn't maintain any persistent resources that require cleanup,
// but this method satisfies the BatchProcessor interface and provides a clean shutdown hook
// for future resource management needs.
func (p *ClassicToCoreProcessor) Close(ctx context.Context) error {
	return nil
}
