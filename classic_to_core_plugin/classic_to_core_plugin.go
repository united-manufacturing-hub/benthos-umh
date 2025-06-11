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

type ClassicToCoreConfig struct {
	TimestampField     string   `json:"timestamp_field" yaml:"timestamp_field"`
	ExcludeFields      []string `json:"exclude_fields" yaml:"exclude_fields"`
	TargetDataContract string   `json:"target_data_contract" yaml:"target_data_contract"`
	PreserveMeta       bool     `json:"preserve_meta" yaml:"preserve_meta"`
	MaxRecursionDepth  int      `json:"max_recursion_depth" yaml:"max_recursion_depth"`
	MaxTagsPerMessage  int      `json:"max_tags_per_message" yaml:"max_tags_per_message"`
}

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
		Field(service.NewStringField("timestamp_field").
			Description("Field name containing the timestamp (default: timestamp_ms)").
			Default("timestamp_ms")).
		Field(service.NewStringListField("exclude_fields").
			Description("List of fields to exclude from conversion (timestamp_field is automatically excluded)").
			Default([]string{}).
			Optional()).
		Field(service.NewStringField("target_data_contract").
			Description("Target data contract for output topics. If empty, uses the input's data contract (e.g., _historian)").
			Default("").
			Optional()).
		Field(service.NewBoolField("preserve_meta").
			Description("Whether to preserve original metadata from source message").
			Default(true)).
		Field(service.NewIntField("max_recursion_depth").
			Description("Maximum recursion depth for flattening nested tag groups (default: 10)").
			Default(10).
			Optional()).
		Field(service.NewIntField("max_tags_per_message").
			Description("Maximum number of tags to extract from a single input message (default: 1000)").
			Default(1000).
			Optional())

	err := service.RegisterBatchProcessor(
		"classic_to_core",
		spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			timestampField, err := conf.FieldString("timestamp_field")
			if err != nil {
				return nil, err
			}

			excludeFields, err := conf.FieldStringList("exclude_fields")
			if err != nil {
				return nil, err
			}

			targetDataContract, _ := conf.FieldString("target_data_contract")

			preserveMeta, err := conf.FieldBool("preserve_meta")
			if err != nil {
				return nil, err
			}

			maxRecursionDepth, err := conf.FieldInt("max_recursion_depth")
			if err != nil {
				return nil, err
			}

			maxTagsPerMessage, err := conf.FieldInt("max_tags_per_message")
			if err != nil {
				return nil, err
			}

			config := ClassicToCoreConfig{
				TimestampField:     timestampField,
				ExcludeFields:      excludeFields,
				TargetDataContract: targetDataContract,
				PreserveMeta:       preserveMeta,
				MaxRecursionDepth:  maxRecursionDepth,
				MaxTagsPerMessage:  maxTagsPerMessage,
			}

			return newClassicToCoreProcessor(config, mgr.Logger(), mgr.Metrics())
		})
	if err != nil {
		panic(err)
	}
}

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

func newClassicToCoreProcessor(config ClassicToCoreConfig, logger *service.Logger, metrics *service.Metrics) (*ClassicToCoreProcessor, error) {
	// Create exclude fields map for fast lookup
	excludeFieldsMap := make(map[string]bool)
	excludeFieldsMap[config.TimestampField] = true // Always exclude timestamp field
	for _, field := range config.ExcludeFields {
		excludeFieldsMap[field] = true
	}

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
		if len(expandedMessages) > p.config.MaxTagsPerMessage {
			p.logger.Errorf("Message produced %d tags, exceeding limit of %d", len(expandedMessages), p.config.MaxTagsPerMessage)
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

// processMessage handles the conversion of a single input message to multiple Core format messages
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
	flattenedTags := p.flattenPayload(payload, "", 0)

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

// parsePayload extracts and validates the JSON payload from the message
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

// validateAndExtractTimestamp validates the timestamp field exists and extracts its value
func (p *ClassicToCoreProcessor) validateAndExtractTimestamp(payload map[string]interface{}) (int64, error) {
	timestampValue, exists := payload[p.config.TimestampField]
	if !exists {
		return 0, fmt.Errorf("timestamp field '%s' not found in payload", p.config.TimestampField)
	}

	timestamp, err := p.extractTimestamp(timestampValue)
	if err != nil {
		return 0, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	return timestamp, nil
}

// validateAndParseTopic extracts and validates the topic from message metadata
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

// TopicComponents represents the parsed parts of a UMH topic
type TopicComponents struct {
	Prefix       string // "umh.v1"
	LocationPath string // "enterprise.site.area"
	DataContract string // "_historian"
	Context      string // Additional context after data contract
}

func (p *ClassicToCoreProcessor) parseClassicTopic(topic string) (*TopicComponents, error) {
	parts := strings.Split(topic, ".")
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid topic structure, expected at least 4 parts: %s", topic)
	}

	// Validate UMH topic prefix
	if len(parts) < 2 || parts[0] != "umh" || parts[1] != "v1" {
		return nil, fmt.Errorf("invalid UMH topic prefix, expected 'umh.v1': %s", topic)
	}

	// Find the data contract (starts with underscore)
	var dataContractIndex = -1
	for i := 2; i < len(parts); i++ { // Start from index 2 (after umh.v1)
		if strings.HasPrefix(parts[i], "_") {
			dataContractIndex = i
			break
		}
	}

	if dataContractIndex == -1 {
		return nil, fmt.Errorf("no data contract found in topic: %s", topic)
	}

	if dataContractIndex == 2 {
		return nil, fmt.Errorf("missing location path in topic: %s", topic)
	}

	// Reconstruct components
	prefix := strings.Join(parts[:2], ".") // umh.v1
	locationPath := strings.Join(parts[2:dataContractIndex], ".")
	dataContract := parts[dataContractIndex]

	var context string
	if len(parts) > dataContractIndex+1 {
		context = strings.Join(parts[dataContractIndex+1:], ".")
	}

	return &TopicComponents{
		Prefix:       prefix,
		LocationPath: locationPath,
		DataContract: dataContract,
		Context:      context,
	}, nil
}

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

	// Preserve original metadata if requested
	if p.config.PreserveMeta {
		_ = originalMsg.MetaWalkMut(func(key string, value any) error {
			if str, ok := value.(string); ok {
				newMsg.MetaSet(key, str)
			}
			return nil
		})
	}

	// Determine target data contract
	targetDataContract := p.config.TargetDataContract
	if targetDataContract == "" {
		// Use the original data contract if not specified
		targetDataContract = topicComponents.DataContract
	}

	// Construct new topic
	newTopic := p.constructCoreTopic(topicComponents, fieldName, targetDataContract)
	newMsg.MetaSet("topic", newTopic)
	newMsg.MetaSet("umh_topic", newTopic)

	// Set Core-specific metadata
	newMsg.MetaSet("location_path", topicComponents.LocationPath)
	newMsg.MetaSet("data_contract", targetDataContract)
	newMsg.MetaSet("tag_name", fieldName)

	// Set virtual path if there was context in the original topic
	if topicComponents.Context != "" {
		newMsg.MetaSet("virtual_path", topicComponents.Context)
	}

	return newMsg, nil
}

func (p *ClassicToCoreProcessor) constructCoreTopic(components *TopicComponents, fieldName string, targetDataContract string) string {
	parts := []string{
		components.Prefix,
		components.LocationPath,
		targetDataContract,
	}

	if components.Context != "" {
		parts = append(parts, components.Context)
	}

	parts = append(parts, fieldName)

	return strings.Join(parts, ".")
}

// flattenPayload recursively flattens nested objects using . as separator
// This creates intuitive dot-notation paths for nested tag groups
func (p *ClassicToCoreProcessor) flattenPayload(payload map[string]interface{}, prefix string, depth int) map[string]interface{} {
	result := make(map[string]interface{})

	// Check recursion depth limit
	if depth >= p.config.MaxRecursionDepth {
		p.recursionLimitHit.Incr(1)
		p.logger.Warnf("Recursion depth limit (%d) reached at prefix '%s', stopping further flattening", p.config.MaxRecursionDepth, prefix)
		return result
	}

	for key, value := range payload {
		// Skip the timestamp field as it's handled separately
		if key == p.config.TimestampField {
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
			nestedResults := p.flattenPayload(nestedMap, fullKey, depth+1)
			for nestedKey, nestedValue := range nestedResults {
				result[nestedKey] = nestedValue
			}
		} else {
			// Regular tag - add to result
			result[fullKey] = value
		}
	}

	return result
}

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

func (p *ClassicToCoreProcessor) Close(ctx context.Context) error {
	return nil
}
