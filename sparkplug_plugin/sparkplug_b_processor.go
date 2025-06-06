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

package sparkplug_plugin

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func init() {
	spec := service.NewConfigSpec().
		Version("1.0.0").
		Summary("Decodes Sparkplug B protobuf payloads and resolves metric aliases using BIRTH packets").
		Description(`The Sparkplug B decoder processes MQTT messages containing Sparkplug B protobuf payloads. 
It maintains an in-memory cache of metric name aliases from BIRTH messages and uses these to resolve 
aliases in DATA messages, enriching them with human-readable metric names.

This processor is essential for working with Sparkplug B data in the UMH data flow, enabling the 
transformation of compact, alias-based DATA messages into fully enriched JSON messages with metric names.

The processor expects messages to have the MQTT topic in metadata (typically added automatically by 
the MQTT input) following the Sparkplug topic structure: spBv1.0/<Group>/<MsgType>/<EdgeNode>[/<Device>]

Key features:
- Maintains thread-safe alias cache for metric name resolution
- Handles NBIRTH/DBIRTH messages to populate alias mappings
- Enriches NDATA/DDATA messages with resolved metric names  
- Automatically splits multi-metric messages into individual metric messages
- Automatically extracts and sets metadata for easy tag_processor integration
- Outputs clean JSON representation of Sparkplug payloads
- Gracefully handles malformed payloads without stopping the pipeline

Message flow:
1. BIRTH messages: Update alias cache and optionally pass through
2. DATA messages: Resolve aliases, split into individual metrics, and output enriched messages
3. Other message types: Optionally pass through or drop

The processor is stateful and maintains alias mappings in memory per device key 
(derived from Group/EdgeNode/Device in the MQTT topic).`).
		Field(service.NewBoolField("drop_birth_messages").
			Description("Whether to drop BIRTH messages after processing them for alias extraction. " +
				"When false (default), BIRTH messages are passed through as JSON. " +
				"When true, only their alias information is cached and the messages are dropped.").
			Default(false).
			Optional()).
		Field(service.NewBoolField("strict_topic_validation").
			Description("Whether to strictly validate Sparkplug topic format. " +
				"When true, messages with invalid topic formats are dropped. " +
				"When false (default), messages with invalid topics are passed through unchanged.").
			Default(false).
			Optional()).
		Field(service.NewStringField("cache_ttl").
			Description("Time-to-live for alias cache entries (e.g., '1h', '30m'). " +
				"Empty string (default) means no expiration. This helps prevent memory leaks " +
				"when devices restart with new aliases.").
			Default("").
			Optional()).
		Field(service.NewBoolField("auto_split_metrics").
			Description("Whether to automatically split multi-metric messages into individual metric messages. " +
				"When true (default), each metric becomes a separate message with enriched metadata. " +
				"When false, the original message structure is preserved.").
			Default(true).
			Optional()).
		Field(service.NewBoolField("data_messages_only").
			Description("Whether to only process DATA messages (NDATA/DDATA) and drop other message types. " +
				"When true (default), only DATA messages are processed for easier UNS integration. " +
				"When false, all message types are processed according to other configuration options.").
			Default(true).
			Optional()).
		Field(service.NewBoolField("auto_extract_values").
			Description("Whether to automatically extract Sparkplug values and set them as the message payload. " +
				"When true (default), the actual metric value is extracted and set as payload.value. " +
				"When false, the full metric object is preserved in the payload.").
			Default(true).
			Optional())

	err := service.RegisterProcessor(
		"sparkplug_b_decode",
		spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			dropBirthMessages, _ := conf.FieldBool("drop_birth_messages")
			strictTopicValidation, _ := conf.FieldBool("strict_topic_validation")
			cacheTTL, _ := conf.FieldString("cache_ttl")
			autoSplitMetrics, _ := conf.FieldBool("auto_split_metrics")
			dataMessagesOnly, _ := conf.FieldBool("data_messages_only")
			autoExtractValues, _ := conf.FieldBool("auto_extract_values")

			return newSparkplugProcessor(
				dropBirthMessages,
				strictTopicValidation,
				cacheTTL,
				autoSplitMetrics,
				dataMessagesOnly,
				autoExtractValues,
				mgr.Logger(),
				mgr.Metrics(),
			)
		})
	if err != nil {
		panic(err)
	}
}

type sparkplugProcessor struct {
	dropBirthMessages     bool
	strictTopicValidation bool
	cacheTTL              string
	autoSplitMetrics      bool
	dataMessagesOnly      bool
	autoExtractValues     bool
	logger                *service.Logger
	aliasCache            map[string]map[uint64]string // deviceKey → (alias → metric name)
	mu                    sync.RWMutex

	// Metrics
	messagesProcessed  *service.MetricCounter
	messagesDropped    *service.MetricCounter
	messagesErrored    *service.MetricCounter
	birthMessagesCache *service.MetricCounter
	aliasResolutions   *service.MetricCounter
	topicParseErrors   *service.MetricCounter
}

func newSparkplugProcessor(
	dropBirthMessages, strictTopicValidation bool,
	cacheTTL string,
	autoSplitMetrics, dataMessagesOnly, autoExtractValues bool,
	logger *service.Logger,
	metrics *service.Metrics,
) (*sparkplugProcessor, error) {
	return &sparkplugProcessor{
		dropBirthMessages:     dropBirthMessages,
		strictTopicValidation: strictTopicValidation,
		cacheTTL:              cacheTTL,
		autoSplitMetrics:      autoSplitMetrics,
		dataMessagesOnly:      dataMessagesOnly,
		autoExtractValues:     autoExtractValues,
		logger:                logger,
		aliasCache:            make(map[string]map[uint64]string),
		messagesProcessed:     metrics.NewCounter("messages_processed"),
		messagesDropped:       metrics.NewCounter("messages_dropped"),
		messagesErrored:       metrics.NewCounter("messages_errored"),
		birthMessagesCache:    metrics.NewCounter("birth_messages_cached"),
		aliasResolutions:      metrics.NewCounter("alias_resolutions"),
		topicParseErrors:      metrics.NewCounter("topic_parse_errors"),
	}, nil
}

func (s *sparkplugProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	// Get the MQTT topic from metadata
	topic, exists := m.MetaGet("mqtt_topic")
	if !exists {
		if s.strictTopicValidation {
			s.logger.Debug("Message missing mqtt_topic metadata, dropping message")
			s.messagesDropped.Incr(1)
			return nil, nil
		}
		s.logger.Debug("Message missing mqtt_topic metadata, passing through unchanged")
		s.messagesProcessed.Incr(1)
		return service.MessageBatch{m}, nil
	}

	// Parse the topic to determine message type and device key
	msgType, deviceKey, topicInfo := s.parseSparkplugTopicDetailed(topic)
	if msgType == "" {
		if s.strictTopicValidation {
			s.logger.Debugf("Invalid Sparkplug topic format: %s, dropping message", topic)
			s.messagesDropped.Incr(1)
			s.topicParseErrors.Incr(1)
			return nil, nil
		}
		s.logger.Debugf("Invalid Sparkplug topic format: %s, passing through unchanged", topic)
		s.messagesProcessed.Incr(1)
		return service.MessageBatch{m}, nil
	}

	// Unmarshal the Sparkplug payload
	var payload sproto.Payload
	msgBytes, err := m.AsBytes()
	if err != nil {
		s.logger.Errorf("Failed to get message bytes from topic %s: %v", topic, err)
		s.messagesErrored.Incr(1)
		s.messagesDropped.Incr(1)
		return nil, nil
	}
	if err := proto.Unmarshal(msgBytes, &payload); err != nil {
		s.logger.Errorf("Failed to unmarshal Sparkplug payload from topic %s: %v", topic, err)
		s.messagesErrored.Incr(1)
		// Drop malformed messages to prevent pipeline backup
		s.messagesDropped.Incr(1)
		return nil, nil
	}

	// Process based on message type
	isBirthMessage := strings.Contains(msgType, "BIRTH")
	isDataMessage := strings.Contains(msgType, "DATA")

	// Filter message types if data_messages_only is enabled
	if s.dataMessagesOnly && !isDataMessage {
		if isBirthMessage {
			// Still cache aliases from BIRTH messages even if we drop them
			aliasCount := s.cacheAliases(deviceKey, payload.Metrics)
			if aliasCount > 0 {
				s.logger.Debugf("Cached %d aliases from %s message for device %s", aliasCount, msgType, deviceKey)
				s.birthMessagesCache.Incr(1)
			}
		}
		s.messagesDropped.Incr(1)
		return nil, nil
	}

	if isBirthMessage {
		// Cache aliases from BIRTH message
		aliasCount := s.cacheAliases(deviceKey, payload.Metrics)
		if aliasCount > 0 {
			s.logger.Debugf("Cached %d aliases from %s message for device %s", aliasCount, msgType, deviceKey)
			s.birthMessagesCache.Incr(1)
		}

		// Drop BIRTH message if configured to do so
		if s.dropBirthMessages {
			s.messagesDropped.Incr(1)
			return nil, nil
		}
	} else if isDataMessage {
		// Resolve aliases in DATA message
		resolutionCount := s.resolveAliases(deviceKey, payload.Metrics)
		if resolutionCount > 0 {
			s.logger.Debugf("Resolved %d aliases in %s message for device %s", resolutionCount, msgType, deviceKey)
			s.aliasResolutions.Incr(1)
		}
	}

	// Handle auto-split metrics for easier processing
	if s.autoSplitMetrics && len(payload.Metrics) > 0 {
		return s.createSplitMessages(m, &payload, msgType, deviceKey, topicInfo)
	}

	// Legacy mode: return single message with full payload
	return s.createSingleMessage(m, &payload, msgType, deviceKey, topicInfo)
}

// cacheAliases extracts and stores alias → name mappings from BIRTH message metrics
func (s *sparkplugProcessor) cacheAliases(deviceKey string, metrics []*sproto.Payload_Metric) int {
	if deviceKey == "" || len(metrics) == 0 {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	aliasMap, exists := s.aliasCache[deviceKey]
	if !exists {
		aliasMap = make(map[uint64]string)
		s.aliasCache[deviceKey] = aliasMap
	}

	count := 0
	for _, metric := range metrics {
		if metric == nil {
			continue
		}
		// Store alias mapping if both alias and name are present
		if metric.Alias != nil && *metric.Alias != 0 && metric.Name != nil && *metric.Name != "" {
			aliasMap[*metric.Alias] = *metric.Name
			count++
		}
	}

	return count
}

// resolveAliases enriches DATA message metrics by replacing aliases with cached names
func (s *sparkplugProcessor) resolveAliases(deviceKey string, metrics []*sproto.Payload_Metric) int {
	if deviceKey == "" || len(metrics) == 0 {
		return 0
	}

	s.mu.RLock()
	aliasMap, exists := s.aliasCache[deviceKey]
	s.mu.RUnlock()

	if !exists || len(aliasMap) == 0 {
		return 0
	}

	count := 0
	for _, metric := range metrics {
		if metric == nil {
			continue
		}
		// If metric has an alias but no name, try to resolve it
		if metric.Alias != nil && *metric.Alias != 0 && (metric.Name == nil || *metric.Name == "") {
			if name, found := aliasMap[*metric.Alias]; found {
				metric.Name = &name
				count++
			}
		}
	}

	return count
}

// parseSparkplugTopic parses a Sparkplug topic and returns (messageType, deviceKey)
// Topic format: spBv1.0/<Group>/<MsgType>/<EdgeNode>[/<Device>]
func (s *sparkplugProcessor) parseSparkplugTopic(topic string) (string, string) {
	if topic == "" {
		return "", ""
	}

	parts := strings.Split(topic, "/")
	if len(parts) < 4 {
		return "", ""
	}

	// Validate Sparkplug namespace
	if parts[0] != "spBv1.0" {
		return "", ""
	}

	msgType := parts[2]
	group := parts[1]
	edgeNode := parts[3]

	// Construct device key for cache lookup
	deviceKey := fmt.Sprintf("%s/%s", group, edgeNode)
	if len(parts) > 4 && parts[4] != "" {
		// Include device ID if present
		deviceKey = fmt.Sprintf("%s/%s", deviceKey, parts[4])
	}

	return msgType, deviceKey
}

func (s *sparkplugProcessor) Close(ctx context.Context) error {
	// Clear the alias cache on shutdown
	s.mu.Lock()
	s.aliasCache = make(map[string]map[uint64]string)
	s.mu.Unlock()

	s.logger.Debug("Sparkplug B processor closed and alias cache cleared")
	return nil
}

// TopicInfo contains parsed Sparkplug topic information
type TopicInfo struct {
	Group    string
	EdgeNode string
	Device   string
}

// parseSparkplugTopicDetailed parses a Sparkplug topic and returns (messageType, deviceKey, topicInfo)
func (s *sparkplugProcessor) parseSparkplugTopicDetailed(topic string) (string, string, *TopicInfo) {
	if topic == "" {
		return "", "", nil
	}

	parts := strings.Split(topic, "/")
	if len(parts) < 4 {
		return "", "", nil
	}

	// Validate Sparkplug namespace
	if parts[0] != "spBv1.0" {
		return "", "", nil
	}

	msgType := parts[2]
	group := parts[1]
	edgeNode := parts[3]
	device := ""
	if len(parts) > 4 {
		device = parts[4]
	}

	// Construct device key for cache lookup
	deviceKey := fmt.Sprintf("%s/%s", group, edgeNode)
	if device != "" {
		deviceKey = fmt.Sprintf("%s/%s", deviceKey, device)
	}

	topicInfo := &TopicInfo{
		Group:    group,
		EdgeNode: edgeNode,
		Device:   device,
	}

	return msgType, deviceKey, topicInfo
}

// createSplitMessages creates individual messages for each metric (auto-split mode)
func (s *sparkplugProcessor) createSplitMessages(originalMsg *service.Message, payload *sproto.Payload, msgType, deviceKey string, topicInfo *TopicInfo) (service.MessageBatch, error) {
	var batch service.MessageBatch

	for _, metric := range payload.Metrics {
		if metric == nil {
			continue
		}

		// Create a new message for this metric
		newMsg := originalMsg.Copy()

		// Set enriched metadata
		newMsg.MetaSet("sparkplug_msg_type", msgType)
		newMsg.MetaSet("sparkplug_device_key", deviceKey)
		newMsg.MetaSet("group_id", topicInfo.Group)
		newMsg.MetaSet("edge_node_id", topicInfo.EdgeNode)
		if topicInfo.Device != "" {
			newMsg.MetaSet("device_id", topicInfo.Device)
		}

		// Set tag_name metadata
		tagName := "unknown_metric"
		if metric.Name != nil && *metric.Name != "" {
			tagName = *metric.Name
		} else if metric.Alias != nil {
			tagName = fmt.Sprintf("alias_%d", *metric.Alias)
		}
		newMsg.MetaSet("tag_name", tagName)

		// Set message payload
		if s.autoExtractValues {
			// Extract just the value and create simple payload
			payload := s.extractMetricValue(metric)
			jsonBytes, err := protojson.MarshalOptions{
				UseProtoNames:   true,
				EmitUnpopulated: false,
			}.Marshal(payload)
			if err != nil {
				s.logger.Errorf("Failed to marshal extracted value to JSON: %v", err)
				s.messagesErrored.Incr(1)
				continue
			}
			newMsg.SetBytes(jsonBytes)
		} else {
			// Include full metric object
			jsonBytes, err := protojson.MarshalOptions{
				UseProtoNames:   true,
				EmitUnpopulated: false,
			}.Marshal(metric)
			if err != nil {
				s.logger.Errorf("Failed to marshal metric to JSON: %v", err)
				s.messagesErrored.Incr(1)
				continue
			}
			newMsg.SetBytes(jsonBytes)
		}

		batch = append(batch, newMsg)
	}

	s.messagesProcessed.Incr(1)
	return batch, nil
}

// createSingleMessage creates a single message with the full payload (legacy mode)
func (s *sparkplugProcessor) createSingleMessage(originalMsg *service.Message, payload *sproto.Payload, msgType, deviceKey string, topicInfo *TopicInfo) (service.MessageBatch, error) {
	// Convert to JSON
	jsonBytes, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}.Marshal(payload)
	if err != nil {
		s.logger.Errorf("Failed to marshal payload to JSON: %v", err)
		s.messagesErrored.Incr(1)
		s.messagesDropped.Incr(1)
		return nil, nil
	}

	// Set the JSON as message body
	originalMsg.SetBytes(jsonBytes)

	// Add metadata about the processing
	originalMsg.MetaSet("sparkplug_msg_type", msgType)
	originalMsg.MetaSet("sparkplug_device_key", deviceKey)
	originalMsg.MetaSet("group_id", topicInfo.Group)
	originalMsg.MetaSet("edge_node_id", topicInfo.EdgeNode)
	if topicInfo.Device != "" {
		originalMsg.MetaSet("device_id", topicInfo.Device)
	}

	s.messagesProcessed.Incr(1)
	return service.MessageBatch{originalMsg}, nil
}

// extractMetricValue extracts the value from a Sparkplug metric and returns a simple payload
func (s *sparkplugProcessor) extractMetricValue(metric *sproto.Payload_Metric) *sproto.Payload {
	extractedPayload := &sproto.Payload{}

	// Determine quality based on datatype and null status
	quality := "GOOD"
	if metric.IsNull != nil && *metric.IsNull {
		quality = "BAD"
	} else if metric.GetValue() == nil {
		quality = "UNCERTAIN"
	}

	// Create a simplified metric with just the value, preserving the original structure
	extractedMetric := &sproto.Payload_Metric{
		Name:     metric.Name,
		Alias:    metric.Alias,
		Datatype: metric.Datatype,
		IsNull:   metric.IsNull,
		Value:    metric.Value, // Preserve the original value oneof
	}

	// Add quality metadata as a property if needed
	qualityName := "quality"
	qualityMetric := &sproto.Payload_Metric{
		Name: &qualityName,
		Value: &sproto.Payload_Metric_StringValue{
			StringValue: quality,
		},
		Datatype: &[]uint32{12}[0], // String datatype
	}

	extractedPayload.Metrics = []*sproto.Payload_Metric{extractedMetric, qualityMetric}

	return extractedPayload
}
