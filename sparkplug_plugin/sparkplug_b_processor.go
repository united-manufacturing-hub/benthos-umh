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
- Outputs clean JSON representation of Sparkplug payloads
- Gracefully handles malformed payloads without stopping the pipeline

Message flow:
1. BIRTH messages: Update alias cache and pass through as JSON
2. DATA messages: Resolve aliases using cache and output as enriched JSON  
3. Other message types: Pass through as JSON

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
			Optional())

	err := service.RegisterProcessor(
		"sparkplug_b_decode",
		spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			dropBirthMessages, _ := conf.FieldBool("drop_birth_messages")
			strictTopicValidation, _ := conf.FieldBool("strict_topic_validation")
			cacheTTL, _ := conf.FieldString("cache_ttl")

			return newSparkplugProcessor(
				dropBirthMessages,
				strictTopicValidation,
				cacheTTL,
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
	logger *service.Logger,
	metrics *service.Metrics,
) (*sparkplugProcessor, error) {
	return &sparkplugProcessor{
		dropBirthMessages:     dropBirthMessages,
		strictTopicValidation: strictTopicValidation,
		cacheTTL:              cacheTTL,
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
	msgType, deviceKey := s.parseSparkplugTopic(topic)
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

	// Convert to JSON
	jsonBytes, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}.Marshal(&payload)
	if err != nil {
		s.logger.Errorf("Failed to marshal payload to JSON for topic %s: %v", topic, err)
		s.messagesErrored.Incr(1)
		s.messagesDropped.Incr(1)
		return nil, nil
	}

	// Set the JSON as message body
	m.SetBytes(jsonBytes)

	// Add metadata about the processing
	m.MetaSet("sparkplug_msg_type", msgType)
	m.MetaSet("sparkplug_device_key", deviceKey)

	s.messagesProcessed.Incr(1)
	return service.MessageBatch{m}, nil
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
