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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/protobuf/encoding/protojson"

	sparkplugb "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

// TopicInfo contains parsed Sparkplug topic information extracted from MQTT topics.
// This structure represents the hierarchical addressing scheme used in Sparkplug B
// for organizing edge nodes and devices within groups.
//
// Topic format: spBv1.0/<Group>/<MsgType>/<EdgeNode>[/<Device>]
type TopicInfo struct {
	Group    string // Sparkplug Group ID (e.g., "FactoryA")
	EdgeNode string // Edge Node ID within the group (e.g., "Line1")
	Device   string // Device ID under the edge node (empty for node-level messages)
}

// NodeKey returns the node-level key (group/edgeNode) for sequence tracking.
// Per Sparkplug B spec, sequence numbers are tracked at NODE scope, not device scope.
// All message types from a node (NBIRTH, NDATA, DBIRTH, DDATA) share one sequence counter.
func (ti *TopicInfo) NodeKey() string {
	if ti == nil || ti.Group == "" || ti.EdgeNode == "" {
		return ""
	}
	return ti.Group + "/" + ti.EdgeNode
}

// NodeState tracks the state of a Sparkplug node/device for sequence management.
type NodeState struct {
	LastSeen time.Time
	LastSeq  uint8
	BdSeq    uint64
	IsOnline bool
}

// AliasCache manages metric name to alias mappings for Sparkplug B optimization.
type AliasCache struct {
	cache map[string]map[uint64]string // deviceKey -> (alias -> metric name)
	mu    sync.RWMutex
}

// NewAliasCache creates a new thread-safe alias cache.
func NewAliasCache() *AliasCache {
	return &AliasCache{
		cache: make(map[string]map[uint64]string),
	}
}

// CacheAliases extracts and stores alias → name mappings from BIRTH message metrics.
// This is fundamental to Sparkplug B operation: BIRTH messages establish the "vocabulary"
// of aliases that can be used in subsequent DATA messages.
//
// Device-Level PARRIS architecture: DBIRTH messages define all metric aliases for a device,
// enabling efficient DDATA transmission using numeric aliases instead of full metric names.
func (ac *AliasCache) CacheAliases(deviceKey string, metrics []*sparkplugb.Payload_Metric) int {
	if deviceKey == "" || len(metrics) == 0 {
		return 0
	}

	ac.mu.Lock()
	defer ac.mu.Unlock()

	aliasMap, exists := ac.cache[deviceKey]
	if !exists {
		aliasMap = make(map[uint64]string)
		ac.cache[deviceKey] = aliasMap
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

// ResolveAliases enriches DATA message metrics by replacing aliases with cached names.
// This completes the Sparkplug B efficiency cycle: while BIRTH messages define aliases
// for metric names, DATA messages use those aliases for compact transmission.
//
// Device-Level PARRIS: DDATA messages contain numeric aliases (e.g., alias=1) which
// this function resolves back to meaningful names (e.g., "temperature:value") using
// the cached DBIRTH certificate vocabulary.
func (ac *AliasCache) ResolveAliases(deviceKey string, metrics []*sparkplugb.Payload_Metric) int {
	if deviceKey == "" || len(metrics) == 0 {
		return 0
	}

	ac.mu.RLock()
	aliasMap, exists := ac.cache[deviceKey]
	if !exists || len(aliasMap) == 0 {
		ac.mu.RUnlock()
		return 0
	}

	// Create a copy of the alias map to avoid holding the lock during metric updates
	aliasMapCopy := make(map[uint64]string, len(aliasMap))
	for k, v := range aliasMap {
		aliasMapCopy[k] = v
	}
	ac.mu.RUnlock()

	count := 0
	for _, metric := range metrics {
		if metric == nil {
			continue
		}
		// If metric has an alias but no name, try to resolve it
		if metric.Alias != nil && *metric.Alias != 0 && (metric.Name == nil || *metric.Name == "") {
			if name, found := aliasMapCopy[*metric.Alias]; found {
				metric.Name = &name
				count++
			}
		}
	}

	return count
}

// Clear removes all cached aliases.
func (ac *AliasCache) Clear() {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.cache = make(map[string]map[uint64]string)
}

// SpbDeviceKey creates a unified device key for Sparkplug B
// Format: <group>/<edge>/<device> – device == "" for node-level
func SpbDeviceKey(gid string, nid string, did string) string {
	if did == "" {
		return gid + "/" + nid
	}
	return gid + "/" + nid + "/" + did
}

// TopicParser provides functions for parsing and constructing Sparkplug B topics.
type TopicParser struct{}

// NewTopicParser creates a new topic parser instance.
func NewTopicParser() *TopicParser {
	return &TopicParser{}
}

// ParseSparkplugTopic parses a Sparkplug topic and returns (messageType, deviceKey).
func (tp *TopicParser) ParseSparkplugTopic(topic string) (string, string) {
	msgType, deviceKey, _ := tp.ParseSparkplugTopicDetailed(topic)
	return msgType, deviceKey
}

// ParseSparkplugTopicDetailed parses a Sparkplug topic and returns (messageType, deviceKey, topicInfo).
func (tp *TopicParser) ParseSparkplugTopicDetailed(topic string) (string, string, *TopicInfo) {
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

// BuildTopic constructs a Sparkplug topic from components.
func (tp *TopicParser) BuildTopic(groupID string, msgType string, edgeNodeID string, deviceID string) string {
	if deviceID == "" {
		return fmt.Sprintf("spBv1.0/%s/%s/%s", groupID, msgType, edgeNodeID)
	}
	return fmt.Sprintf("spBv1.0/%s/%s/%s/%s", groupID, msgType, edgeNodeID, deviceID)
}

// IsValidMessageType checks if a message type is valid Sparkplug B.
func (tp *TopicParser) IsValidMessageType(msgType string) bool {
	validTypes := []string{"NBIRTH", "NDATA", "NDEATH", "DBIRTH", "DDATA", "DDEATH", "NCMD", "DCMD", "STATE"}
	for _, valid := range validTypes {
		if msgType == valid {
			return true
		}
	}
	return false
}

// IsBirthMessage checks if the message type is a BIRTH message.
func (tp *TopicParser) IsBirthMessage(msgType string) bool {
	return strings.Contains(msgType, "BIRTH")
}

// IsDataMessage checks if the message type is a DATA message.
func (tp *TopicParser) IsDataMessage(msgType string) bool {
	return strings.Contains(msgType, "DATA")
}

// IsDeathMessage checks if the message type is a DEATH message.
func (tp *TopicParser) IsDeathMessage(msgType string) bool {
	return strings.Contains(msgType, "DEATH")
}

// IsCommandMessage checks if the message type is a CMD message.
func (tp *TopicParser) IsCommandMessage(msgType string) bool {
	return strings.Contains(msgType, "CMD")
}

// IsNodeMessage checks if the message type is node-level (N*).
func (tp *TopicParser) IsNodeMessage(msgType string) bool {
	return strings.HasPrefix(msgType, "N")
}

// IsDeviceMessage checks if the message type is device-level (D*).
func (tp *TopicParser) IsDeviceMessage(msgType string) bool {
	return strings.HasPrefix(msgType, "D")
}

// MessageProcessor provides message transformation utilities for Sparkplug B.
type MessageProcessor struct {
	logger *service.Logger
}

// NewMessageProcessor creates a new message processor.
func NewMessageProcessor(logger *service.Logger) *MessageProcessor {
	return &MessageProcessor{logger: logger}
}

// CreateSplitMessages creates individual messages for each metric.
func (mp *MessageProcessor) CreateSplitMessages(originalMsg *service.Message, payload *sparkplugb.Payload, msgType string, deviceKey string, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	var batch service.MessageBatch

	for _, metric := range payload.Metrics {
		if metric == nil {
			continue
		}

		// Create a new message for this metric
		newMsg := originalMsg.Copy()

		// Set enriched metadata
		mp.setCommonMetadata(newMsg, msgType, deviceKey, topicInfo, originalTopic)

		// Set tag_name metadata
		tagName := "unknown_metric"
		if metric.Name != nil && *metric.Name != "" {
			tagName = *metric.Name
		} else if metric.Alias != nil {
			tagName = fmt.Sprintf("alias_%d", *metric.Alias)
		}
		newMsg.MetaSet("tag_name", tagName)

		// Set message payload with extracted value
		jsonBytes := mp.extractMetricValue(metric)
		newMsg.SetBytes(jsonBytes)

		batch = append(batch, newMsg)
	}

	return batch
}

// CreateSingleMessage creates a single message with the full payload.
func (mp *MessageProcessor) CreateSingleMessage(originalMsg *service.Message, payload *sparkplugb.Payload, msgType string, deviceKey string, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	// Convert to JSON
	jsonBytes, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}.Marshal(payload)
	if err != nil {
		mp.logger.Errorf("Failed to marshal payload to JSON: %v", err)
		return nil
	}

	// Set the JSON as message body
	originalMsg.SetBytes(jsonBytes)

	// Add metadata about the processing
	mp.setCommonMetadata(originalMsg, msgType, deviceKey, topicInfo, originalTopic)

	return service.MessageBatch{originalMsg}
}

// setCommonMetadata sets common metadata fields for Sparkplug messages.
func (mp *MessageProcessor) setCommonMetadata(msg *service.Message, msgType string, deviceKey string, topicInfo *TopicInfo, originalTopic string) {
	msg.MetaSet("sparkplug_msg_type", msgType)
	msg.MetaSet("sparkplug_device_key", deviceKey)
	msg.MetaSet("group_id", topicInfo.Group)
	msg.MetaSet("edge_node_id", topicInfo.EdgeNode)
	if topicInfo.Device != "" {
		msg.MetaSet("device_id", topicInfo.Device)
	}
	if originalTopic != "" {
		msg.MetaSet("mqtt_topic", originalTopic)
	}
}

// extractMetricValue extracts the value from a Sparkplug metric and returns JSON bytes.
func (mp *MessageProcessor) extractMetricValue(metric *sparkplugb.Payload_Metric) []byte {
	// Create a simple JSON structure with the metric value
	result := make(map[string]interface{})

	// Extract the actual value based on the metric's Value oneof
	if metric.IsNull != nil && *metric.IsNull {
		result["value"] = nil
		result["quality"] = "BAD"
	} else if metric.Value != nil {
		switch v := metric.Value.(type) {
		case *sparkplugb.Payload_Metric_IntValue:
			result["value"] = v.IntValue
		case *sparkplugb.Payload_Metric_LongValue:
			result["value"] = v.LongValue
		case *sparkplugb.Payload_Metric_FloatValue:
			result["value"] = v.FloatValue
		case *sparkplugb.Payload_Metric_DoubleValue:
			result["value"] = v.DoubleValue
		case *sparkplugb.Payload_Metric_BooleanValue:
			result["value"] = v.BooleanValue
		case *sparkplugb.Payload_Metric_StringValue:
			result["value"] = v.StringValue
		default:
			result["value"] = nil
			result["quality"] = "UNCERTAIN"
		}

		if result["quality"] == nil {
			result["quality"] = "GOOD"
		}
	} else {
		result["value"] = nil
		result["quality"] = "UNCERTAIN"
	}

	// Note: Timestamp is at payload level in Sparkplug B, not metric level
	// Individual metrics don't have timestamps

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		mp.logger.Errorf("Failed to marshal metric value to JSON: %v", err)
		return []byte(`{"value": null, "quality": "BAD"}`)
	}

	return jsonBytes
}

// SequenceManager manages Sparkplug B sequence numbers.
type SequenceManager struct {
	counter uint8
	mu      sync.Mutex
}

// NewSequenceManager creates a new sequence manager.
func NewSequenceManager() *SequenceManager {
	return &SequenceManager{}
}

// NextSequence returns the next sequence number and increments the counter.
func (sm *SequenceManager) NextSequence() uint8 {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	current := sm.counter
	sm.counter++ // Automatically wraps at 256 due to uint8 overflow

	return current
}

// GetCurrent returns the current sequence number without incrementing.
func (sm *SequenceManager) GetCurrent() uint8 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.counter
}

// SetSequence sets the sequence counter to a specific value.
func (sm *SequenceManager) SetSequence(seq uint8) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.counter = seq
}

// IsSequenceValid checks if a received sequence number is the expected next one.
func (sm *SequenceManager) IsSequenceValid(expected uint8, received uint8) bool {
	// Handle wrap-around case - uint8 automatically wraps at 255
	expectedNext := expected + 1
	return received == expectedNext
}

// TypeConverter provides utilities for converting between Go types and Sparkplug B data types.
type TypeConverter struct{}

// NewTypeConverter creates a new type converter.
func NewTypeConverter() *TypeConverter {
	return &TypeConverter{}
}

// GetSparkplugDataType returns the Sparkplug B data type constant for a type string.
func (tc *TypeConverter) GetSparkplugDataType(typeStr string) *uint32 {
	typeMap := map[string]uint32{
		"int8":    1,
		"int16":   2,
		"int32":   3,
		"int64":   4,
		"uint8":   5,
		"uint16":  6,
		"uint32":  7,
		"uint64":  8,
		"float":   9,
		"double":  10,
		"boolean": 11,
		"string":  12,
	}

	if dt, exists := typeMap[typeStr]; exists {
		return &dt
	}
	// Default to double
	defaultType := uint32(10)
	return &defaultType
}

// SetMetricValue sets the appropriate value field in a Sparkplug metric based on the type.
func (tc *TypeConverter) SetMetricValue(metric *sparkplugb.Payload_Metric, value interface{}, metricType string) {
	if value == nil {
		isNull := true
		metric.IsNull = &isNull
		return
	}

	switch metricType {
	case "int8", "int16", "int32":
		if v, ok := tc.ConvertToInt32(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_IntValue{IntValue: v}
		}
	case "int64":
		if v, ok := tc.convertToInt64(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_LongValue{LongValue: v}
		}
	case "uint8", "uint16", "uint32":
		if v, ok := tc.ConvertToInt32(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_IntValue{IntValue: v}
		}
	case "uint64":
		if v, ok := tc.convertToInt64(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_LongValue{LongValue: v}
		}
	case "float":
		if v, ok := tc.convertToFloat32(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_FloatValue{FloatValue: v}
		}
	case "double":
		if v, ok := tc.convertToFloat64(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: v}
		}
	case "boolean":
		if v, ok := tc.ConvertToBool(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_BooleanValue{BooleanValue: v}
		}
	case "string":
		if v, ok := tc.convertToString(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_StringValue{StringValue: v}
		}
	default:
		// Default to double
		if v, ok := tc.convertToFloat64(value); ok {
			metric.Value = &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: v}
		}
	}
}

// ConvertToInt32 converts a value to uint32 for Sparkplug int types.
func (tc *TypeConverter) ConvertToInt32(value interface{}) (uint32, bool) {
	switch v := value.(type) {
	case int:
		if v < 0 {
			return 0, false
		}
		return uint32(v), true
	case int32:
		if v < 0 {
			return 0, false
		}
		return uint32(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint32(v), true
	case uint32:
		return v, true
	case uint64:
		return uint32(v), true
	case float32:
		if v < 0 {
			return 0, false
		}
		return uint32(v), true
	case float64:
		if v < 0 {
			return 0, false
		}
		return uint32(v), true
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 32); err == nil {
			if parsed < 0 {
				return 0, false
			}
			return uint32(parsed), true
		}
	}
	return 0, false
}

// convertToInt64 converts a value to uint64 for Sparkplug long types.
func (tc *TypeConverter) convertToInt64(value interface{}) (uint64, bool) {
	switch v := value.(type) {
	case int:
		if v < 0 {
			return 0, false // Consistent with ConvertToInt32 behavior
		}
		return uint64(v), true
	case int32:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case uint32:
		return uint64(v), true
	case uint64:
		return v, true
	case float32:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case float64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			if parsed < 0 {
				return 0, false
			}
			return uint64(parsed), true
		}
	}
	return 0, false
}

// convertToFloat32 converts a value to float32.
func (tc *TypeConverter) convertToFloat32(value interface{}) (float32, bool) {
	switch v := value.(type) {
	case int:
		return float32(v), true
	case int32:
		return float32(v), true
	case int64:
		return float32(v), true
	case uint32:
		return float32(v), true
	case uint64:
		return float32(v), true
	case float32:
		return v, true
	case float64:
		return float32(v), true
	case string:
		if parsed, err := strconv.ParseFloat(v, 32); err == nil {
			return float32(parsed), true
		}
	}
	return 0, false
}

// convertToFloat64 converts a value to float64.
func (tc *TypeConverter) convertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

// ConvertToBool converts a value to bool.
func (tc *TypeConverter) ConvertToBool(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case int:
		return v != 0, true
	case int32:
		return v != 0, true
	case int64:
		return v != 0, true
	case uint32:
		return v != 0, true
	case uint64:
		return v != 0, true
	case float32:
		return v != 0, true
	case float64:
		return v != 0, true
	case string:
		if parsed, err := strconv.ParseBool(v); err == nil {
			return parsed, true
		}
		// Also accept numeric strings
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed != 0, true
		}
	}
	return false, false
}

// convertToString converts a value to string.
func (tc *TypeConverter) convertToString(value interface{}) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case int:
		return strconv.Itoa(v), true
	case int32:
		return strconv.FormatInt(int64(v), 10), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case uint32:
		return strconv.FormatUint(uint64(v), 10), true
	case uint64:
		return strconv.FormatUint(v, 10), true
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32), true
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), true
	case bool:
		return strconv.FormatBool(v), true
	}
	return "", false
}

// InferMetricType determines the Sparkplug data type based on the Go value type.
func (tc *TypeConverter) InferMetricType(value interface{}) string {
	switch v := value.(type) {
	case bool:
		return "boolean"
	case int, int8, int16, int32:
		return "int32"
	case int64:
		return "int64"
	case uint, uint8, uint16, uint32:
		return "uint32"
	case uint64:
		return "uint64"
	case float32:
		return "float"
	case float64:
		return "double"
	case string:
		return "string"
	case json.Number:
		// Try to determine if it's integer or float
		if numStr := string(v); strings.Contains(numStr, ".") {
			return "double"
		}
		return "int64"
	default:
		return "string" // Default to string for unknown types
	}
}

// MQTTClientBuilder provides a Benthos-style abstraction for creating MQTT clients.
// This builder wraps Paho MQTT functionality with Benthos patterns for configuration,
// logging, metrics, and resource management.
type MQTTClientBuilder struct {
	logger  *service.Logger
	metrics *MQTTClientMetrics
}

// MQTTClientMetrics holds metrics for MQTT client operations.
type MQTTClientMetrics struct {
	ConnectionAttempts *service.MetricCounter
	ConnectionFailures *service.MetricCounter
	ConnectionsActive  *service.MetricGauge
	MessagesPublished  *service.MetricCounter
	MessagesReceived   *service.MetricCounter
	PublishFailures    *service.MetricCounter
	SubscriptionErrors *service.MetricCounter
}

// MQTTClientConfig holds configuration for MQTT client creation.
type MQTTClientConfig struct {
	BrokerURLs     []string
	ClientID       string
	Username       string
	Password       string
	KeepAlive      time.Duration
	ConnectTimeout time.Duration
	CleanSession   bool

	// Last Will Testament
	WillTopic   string
	WillPayload []byte
	WillQoS     byte
	WillRetain  bool

	// Connection handlers
	OnConnect        func(client mqtt.Client)
	OnConnectionLost func(client mqtt.Client, err error)
	MessageHandler   func(client mqtt.Client, msg mqtt.Message)
}

// NewMQTTClientBuilder creates a new MQTT client builder with Benthos integration.
func NewMQTTClientBuilder(mgr *service.Resources) *MQTTClientBuilder {
	metrics := &MQTTClientMetrics{
		ConnectionAttempts: mgr.Metrics().NewCounter("mqtt_connection_attempts"),
		ConnectionFailures: mgr.Metrics().NewCounter("mqtt_connection_failures"),
		ConnectionsActive:  mgr.Metrics().NewGauge("mqtt_connections_active"),
		MessagesPublished:  mgr.Metrics().NewCounter("mqtt_messages_published"),
		MessagesReceived:   mgr.Metrics().NewCounter("mqtt_messages_received"),
		PublishFailures:    mgr.Metrics().NewCounter("mqtt_publish_failures"),
		SubscriptionErrors: mgr.Metrics().NewCounter("mqtt_subscription_errors"),
	}

	return &MQTTClientBuilder{
		logger:  mgr.Logger(),
		metrics: metrics,
	}
}

// CreateClient creates an MQTT client with Benthos-style configuration and monitoring.
func (mcb *MQTTClientBuilder) CreateClient(config MQTTClientConfig) (mqtt.Client, error) {
	// Add validation at the start
	if config.ClientID == "" {
		return nil, fmt.Errorf("client ID is required")
	}
	if config.KeepAlive <= 0 {
		return nil, fmt.Errorf("keep alive must be positive, got %v", config.KeepAlive)
	}
	if config.ConnectTimeout <= 0 {
		return nil, fmt.Errorf("connect timeout must be positive, got %v", config.ConnectTimeout)
	}

	opts := mqtt.NewClientOptions()

	// Set broker URLs
	if len(config.BrokerURLs) == 0 {
		return nil, fmt.Errorf("at least one broker URL is required")
	}
	for _, url := range config.BrokerURLs {
		opts.AddBroker(url)
	}

	// Set basic connection options
	opts.SetClientID(config.ClientID)
	opts.SetKeepAlive(config.KeepAlive)
	opts.SetConnectTimeout(config.ConnectTimeout)
	opts.SetCleanSession(config.CleanSession)

	// Set authentication if provided
	if config.Username != "" {
		opts.SetUsername(config.Username)
		if config.Password != "" {
			opts.SetPassword(config.Password)
		}
	}

	// Set Last Will Testament if provided
	if config.WillTopic != "" && config.WillPayload != nil {
		opts.SetWill(config.WillTopic, string(config.WillPayload), config.WillQoS, config.WillRetain)
	}

	// Wrap connection handlers with metrics
	if config.OnConnect != nil {
		opts.SetOnConnectHandler(func(client mqtt.Client) {
			mcb.metrics.ConnectionsActive.Set(1)
			mcb.logger.Debug("MQTT client connected")
			config.OnConnect(client)
		})
	}

	if config.OnConnectionLost != nil {
		opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
			mcb.metrics.ConnectionsActive.Set(0)
			mcb.logger.Errorf("MQTT connection lost: %v", err)
			config.OnConnectionLost(client, err)
		})
	}

	// Set default message handler with metrics
	if config.MessageHandler != nil {
		opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
			mcb.metrics.MessagesReceived.Incr(1)
			config.MessageHandler(client, msg)
		})
	}

	client := mqtt.NewClient(opts)
	return client, nil
}

// ConnectWithRetry connects an MQTT client with Benthos-style retry and monitoring.
func (mcb *MQTTClientBuilder) ConnectWithRetry(client mqtt.Client, timeout time.Duration) error {
	mcb.metrics.ConnectionAttempts.Incr(1)
	mcb.logger.Debug("Attempting MQTT connection")

	token := client.Connect()
	if !token.WaitTimeout(timeout) {
		mcb.metrics.ConnectionFailures.Incr(1)
		return fmt.Errorf("MQTT connection timeout after %v", timeout)
	}

	if err := token.Error(); err != nil {
		mcb.metrics.ConnectionFailures.Incr(1)
		return fmt.Errorf("MQTT connection failed: %w", err)
	}

	mcb.logger.Debug("MQTT connection successful")
	return nil
}

// PublishWithMetrics publishes a message with automatic metrics tracking.
func (mcb *MQTTClientBuilder) PublishWithMetrics(client mqtt.Client, topic string, qos byte, retained bool, payload interface{}) error {
	token := client.Publish(topic, qos, retained, payload)
	if token.Wait() && token.Error() != nil {
		mcb.metrics.PublishFailures.Incr(1)
		return token.Error()
	}

	mcb.metrics.MessagesPublished.Incr(1)
	return nil
}

// SubscribeWithMetrics subscribes to topics with automatic metrics tracking.
func (mcb *MQTTClientBuilder) SubscribeWithMetrics(client mqtt.Client, topicFilter string, qos byte, callback mqtt.MessageHandler) error {
	// Wrap the callback with metrics
	wrappedCallback := func(client mqtt.Client, msg mqtt.Message) {
		mcb.metrics.MessagesReceived.Incr(1)
		callback(client, msg)
	}

	token := client.Subscribe(topicFilter, qos, wrappedCallback)
	if token.Wait() && token.Error() != nil {
		mcb.metrics.SubscriptionErrors.Incr(1)
		return token.Error()
	}

	mcb.logger.Debugf("Successfully subscribed to MQTT topic: %s", topicFilter)
	return nil
}
