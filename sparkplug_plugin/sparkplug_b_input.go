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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/proto"
)

func init() {
	inputSpec := service.NewConfigSpec().
		Version("1.0.0").
		Summary("Sparkplug B MQTT input acting as Primary SCADA Host").
		Description(`The Sparkplug B input acts as a Primary SCADA Host, subscribing to Sparkplug MQTT topics 
and managing the complete session lifecycle. It handles BIRTH/DEATH messages, maintains STATE topic 
for primary application coordination, and automatically requests rebirths when needed.

Key features:
- Primary application STATE topic management with LWT
- Automatic subscription to spBv1.0/<Group>/# topics  
- Session state tracking and rebirth coordination
- Sequence number validation and out-of-order detection
- Alias resolution using BIRTH message metadata
- Automatic message splitting for individual metric processing
- Comprehensive metadata extraction for UNS integration

The input connects to an MQTT broker, declares itself as the primary application via STATE topic,
and processes all Sparkplug messages from edge nodes and devices in the specified group.`).
		Field(service.NewStringListField("broker_urls").
			Description("List of MQTT broker URLs to connect to").
			Example([]string{"tcp://localhost:1883", "ssl://broker.hivemq.com:8883"}).
			Default([]string{"tcp://localhost:1883"})).
		Field(service.NewStringField("client_id").
			Description("MQTT client ID for this primary application").
			Default("benthos-sparkplug-host")).
		Field(service.NewStringField("username").
			Description("MQTT username for authentication").
			Default("").
			Optional()).
		Field(service.NewStringField("password").
			Description("MQTT password for authentication").
			Default("").
			Secret().
			Optional()).
		Field(service.NewStringField("group_id").
			Description("Sparkplug Group ID to subscribe to (e.g., 'FactoryA')").
			Example("FactoryA")).
		Field(service.NewStringField("primary_host_id").
			Description("Primary Host ID for STATE topic (defaults to client_id)").
			Default("").
			Optional()).
		Field(service.NewBoolField("split_metrics").
			Description("Whether to split multi-metric messages into individual metric messages").
			Default(true)).
		Field(service.NewBoolField("enable_rebirth_requests").
			Description("Whether to automatically send rebirth requests on sequence gaps").
			Default(true)).
		Field(service.NewIntField("qos").
			Description("QoS level for MQTT subscriptions (0, 1, or 2)").
			Default(1)).
		Field(service.NewDurationField("keep_alive").
			Description("MQTT keep alive interval").
			Default("30s")).
		Field(service.NewDurationField("connect_timeout").
			Description("MQTT connection timeout").
			Default("10s")).
		Field(service.NewBoolField("clean_session").
			Description("MQTT clean session flag").
			Default(true))

	err := service.RegisterBatchInput(
		"sparkplug_input",
		inputSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newSparkplugInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type sparkplugInput struct {
	brokerURLs        []string
	clientID          string
	username          string
	password          string
	groupID           string
	primaryHostID     string
	splitMetrics      bool
	enableRebirthReqs bool
	qos               byte
	keepAlive         time.Duration
	connectTimeout    time.Duration
	cleanSession      bool
	logger            *service.Logger

	// MQTT client and state
	client   mqtt.Client
	messages chan mqttMessage
	mu       sync.RWMutex
	closed   bool

	// Sparkplug state management
	nodeStates map[string]*nodeState        // deviceKey -> state
	aliasCache map[string]map[uint64]string // deviceKey -> (alias -> metric name)
	stateMu    sync.RWMutex

	// Metrics
	messagesReceived  *service.MetricCounter
	birthsProcessed   *service.MetricCounter
	deathsProcessed   *service.MetricCounter
	rebirthsRequested *service.MetricCounter
	sequenceErrors    *service.MetricCounter
	aliasResolutions  *service.MetricCounter
}

type mqttMessage struct {
	topic   string
	payload []byte
}

type nodeState struct {
	lastSeen time.Time
	lastSeq  uint8
	bdSeq    uint64
	isOnline bool
}

func newSparkplugInput(conf *service.ParsedConfig, mgr *service.Resources) (*sparkplugInput, error) {
	brokerURLs, err := conf.FieldStringList("broker_urls")
	if err != nil {
		return nil, err
	}

	clientID, err := conf.FieldString("client_id")
	if err != nil {
		return nil, err
	}

	username, _ := conf.FieldString("username")
	password, _ := conf.FieldString("password")

	groupID, err := conf.FieldString("group_id")
	if err != nil {
		return nil, err
	}

	primaryHostID, _ := conf.FieldString("primary_host_id")
	if primaryHostID == "" {
		primaryHostID = clientID
	}

	splitMetrics, err := conf.FieldBool("split_metrics")
	if err != nil {
		return nil, err
	}

	enableRebirthReqs, err := conf.FieldBool("enable_rebirth_requests")
	if err != nil {
		return nil, err
	}

	qosInt, err := conf.FieldInt("qos")
	if err != nil {
		return nil, err
	}
	qos := byte(qosInt)

	keepAlive, err := conf.FieldDuration("keep_alive")
	if err != nil {
		return nil, err
	}

	connectTimeout, err := conf.FieldDuration("connect_timeout")
	if err != nil {
		return nil, err
	}

	cleanSession, err := conf.FieldBool("clean_session")
	if err != nil {
		return nil, err
	}

	return &sparkplugInput{
		brokerURLs:        brokerURLs,
		clientID:          clientID,
		username:          username,
		password:          password,
		groupID:           groupID,
		primaryHostID:     primaryHostID,
		splitMetrics:      splitMetrics,
		enableRebirthReqs: enableRebirthReqs,
		qos:               qos,
		keepAlive:         keepAlive,
		connectTimeout:    connectTimeout,
		cleanSession:      cleanSession,
		logger:            mgr.Logger(),
		messages:          make(chan mqttMessage, 1000),
		nodeStates:        make(map[string]*nodeState),
		aliasCache:        make(map[string]map[uint64]string),
		messagesReceived:  mgr.Metrics().NewCounter("messages_received"),
		birthsProcessed:   mgr.Metrics().NewCounter("births_processed"),
		deathsProcessed:   mgr.Metrics().NewCounter("deaths_processed"),
		rebirthsRequested: mgr.Metrics().NewCounter("rebirths_requested"),
		sequenceErrors:    mgr.Metrics().NewCounter("sequence_errors"),
		aliasResolutions:  mgr.Metrics().NewCounter("alias_resolutions"),
	}, nil
}

func (s *sparkplugInput) Connect(ctx context.Context) error {
	opts := mqtt.NewClientOptions()

	// Add broker URLs
	for _, url := range s.brokerURLs {
		opts.AddBroker(url)
	}

	opts.SetClientID(s.clientID)
	opts.SetKeepAlive(s.keepAlive)
	opts.SetCleanSession(s.cleanSession)
	opts.SetConnectTimeout(s.connectTimeout)

	if s.username != "" {
		opts.SetUsername(s.username)
		if s.password != "" {
			opts.SetPassword(s.password)
		}
	}

	// Set up STATE topic Last Will Testament
	stateTopic := fmt.Sprintf("spBv1.0/%s/STATE/%s", s.groupID, s.primaryHostID)
	opts.SetWill(stateTopic, "OFFLINE", s.qos, false)

	// Set connection handlers
	opts.SetOnConnectHandler(s.onConnect)
	opts.SetConnectionLostHandler(s.onConnectionLost)

	// Set default message handler (should not be called if we subscribe properly)
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		s.logger.Warnf("Received message on unhandled topic: %s", msg.Topic())
	})

	s.client = mqtt.NewClient(opts)

	s.logger.Infof("Connecting to Sparkplug MQTT brokers: %v", s.brokerURLs)
	token := s.client.Connect()
	if !token.WaitTimeout(s.connectTimeout) {
		return fmt.Errorf("connection timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %w", err)
	}

	s.logger.Info("Successfully connected to Sparkplug MQTT broker")
	return nil
}

func (s *sparkplugInput) onConnect(client mqtt.Client) {
	s.logger.Info("MQTT client connected, setting up Sparkplug subscriptions")

	// Subscribe to all Sparkplug topics for this group
	topicFilter := fmt.Sprintf("spBv1.0/%s/#", s.groupID)
	token := client.Subscribe(topicFilter, s.qos, s.messageHandler)
	if token.Wait() && token.Error() != nil {
		s.logger.Errorf("Failed to subscribe to Sparkplug topics: %v", token.Error())
		return
	}

	s.logger.Infof("Subscribed to Sparkplug topics: %s", topicFilter)

	// Publish STATE ONLINE to announce this primary application is ready
	stateTopic := fmt.Sprintf("spBv1.0/%s/STATE/%s", s.groupID, s.primaryHostID)
	token = client.Publish(stateTopic, s.qos, false, "ONLINE")
	if token.Wait() && token.Error() != nil {
		s.logger.Errorf("Failed to publish STATE ONLINE: %v", token.Error())
		return
	}

	s.logger.Infof("Published STATE ONLINE on topic: %s", stateTopic)
}

func (s *sparkplugInput) onConnectionLost(client mqtt.Client, err error) {
	s.logger.Errorf("MQTT connection lost: %v", err)
}

func (s *sparkplugInput) messageHandler(client mqtt.Client, msg mqtt.Message) {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()

	if closed {
		return
	}

	s.messagesReceived.Incr(1)

	// Non-blocking send to message channel
	select {
	case s.messages <- mqttMessage{topic: msg.Topic(), payload: msg.Payload()}:
	default:
		s.logger.Warn("Message buffer full, dropping message")
	}
}

func (s *sparkplugInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case mqttMsg := <-s.messages:
		batch, err := s.processSparkplugMessage(mqttMsg)
		return batch, func(ctx context.Context, err error) error { return nil }, err
	}
}

func (s *sparkplugInput) processSparkplugMessage(mqttMsg mqttMessage) (service.MessageBatch, error) {
	// Parse topic to extract Sparkplug components
	msgType, deviceKey, topicInfo := s.parseSparkplugTopicDetailed(mqttMsg.topic)
	if msgType == "" {
		s.logger.Debugf("Ignoring non-Sparkplug topic: %s", mqttMsg.topic)
		return nil, nil
	}

	// Decode Sparkplug payload
	var payload sproto.Payload
	if err := proto.Unmarshal(mqttMsg.payload, &payload); err != nil {
		s.logger.Errorf("Failed to unmarshal Sparkplug payload from topic %s: %v", mqttMsg.topic, err)
		return nil, nil
	}

	isBirthMessage := strings.Contains(msgType, "BIRTH")
	isDataMessage := strings.Contains(msgType, "DATA")
	isDeathMessage := strings.Contains(msgType, "DEATH")

	var batch service.MessageBatch

	if isBirthMessage {
		s.processBirthMessage(deviceKey, msgType, &payload)
		s.birthsProcessed.Incr(1)

		if s.splitMetrics {
			batch = s.createSplitMessages(&payload, msgType, deviceKey, topicInfo, mqttMsg.topic)
		} else {
			batch = s.createSingleMessage(&payload, msgType, deviceKey, topicInfo, mqttMsg.topic)
		}
	} else if isDataMessage {
		s.processDataMessage(deviceKey, msgType, &payload)

		if s.splitMetrics {
			batch = s.createSplitMessages(&payload, msgType, deviceKey, topicInfo, mqttMsg.topic)
		} else {
			batch = s.createSingleMessage(&payload, msgType, deviceKey, topicInfo, mqttMsg.topic)
		}
	} else if isDeathMessage {
		s.processDeathMessage(deviceKey, msgType, &payload)
		s.deathsProcessed.Incr(1)

		// Create status event message for death
		batch = s.createDeathEventMessage(msgType, deviceKey, topicInfo, mqttMsg.topic)
	}

	return batch, nil
}

func (s *sparkplugInput) processBirthMessage(deviceKey, msgType string, payload *sproto.Payload) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// Update node state
	if state, exists := s.nodeStates[deviceKey]; exists {
		state.isOnline = true
		state.lastSeen = time.Now()
		if payload.Seq != nil {
			state.lastSeq = uint8(*payload.Seq)
		}
		if payload.Timestamp != nil {
			// Extract bdSeq from metrics if present
			for _, metric := range payload.Metrics {
				if metric.Name != nil && *metric.Name == "bdSeq" {
					if metric.GetLongValue() != 0 {
						state.bdSeq = metric.GetLongValue()
					}
				}
			}
		}
	} else {
		state := &nodeState{
			isOnline: true,
			lastSeen: time.Now(),
		}
		if payload.Seq != nil {
			state.lastSeq = uint8(*payload.Seq)
		}
		s.nodeStates[deviceKey] = state
	}

	// Cache aliases from birth message
	s.cacheAliases(deviceKey, payload.Metrics)

	s.logger.Debugf("Processed %s for device %s", msgType, deviceKey)
}

func (s *sparkplugInput) processDataMessage(deviceKey, msgType string, payload *sproto.Payload) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// Check sequence numbers for out-of-order detection
	if state, exists := s.nodeStates[deviceKey]; exists && payload.Seq != nil {
		currentSeq := uint8(*payload.Seq)
		expectedSeq := uint8((int(state.lastSeq) + 1) % 256)

		if currentSeq != expectedSeq {
			s.logger.Warnf("Sequence gap detected for device %s: expected %d, got %d",
				deviceKey, expectedSeq, currentSeq)
			s.sequenceErrors.Incr(1)

			// Send rebirth request if enabled
			if s.enableRebirthReqs {
				s.sendRebirthRequest(deviceKey)
			}
		}

		state.lastSeq = currentSeq
		state.lastSeen = time.Now()
	}

	// Resolve aliases in data message
	s.resolveAliases(deviceKey, payload.Metrics)
}

func (s *sparkplugInput) processDeathMessage(deviceKey, msgType string, payload *sproto.Payload) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if state, exists := s.nodeStates[deviceKey]; exists {
		state.isOnline = false
		state.lastSeen = time.Now()
	}

	s.logger.Debugf("Processed %s for device %s", msgType, deviceKey)
}

func (s *sparkplugInput) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	if s.client != nil && s.client.IsConnected() {
		// Publish STATE OFFLINE before disconnecting
		stateTopic := fmt.Sprintf("spBv1.0/%s/STATE/%s", s.groupID, s.primaryHostID)
		token := s.client.Publish(stateTopic, s.qos, false, "OFFLINE")
		token.WaitTimeout(5 * time.Second)

		s.client.Disconnect(1000)
	}

	close(s.messages)

	s.logger.Info("Sparkplug input closed")
	return nil
}

// Helper methods that delegate to the existing processor logic where possible
func (s *sparkplugInput) cacheAliases(deviceKey string, metrics []*sproto.Payload_Metric) {
	// Use existing processor logic
	processor := &sparkplugProcessor{
		aliasCache: s.aliasCache,
		logger:     s.logger,
	}
	count := processor.cacheAliases(deviceKey, metrics)
	if count > 0 {
		s.logger.Debugf("Cached %d aliases for device %s", count, deviceKey)
	}
}

func (s *sparkplugInput) resolveAliases(deviceKey string, metrics []*sproto.Payload_Metric) {
	// Use existing processor logic
	processor := &sparkplugProcessor{
		aliasCache: s.aliasCache,
		logger:     s.logger,
	}
	count := processor.resolveAliases(deviceKey, metrics)
	if count > 0 {
		s.aliasResolutions.Incr(int64(count))
		s.logger.Debugf("Resolved %d aliases for device %s", count, deviceKey)
	}
}

func (s *sparkplugInput) parseSparkplugTopicDetailed(topic string) (string, string, *TopicInfo) {
	// Use existing processor logic
	processor := &sparkplugProcessor{}
	return processor.parseSparkplugTopicDetailed(topic)
}

// Message creation methods
func (s *sparkplugInput) createSplitMessages(payload *sproto.Payload, msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	var batch service.MessageBatch

	for _, metric := range payload.Metrics {
		if metric == nil {
			continue
		}

		msg := s.createMessageFromMetric(metric, payload, msgType, deviceKey, topicInfo, originalTopic)
		if msg != nil {
			batch = append(batch, msg)
		}
	}

	return batch
}

func (s *sparkplugInput) createSingleMessage(payload *sproto.Payload, msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	msg := s.createMessageFromPayload(payload, msgType, deviceKey, topicInfo, originalTopic)
	if msg == nil {
		return nil
	}
	return service.MessageBatch{msg}
}

func (s *sparkplugInput) createMessageFromMetric(metric *sproto.Payload_Metric, payload *sproto.Payload, msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) *service.Message {
	// Extract metric value as JSON
	value := s.extractMetricValue(metric)
	if value == nil {
		return nil
	}

	msg := service.NewMessage(value)

	// Set Sparkplug metadata
	msg.MetaSet("sparkplug_msg_type", msgType)
	msg.MetaSet("sparkplug_device_key", deviceKey)
	msg.MetaSet("mqtt_topic", originalTopic)
	msg.MetaSet("group_id", topicInfo.Group)
	msg.MetaSet("edge_node_id", topicInfo.EdgeNode)
	if topicInfo.Device != "" {
		msg.MetaSet("device_id", topicInfo.Device)
	}

	// Set metric name as tag
	tagName := "unknown_metric"
	if metric.Name != nil && *metric.Name != "" {
		tagName = *metric.Name
	} else if metric.Alias != nil {
		tagName = fmt.Sprintf("alias_%d", *metric.Alias)
	}
	msg.MetaSet("tag_name", tagName)

	// Set timestamp if available
	if payload.Timestamp != nil {
		msg.MetaSet("timestamp_ms", fmt.Sprintf("%d", *payload.Timestamp))
	}

	// Set sequence number if available
	if payload.Seq != nil {
		msg.MetaSet("sparkplug_seq", fmt.Sprintf("%d", *payload.Seq))
	}

	return msg
}

func (s *sparkplugInput) createMessageFromPayload(payload *sproto.Payload, msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) *service.Message {
	// Convert entire payload to JSON
	jsonData := s.payloadToJSON(payload)
	if jsonData == nil {
		return nil
	}

	msg := service.NewMessage(jsonData)

	// Set metadata
	msg.MetaSet("sparkplug_msg_type", msgType)
	msg.MetaSet("sparkplug_device_key", deviceKey)
	msg.MetaSet("mqtt_topic", originalTopic)
	msg.MetaSet("group_id", topicInfo.Group)
	msg.MetaSet("edge_node_id", topicInfo.EdgeNode)
	if topicInfo.Device != "" {
		msg.MetaSet("device_id", topicInfo.Device)
	}

	return msg
}

func (s *sparkplugInput) createDeathEventMessage(msgType, deviceKey string, topicInfo *TopicInfo, originalTopic string) service.MessageBatch {
	event := map[string]interface{}{
		"event":        "DeviceOffline",
		"device_key":   deviceKey,
		"group_id":     topicInfo.Group,
		"edge_node_id": topicInfo.EdgeNode,
		"timestamp_ms": time.Now().UnixMilli(),
	}

	if topicInfo.Device != "" {
		event["device_id"] = topicInfo.Device
	}

	jsonBytes, err := json.Marshal(event)
	if err != nil {
		s.logger.Errorf("Failed to marshal death event: %v", err)
		return nil
	}

	msg := service.NewMessage(jsonBytes)
	msg.MetaSet("sparkplug_msg_type", msgType)
	msg.MetaSet("sparkplug_device_key", deviceKey)
	msg.MetaSet("mqtt_topic", originalTopic)
	msg.MetaSet("event_type", "device_offline")

	return service.MessageBatch{msg}
}

func (s *sparkplugInput) extractMetricValue(metric *sproto.Payload_Metric) []byte {
	// Create a simple JSON structure with the metric value
	result := make(map[string]interface{})

	// Add metric name if available
	if metric.Name != nil && *metric.Name != "" {
		result["name"] = *metric.Name
	}

	// Add alias if available
	if metric.Alias != nil && *metric.Alias != 0 {
		result["alias"] = *metric.Alias
	}

	// Extract value based on type
	if metric.IsNull != nil && *metric.IsNull {
		result["value"] = nil
	} else if value := metric.GetValue(); value != nil {
		switch v := value.(type) {
		case *sproto.Payload_Metric_IntValue:
			result["value"] = v.IntValue
		case *sproto.Payload_Metric_LongValue:
			result["value"] = v.LongValue
		case *sproto.Payload_Metric_FloatValue:
			result["value"] = v.FloatValue
		case *sproto.Payload_Metric_DoubleValue:
			result["value"] = v.DoubleValue
		case *sproto.Payload_Metric_BooleanValue:
			result["value"] = v.BooleanValue
		case *sproto.Payload_Metric_StringValue:
			result["value"] = v.StringValue
		default:
			result["value"] = nil
		}
	}

	// Note: Individual metrics don't have timestamps in Sparkplug B
	// Timestamp is at the payload level

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		s.logger.Errorf("Failed to marshal metric value: %v", err)
		return nil
	}

	return jsonBytes
}

func (s *sparkplugInput) payloadToJSON(payload *sproto.Payload) []byte {
	// Create JSON representation of entire payload
	result := make(map[string]interface{})

	if payload.Timestamp != nil {
		result["timestamp_ms"] = *payload.Timestamp
	}

	if payload.Seq != nil {
		result["seq"] = *payload.Seq
	}

	if len(payload.Metrics) > 0 {
		metrics := make([]map[string]interface{}, 0, len(payload.Metrics))
		for _, metric := range payload.Metrics {
			if metric == nil {
				continue
			}

			metricJSON := make(map[string]interface{})
			if metric.Name != nil {
				metricJSON["name"] = *metric.Name
			}
			if metric.Alias != nil {
				metricJSON["alias"] = *metric.Alias
			}

			// Add value
			if metric.IsNull != nil && *metric.IsNull {
				metricJSON["value"] = nil
			} else if value := metric.GetValue(); value != nil {
				switch v := value.(type) {
				case *sproto.Payload_Metric_IntValue:
					metricJSON["value"] = v.IntValue
				case *sproto.Payload_Metric_LongValue:
					metricJSON["value"] = v.LongValue
				case *sproto.Payload_Metric_FloatValue:
					metricJSON["value"] = v.FloatValue
				case *sproto.Payload_Metric_DoubleValue:
					metricJSON["value"] = v.DoubleValue
				case *sproto.Payload_Metric_BooleanValue:
					metricJSON["value"] = v.BooleanValue
				case *sproto.Payload_Metric_StringValue:
					metricJSON["value"] = v.StringValue
				}
			}

			metrics = append(metrics, metricJSON)
		}
		result["metrics"] = metrics
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		s.logger.Errorf("Failed to marshal payload to JSON: %v", err)
		return nil
	}

	return jsonBytes
}

func (s *sparkplugInput) sendRebirthRequest(deviceKey string) {
	if s.client == nil || !s.client.IsConnected() {
		return
	}

	// Parse device key to get topic components
	parts := strings.Split(deviceKey, "/")
	if len(parts) < 2 {
		return
	}

	var topic string
	if len(parts) == 2 {
		// Node level rebirth
		topic = fmt.Sprintf("spBv1.0/%s/NCMD/%s", parts[0], parts[1])
	} else {
		// Device level rebirth
		topic = fmt.Sprintf("spBv1.0/%s/DCMD/%s/%s", parts[0], parts[1], parts[2])
	}

	// Create rebirth command payload
	rebirthMetric := &sproto.Payload_Metric{
		Name: func() *string { s := "Node Control/Rebirth"; return &s }(),
		Value: &sproto.Payload_Metric_BooleanValue{
			BooleanValue: true,
		},
		Datatype: func() *uint32 { d := uint32(11); return &d }(), // Boolean type
	}

	cmdPayload := &sproto.Payload{
		Timestamp: func() *uint64 { t := uint64(time.Now().UnixMilli()); return &t }(),
		Metrics:   []*sproto.Payload_Metric{rebirthMetric},
	}

	payloadBytes, err := proto.Marshal(cmdPayload)
	if err != nil {
		s.logger.Errorf("Failed to marshal rebirth command: %v", err)
		return
	}

	token := s.client.Publish(topic, s.qos, false, payloadBytes)
	if token.Wait() && token.Error() != nil {
		s.logger.Errorf("Failed to publish rebirth command: %v", token.Error())
		return
	}

	s.rebirthsRequested.Incr(1)
	s.logger.Infof("Sent rebirth request to %s on topic %s", deviceKey, topic)
}
